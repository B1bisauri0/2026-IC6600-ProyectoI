/* sync.c - locks y barreras distribuidos.
 *
 * Lock: cada lock tiene un nodo gestor (lock_id % n_peers). Para pedirlo se
 * envía LOCK_REQ al gestor; si está libre responde LOCK_GRANT, si no, pone la
 * solicitud en una cola FIFO y responde cuando se libere (LOCK_REL).
 * El dueño de un lock es el par (nodo, hilo).
 *
 * Barrera: cada barrera tiene un coordinador (barrier_id % n_peers). Cada
 * participante envía BARRIER_ARRIVE; cuando llegan n, el coordinador responde
 * BARRIER_GO a todos.
 *
 * Si el gestor es este mismo nodo, no se envían mensajes: se llama directo a
 * las mismas funciones. Mientras un hilo espera, revisa cada 200 ms que el
 * gestor siga conectado; si se cayó, devuelve DSM_ERR_NODE_DOWN.
 */
#define _GNU_SOURCE
#include "sync.h"
#include "network.h"
#include "dsm.h"
#include "task.h"

#include <stdarg.h>
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#define ERR_NODE_DOWN (-4)
#define ERR_INVAL     (-5)

typedef struct __attribute__((packed)) { uint32_t id; uint32_t arg; } body_sync_t;   /* lock: arg=tid; barrera: arg=n */

static const dsm_config_t *cfg;
static node_id_t          self_id;
static pthread_mutex_t    mtx = PTHREAD_MUTEX_INITIALIZER;
static req_id_t           next_req = 1;

/* ---------------- esperas locales ---------------- */

typedef struct wait {
    req_id_t        req;
    int             done;
    pthread_cond_t  cond;
    struct wait    *next;
} wait_t;

static wait_t *waits;   /* protegido por mtx */

static void wake(req_id_t req) {             /* llamar con mtx tomado */
    for (wait_t *w = waits; w; w = w->next)
        if (w->req == req) { w->done = 1; pthread_cond_signal(&w->cond); return; }
}

/* Espera a que req termine. Llamar con mtx tomado; lo devuelve tomado. */
static int wait_for(wait_t *w, node_id_t manager) {
    int rc = 0;
    while (!w->done) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 200 * 1000000L;
        if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
        pthread_cond_timedwait(&w->cond, &mtx, &ts);
        if (!w->done && manager != self_id && !net_is_connected(manager)) { rc = ERR_NODE_DOWN; break; }
    }
    for (wait_t **p = &waits; *p; p = &(*p)->next)
        if (*p == w) { *p = w->next; break; }
    pthread_cond_destroy(&w->cond);
    return rc;
}

static void wait_init(wait_t *w, req_id_t req) {
    w->req = req;
    w->done = 0;
    pthread_cond_init(&w->cond, NULL);
    w->next = waits;
    waits = w;
}

/* ---------------- estado del gestor ---------------- */

typedef struct waiter { node_id_t node; uint32_t tid; req_id_t req; struct waiter *next; } waiter_t;

typedef struct lockst {
    uint32_t       id;
    int            held;
    node_id_t      owner_node;
    uint32_t       owner_tid;
    waiter_t      *head, *tail;  /* cola FIFO */
    struct lockst *next;
} lockst_t;

typedef struct barst {
    uint32_t      id;
    uint32_t      count;
    waiter_t     *arrived;
    struct barst *next;
} barst_t;

static lockst_t *locks;
static barst_t  *barriers;

static lockst_t *get_lock(uint32_t id) {
    for (lockst_t *l = locks; l; l = l->next) if (l->id == id) return l;
    lockst_t *l = calloc(1, sizeof *l);
    l->id = id;
    l->next = locks;
    locks = l;
    return l;
}

static barst_t *get_barrier(uint32_t id) {
    for (barst_t *b = barriers; b; b = b->next) if (b->id == id) return b;
    barst_t *b = calloc(1, sizeof *b);
    b->id = id;
    b->next = barriers;
    barriers = b;
    return b;
}

/* Concede el lock a (node, tid, req). Llamar con mtx tomado. */
static void grant(lockst_t *l, node_id_t node, uint32_t tid, req_id_t req) {
    l->held = 1;
    l->owner_node = node;
    l->owner_tid = tid;
    if (node == self_id) {
        wake(req);
    } else {
        body_sync_t b = { htonl(l->id), 0 };
        net_send(node, MSG_LOCK_GRANT, F_REPLY, req, &b, sizeof b);
    }
}

static void mgr_lock_req(uint32_t id, node_id_t node, uint32_t tid, req_id_t req) {
    lockst_t *l = get_lock(id);
    if (!l->held) { grant(l, node, tid, req); return; }
    waiter_t *w = calloc(1, sizeof *w);
    w->node = node; w->tid = tid; w->req = req;
    if (l->tail) l->tail->next = w; else l->head = w;
    l->tail = w;
}

static int mgr_lock_rel(uint32_t id, node_id_t node, uint32_t tid) {
    lockst_t *l = get_lock(id);
    if (!l->held || l->owner_node != node || l->owner_tid != tid) return ERR_INVAL;
    l->held = 0;
    waiter_t *w = l->head;
    if (w) {
        l->head = w->next;
        if (!l->head) l->tail = NULL;
        grant(l, w->node, w->tid, w->req);
        free(w);
    }
    return 0;
}

static void mgr_barrier_arrive(uint32_t id, uint32_t n, node_id_t node, req_id_t req) {
    barst_t *b = get_barrier(id);
    waiter_t *w = calloc(1, sizeof *w);
    w->node = node; w->req = req;
    w->next = b->arrived;
    b->arrived = w;
    if (++b->count < n) return;

    /* llegaron todos: sacar la barrera de la lista y soltar a todos
     * (life usa un id nuevo por generación, así no se acumulan) */
    waiter_t *list = b->arrived;
    for (barst_t **p = &barriers; *p; p = &(*p)->next)
        if (*p == b) { *p = b->next; break; }
    free(b);

    body_sync_t body = { htonl(id), 0 };
    while (list) {
        waiter_t *next = list->next;
        if (list->node == self_id) wake(list->req);
        else net_send(list->node, MSG_BARRIER_GO, F_REPLY, list->req, &body, sizeof body);
        free(list);
        list = next;
    }
}

/* ---------------- handlers de red ---------------- */

static void on_lock_req(const msg_hdr_t *h, const void *body) {
    const body_sync_t *b = body;
    pthread_mutex_lock(&mtx);
    mgr_lock_req(ntohl(b->id), h->src, ntohl(b->arg), h->req_id);
    pthread_mutex_unlock(&mtx);
}

static void on_lock_rel(const msg_hdr_t *h, const void *body) {
    const body_sync_t *b = body;
    pthread_mutex_lock(&mtx);
    mgr_lock_rel(ntohl(b->id), h->src, ntohl(b->arg));
    pthread_mutex_unlock(&mtx);
}

static void on_barrier_arrive(const msg_hdr_t *h, const void *body) {
    const body_sync_t *b = body;
    pthread_mutex_lock(&mtx);
    mgr_barrier_arrive(ntohl(b->id), ntohl(b->arg), h->src, h->req_id);
    pthread_mutex_unlock(&mtx);
}

static void on_reply(const msg_hdr_t *h, const void *body) {   /* LOCK_GRANT y BARRIER_GO */
    (void)body;
    pthread_mutex_lock(&mtx);
    wake(h->req_id);
    pthread_mutex_unlock(&mtx);
}

/* ---------------- interfaz ---------------- */

static uint32_t my_tid(void) { return (uint32_t)syscall(SYS_gettid); }

int sync_lock(int lock_id) {
    if (lock_id < 0) return ERR_INVAL;
    node_id_t mgr = (node_id_t)(lock_id % cfg->n_peers);
    uint32_t tid = my_tid();
    wait_t w;

    pthread_mutex_lock(&mtx);
    req_id_t req = next_req++;
    wait_init(&w, req);
    if (mgr == self_id) {
        mgr_lock_req((uint32_t)lock_id, self_id, tid, req);
    } else {
        body_sync_t b = { htonl((uint32_t)lock_id), htonl(tid) };
        if (net_send(mgr, MSG_LOCK_REQ, 0, req, &b, sizeof b) < 0) {
            w.done = 1;                  /* no se pudo enviar: el gestor no está */
            wait_for(&w, mgr);           /* solo para quitar la espera de la lista */
            pthread_mutex_unlock(&mtx);
            return ERR_NODE_DOWN;
        }
    }
    int rc = wait_for(&w, mgr);
    pthread_mutex_unlock(&mtx);
    return rc;
}

int sync_unlock(int lock_id) {
    if (lock_id < 0) return ERR_INVAL;
    node_id_t mgr = (node_id_t)(lock_id % cfg->n_peers);
    uint32_t tid = my_tid();
    int rc = 0;
    pthread_mutex_lock(&mtx);
    if (mgr == self_id) {
        rc = mgr_lock_rel((uint32_t)lock_id, self_id, tid);
    } else {
        body_sync_t b = { htonl((uint32_t)lock_id), htonl(tid) };
        if (net_send(mgr, MSG_LOCK_REL, 0, 0, &b, sizeof b) < 0) rc = ERR_NODE_DOWN;
    }
    pthread_mutex_unlock(&mtx);
    return rc;
}

int sync_barrier(int barrier_id, int n) {
    if (barrier_id < 0 || n <= 0) return ERR_INVAL;
    node_id_t coord = (node_id_t)(barrier_id % cfg->n_peers);
    wait_t w;

    pthread_mutex_lock(&mtx);
    req_id_t req = next_req++;
    wait_init(&w, req);
    if (coord == self_id) {
        mgr_barrier_arrive((uint32_t)barrier_id, (uint32_t)n, self_id, req);
    } else {
        body_sync_t b = { htonl((uint32_t)barrier_id), htonl((uint32_t)n) };
        if (net_send(coord, MSG_BARRIER_ARRIVE, 0, req, &b, sizeof b) < 0) {
            w.done = 1;
            wait_for(&w, coord);
            pthread_mutex_unlock(&mtx);
            return ERR_NODE_DOWN;
        }
    }
    int rc = wait_for(&w, coord);
    pthread_mutex_unlock(&mtx);
    return rc;
}

int sync_start(const dsm_config_t *c, node_id_t self) {
    cfg = c;
    self_id = self;
    /* los req_id empiezan distinto en cada nodo para que sea fácil depurar */
    next_req = ((req_id_t)self << 48) | 1;
    net_register_handler(MSG_LOCK_REQ, on_lock_req);
    net_register_handler(MSG_LOCK_REL, on_lock_rel);
    net_register_handler(MSG_LOCK_GRANT, on_reply);
    net_register_handler(MSG_BARRIER_ARRIVE, on_barrier_arrive);
    net_register_handler(MSG_BARRIER_GO, on_reply);
    return 0;
}

/* ---------------- lo que usan las apps (dsm.h) ---------------- */
/* son las mismas de arriba, solo con el ctx que pide la parte de memoria */

int dsm_lock(dsm_ctx_t *ctx, int lock_id)   { (void)ctx; return sync_lock(lock_id); }
int dsm_unlock(dsm_ctx_t *ctx, int lock_id) { (void)ctx; return sync_unlock(lock_id); }
int dsm_barrier(dsm_ctx_t *ctx, int id, int n) { (void)ctx; return sync_barrier(id, n); }

/* printf con el número de nodo adelante, para saber quién imprimió.
Sale en la terminal del nodo y, si la app la lanzó dsm_run, también en la
terminal de dsm_run (task.c) */
int dsm_printf(dsm_ctx_t *ctx, const char *fmt, ...) {
    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    int r = printf("[nodo %u] %s", self_id, buf);
    fflush(stdout);
    char linea[1100];
    snprintf(linea, sizeof linea, "[nodo %u] %s", self_id, buf);
    task_escribir(ctx, linea);
    return r;
}