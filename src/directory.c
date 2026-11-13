/*
 * directory.c -- Directorio de paginas del nodo home (documento de diseño, sec. 4, 5.5, 9.4).
 *
 * El home mantiene, por cada pagina que respalda: estado global MSI, propietario (si M),
 * conjunto de copias (si S), version y una cola FIFO de solicitudes (campo waitq_head).
 *
 * Una sola transaccion activa por pagina (campo busy). Una transaccion NO ocupa un hilo
 * mientras espera (sec. 9.4 punto 2): envia FETCH o INVALIDATE, anota quien debe responder,
 * suelta el semaforo y termina. El mensaje que completa la cuenta, atendido por cualquier
 * hilo del pool, continua la transaccion.
 *
 * La solicitud ACTIVA es siempre la cabeza de la cola de la pagina; las demas esperan detras.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "directory.h"
#include "pager.h"
#include "wire.h"

enum { PH_QUEUED = 0, PH_FETCH = 1, PH_INVAL = 2 };

typedef struct {
    uint32_t  next;          /* siguiente en la cola de la pagina (base 1) o libre */
    vpn_t     vpn;
    node_id_t requester;
    uint8_t   access, phase, inuse, had_copy;
    uint16_t  wait_mask;     /* nodos de los que se espera INVALIDATE_ACK o la respuesta a FETCH */
    uint64_t  req_id;        /* id de la solicitud del solicitante */
    uint64_t  net_req;       /* id de FETCH / INVALIDATE que se espera de vuelta */
    uint64_t  deadline_ms;
} dreq_t;

static dreq_t *pool;
static uint32_t pool_n, pool_free;
static pthread_mutex_t pool_mu = PTHREAD_MUTEX_INITIALIZER;

#define E(v)  (&g_n.dir[(v)])
#define SELF  ((node_id_t)g_n.self)

int dir_init(void) {
    pool_n = g_n.cfg.max_nodes * g_n.cfg.max_user_threads + 64;
    pool = calloc(pool_n + 1, sizeof *pool);
    if (!pool) return DSM_ERR_NOMEM;
    for (uint32_t i = 1; i <= pool_n; i++) pool[i].next = (i < pool_n) ? i + 1 : 0;
    pool_free = 1;
    return DSM_OK;
}

static uint32_t pool_get(void) {
    pthread_mutex_lock(&pool_mu);
    uint32_t i = pool_free;
    if (i) { pool_free = pool[i].next; memset(&pool[i], 0, sizeof pool[i]); pool[i].inuse = 1; }
    pthread_mutex_unlock(&pool_mu);
    return i;
}
static void pool_put(uint32_t i) {
    pthread_mutex_lock(&pool_mu);
    pool[i].inuse = 0; pool[i].next = pool_free; pool_free = i;
    pthread_mutex_unlock(&pool_mu);
}

static void dlock(vpn_t v)   { while (sem_wait(&g_n.dir_sem[v]) < 0 && errno == EINTR) { } }
static void dunlock(vpn_t v) { sem_post(&g_n.dir_sem[v]); }

static int node_down(node_id_t n) { return n != SELF && membership_status(n) == NODE_OFFLINE; }
static int popcnt(uint16_t m) { return __builtin_popcount(m); }

/* ------------------------------------------------------------------------- */
/* Alta y baja de paginas home                                               */
/* ------------------------------------------------------------------------- */
void dir_page_create(vpn_t vpn) {
    dir_entry_t *e = E(vpn);
    sem_init(&g_n.dir_sem[vpn], 0, 1);
    memset(e, 0, sizeof *e);
    e->state = PG_MODIFIED;               /* sec. 3.2: el home arranca con la pagina en M, en ceros */
    e->owner = SELF;
    e->sharers = BIT(SELF);
    g_n.transfers[vpn] = 0;
}

void dir_range_destroy(vpn_t start, uint32_t n) {
    for (uint32_t i = 0; i < n; i++) {
        vpn_t v = start + i;
        dir_entry_t *e = E(v);
        uint32_t q = e->waitq_head;
        while (q) { uint32_t nx = pool[q].next; pool_put(q); q = nx; }
        sem_destroy(&g_n.dir_sem[v]);
        memset(e, 0, sizeof *e);
        g_n.transfers[v] = 0;
    }
}

/* ------------------------------------------------------------------------- */
/* Respuestas                                                                */
/* ------------------------------------------------------------------------- */
static void reply_error(vpn_t vpn, dreq_t *r, int code, node_id_t node) {
    if (r->requester == SELF) {
        int st = code == E_NODE_DOWN ? DSM_ERR_NODE_DOWN : code == E_NO_MEM ? DSM_ERR_NOMEM : DSM_ERR_NET;
        pager_local_fail(r->req_id, st, node);
    } else {
        uint8_t b[6], *p = b;
        w16(&p, (uint16_t)code); w32(&p, vpn);
        struct iovec iov = { b, 6 };
        net_sendv(r->requester, MSG_ERROR, MSG_F_RESP | MSG_F_ERR, r->req_id, &iov, 1);
    }
}

/* Entrega la pagina al solicitante. Si es el propio home, el marco ya esta en su memfd. */
static void reply_data(vpn_t vpn, dreq_t *r, pg_state_t perm, uint32_t ver, int payload) {
    if (r->requester == SELF) {
        pager_local_complete(vpn, perm, ver, r->req_id);
        return;
    }
    uint8_t hd[9], *p = hd;
    w32(&p, vpn); w8(&p, (uint8_t)perm); w32(&p, ver);
    struct iovec iov[2] = { { hd, 9 }, { VPN_SYS(vpn), g_n.page_size } };
    net_sendv(r->requester, MSG_PAGE_DATA, MSG_F_RESP | (payload ? MSG_F_PAYLOAD : 0), r->req_id,
              iov, payload ? 2 : 1);
}

/* Concede M a R: propietario = R, copias = {R}, version + 1 (sec. 4.3) */
static void grant_m(vpn_t vpn, dir_entry_t *e, dreq_t *r, int had_copy) {
    node_id_t R = r->requester;
    e->state = PG_MODIFIED; e->owner = R; e->sharers = BIT(R);
    e->acks_pending = 0;
    e->version++;
    g_n.transfers[vpn]++;
    int payload = (r->access == ACC_W) || !had_copy;   /* UPGRADE con copia valida: solo permiso */
    reply_data(vpn, r, PG_MODIFIED, e->version, payload);
}

/* Termina la solicitud activa (cabeza de la cola) */
static void finish(dir_entry_t *e) {
    uint32_t h = e->waitq_head;
    e->waitq_head = pool[h].next;
    pool_put(h);
    e->busy &= (uint8_t)~DIR_BUSY;
    e->acks_pending = 0;
}

/* ------------------------------------------------------------------------- */
/* Un paso de la transaccion. Devuelve 1 = terminada, 0 = espera mensajes     */
/* ------------------------------------------------------------------------- */
static int send_fetch(vpn_t vpn, dir_entry_t *e, dreq_t *r, node_id_t O, int mode) {
    uint8_t b[5], *p = b;
    w32(&p, vpn); w8(&p, (uint8_t)mode);
    struct iovec iov = { b, 5 };
    r->net_req = net_new_req_id();
    r->wait_mask = BIT(O);
    r->phase = PH_FETCH;
    r->deadline_ms = now_ms() + g_n.cfg.request_timeout_ms;
    e->acks_pending = 1;
    if (net_sendv(O, MSG_FETCH, 0, r->net_req, &iov, 1) < 0) return -1;
    return 0;
}

static int step(vpn_t vpn, dir_entry_t *e, dreq_t *r) {
    node_id_t R = r->requester;
    if (r->access == ACC_R) {
        if (e->state == PG_MODIFIED) {
            node_id_t O = e->owner;
            if (O == R) { reply_data(vpn, r, PG_MODIFIED, e->version, 0); return 1; }   /* ya es dueno */
            if (O == SELF) {                                  /* el home baja su propio permiso */
                if (pager_home_downgrade(vpn) < 0) {
                    reply_error(vpn, r, E_NO_MEM, SELF);
                    return 1;
                }
                e->state = PG_SHARED;
                e->sharers = BIT(SELF) | BIT(R);
                reply_data(vpn, r, PG_SHARED, e->version, 1);
                return 1;
            }
            if (node_down(O) || send_fetch(vpn, e, r, O, 0) < 0) {   /* 4.4 caso 6 */
                if (g_n.cfg.replication_factor >= 2) {
                    /* Caso 6 sec. 4.4 y sec. 7.1 paso 6: el home recupera la version de su marco (ultimo checkpoint) */
                    e->owner = DSM_NODE_NONE;
                    e->state = PG_SHARED;
                    e->sharers = BIT(R);
                    reply_data(vpn, r, PG_SHARED, e->version, 1);
                    return 1;
                }
                e->state = PG_INVALID; e->sharers = 0; e->busy |= DIR_LOST;
                reply_error(vpn, r, E_NODE_DOWN, O);
                return 1;
            }
            return 0;
        }
        e->sharers |= BIT(R);                                 /* I o S: el marco del home es valido */
        e->state = PG_SHARED;
        reply_data(vpn, r, PG_SHARED, e->version, 1);
        return 1;
    }

    /* escritura o upgrade */
    if (e->state == PG_MODIFIED) {
        node_id_t O = e->owner;
        if (O == R) { reply_data(vpn, r, PG_MODIFIED, e->version, 0); return 1; }
        if (O == SELF) {                                      /* el home cede su marco: primero PROT_NONE */
            if (pager_home_invalidate(vpn) < 0) {
                reply_error(vpn, r, E_NO_MEM, SELF);
                return 1;
            }
            grant_m(vpn, e, r, 0);
            return 1;
        }
        if (node_down(O) || send_fetch(vpn, e, r, O, 1) < 0) {
            if (g_n.cfg.replication_factor >= 2) {
                /* Caso 6 sec. 4.4 y sec. 7.1 paso 6: conceder M desde la copia del marco home */
                e->owner = DSM_NODE_NONE;
                e->state = PG_INVALID;
                grant_m(vpn, e, r, 0);
                return 1;
            }
            e->state = PG_INVALID; e->sharers = 0; e->busy |= DIR_LOST;
            reply_error(vpn, r, E_NODE_DOWN, O);
            return 1;
        }
        return 0;
    }

    /* I o S: invalidar las demas copias */
    uint16_t had = e->sharers & BIT(R);
    uint16_t targets = e->sharers & (uint16_t)~BIT(R);
    if (targets & BIT(SELF)) {                                /* la copia del home se invalida sin mensajes */
        if (pager_home_invalidate(vpn) < 0) {
            reply_error(vpn, r, E_NO_MEM, SELF);
            return 1;
        }
        targets &= (uint16_t)~BIT(SELF);
    }
    for (int n = 0; n < DSM_MAX_NODES; n++)
        if ((targets & BIT(n)) && node_down((node_id_t)n)) targets &= (uint16_t)~BIT(n);   /* caso 4.4.5 */
    if (!targets) { grant_m(vpn, e, r, had != 0); return 1; }

    uint8_t b[8], *p = b;
    w32(&p, vpn); w32(&p, e->version);
    struct iovec iov = { b, 8 };
    r->net_req = net_new_req_id();
    r->had_copy = had != 0;
    r->phase = PH_INVAL;
    r->deadline_ms = now_ms() + g_n.cfg.request_timeout_ms;
    r->wait_mask = 0;
    for (int n = 0; n < DSM_MAX_NODES; n++) {
        if (!(targets & BIT(n))) continue;
        if (net_sendv((node_id_t)n, MSG_INVALIDATE, 0, r->net_req, &iov, 1) == 0) {
            r->wait_mask |= BIT(n);
            STAT_INC(invalidations_sent);
        }
    }
    e->acks_pending = (uint16_t)popcnt(r->wait_mask);
    if (!r->wait_mask) { grant_m(vpn, e, r, r->had_copy); return 1; }
    return 0;
}

static void start_next(vpn_t vpn) {
    dir_entry_t *e = E(vpn);
    while (e->waitq_head && !(e->busy & DIR_BUSY)) {
        dreq_t *r = &pool[e->waitq_head];
        if (r->requester != SELF && node_down(r->requester)) {          /* el solicitante murio */
            uint32_t h = e->waitq_head; e->waitq_head = r->next; pool_put(h);
            continue;
        }
        e->busy |= DIR_BUSY;
        if (e->busy & DIR_LOST) { reply_error(vpn, r, E_NODE_DOWN, e->owner); finish(e); continue; }
        if (step(vpn, e, r)) finish(e);
    }
}

static void enqueue(vpn_t vpn, node_id_t R, int access, uint64_t req_id) {
    dir_entry_t *e = E(vpn);
    uint32_t i = pool_get();
    if (!i) {
        dreq_t tmp = { .requester = R, .req_id = req_id };
        reply_error(vpn, &tmp, E_BUSY_TIMEOUT, SELF);
        return;
    }
    dreq_t *r = &pool[i];
    r->vpn = vpn; r->requester = R; r->access = (uint8_t)access; r->req_id = req_id; r->phase = PH_QUEUED;
    if (!e->waitq_head) e->waitq_head = i;
    else { uint32_t q = e->waitq_head; while (pool[q].next) q = pool[q].next; pool[q].next = i; }
    start_next(vpn);
}

/* ------------------------------------------------------------------------- */
/* Entradas                                                                  */
/* ------------------------------------------------------------------------- */
static void request(vpn_t vpn, node_id_t R, int access, uint64_t req_id, const msg_hdr_t *h) {
    if (vpn >= g_n.n_vpages || alloc_home_of(vpn) != SELF) {
        if (h) {
            uint8_t b[6], *p = b;
            w16(&p, vpn >= g_n.n_vpages ? E_BAD_VPN : E_NOT_HOME); w32(&p, vpn);
            struct iovec iov = { b, 6 };
            net_sendv(R, MSG_ERROR, MSG_F_RESP | MSG_F_ERR, req_id, &iov, 1);
        } else {
            pager_local_fail(req_id, DSM_ERR_INVAL, SELF);
        }
        return;
    }
    dlock(vpn);
    enqueue(vpn, R, access, req_id);
    dunlock(vpn);
}

void dir_request_local(vpn_t vpn, int access, uint64_t req_id) { request(vpn, SELF, access, req_id, NULL); }

void dir_on_page_request(const msg_hdr_t *h, const uint8_t *body) {
    const uint8_t *p = body;
    vpn_t vpn = r32(&p);
    int access = r8(&p);
    request(vpn, h->src, access, h->req_id, h);
}

/* Datos que llegan del propietario o de la copia, segun el modo del FETCH */
static void complete_fetch(vpn_t vpn, dir_entry_t *e, dreq_t *r, int owner_keeps_copy) {
    node_id_t O = e->owner;
    if (r->access == ACC_R) {
        e->state = PG_SHARED;
        e->sharers = BIT(r->requester) | (owner_keeps_copy ? BIT(O) : 0);
        reply_data(vpn, r, PG_SHARED, e->version, 1);
    } else {
        grant_m(vpn, e, r, 0);
    }
}

void dir_on_page_data(const msg_hdr_t *h, const uint8_t *body) {
    const uint8_t *p = body;
    vpn_t vpn = r32(&p);
    uint8_t perm = r8(&p);
    (void)r32(&p);                                    /* version: la del directorio manda */
    if (vpn >= g_n.n_vpages || !(h->flags & MSG_F_PAYLOAD)) return;
    dlock(vpn);
    dir_entry_t *e = E(vpn);
    if (e->waitq_head && (e->busy & DIR_BUSY)) {
        dreq_t *r = &pool[e->waitq_head];
        if (r->phase == PH_FETCH && r->net_req == h->req_id) {        /* si no, es una respuesta tardia */
            memcpy(VPN_SYS(vpn), p, g_n.page_size);                   /* "H actualiza su marco" */
            complete_fetch(vpn, e, r, perm == PG_SHARED);
            finish(e);
            start_next(vpn);
        }
    }
    dunlock(vpn);
}

void dir_on_invalidate_ack(const msg_hdr_t *h, const uint8_t *body) {
    const uint8_t *p = body;
    vpn_t vpn = r32(&p);
    if (vpn >= g_n.n_vpages) return;
    dlock(vpn);
    dir_entry_t *e = E(vpn);
    if (e->waitq_head && (e->busy & DIR_BUSY)) {
        dreq_t *r = &pool[e->waitq_head];
        if (r->phase == PH_INVAL && r->net_req == h->req_id && (r->wait_mask & BIT(h->src))) {
            r->wait_mask &= (uint16_t)~BIT(h->src);
            e->acks_pending = (uint16_t)popcnt(r->wait_mask);
            if (!r->wait_mask) {
                grant_m(vpn, e, r, r->had_copy);
                finish(e);
                start_next(vpn);
            }
        }
    }
    dunlock(vpn);
}

void dir_on_evict_notice(const msg_hdr_t *h, const uint8_t *body) {
    const uint8_t *p = body;
    vpn_t vpn = r32(&p);
    if (vpn >= g_n.n_vpages || alloc_home_of(vpn) != SELF) return;
    dlock(vpn);
    dir_entry_t *e = E(vpn);
    e->sharers &= (uint16_t)~BIT(h->src);
    if (e->state == PG_SHARED && !e->sharers) e->state = PG_INVALID;   /* el marco del home queda como unica copia */
    dunlock(vpn);
}

void dir_on_writeback(const msg_hdr_t *h, const uint8_t *body) {
    const uint8_t *p = body;
    vpn_t vpn = r32(&p);
    (void)r32(&p);                                    /* version */
    if (vpn >= g_n.n_vpages || alloc_home_of(vpn) != SELF || !(h->flags & MSG_F_PAYLOAD)) {
        uint8_t b[6], *q = b;
        w16(&q, E_NOT_HOME); w32(&q, vpn);
        struct iovec iov = { b, 6 };
        net_sendv(h->src, MSG_ERROR, MSG_F_RESP | MSG_F_ERR, h->req_id, &iov, 1);
        return;
    }
    dlock(vpn);
    dir_entry_t *e = E(vpn);
    if (e->state == PG_MODIFIED && e->owner == h->src) {
        memcpy(VPN_SYS(vpn), p, g_n.page_size);
        dreq_t *r = e->waitq_head ? &pool[e->waitq_head] : NULL;
        if ((e->busy & DIR_BUSY) && r && r->phase == PH_FETCH && r->wait_mask == BIT(h->src)) {
            /* caso 4.4.4: el WRITEBACK se cruzo con nuestro FETCH; sirve como respuesta.
             * La respuesta tardia del nodo se descarta porque net_req ya no coincide. */
            r->net_req = 0;
            complete_fetch(vpn, e, r, 0);
            finish(e);
            start_next(vpn);
        } else {
            e->state = PG_INVALID; e->sharers = 0; e->owner = DSM_NODE_NONE;
        }
    }                                                 /* si el origen ya no es el propietario: se descarta */
    dunlock(vpn);
    uint8_t b[4], *q = b;
    w32(&q, vpn);
    struct iovec iov = { b, 4 };
    net_sendv(h->src, MSG_WRITEBACK_ACK, MSG_F_RESP, h->req_id, &iov, 1);   /* y aun asi se confirma */
}

void dir_on_error(const msg_hdr_t *h, const uint8_t *body) {
    const uint8_t *p = body;
    (void)r16(&p);
    vpn_t vpn = r32(&p);
    if (vpn >= g_n.n_vpages) return;
    dlock(vpn);
    dir_entry_t *e = E(vpn);
    if (e->waitq_head && (e->busy & DIR_BUSY)) {
        dreq_t *r = &pool[e->waitq_head];
        if (r->phase == PH_FETCH && r->net_req == h->req_id) {
            reply_error(vpn, r, E_BAD_VPN, h->src);
            finish(e);
            start_next(vpn);
        }
    }
    dunlock(vpn);
}

/* ------------------------------------------------------------------------- */
/* Nodo caido y timeouts                                                     */
/* ------------------------------------------------------------------------- */
void dir_on_node_down(node_id_t n) {
    vpn_range_t *rg; int nr = alloc_home_ranges(SELF, &rg);
    for (int i = 0; i < nr; i++)
        for (uint32_t k = 0; k < rg[i].n; k++) {
            vpn_t vpn = rg[i].start + k;
            dlock(vpn);
            dir_entry_t *e = E(vpn);
            if (e->state == PG_MODIFIED && e->owner == n) {               /* caso 4.4.6: la ultima version se perdio */
                e->state = PG_INVALID; e->sharers = 0; e->busy |= DIR_LOST;
            }
            e->sharers &= (uint16_t)~BIT(n);                              /* caso 4.4.5 */
            if (e->state == PG_SHARED && !e->sharers) e->state = PG_INVALID;
            if (e->waitq_head && (e->busy & DIR_BUSY)) {
                dreq_t *r = &pool[e->waitq_head];
                if (r->phase == PH_FETCH && (r->wait_mask & BIT(n))) {
                    reply_error(vpn, r, E_NODE_DOWN, n);
                    finish(e);
                    start_next(vpn);
                } else if (r->phase == PH_INVAL && (r->wait_mask & BIT(n))) {
                    r->wait_mask &= (uint16_t)~BIT(n);
                    e->acks_pending = (uint16_t)popcnt(r->wait_mask);
                    if (!r->wait_mask) { grant_m(vpn, e, r, r->had_copy); finish(e); start_next(vpn); }
                }
            }
            dunlock(vpn);
        }
    free(rg);
}

void dir_check_timeouts(uint64_t now) {
    vpn_t cand[64]; int nc = 0;
    pthread_mutex_lock(&pool_mu);
    for (uint32_t i = 1; i <= pool_n && nc < 64; i++)
        if (pool[i].inuse && pool[i].phase != PH_QUEUED && pool[i].deadline_ms <= now) cand[nc++] = pool[i].vpn;
    pthread_mutex_unlock(&pool_mu);
    for (int i = 0; i < nc; i++) {
        vpn_t vpn = cand[i];
        dlock(vpn);
        dir_entry_t *e = E(vpn);
        if (e->waitq_head && (e->busy & DIR_BUSY)) {
            dreq_t *r = &pool[e->waitq_head];
            if (r->phase != PH_QUEUED && r->deadline_ms <= now) {        /* politica de error (sec. 7) */
                reply_error(vpn, r, E_BUSY_TIMEOUT, SELF);
                finish(e);
                start_next(vpn);
            }
        }
        dunlock(vpn);
    }
}

/* ------------------------------------------------------------------------- */
/* Consulta (PAGETABLE_REQUEST): copia sin esperar a ninguna transaccion      */
/* ------------------------------------------------------------------------- */
uint32_t dir_snapshot(vpn_t vpn_start, uint32_t max, pt_entry_t *out, int *more) {
    vpn_range_t *rg; int nr = alloc_home_ranges(SELF, &rg);
    uint32_t n = 0;
    *more = 0;
    for (int i = 0; i < nr && !*more; i++)
        for (uint32_t k = 0; k < rg[i].n; k++) {
            vpn_t v = rg[i].start + k;
            if (v < vpn_start) continue;
            if (n == max) { *more = 1; break; }
            dir_entry_t *e = E(v);
            out[n].vpn = v;
            out[n].state = e->state;
            out[n].flags = (e->busy & DIR_BUSY) ? 1 : 0;
            out[n].owner = e->state == PG_MODIFIED ? e->owner : DSM_NODE_NONE;
            out[n].sharers = e->sharers;
            out[n].version = e->version;
            out[n].transfers = g_n.transfers[v];
            n++;
        }
    free(rg);
    return n;
}
