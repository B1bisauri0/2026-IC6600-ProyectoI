/*
 * pager.c -- Manejador de SIGSEGV, hilo Pager y lado "nodo" del protocolo MSI.
 *
 * Implementa (documento de diseño):
 *   D3/D4  doble mapeo + manejador que solo usa llamadas async-signal-safe
 *   3.3    deteccion lectura/escritura con REG_ERR
 *   4.2    transiciones MSI del nodo local
 *   4.4    casos especiales 3 (INVALIDATE idempotente) y 4 (FETCH cruzado con WRITEBACK)
 *   9.2    mutex por franja (vpn % 1024), cola de fallos sin locks, eventfd por hilo
 *   9.3    secuencia del manejador de señal
 *   11.2   reemplazo de la cache remota: FIFO exacto (Prototipo 1)
 *
 * Reglas de bloqueo (sec. 9.2 y 9.4):
 *   orden global: tabla de asignaciones -> semaforo del directorio -> mutex de franja
 *   - nunca se mantienen dos mutex de franja a la vez;
 *   - nunca se espera una respuesta de red con un mutex tomado (si se ENVIA con la
 *     franja tomada, es solo para conservar el orden WRITEBACK/EVICT -> PAGE_REQUEST;
 *     el lector del otro nodo siempre drena el socket, sec. 9.4 punto 4).
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <time.h>
#include <ucontext.h>
#include <unistd.h>

#include "directory.h"
#include "pager.h"
#include "wire.h"

#define NSTRIPE 1024
#define NIL     0xFFFFFFFFu

/* ------------------------------------------------------------------------- */
/* Mutex por franja                                                          */
/* ------------------------------------------------------------------------- */
static pthread_mutex_t stripe[NSTRIPE];
void pager_page_lock(vpn_t v)   { pthread_mutex_lock(&stripe[v % NSTRIPE]); }
void pager_page_unlock(vpn_t v) { pthread_mutex_unlock(&stripe[v % NSTRIPE]); }

static int evict_one(void);

static inline void pager_set_state(local_page_t *lp, uint8_t new_state) {
    uint8_t old = lp->state;
    if (old == new_state) return;
    lp->state = new_state;
    if (old == PG_INVALID) STAT_SUB(pages_invalid, 1);
    else if (old == PG_SHARED) STAT_SUB(pages_shared, 1);
    else if (old == PG_MODIFIED) STAT_SUB(pages_modified, 1);

    if (new_state == PG_INVALID) STAT_ADD(pages_invalid, 1);
    else if (new_state == PG_SHARED) STAT_ADD(pages_shared, 1);
    else if (new_state == PG_MODIFIED) STAT_ADD(pages_modified, 1);
}

static void mprot(void *addr, size_t len, int prot) {
    if (mprotect(addr, len, prot) == 0) return;
    int e = errno;
    if (e == ENOMEM) {
        for (int tries = 0; tries < 5; tries++) {
            if (evict_one() && mprotect(addr, len, prot) == 0) return;
        }
    }
    LOG_ERR("mprotect fallo (%s). Si es ENOMEM, suba vm.max_map_count o reduzca cache_max_pages (sec. 3.4)\n",
            strerror(e));
    abort();
}

/* ------------------------------------------------------------------------- */
/* Hilos de usuario: fault_slot_t, cola sin locks, eventfd                    */
/* ------------------------------------------------------------------------- */
static fault_slot_t *slots;
static uint8_t      *slot_used;
static uint32_t      nslots;
static pthread_mutex_t slot_mu = PTHREAD_MUTEX_INITIALIZER;
static __thread fault_slot_t *tls_slot __attribute__((tls_model("initial-exec")));
#if !defined(__x86_64__)
static __thread vpn_t tls_last_vpn = NIL;     /* respaldo sec. 3.3 (solo no-x86) */
#endif

static _Atomic(fault_slot_t *) *ring;         /* anillo MPSC (C11 atomics) */
static size_t         ring_mask;
static atomic_ulong   ring_tail;
static unsigned long  ring_head;              /* solo lo toca el Pager */
static sem_t          pager_sem;
static pthread_t      pager_th;
static volatile int   pager_running;

int pager_thread_register(void) {
    if (tls_slot) return DSM_OK;
    int rc = DSM_ERR_THREAD;
    pthread_mutex_lock(&slot_mu);
    for (uint32_t i = 0; i < nslots; i++) {
        if (slot_used[i]) continue;
        int fd = eventfd(0, EFD_CLOEXEC);
        if (fd < 0) break;
        slot_used[i] = 1;
        slots[i].wake_fd = fd;
        slots[i].tid = (pid_t)syscall(SYS_gettid);
        atomic_store(&slots[i].state, 0);
        tls_slot = &slots[i];
        rc = DSM_OK;
        break;
    }
    pthread_mutex_unlock(&slot_mu);
    return rc;
}

int pager_thread_unregister(void) {
    if (!tls_slot) return DSM_ERR_THREAD;
    pthread_mutex_lock(&slot_mu);
    fault_slot_t *s = tls_slot;
    close(s->wake_fd);
    s->wake_fd = -1;
    slot_used[s - slots] = 0;
    pthread_mutex_unlock(&slot_mu);
    tls_slot = NULL;
    return DSM_OK;
}

/* ------------------------------------------------------------------------- */
/* Manejador de SIGSEGV (solo async-signal-safe)                              */
/* ------------------------------------------------------------------------- */
static size_t put_str(char *b, size_t n, const char *s) { size_t l = strlen(s); memcpy(b + n, s, l); return n + l; }
static size_t put_num(char *b, size_t n, unsigned long v) {
    char t[24]; int l = 0;
    do { t[l++] = (char)('0' + v % 10); v /= 10; } while (v);
    while (l) b[n++] = t[--l];
    return n;
}

static void restore_default(void) {
    struct sigaction d;
    memset(&d, 0, sizeof d);
    d.sa_handler = SIG_DFL;
    sigaction(SIGSEGV, &d, NULL);
}

static void segv_handler(int sig, siginfo_t *si, void *ucv) {
    int saved = errno;
    char *a = (char *)si->si_addr;
    fault_slot_t *s = tls_slot;

    /* paso 2 de 9.3: direccion fuera de la ventana o hilo no registrado */
    if (a < g_n.app || a >= g_n.app + g_n.win_size || !s) {
        if (!s && a >= g_n.app && a < g_n.app + g_n.win_size) {
            static const char m[] = "[dsm] SIGSEGV: un hilo no registrado toco memoria DSM (use dsm_thread_register)\n";
            if (write(2, m, sizeof m - 1) < 0) { /* nada que hacer */ }
        }
        restore_default();
        raise(sig);
        errno = saved;
        return;
    }

    /* paso 3: vpn y tipo de acceso */
    vpn_t vpn = (vpn_t)((size_t)(a - g_n.app) >> g_n.page_shift);
    s->vpn = vpn;
#if defined(__x86_64__)
    ucontext_t *uc = (ucontext_t *)ucv;
    s->is_write = (uc->uc_mcontext.gregs[REG_ERR] & 2) ? 1 : 0;
#else
    (void)ucv;
    s->is_write = (tls_last_vpn == vpn);       /* 2o fallo seguido en la misma pagina => escritura */
    tls_last_vpn = vpn;
#endif
    s->result = 0;

    /* paso 4: encolar sin locks y despertar al Pager */
    atomic_store(&s->state, 1);
    unsigned long t = atomic_fetch_add(&ring_tail, 1);
    atomic_store(&ring[t & ring_mask], s);
    sem_post(&pager_sem);

    /* paso 5: suspenderse en el eventfd propio */
    uint64_t v;
    while (read(s->wake_fd, &v, sizeof v) < 0 && errno == EINTR) { }

    int r = s->result;
    atomic_store(&s->state, 0);
#if !defined(__x86_64__)
    tls_last_vpn = NIL;
#endif
    if (r == DSM_ERR_INVAL) {            /* pagina no asignada: error real, no se oculta */
        restore_default();
        errno = saved;
        return;                          /* la instruccion se repite y mata el proceso */
    }
    if (r < 0) {                         /* politica P3 (sec. 7) */
        char msg[200]; size_t n = 0;
        n = put_str(msg, n, "[dsm] ERROR: acceso a la pagina ");
        n = put_num(msg, n, vpn);
        n = put_str(msg, n, r == DSM_ERR_NODE_DOWN ? " perdida: el nodo " : " fallo de red con el nodo ");
        n = put_num(msg, n, s->err_node);
        n = put_str(msg, n, " no responde; el hilo queda suspendido\n");
        if (write(2, msg, n) < 0) { }
        dsm_task_notify_failure(r, s->err_node, vpn);
        for (;;) pause();
    }
    errno = saved;                       /* paso 6: la instruccion se reintenta */
}

static void slot_done(fault_slot_t *s, int result, node_id_t node) {
    uint64_t one = 1;
    s->result = result;
    s->err_node = node;
    atomic_store(&s->state, 2);
    if (write(s->wake_fd, &one, sizeof one) < 0) LOG_ERR("write(eventfd) fallo\n");
}

/* ------------------------------------------------------------------------- */
/* Cache remota: lista FIFO / Clock intrusiva (indexada por vpn)              */
/* ------------------------------------------------------------------------- */
static uint32_t *fprev, *fnext;
static uint32_t fhead = NIL, ftail = NIL;
static uint32_t clock_hand = NIL;
static pthread_mutex_t fifo_mu = PTHREAD_MUTEX_INITIALIZER;
static _Atomic uint64_t cache_count;
static int pager_policy = PAGER_POLICY_FIFO;

void pager_set_policy(int policy) {
    pager_policy = (policy == PAGER_POLICY_CLOCK) ? PAGER_POLICY_CLOCK : PAGER_POLICY_FIFO;
}

int pager_get_policy(void) {
    return pager_policy;
}

void pager_touch_page(vpn_t v) {
    pager_page_lock(v);
    g_n.lp[v].flags |= PF_REF;
    pager_page_unlock(v);
}

uint64_t pager_cache_count(void) { return atomic_load(&cache_count); }

/* el llamador tiene el mutex de franja de v */
static void cache_add(vpn_t v) {
    pthread_mutex_lock(&fifo_mu);
    fprev[v] = ftail; fnext[v] = NIL;
    if (ftail != NIL) fnext[ftail] = v; else fhead = v;
    ftail = v;
    pthread_mutex_unlock(&fifo_mu);
    g_n.lp[v].flags |= (PF_CACHED | PF_REF);
    atomic_fetch_add(&cache_count, 1);
    STAT_INC(pages_cached);
}

static void cache_remove(vpn_t v) {
    if (!(g_n.lp[v].flags & PF_CACHED)) return;
    pthread_mutex_lock(&fifo_mu);
    if (clock_hand == v) {
        clock_hand = (fnext[v] != NIL) ? fnext[v] : fhead;
        if (clock_hand == v) clock_hand = NIL;
    }
    if (fprev[v] != NIL) fnext[fprev[v]] = fnext[v]; else fhead = fnext[v];
    if (fnext[v] != NIL) fprev[fnext[v]] = fprev[v]; else ftail = fprev[v];
    pthread_mutex_unlock(&fifo_mu);
    g_n.lp[v].flags &= (uint8_t)~(PF_CACHED | PF_REF);
    atomic_fetch_sub(&cache_count, 1);
    STAT_SUB(pages_cached, 1);
}

static int fifo_nth(unsigned k, vpn_t *out) {
    int ok = 0;
    pthread_mutex_lock(&fifo_mu);
    uint32_t v = fhead;
    while (v != NIL && k--) v = fnext[v];
    if (v != NIL) { *out = v; ok = 1; }
    pthread_mutex_unlock(&fifo_mu);
    return ok;
}

/* ------------------------------------------------------------------------- */
/* Paginas devueltas con WRITEBACK que esperan su ACK (sec. 4.4 caso 4)       */
/* ------------------------------------------------------------------------- */
typedef struct wbp { vpn_t vpn; struct wbp *next; } wbp_t;
static wbp_t *wbp_list;
static pthread_mutex_t wbp_mu = PTHREAD_MUTEX_INITIALIZER;

static void wbp_add(vpn_t v) {
    wbp_t *w = malloc(sizeof *w);
    if (!w) abort();
    w->vpn = v;
    pthread_mutex_lock(&wbp_mu); w->next = wbp_list; wbp_list = w; pthread_mutex_unlock(&wbp_mu);
}
static int wbp_has(vpn_t v) {
    int r = 0;
    pthread_mutex_lock(&wbp_mu);
    for (wbp_t *w = wbp_list; w; w = w->next) if (w->vpn == v) { r = 1; break; }
    pthread_mutex_unlock(&wbp_mu);
    return r;
}
static int wbp_del(vpn_t v) {
    int r = 0;
    pthread_mutex_lock(&wbp_mu);
    for (wbp_t **pp = &wbp_list; *pp; pp = &(*pp)->next)
        if ((*pp)->vpn == v) { wbp_t *d = *pp; *pp = d->next; free(d); r = 1; break; }
    pthread_mutex_unlock(&wbp_mu);
    return r;
}
static int wbp_empty(void) {
    pthread_mutex_lock(&wbp_mu); int e = (wbp_list == NULL); pthread_mutex_unlock(&wbp_mu);
    return e;
}

/* ------------------------------------------------------------------------- */
/* Solicitudes pendientes de red (un PAGE_REQUEST por pagina, sec. 9.2 regla 3)*/
/* ------------------------------------------------------------------------- */
typedef struct waiter { fault_slot_t *slot; struct waiter *next; } waiter_t;
typedef struct pend {
    vpn_t vpn; uint64_t req_id; uint8_t access; node_id_t home;
    uint64_t t0_us, deadline_ms;
    waiter_t *waiters;
    struct pend *next;
} pend_t;
static pend_t *pend_list;
static pthread_mutex_t pend_mu = PTHREAD_MUTEX_INITIALIZER;

static int pend_add_waiter(vpn_t vpn, fault_slot_t *s) {
    int ok = 0;
    pthread_mutex_lock(&pend_mu);
    for (pend_t *p = pend_list; p; p = p->next) {
        if (p->vpn != vpn) continue;
        waiter_t *w = malloc(sizeof *w);
        if (!w) abort();
        w->slot = s; w->next = p->waiters; p->waiters = w;
        ok = 1;
        break;
    }
    pthread_mutex_unlock(&pend_mu);
    return ok;
}

static pend_t *pend_take_req(uint64_t req_id) {
    pend_t *r = NULL;
    pthread_mutex_lock(&pend_mu);
    for (pend_t **pp = &pend_list; *pp; pp = &(*pp)->next)
        if ((*pp)->req_id == req_id) { r = *pp; *pp = r->next; break; }
    pthread_mutex_unlock(&pend_mu);
    return r;
}

/* despierta a todos los hilos agrupados; reintentan la instruccion (o fallan) */
static void pend_finish(pend_t *pe, int status, node_id_t node) {
    if (status == 0) {
        STAT_ADD(fault_time_us_total, now_us() - pe->t0_us);
        STAT_INC(fault_count);
    }
    waiter_t *w = pe->waiters;
    while (w) {
        waiter_t *n = w->next;
        slot_done(w->slot, status, node);
        free(w);
        w = n;
    }
    free(pe);
}

static void clear_pending_flag(vpn_t vpn) {
    pager_page_lock(vpn);
    g_n.lp[vpn].flags &= (uint8_t)~PF_PENDING;
    pager_page_unlock(vpn);
}

static void pend_fail(pend_t *pe, int status, node_id_t node) {
    clear_pending_flag(pe->vpn);
    pend_finish(pe, status, node);
}

/* ------------------------------------------------------------------------- */
/* Envio de mensajes de pagina                                               */
/* ------------------------------------------------------------------------- */
static int send_vpn_msg(node_id_t dst, msg_type_t t, uint16_t flags, uint64_t req, vpn_t vpn) {
    uint8_t b[4], *p = b;
    w32(&p, vpn);
    struct iovec iov = { b, 4 };
    return net_sendv(dst, t, flags, req, &iov, 1);
}

static int send_error(node_id_t dst, uint64_t req, int code, vpn_t vpn) {
    uint8_t b[6], *p = b;
    w16(&p, (uint16_t)code); w32(&p, vpn);
    struct iovec iov = { b, 6 };
    return net_sendv(dst, MSG_ERROR, MSG_F_RESP | MSG_F_ERR, req, &iov, 1);
}

static char *thread_buf(void) {
    static __thread char *buf; static __thread size_t sz;
    if (sz < g_n.page_size) { free(buf); buf = malloc(g_n.page_size); sz = buf ? g_n.page_size : 0; if (!buf) abort(); }
    return buf;
}

/* ------------------------------------------------------------------------- */
/* Reemplazo de la cache remota (11.2, prototipo 1: FIFO)                     */
/* ------------------------------------------------------------------------- */
static void punch(vpn_t v, uint32_t n) {
    if (fallocate(g_n.memfd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                  (off_t)v << g_n.page_shift, (off_t)n << g_n.page_shift) < 0)
        LOG_ERR("fallocate(PUNCH_HOLE) fallo: %s\n", strerror(errno));
}

/* Desaloja una pagina remota. Devuelve 1 si la desalojo. */
static int evict_page(vpn_t vpn) {
    node_id_t home = alloc_home_of(vpn);          /* antes de tomar la franja (orden de bloqueo) */
    size_t off = (size_t)vpn << g_n.page_shift;
    pager_page_lock(vpn);
    local_page_t *lp = &g_n.lp[vpn];
    if (!(lp->flags & PF_CACHED) || (lp->flags & PF_PENDING) || lp->state == PG_INVALID) {
        pager_page_unlock(vpn);
        return 0;
    }
    int was_m = (lp->state == PG_MODIFIED);
    for (int t = 0;; t++) {
        if (mprotect(g_n.app + off, g_n.page_size, PROT_NONE) == 0) break;
        int e = errno;
        if (e == ENOMEM && t < 3) { sched_yield(); continue; }
        LOG_ERR("mprotect(PROT_NONE) en evict fallo: %s\n", strerror(e));
        pager_page_unlock(vpn);
        return 0;
    }
    pager_set_state(lp, PG_INVALID);
    cache_remove(vpn);
    STAT_INC(evictions);
    if (home == DSM_NODE_NONE || home == (node_id_t)g_n.self) {
        punch(vpn, 1);
    } else if (!was_m) {
        punch(vpn, 1);                                         /* S: se descarta y se avisa */
        send_vpn_msg(home, MSG_EVICT_NOTICE, 0, net_new_req_id(), vpn);
    } else {                                                   /* M ajena: WRITEBACK, se conserva hasta el ACK */
        wbp_add(vpn);
        uint8_t hd[8], *p = hd;
        w32(&p, vpn); w32(&p, lp->version);
        struct iovec iov[2] = { { hd, 8 }, { g_n.sys + off, g_n.page_size } };
        STAT_INC(writebacks);
        if (net_sendv(home, MSG_WRITEBACK, MSG_F_PAYLOAD, net_new_req_id(), iov, 2) < 0) {
            wbp_del(vpn);                                      /* nodo inalcanzable: la pagina se pierde */
            punch(vpn, 1);
        }
    }
    pager_page_unlock(vpn);
    return 1;
}

static int evict_one_fifo(void) {
    for (unsigned k = 0; k < 16; k++) {
        vpn_t v;
        if (!fifo_nth(k, &v)) return 0;
        if (evict_page(v)) return 1;
    }
    return 0;
}

static int evict_one_clock(void) {
    uint64_t total = atomic_load(&cache_count);
    if (!total) return 0;

    uint64_t max_steps = total * 2 + 16;
    for (uint64_t step = 0; step < max_steps; step++) {
        vpn_t v = NIL;
        pthread_mutex_lock(&fifo_mu);
        if (fhead == NIL) {
            clock_hand = NIL;
            pthread_mutex_unlock(&fifo_mu);
            return 0;
        }
        if (clock_hand == NIL) clock_hand = fhead;
        v = clock_hand;
        clock_hand = (fnext[v] != NIL) ? fnext[v] : fhead;
        pthread_mutex_unlock(&fifo_mu);

        pager_page_lock(v);
        local_page_t *lp = &g_n.lp[v];
        if (!(lp->flags & PF_CACHED) || (lp->flags & PF_PENDING) || lp->state == PG_INVALID) {
            pager_page_unlock(v);
            continue;
        }

        if (lp->flags & PF_REF) {
            /* Segunda oportunidad: apagar bit de referencia y retirar temporalmente permiso con PROT_NONE */
            lp->flags &= (uint8_t)~PF_REF;
            size_t off = (size_t)v << g_n.page_shift;
            mprotect(g_n.app + off, g_n.page_size, PROT_NONE);
            pager_page_unlock(v);
            continue;
        }

        /* Pagina sin referencia: candidata para desalojo */
        pager_page_unlock(v);
        if (evict_page(v)) return 1;
    }
    return 0;
}

static int evict_one(void) {
    if (pager_policy == PAGER_POLICY_CLOCK)
        return evict_one_clock();
    return evict_one_fifo();
}

/* ------------------------------------------------------------------------- */
/* Resolucion de un fallo (hilo Pager)                                        */
/* ------------------------------------------------------------------------- */
static void resolve(fault_slot_t *s) {
    vpn_t vpn = s->vpn;
    int w = s->is_write;
    node_id_t home = alloc_home_of(vpn);
    if (home == DSM_NODE_NONE) { slot_done(s, DSM_ERR_INVAL, 0); return; }

    int access;
    for (;;) {
        pager_page_lock(vpn);
        local_page_t *p = &g_n.lp[vpn];
        if (p->state == PG_MODIFIED || (p->state == PG_SHARED && !w)) {   /* ya resuelto por otro hilo o reactivacion por reloj */
            size_t off = (size_t)vpn << g_n.page_shift;
            int prot = (p->state == PG_MODIFIED) ? (PROT_READ | PROT_WRITE) : PROT_READ;
            mprot(g_n.app + off, g_n.page_size, prot);
            p->flags |= PF_REF;
            if (p->state == PG_MODIFIED && w) p->flags |= PF_DIRTY;
            pager_page_unlock(vpn);
            slot_done(s, 0, 0);
            return;
        }
        if (p->flags & PF_PENDING) {                                      /* agrupar (sec. 9.2 regla 3) */
            pager_page_unlock(vpn);
            if (pend_add_waiter(vpn, s)) return;
            sched_yield();
            continue;
        }
        if (home != (node_id_t)g_n.self && membership_status(home) == NODE_OFFLINE) {
            pager_page_unlock(vpn);
            slot_done(s, DSM_ERR_NODE_DOWN, home);
            return;
        }
        access = w ? (p->state == PG_SHARED ? ACC_UPG : ACC_W) : ACC_R;
        p->flags |= PF_PENDING;
        pager_page_unlock(vpn);
        break;
    }

    if (home != (node_id_t)g_n.self && access != ACC_UPG)
        while (atomic_load(&cache_count) >= g_n.cfg.cache_max_pages && evict_one()) { }

    pend_t *pe = calloc(1, sizeof *pe);
    waiter_t *wt = malloc(sizeof *wt);
    if (!pe || !wt) abort();
    wt->slot = s; wt->next = NULL;
    pe->vpn = vpn; pe->access = (uint8_t)access; pe->home = home;
    pe->req_id = net_new_req_id();
    pe->t0_us = now_us();
    pe->deadline_ms = now_ms() + g_n.cfg.request_timeout_ms;
    pe->waiters = wt;
    pthread_mutex_lock(&pend_mu); pe->next = pend_list; pend_list = pe; pthread_mutex_unlock(&pend_mu);

    if (access == ACC_R) STAT_INC(read_faults);
    else if (access == ACC_W) STAT_INC(write_faults);
    else STAT_INC(upgrade_faults);

    if (home == (node_id_t)g_n.self) {
        dir_request_local(vpn, access, pe->req_id);
    } else {
        uint8_t b[5], *p = b;
        w32(&p, vpn); w8(&p, (uint8_t)access);
        struct iovec iov = { b, 5 };
        if (net_sendv(home, MSG_PAGE_REQUEST, 0, pe->req_id, &iov, 1) < 0) {
            pend_t *t = pend_take_req(pe->req_id);
            if (t) pend_fail(t, DSM_ERR_NODE_DOWN, home);
        }
    }
}

static void check_timeouts(void) {
    uint64_t now = now_ms();
    for (;;) {
        pend_t *dead = NULL;
        pthread_mutex_lock(&pend_mu);
        for (pend_t **pp = &pend_list; *pp; pp = &(*pp)->next)
            if ((*pp)->deadline_ms <= now) { dead = *pp; *pp = dead->next; break; }
        pthread_mutex_unlock(&pend_mu);
        if (!dead) break;
        int down = dead->home != (node_id_t)g_n.self && membership_status(dead->home) == NODE_OFFLINE;
        pend_fail(dead, down ? DSM_ERR_NODE_DOWN : DSM_ERR_NET, dead->home);
    }
    dir_check_timeouts(now);
}

static void *pager_main(void *arg) {
    (void)arg;
    uint64_t last_check = now_ms();
    while (pager_running) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 50 * 1000000L;
        if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
        if (sem_timedwait(&pager_sem, &ts) == 0) {
            unsigned long idx = ring_head++;
            fault_slot_t *s;
            while (!(s = atomic_exchange(&ring[idx & ring_mask], NULL))) sched_yield();
            resolve(s);
        }
        if (now_ms() - last_check >= 100) { check_timeouts(); last_check = now_ms(); }
    }
    return NULL;
}

/* ------------------------------------------------------------------------- */
/* Mensajes que atiende el nodo                                               */
/* ------------------------------------------------------------------------- */

/* PAGE_DATA (o concesion de permiso sin datos) al solicitante */
void pager_on_page_data(const msg_hdr_t *h, const uint8_t *body) {
    const uint8_t *p = body;
    vpn_t vpn = r32(&p);
    uint8_t perm = r8(&p);
    uint32_t ver = r32(&p);
    const uint8_t *data = (h->flags & MSG_F_PAYLOAD) ? p : NULL;

    pend_t *pe = pend_take_req(h->req_id);
    if (!pe) return;                                  /* respuesta tardia: ya vencio */
    size_t off = (size_t)vpn << g_n.page_shift;

    for (int tries = 0;; tries++) {
        pager_page_lock(vpn);
        local_page_t *lp = &g_n.lp[vpn];
        if (!data && lp->state == PG_INVALID) {       /* permiso sin datos pero ya no hay copia */
            lp->flags &= (uint8_t)~PF_PENDING;
            pager_page_unlock(vpn);
            pend_finish(pe, DSM_ERR_NET, h->src);
            return;
        }
        if (data) memcpy(g_n.sys + off, data, g_n.page_size);      /* vista de sistema: sin fallos */
        int prot = (perm == PG_MODIFIED) ? (PROT_READ | PROT_WRITE) : PROT_READ;
        if (mprotect(g_n.app + off, g_n.page_size, prot) == 0) {
            pager_set_state(lp, perm);
            lp->version = ver;
            lp->flags &= (uint8_t)~PF_PENDING;
            if (!(lp->flags & PF_ISHOME) && !(lp->flags & PF_CACHED)) cache_add(vpn);
            else if (lp->flags & PF_CACHED) lp->flags |= PF_REF;
            pager_page_unlock(vpn);
            break;
        }
        int e = errno;
        pager_page_unlock(vpn);
        if (e == ENOMEM && tries < 5 && evict_one()) continue;   /* sec. 3.4 punto 3 */
        LOG_ERR("mprotect fallo al instalar vpn %u: %s\n", vpn, strerror(e));
        pend_fail(pe, DSM_ERR_NOMEM, h->src);
        return;
    }
    pend_finish(pe, 0, 0);
}

void pager_on_error(const msg_hdr_t *h, const uint8_t *body) {
    const uint8_t *p = body;
    int code = r16(&p);
    pend_t *pe = pend_take_req(h->req_id);
    if (!pe) { dir_on_error(h, body); return; }       /* tal vez era la respuesta a un FETCH del home */
    int st = DSM_ERR_NET;
    if (code == E_NODE_DOWN) st = DSM_ERR_NODE_DOWN;
    else if (code == E_NO_MEM) st = DSM_ERR_NOMEM;
    else if (code == E_BAD_VPN) st = DSM_ERR_INVAL;
    pend_fail(pe, st, h->src);
}

/* FETCH: el home pide la pagina que este nodo tiene en M (tabla 4.2, ultimas dos filas) */
void pager_on_fetch(const msg_hdr_t *h, const uint8_t *body) {
    const uint8_t *p = body;
    vpn_t vpn = r32(&p);
    int mode = r8(&p);                                 /* 0 = R (pasa a S), 1 = W (pasa a I) */
    size_t off = (size_t)vpn << g_n.page_shift;
    char *buf = thread_buf();
    uint32_t ver = 0;
    int ok = 0;
    uint8_t keep = PG_INVALID;

    pager_page_lock(vpn);
    local_page_t *lp = &g_n.lp[vpn];
    if (wbp_has(vpn)) {                                /* caso 4.4.4: FETCH cruzado con WRITEBACK */
        memcpy(buf, g_n.sys + off, g_n.page_size);
        ver = lp->version; ok = 1;
    } else if (lp->state == PG_MODIFIED) {
        /* el orden importa: primero se baja el permiso y luego se copia */
        int prot = (mode == 0) ? PROT_READ : PROT_NONE;
        int mprot_ok = 0;
        for (int tries = 0; tries < 5; tries++) {
            if (mprotect(g_n.app + off, g_n.page_size, prot) == 0) { mprot_ok = 1; break; }
            int e = errno;
            if (e == ENOMEM) {
                pager_page_unlock(vpn);
                if (!evict_one()) { pager_page_lock(vpn); break; }
                pager_page_lock(vpn);
                if (lp->state != PG_MODIFIED) break;
            } else break;
        }
        if (mprot_ok) {
            if (mode == 0) { pager_set_state(lp, PG_SHARED); keep = PG_SHARED; }
            else           { pager_set_state(lp, PG_INVALID); }
            memcpy(buf, g_n.sys + off, g_n.page_size);
            ver = lp->version; ok = 1;
            if (mode != 0) { cache_remove(vpn); if (!(lp->flags & PF_ISHOME)) punch(vpn, 1); }
        } else {
            LOG_ERR("mprotect fallo en on_fetch para vpn %u: %s\n", vpn, strerror(errno));
        }
    }
    pager_page_unlock(vpn);

    if (!ok) { send_error(h->src, h->req_id, E_BAD_VPN, vpn); return; }
    uint8_t hd[9], *q = hd;
    w32(&q, vpn); w8(&q, keep); w32(&q, ver);
    struct iovec iov[2] = { { hd, 9 }, { buf, g_n.page_size } };
    net_sendv(h->src, MSG_PAGE_DATA, MSG_F_RESP | MSG_F_PAYLOAD, h->req_id, iov, 2);
}

/* INVALIDATE: idempotente (caso 4.4.3) */
void pager_on_invalidate(const msg_hdr_t *h, const uint8_t *body) {
    const uint8_t *p = body;
    vpn_t vpn = r32(&p);
    size_t off = (size_t)vpn << g_n.page_shift;

    pager_page_lock(vpn);
    local_page_t *lp = &g_n.lp[vpn];
    if (lp->state == PG_SHARED) {
        for (int tries = 0; tries < 5; tries++) {
            if (mprotect(g_n.app + off, g_n.page_size, PROT_NONE) == 0) break;
            int e = errno;
            if (e == ENOMEM) {
                pager_page_unlock(vpn);
                if (!evict_one()) { pager_page_lock(vpn); break; }
                pager_page_lock(vpn);
                if (lp->state != PG_SHARED) break;
            } else break;
        }
        pager_set_state(lp, PG_INVALID);
        cache_remove(vpn);
        if (!(lp->flags & PF_ISHOME)) punch(vpn, 1);
        STAT_INC(invalidations_recv);
    }
    pager_page_unlock(vpn);
    send_vpn_msg(h->src, MSG_INVALIDATE_ACK, MSG_F_RESP, h->req_id, vpn);   /* siempre se confirma */
}

void pager_on_writeback_ack(const msg_hdr_t *h, const uint8_t *body) {
    (void)h;
    const uint8_t *p = body;
    vpn_t vpn = r32(&p);
    if (!wbp_del(vpn)) return;
    pager_page_lock(vpn);
    if (g_n.lp[vpn].state == PG_INVALID) punch(vpn, 1);     /* libera el marco retenido */
    pager_page_unlock(vpn);
}

/* ------------------------------------------------------------------------- */
/* Lado "solicitante == home"                                                 */
/* ------------------------------------------------------------------------- */
void pager_local_complete(vpn_t vpn, pg_state_t perm, uint32_t version, uint64_t req_id) {
    pend_t *pe = pend_take_req(req_id);
    if (!pe) return;
    size_t off = (size_t)vpn << g_n.page_shift;
    int prot = (perm == PG_MODIFIED) ? (PROT_READ | PROT_WRITE) : PROT_READ;
    for (int tries = 0;; tries++) {
        pager_page_lock(vpn);
        local_page_t *lp = &g_n.lp[vpn];
        if (mprotect(g_n.app + off, g_n.page_size, prot) == 0) {
            pager_set_state(lp, perm);
            lp->version = version;
            lp->flags &= (uint8_t)~PF_PENDING;
            pager_page_unlock(vpn);
            break;
        }
        int e = errno;
        pager_page_unlock(vpn);
        if (e == ENOMEM && tries < 5 && evict_one()) continue;
        LOG_ERR("mprotect fallo en local_complete para vpn %u: %s\n", vpn, strerror(e));
        pend_fail(pe, DSM_ERR_NOMEM, 0);
        return;
    }
    pend_finish(pe, 0, 0);
}

void pager_local_fail(uint64_t req_id, int status, node_id_t node) {
    pend_t *pe = pend_take_req(req_id);
    if (pe) pend_fail(pe, status, node);
}

int pager_home_downgrade(vpn_t vpn) {
    size_t off = (size_t)vpn << g_n.page_shift;
    for (int tries = 0;; tries++) {
        pager_page_lock(vpn);
        local_page_t *lp = &g_n.lp[vpn];
        if (lp->state != PG_MODIFIED) {
            pager_page_unlock(vpn);
            return 0;
        }
        if (mprotect(g_n.app + off, g_n.page_size, PROT_READ) == 0) {
            pager_set_state(lp, PG_SHARED);
            pager_page_unlock(vpn);
            return 0;
        }
        int e = errno;
        pager_page_unlock(vpn);
        if (e == ENOMEM && tries < 5 && evict_one()) continue;
        LOG_ERR("mprotect fallo en home_downgrade para vpn %u: %s\n", vpn, strerror(e));
        return DSM_ERR_NOMEM;
    }
}

int pager_home_invalidate(vpn_t vpn) {
    size_t off = (size_t)vpn << g_n.page_shift;
    for (int tries = 0;; tries++) {
        pager_page_lock(vpn);
        local_page_t *lp = &g_n.lp[vpn];
        if (lp->state == PG_INVALID) {
            pager_page_unlock(vpn);
            return 0;
        }
        if (mprotect(g_n.app + off, g_n.page_size, PROT_NONE) == 0) {
            pager_set_state(lp, PG_INVALID);
            pager_page_unlock(vpn);
            return 0;
        }
        int e = errno;
        pager_page_unlock(vpn);
        if (e == ENOMEM && tries < 5 && evict_one()) continue;
        LOG_ERR("mprotect fallo en home_invalidate para vpn %u: %s\n", vpn, strerror(e));
        return DSM_ERR_NOMEM;
    }
}

/* ------------------------------------------------------------------------- */
/* Asignacion y liberacion de rangos                                          */
/* ------------------------------------------------------------------------- */
void pager_init_home_range(vpn_t start, uint32_t n) {
    for (uint32_t i = 0; i < n; i++) {
        vpn_t v = start + i;
        pager_page_lock(v);
        local_page_t *lp = &g_n.lp[v];
        pager_set_state(lp, PG_MODIFIED);
        lp->flags = PF_ISHOME | PF_DIRTY | PF_REF;
        lp->version = 0;
        pager_page_unlock(v);
    }
    mprot(VPN_APP(start), (size_t)n << g_n.page_shift, PROT_READ | PROT_WRITE);   /* una sola VMA */
    STAT_ADD(pages_home, n);
}

void pager_reset_range(vpn_t start, uint32_t n) {
    uint32_t homes = 0;
    for (uint32_t i = 0; i < n; i++) {
        vpn_t v = start + i;
        pager_page_lock(v);
        local_page_t *lp = &g_n.lp[v];
        if (lp->flags & PF_ISHOME) homes++;
        cache_remove(v);
        pager_set_state(lp, PG_INVALID);
        lp->flags = 0;
        lp->version = 0;
        pager_page_unlock(v);
    }
    mprot(VPN_APP(start), (size_t)n << g_n.page_shift, PROT_NONE);
    punch(start, n);
    if (homes) STAT_SUB(pages_home, homes);
}

pg_state_t pager_local_state(vpn_t vpn) {
    pager_page_lock(vpn);
    pg_state_t s = (pg_state_t)g_n.lp[vpn].state;
    pager_page_unlock(vpn);
    return s;
}

/* ------------------------------------------------------------------------- */
/* Caida de un nodo y cierre ordenado                                         */
/* ------------------------------------------------------------------------- */
void pager_on_node_down(node_id_t n) {
    for (;;) {                                         /* solicitudes pendientes hacia el nodo caido */
        pend_t *dead = NULL;
        pthread_mutex_lock(&pend_mu);
        for (pend_t **pp = &pend_list; *pp; pp = &(*pp)->next)
            if ((*pp)->home == n) { dead = *pp; *pp = dead->next; break; }
        pthread_mutex_unlock(&pend_mu);
        if (!dead) break;
        pend_fail(dead, DSM_ERR_NODE_DOWN, n);
    }
    /* paginas cuyo home cayo: se consideran perdidas (factor 1, sec. 7); se invalida la copia local
     * para que el siguiente acceso falle por la via normal y reciba DSM_ERR_NODE_DOWN */
    vpn_range_t *r; int nr = alloc_home_ranges(n, &r);
    for (int i = 0; i < nr; i++)
        for (uint32_t k = 0; k < r[i].n; k++) {
            vpn_t v = r[i].start + k;
            pager_page_lock(v);
            local_page_t *lp = &g_n.lp[v];
            if (lp->state != PG_INVALID) {
                mprot(VPN_APP(v), g_n.page_size, PROT_NONE);
                pager_set_state(lp, PG_INVALID);
                cache_remove(v);
                punch(v, 1);
            }
            pager_page_unlock(v);
        }
    free(r);
}

int pager_writeback_all(void) {
    vpn_range_t *r; int nr = alloc_home_ranges(DSM_NODE_NONE, &r);   /* NONE = todos los ajenos */
    for (int i = 0; i < nr; i++)
        for (uint32_t k = 0; k < r[i].n; k++) evict_page(r[i].start + k);
    free(r);
    uint64_t end = now_ms() + g_n.cfg.request_timeout_ms;
    while (!wbp_empty() && now_ms() < end) usleep(2000);
    return wbp_empty() ? DSM_OK : DSM_ERR_NET;
}

/* ------------------------------------------------------------------------- */
/* Arranque y parada                                                         */
/* ------------------------------------------------------------------------- */
static void *map_sparse(size_t bytes) {
    void *p = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    return p == MAP_FAILED ? NULL : p;
}

int pager_init(void) {
    nslots = g_n.cfg.max_user_threads;
    slots = calloc(nslots, sizeof *slots);
    slot_used = calloc(nslots, 1);
    size_t rs = 2; while (rs < 2u * nslots) rs <<= 1;
    ring = calloc(rs, sizeof *ring);
    if (!slots || !slot_used || !ring) return DSM_ERR_NOMEM;
    for (uint32_t i = 0; i < nslots; i++) slots[i].wake_fd = -1;
    ring_mask = rs - 1;
    for (int i = 0; i < NSTRIPE; i++) pthread_mutex_init(&stripe[i], NULL);
    fprev = map_sparse(g_n.n_vpages * sizeof(uint32_t));
    fnext = map_sparse(g_n.n_vpages * sizeof(uint32_t));
    if (!fprev || !fnext) return DSM_ERR_NOMEM;
    sem_init(&pager_sem, 0, 0);

    clock_hand = NIL;
    const char *pol = getenv("DSM_POLICY");
    if (!pol) pol = getenv("POLICY");
    if ((pol && (!strcasecmp(pol, "clock") || !strcasecmp(pol, "reloj") || !strcmp(pol, "1"))) ||
        g_n.cfg.replacement_policy == 1) {
        pager_policy = PAGER_POLICY_CLOCK;
    } else {
        pager_policy = PAGER_POLICY_FIFO;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_sigaction = segv_handler;
    sa.sa_flags = SA_SIGINFO;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGSEGV, &sa, NULL) < 0) return DSM_ERR_CONFIG;

    pager_running = 1;
    if (pthread_create(&pager_th, NULL, pager_main, NULL) != 0) return DSM_ERR_THREAD;
    return DSM_OK;
}

void pager_stop(void) {
    if (!pager_running) return;
    pager_running = 0;
    pthread_join(pager_th, NULL);
    if (fprev) { munmap(fprev, g_n.n_vpages * sizeof(uint32_t)); fprev = NULL; }
    if (fnext) { munmap(fnext, g_n.n_vpages * sizeof(uint32_t)); fnext = NULL; }
    free(slots); slots = NULL;
    free(slot_used); slot_used = NULL;
    free(ring); ring = NULL;
    fhead = ftail = clock_hand = NIL;
}
