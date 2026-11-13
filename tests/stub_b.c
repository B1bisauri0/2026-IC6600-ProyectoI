/*
 * stub_b.c -- SUSTITUTO DE PRUEBA de los modulos del integrante B (network, membership,
 * sync, dsm_printf, config). NO es parte del proyecto entregable: existe solo para poder
 * ejecutar y probar el modulo A (pager, directory, dsm, life) sin esperar a B.
 *
 * Los nodos son procesos hijos (fork) unidos por socketpair() AF_UNIX en malla completa.
 * Cumple las reglas R1..R4 de src/network.h: cola por origen servida por un pool, un
 * mensaje a la vez por origen.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "../include/dsm.h"
#include "../src/dsm_internal.h"
#include "../src/wire.h"
#include "stub_b.h"

int stub_n, stub_self;
int stub_fd[DSM_MAX_NODES];
static pthread_mutex_t send_mu[DSM_MAX_NODES];
static volatile int running;
static _Atomic uint32_t epoch_ctr = 1;
static _Atomic uint64_t reqctr;

/* ---------- cola por origen ---------- */
typedef struct qmsg { msg_hdr_t h; uint8_t *body; struct qmsg *next; } qmsg_t;
static struct { qmsg_t *head, *tail; int busy; } sq[DSM_MAX_NODES];
static pthread_mutex_t q_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t q_cv = PTHREAD_COND_INITIALIZER;
static pthread_t workers[4], readers[DSM_MAX_NODES], hbt;

static node_status_t st[DSM_MAX_NODES];
static uint64_t mem_used_of[DSM_MAX_NODES];

uint64_t net_new_req_id(void) { return ((uint64_t)stub_self << 48) | (++reqctr); }

static int write_full(int fd, const void *b, size_t n) {
    const char *p = b;
    while (n) { ssize_t k = write(fd, p, n); if (k < 0) { if (errno == EINTR) continue; return -1; } p += k; n -= (size_t)k; }
    return 0;
}

static void dispatch_local(const msg_hdr_t *h, const uint8_t *body);

int net_sendv(node_id_t dst, msg_type_t type, uint16_t flags, uint64_t req_id, const struct iovec *iov, int cnt) {
    size_t len = 0;
    for (int i = 0; i < cnt; i++) len += iov[i].iov_len;
    STAT_INC(msgs_sent);
    STAT_ADD(bytes_sent, sizeof(msg_hdr_t) + len);
    msg_hdr_t h = { DSM_MAGIC, DSM_PROTO_VER, (uint8_t)type, flags, (uint16_t)stub_self, dst, (uint32_t)len, req_id, 1, 0 };
    if (dst == stub_self) {                               /* mensaje a si mismo: se despacha en linea */
        uint8_t *b = malloc(len ? len : 1), *p = b;
        for (int i = 0; i < cnt; i++) { memcpy(p, iov[i].iov_base, iov[i].iov_len); p += iov[i].iov_len; }
        STAT_INC(msgs_recv);
        STAT_ADD(bytes_recv, sizeof(msg_hdr_t) + len);
        dispatch_local(&h, b); free(b);
        return 0;
    }
    if (dst >= stub_n || st[dst] == NODE_OFFLINE || stub_fd[dst] < 0) return -1;
    int rc = 0;
    pthread_mutex_lock(&send_mu[dst]);
    if (write_full(stub_fd[dst], &h, sizeof h) < 0) rc = -1;
    for (int i = 0; !rc && i < cnt; i++) if (write_full(stub_fd[dst], iov[i].iov_base, iov[i].iov_len) < 0) rc = -1;
    pthread_mutex_unlock(&send_mu[dst]);
    return rc;
}

/* ---------- sync (barreras y locks) ---------- */
#define NBAR 65536
static int *bar_cnt, *bar_go;
static pthread_mutex_t sy_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t sy_cv = PTHREAD_COND_INITIALIZER;
#define NLK 64
static int lk_held[NLK], lk_granted[NLK];
static node_id_t lk_q[NLK][16]; static int lk_qn[NLK];
static pthread_mutex_t lk_local[NLK];

static void send_u32(node_id_t dst, msg_type_t t, uint32_t a, uint32_t b) {
    uint8_t buf[8], *p = buf; w32(&p, a); w32(&p, b);
    struct iovec iov = { buf, 8 };
    net_sendv(dst, t, 0, net_new_req_id(), &iov, 1);
}
static void grant_lock(node_id_t n, uint32_t id) {
    if (n == stub_self) { pthread_mutex_lock(&sy_mu); lk_granted[id % NLK] = 1; pthread_cond_broadcast(&sy_cv); pthread_mutex_unlock(&sy_mu); }
    else send_u32(n, MSG_LOCK_GRANT, id, 0);
}
static void sync_msg(const msg_hdr_t *h, const uint8_t *b) {
    const uint8_t *p = b; uint32_t id = r32(&p), x = h->length >= 8 ? r32(&p) : 0;
    switch (h->type) {
    case MSG_BARRIER_ARRIVE: {
        pthread_mutex_lock(&sy_mu);
        int done = (++bar_cnt[id % NBAR] == (int)x);
        pthread_mutex_unlock(&sy_mu);
        if (done) {
            for (int n = 0; n < stub_n; n++) if (n != stub_self) send_u32((node_id_t)n, MSG_BARRIER_GO, id, 0);
            pthread_mutex_lock(&sy_mu); bar_go[id % NBAR] = 1; pthread_cond_broadcast(&sy_cv); pthread_mutex_unlock(&sy_mu);
        }
        break; }
    case MSG_BARRIER_GO:
        pthread_mutex_lock(&sy_mu); bar_go[id % NBAR] = 1; pthread_cond_broadcast(&sy_cv); pthread_mutex_unlock(&sy_mu);
        break;
    case MSG_LOCK_REQ: {
        pthread_mutex_lock(&sy_mu);
        int g = 0;
        if (!lk_held[id % NLK]) { lk_held[id % NLK] = 1; g = 1; } else lk_q[id % NLK][lk_qn[id % NLK]++] = h->src;
        pthread_mutex_unlock(&sy_mu);
        if (g) grant_lock(h->src, id);
        break; }
    case MSG_LOCK_REL: {
        pthread_mutex_lock(&sy_mu);
        int has = lk_qn[id % NLK] > 0; node_id_t nx = 0;
        if (has) { nx = lk_q[id % NLK][0]; memmove(&lk_q[id % NLK][0], &lk_q[id % NLK][1], (size_t)(--lk_qn[id % NLK]) * sizeof(node_id_t)); }
        else lk_held[id % NLK] = 0;
        pthread_mutex_unlock(&sy_mu);
        if (has) grant_lock(nx, id);
        break; }
    case MSG_LOCK_GRANT:
        pthread_mutex_lock(&sy_mu); lk_granted[id % NLK] = 1; pthread_cond_broadcast(&sy_cv); pthread_mutex_unlock(&sy_mu);
        break;
    default: break;
    }
}
static void local_sync(msg_type_t t, uint32_t a, uint32_t b) {
    uint8_t buf[8], *p = buf; w32(&p, a); w32(&p, b);
    msg_hdr_t h = { DSM_MAGIC, 1, (uint8_t)t, 0, (uint16_t)stub_self, (uint16_t)stub_self, 8, 0, 1, 0 };
    sync_msg(&h, buf);
}

int dsm_barrier(dsm_ctx_t *ctx, int id, int n) {
    (void)ctx;
    if (atomic_load(&g_task_failed)) return DSM_ERR_NODE_DOWN;
    node_id_t co = (node_id_t)(id % stub_n);
    if (co != stub_self && st[co] == NODE_OFFLINE) return DSM_ERR_NODE_DOWN;
    if (co == stub_self) local_sync(MSG_BARRIER_ARRIVE, (uint32_t)id, (uint32_t)n);
    else send_u32(co, MSG_BARRIER_ARRIVE, (uint32_t)id, (uint32_t)n);
    pthread_mutex_lock(&sy_mu);
    while (!bar_go[(uint32_t)id % NBAR] && !atomic_load(&g_task_failed) && (co == stub_self || st[co] != NODE_OFFLINE))
        pthread_cond_wait(&sy_cv, &sy_mu);
    int err = atomic_load(&g_task_failed) || (co != stub_self && st[co] == NODE_OFFLINE);
    pthread_mutex_unlock(&sy_mu);
    return err ? DSM_ERR_NODE_DOWN : DSM_OK;
}
int dsm_lock(dsm_ctx_t *ctx, int id) {
    (void)ctx;
    if (atomic_load(&g_task_failed)) return DSM_ERR_NODE_DOWN;
    pthread_mutex_lock(&lk_local[id % NLK]);               /* un solo hilo por nodo pide al gestor */
    node_id_t mg = (node_id_t)(id % stub_n);
    if (mg != stub_self && st[mg] == NODE_OFFLINE) {
        pthread_mutex_unlock(&lk_local[id % NLK]);
        return DSM_ERR_NODE_DOWN;
    }
    if (mg == stub_self) local_sync(MSG_LOCK_REQ, (uint32_t)id, 0); else send_u32(mg, MSG_LOCK_REQ, (uint32_t)id, 0);
    pthread_mutex_lock(&sy_mu);
    while (!lk_granted[id % NLK] && !atomic_load(&g_task_failed) && (mg == stub_self || st[mg] != NODE_OFFLINE))
        pthread_cond_wait(&sy_cv, &sy_mu);
    int err = atomic_load(&g_task_failed) || (mg != stub_self && st[mg] == NODE_OFFLINE);
    lk_granted[id % NLK] = 0;
    pthread_mutex_unlock(&sy_mu);
    if (err) {
        pthread_mutex_unlock(&lk_local[id % NLK]);
        return DSM_ERR_NODE_DOWN;
    }
    return DSM_OK;
}
int dsm_unlock(dsm_ctx_t *ctx, int id) {
    (void)ctx;
    node_id_t mg = (node_id_t)(id % stub_n);
    if (mg == stub_self) local_sync(MSG_LOCK_REL, (uint32_t)id, 0); else send_u32(mg, MSG_LOCK_REL, (uint32_t)id, 0);
    pthread_mutex_unlock(&lk_local[id % NLK]);
    return DSM_OK;
}
int dsm_printf(dsm_ctx_t *ctx, const char *fmt, ...) {
    (void)ctx;
    va_list ap; va_start(ap, fmt);
    int r = vprintf(fmt, ap);
    va_end(ap); fflush(stdout);
    return r;
}

/* ---------- recepcion ---------- */
static void dispatch_local(const msg_hdr_t *h, const uint8_t *body) {
    switch (h->type) {
    case MSG_HEARTBEAT: { const uint8_t *p = body; (void)r64(&p); mem_used_of[h->src] = r64(&p); break; }
    case MSG_BARRIER_ARRIVE: case MSG_BARRIER_GO: case MSG_LOCK_REQ: case MSG_LOCK_REL: case MSG_LOCK_GRANT:
        sync_msg(h, body); break;
    default: dsm_msg_dispatch(h, body); break;
    }
}

static void *worker_main(void *a) {
    (void)a;
    pthread_mutex_lock(&q_mu);
    while (running) {
        int pick = -1;
        for (int i = 0; i < stub_n; i++) if (sq[i].head && !sq[i].busy) { pick = i; break; }
        if (pick < 0) { pthread_cond_wait(&q_cv, &q_mu); continue; }
        qmsg_t *m = sq[pick].head; sq[pick].head = m->next; if (!m->next) sq[pick].tail = NULL;
        sq[pick].busy = 1;                                  /* R1: uno a la vez por origen */
        pthread_mutex_unlock(&q_mu);
        dispatch_local(&m->h, m->body);
        free(m->body); free(m);
        pthread_mutex_lock(&q_mu);
        sq[pick].busy = 0;
        pthread_cond_broadcast(&q_cv);
    }
    pthread_mutex_unlock(&q_mu);
    return NULL;
}

static int read_full(int fd, void *b, size_t n) {
    char *p = b;
    while (n) { ssize_t k = read(fd, p, n); if (k <= 0) { if (k < 0 && errno == EINTR) continue; return -1; } p += k; n -= (size_t)k; }
    return 0;
}

static void *reader_main(void *a) {
    int peer = (int)(intptr_t)a;
    for (;;) {
        qmsg_t *m = calloc(1, sizeof *m);
        if (read_full(stub_fd[peer], &m->h, sizeof m->h) < 0) { free(m); break; }
        m->body = malloc(m->h.length ? m->h.length : 1);
        if (m->h.length && read_full(stub_fd[peer], m->body, m->h.length) < 0) { free(m->body); free(m); break; }
        STAT_INC(msgs_recv);
        STAT_ADD(bytes_recv, sizeof(msg_hdr_t) + m->h.length);
        pthread_mutex_lock(&q_mu);
        if (sq[peer].tail) sq[peer].tail->next = m; else sq[peer].head = m;
        sq[peer].tail = m;
        pthread_cond_broadcast(&q_cv);
        pthread_mutex_unlock(&q_mu);
    }
    if (running && st[peer] != NODE_OFFLINE) {
        st[peer] = NODE_OFFLINE;
        pthread_mutex_lock(&sy_mu);
        pthread_cond_broadcast(&sy_cv);
        pthread_mutex_unlock(&sy_mu);
        dsm_on_node_down((node_id_t)peer);
    }
    return NULL;
}

static void *hb_main(void *a) {
    (void)a;
    while (running) {
        uint8_t b[16], *p = b; w64(&p, now_ms()); w64(&p, dsm_local_mem_used_bytes());
        struct iovec iov = { b, 16 };
        for (int n = 0; n < stub_n; n++) if (n != stub_self) net_sendv((node_id_t)n, MSG_HEARTBEAT, 0, net_new_req_id(), &iov, 1);
        usleep(50000);
    }
    return NULL;
}

int net_start(const dsm_config_t *cfg, int self_id) {
    (void)cfg; (void)self_id;
    signal(SIGPIPE, SIG_IGN);
    running = 1;
    bar_cnt = calloc(NBAR, sizeof(int)); bar_go = calloc(NBAR, sizeof(int));
    for (int i = 0; i < NLK; i++) pthread_mutex_init(&lk_local[i], NULL);
    for (int i = 0; i < DSM_MAX_NODES; i++) pthread_mutex_init(&send_mu[i], NULL);
    for (int i = 0; i < stub_n; i++) st[i] = NODE_ONLINE;
    for (int i = 0; i < 4; i++) pthread_create(&workers[i], NULL, worker_main, NULL);
    for (int i = 0; i < stub_n; i++) if (i != stub_self) pthread_create(&readers[i], NULL, reader_main, (void *)(intptr_t)i);
    pthread_create(&hbt, NULL, hb_main, NULL);
    (void)epoch_ctr;
    return DSM_OK;
}
void net_stop(void) { running = 0; }
int  membership_start(const dsm_config_t *cfg, int self_id) { (void)cfg; (void)self_id; return DSM_OK; }
void membership_stop(void) { }
node_status_t membership_status(node_id_t id) { return id < stub_n ? st[id] : NODE_OFFLINE; }
int membership_online_ids(node_id_t *out, int max) {
    int k = 0;
    for (int i = 0; i < stub_n && k < max; i++) if (st[i] == NODE_ONLINE) out[k++] = (node_id_t)i;
    return k;
}
int membership_get(node_id_t id, node_info_t *o) {
    if (id >= stub_n) return -1;
    memset(o, 0, sizeof *o);
    o->id = id; o->status = st[id]; o->mem_bytes = g_n.cfg.node_mem_bytes; o->mem_used = mem_used_of[id];
    return 0;
}
int dsm_get_stats(dsm_ctx_t *ctx, dsm_stats_t *out, int node) {
    (void)ctx;
    if (!out) return DSM_ERR_INVAL;
    if (node == -1 || node == stub_self) {
        stats_snapshot(out);
        return DSM_OK;
    }
    return DSM_ERR_NOTFOUND;
}

int config_load(const char *path, int id_arg, dsm_config_t *cfg, int *self_id) {
    if (!path || !cfg || !self_id) return DSM_ERR_CONFIG;
    FILE *f = fopen(path, "r");
    if (!f) return DSM_ERR_CONFIG;

    memset(cfg, 0, sizeof *cfg);
    cfg->page_size = 4096;
    cfg->node_mem_bytes = 512ull * 1024 * 1024;
    cfg->max_nodes = 3;
    cfg->va_slab_bytes = 2048ull * 1024 * 1024;
    cfg->base_addr = 0x200000000000ull;
    cfg->heartbeat_ms = 1000;
    cfg->heartbeat_miss_max = 3;
    cfg->connect_timeout_ms = 2000;
    cfg->request_timeout_ms = 5000;
    cfg->cache_max_pages = 24000;
    cfg->worker_threads = 4;
    cfg->max_user_threads = 64;
    cfg->replication_factor = 1;
    cfg->replacement_policy = 0;

    char line[256];
    while (fgets(line, sizeof line, f)) {
        char *s = line;
        while (*s == ' ' || *s == '\t') s++;
        if (*s == '#' || *s == '\r' || *s == '\n' || !*s) continue;
        char *eq = strchr(s, '=');
        if (!eq) continue;
        *eq = '\0';
        char *key = s;
        char *val = eq + 1;
        char *ke = key + strlen(key) - 1;
        while (ke > key && (*ke == ' ' || *ke == '\t')) *ke-- = '\0';
        char *ve = val + strlen(val) - 1;
        while (ve >= val && (*ve == ' ' || *ve == '\t' || *ve == '\r' || *ve == '\n')) *ve-- = '\0';
        while (*val == ' ' || *val == '\t') val++;

        if (!strcmp(key, "page_size")) cfg->page_size = (uint32_t)strtoul(val, NULL, 0);
        else if (!strcmp(key, "node_mem_mb")) cfg->node_mem_bytes = (uint64_t)strtoull(val, NULL, 0) << 20;
        else if (!strcmp(key, "max_nodes")) cfg->max_nodes = (uint32_t)strtoul(val, NULL, 0);
        else if (!strcmp(key, "va_slab_mb")) cfg->va_slab_bytes = (uint64_t)strtoull(val, NULL, 0) << 20;
        else if (!strcmp(key, "base_addr")) cfg->base_addr = (uintptr_t)strtoull(val, NULL, 0);
        else if (!strcmp(key, "heartbeat_ms")) cfg->heartbeat_ms = (uint32_t)strtoul(val, NULL, 0);
        else if (!strcmp(key, "heartbeat_miss_max")) cfg->heartbeat_miss_max = (uint32_t)strtoul(val, NULL, 0);
        else if (!strcmp(key, "connect_timeout_ms")) cfg->connect_timeout_ms = (uint32_t)strtoul(val, NULL, 0);
        else if (!strcmp(key, "request_timeout_ms")) cfg->request_timeout_ms = (uint32_t)strtoul(val, NULL, 0);
        else if (!strcmp(key, "cache_max_pages")) cfg->cache_max_pages = (uint32_t)strtoul(val, NULL, 0);
        else if (!strcmp(key, "worker_threads")) cfg->worker_threads = (uint32_t)strtoul(val, NULL, 0);
        else if (!strcmp(key, "max_user_threads")) cfg->max_user_threads = (uint32_t)strtoul(val, NULL, 0);
        else if (!strcmp(key, "replication_factor")) cfg->replication_factor = atoi(val);
        else if (!strcmp(key, "replacement") || !strcmp(key, "replacement_policy")) {
            if (!strcasecmp(val, "clock") || !strcasecmp(val, "reloj") || !strcmp(val, "1"))
                cfg->replacement_policy = 1;
            else
                cfg->replacement_policy = 0;
        }
        else if (!strcmp(key, "peers")) {
            char *tok = strtok(val, ",");
            cfg->n_peers = 0;
            while (tok && cfg->n_peers < DSM_MAX_NODES) {
                while (*tok == ' ' || *tok == '\t') tok++;
                char *colon = strchr(tok, ':');
                if (colon) {
                    *colon = '\0';
                    strncpy(cfg->peers[cfg->n_peers].host, tok, sizeof(cfg->peers[cfg->n_peers].host) - 1);
                    cfg->peers[cfg->n_peers].port_base = (uint16_t)atoi(colon + 1);
                } else {
                    strncpy(cfg->peers[cfg->n_peers].host, tok, sizeof(cfg->peers[cfg->n_peers].host) - 1);
                    cfg->peers[cfg->n_peers].port_base = 27000;
                }
                cfg->n_peers++;
                tok = strtok(NULL, ",");
            }
        }
    }
    fclose(f);
    *self_id = (id_arg >= 0) ? id_arg : 0;
    return DSM_OK;
}
