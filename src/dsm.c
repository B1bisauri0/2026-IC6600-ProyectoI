/*
 * dsm.c -- API publica, inicializacion, asignacion de memoria DSM y registro de hilos.
 *
 * Documento de diseño: D1 (misma direccion base en todos los nodos), D3 (doble mapeo),
 * D5 (franjas virtuales por nodo de origen), 3.1/3.2 (reserva), 5.3 (extents y tabla de
 * asignaciones), 8 (ALLOC_*), 10.3 (funciones), 11.3 (reparto de marcos entre nodos).
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "directory.h"
#include "dsm.h"
#include "dsm_internal.h"
#include "pager.h"
#include "wire.h"
#include "sync.h"
#include "replica.h"

dsm_node_t g_n;

struct dsm_ctx { uint32_t owner_pid; };
static dsm_ctx_t node_ctx;
static __thread dsm_status_t tl_errno;

#ifndef MAP_FIXED_NOREPLACE
#define MAP_FIXED_NOREPLACE 0x100000
#endif

uint64_t now_us(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return (uint64_t)t.tv_sec * 1000000u + (uint64_t)t.tv_nsec / 1000u; }
uint64_t now_ms(void) { return now_us() / 1000u; }

dsm_status_t dsm_errno(void) { return tl_errno; }
static void *fail_ptr(dsm_status_t s) { tl_errno = s; return NULL; }
const char *dsm_strerror(dsm_status_t s) {
    switch (s) {
    case DSM_OK: return "ok";
    case DSM_ERR_CONFIG: return "archivo de configuracion invalido o inconsistente";
    case DSM_ERR_NOMEM: return "sin memoria DSM disponible";
    case DSM_ERR_NET: return "falla de red o timeout";
    case DSM_ERR_NODE_DOWN: return "el home o el dueno de la pagina esta caido";
    case DSM_ERR_INVAL: return "parametro invalido";
    case DSM_ERR_NOT_INIT: return "DSM no inicializado";
    case DSM_ERR_EXISTS: return "el nombre ya esta publicado";
    case DSM_ERR_NOTFOUND: return "el nombre no existe";
    case DSM_ERR_THREAD: return "hilo no registrado o sin slots libres";
    }
    return "error desconocido";
}

/* ========================================================================= */
/* Tabla de asignaciones (replicada por ALLOC_ANNOUNCE) e indice de extents  */
/* ========================================================================= */
typedef struct arec { alloc_t a; char name[DSM_NAME_MAX]; } arec_t;
typedef struct { vpn_t start; uint32_t n; node_id_t home, backup; arec_t *rec; } xent_t;
static void apply_announce(arec_t *rec);

static arec_t **allocs; static size_t n_allocs, cap_allocs;
static xent_t *xs; static size_t n_xs, cap_xs;
static pthread_rwlock_t alloc_rw = PTHREAD_RWLOCK_INITIALIZER;

static long x_find(vpn_t vpn) {             /* ultimo extent con start <= vpn; el llamador tiene el lock */
    size_t lo = 0, hi = n_xs;
    while (lo < hi) { size_t m = (lo + hi) / 2; if (xs[m].start <= vpn) lo = m + 1; else hi = m; }
    if (!lo) return -1;
    xent_t *x = &xs[lo - 1];
    return (vpn < x->start + x->n) ? (long)(lo - 1) : -1;
}

node_id_t alloc_home_of(vpn_t vpn) {
    node_id_t h = DSM_NODE_NONE;
    pthread_rwlock_rdlock(&alloc_rw);
    long i = x_find(vpn);
    if (i >= 0) h = xs[i].home;
    pthread_rwlock_unlock(&alloc_rw);
    return h;
}

int alloc_home_ranges(node_id_t home, vpn_range_t **out) {
    pthread_rwlock_rdlock(&alloc_rw);
    vpn_range_t *r = malloc((n_xs + 1) * sizeof *r);
    int n = 0;
    for (size_t i = 0; r && i < n_xs; i++)
        if (xs[i].home == home || (home == DSM_NODE_NONE && xs[i].home != (node_id_t)g_n.self)) {
            r[n].start = xs[i].start; r[n].n = xs[i].n; n++;
        }
    pthread_rwlock_unlock(&alloc_rw);
    *out = r;
    return r ? n : 0;
}

node_id_t alloc_backup_of(vpn_t vpn) {
    node_id_t b = DSM_NODE_NONE;
    pthread_rwlock_rdlock(&alloc_rw);
    long i = x_find(vpn);
    if (i >= 0) b = xs[i].backup;
    pthread_rwlock_unlock(&alloc_rw);
    return b;
}

size_t dsm_alloc_count(void) {
    pthread_rwlock_rdlock(&alloc_rw);
    size_t c = n_allocs;
    pthread_rwlock_unlock(&alloc_rw);
    return c;
}

int dsm_alloc_get_info(size_t index, alloc_t *out_a, char *out_name, size_t name_max) {
    if (!out_a) return -1;
    pthread_rwlock_rdlock(&alloc_rw);
    if (index >= n_allocs) { pthread_rwlock_unlock(&alloc_rw); return -1; }
    arec_t *rec = allocs[index];
    *out_a = rec->a;
    if (rec->a.n_extents > 0) {
        out_a->extents = malloc((size_t)rec->a.n_extents * sizeof(extent_t));
        if (out_a->extents) memcpy(out_a->extents, rec->a.extents, (size_t)rec->a.n_extents * sizeof(extent_t));
    } else {
        out_a->extents = NULL;
    }
    if (out_name && name_max > 0) {
        strncpy(out_name, rec->name, name_max - 1);
        out_name[name_max - 1] = '\0';
    }
    pthread_rwlock_unlock(&alloc_rw);
    return 0;
}

void dsm_alloc_free_info(alloc_t *a) {
    if (a && a->extents) { free(a->extents); a->extents = NULL; }
}

int dsm_alloc_find_vpn(vpn_t vpn, uint32_t *out_alloc_id, node_id_t *out_origin,
                       uint32_t *out_owner_pid, uint32_t *out_order, uint32_t *out_n_pages) {
    pthread_rwlock_rdlock(&alloc_rw);
    long i = x_find(vpn);
    if (i < 0) { pthread_rwlock_unlock(&alloc_rw); return -1; }
    arec_t *rec = xs[i].rec;
    if (out_alloc_id) *out_alloc_id = rec->a.alloc_id;
    if (out_origin)   *out_origin = rec->a.origin;
    if (out_owner_pid) *out_owner_pid = rec->a.owner_pid;
    if (out_n_pages)  *out_n_pages = rec->a.n_pages;
    if (out_order) {
        if (vpn >= rec->a.vpn_start && vpn < rec->a.vpn_start + rec->a.n_pages)
            *out_order = vpn - rec->a.vpn_start;
        else
            *out_order = 0;
    }
    pthread_rwlock_unlock(&alloc_rw);
    return 0;
}

static arec_t *rec_by_id(uint32_t id) {      /* lock tomado por el llamador */
    for (size_t i = 0; i < n_allocs; i++) if (allocs[i]->a.alloc_id == id) return allocs[i];
    return NULL;
}

/* inserta; si ya existia devuelve el existente y libera 'rec' */
static arec_t *table_insert(arec_t *rec) {
    pthread_rwlock_wrlock(&alloc_rw);
    arec_t *old = rec_by_id(rec->a.alloc_id);
    if (old) { pthread_rwlock_unlock(&alloc_rw); free(rec->a.extents); free(rec); return old; }
    if (n_allocs == cap_allocs) { cap_allocs = cap_allocs ? cap_allocs * 2 : 16; allocs = realloc(allocs, cap_allocs * sizeof *allocs); }
    allocs[n_allocs++] = rec;
    for (int k = 0; k < rec->a.n_extents; k++) {
        extent_t *e = &rec->a.extents[k];
        if (n_xs == cap_xs) { cap_xs = cap_xs ? cap_xs * 2 : 64; xs = realloc(xs, cap_xs * sizeof *xs); }
        size_t pos = 0;
        while (pos < n_xs && xs[pos].start < e->vpn_start) pos++;
        memmove(&xs[pos + 1], &xs[pos], (n_xs - pos) * sizeof *xs);
        xs[pos] = (xent_t){ e->vpn_start, e->n_pages, e->home, e->backup, rec };
        n_xs++;
    }
    pthread_rwlock_unlock(&alloc_rw);
    return rec;
}

static arec_t *table_remove(uint32_t id) {
    pthread_rwlock_wrlock(&alloc_rw);
    arec_t *rec = rec_by_id(id);
    if (rec) {
        for (size_t i = 0; i < n_allocs; i++) if (allocs[i] == rec) { allocs[i] = allocs[--n_allocs]; break; }
        size_t j = 0;
        for (size_t i = 0; i < n_xs; i++) if (xs[i].rec != rec) xs[j++] = xs[i];
        n_xs = j;
    }
    pthread_rwlock_unlock(&alloc_rw);
    return rec;
}

/* ========================================================================= */
/* Franja virtual de este nodo (D5): primer ajuste con fusion                */
/* ========================================================================= */
static vpn_range_t *fr; static size_t n_fr;
static pthread_mutex_t fr_mu = PTHREAD_MUTEX_INITIALIZER;

static void vpn_init(void) {
    fr = malloc(sizeof *fr);
    fr[0].start = (vpn_t)((uint64_t)g_n.self * g_n.slab_pages);
    fr[0].n = (uint32_t)g_n.slab_pages;
    n_fr = 1;
}
static int vpn_alloc(uint32_t n, vpn_t *out) {
    int ok = 0;
    pthread_mutex_lock(&fr_mu);
    for (size_t i = 0; i < n_fr; i++) {
        if (fr[i].n < n) continue;
        *out = fr[i].start; fr[i].start += n; fr[i].n -= n;
        if (!fr[i].n) { memmove(&fr[i], &fr[i + 1], (n_fr - i - 1) * sizeof *fr); n_fr--; }
        ok = 1; break;
    }
    pthread_mutex_unlock(&fr_mu);
    return ok;
}
static void vpn_free(vpn_t s, uint32_t n) {
    pthread_mutex_lock(&fr_mu);
    fr = realloc(fr, (n_fr + 1) * sizeof *fr);
    size_t pos = 0;
    while (pos < n_fr && fr[pos].start < s) pos++;
    memmove(&fr[pos + 1], &fr[pos], (n_fr - pos) * sizeof *fr);
    fr[pos].start = s; fr[pos].n = n; n_fr++;
    if (pos + 1 < n_fr && fr[pos].start + fr[pos].n == fr[pos + 1].start) {
        fr[pos].n += fr[pos + 1].n; memmove(&fr[pos + 1], &fr[pos + 2], (n_fr - pos - 2) * sizeof *fr); n_fr--;
    }
    if (pos > 0 && fr[pos - 1].start + fr[pos - 1].n == fr[pos].start) {
        fr[pos - 1].n += fr[pos].n; memmove(&fr[pos], &fr[pos + 1], (n_fr - pos - 1) * sizeof *fr); n_fr--;
    }
    pthread_mutex_unlock(&fr_mu);
}

/* ========================================================================= */
/* Marcos: reservas locales y remotas                                        */
/* ========================================================================= */
static uint64_t reserve_upto(uint64_t n) {            /* reserva min(n, libres) */
    uint64_t u = atomic_load(&g_n.used_pages), g;
    do { uint64_t fre = g_n.node_mem_pages > u ? g_n.node_mem_pages - u : 0; g = n < fre ? n : fre; }
    while (!atomic_compare_exchange_weak(&g_n.used_pages, &u, u + g));
    return g;
}
static void release_frames(uint64_t n) { atomic_fetch_sub(&g_n.used_pages, n); }
uint64_t dsm_local_mem_used_bytes(void) { return atomic_load(&g_n.used_pages) * g_n.page_size; }

typedef struct resv { uint32_t alloc_id; uint64_t n; struct resv *next; } resv_t;
static resv_t *resvs;
static pthread_mutex_t resv_mu = PTHREAD_MUTEX_INITIALIZER;
static void resv_add(uint32_t id, uint64_t n) {
    pthread_mutex_lock(&resv_mu);
    resv_t *r = resvs;
    while (r && r->alloc_id != id) r = r->next;
    if (!r) { r = calloc(1, sizeof *r); r->alloc_id = id; r->next = resvs; resvs = r; }
    r->n += n;
    pthread_mutex_unlock(&resv_mu);
}
/* quita la reserva; devuelve las paginas que tenia */
static uint64_t resv_take(uint32_t id) {
    uint64_t n = 0;
    pthread_mutex_lock(&resv_mu);
    for (resv_t **pp = &resvs; *pp; pp = &(*pp)->next)
        if ((*pp)->alloc_id == id) { resv_t *d = *pp; n = d->n; *pp = d->next; free(d); break; }
    pthread_mutex_unlock(&resv_mu);
    return n;
}

/* ========================================================================= */
/* RPC de ALLOC_REQUEST / ALLOC_ANNOUNCE / REPLICA                           */
/* ========================================================================= */
static rpc_t *rpcs;
static pthread_mutex_t rpc_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t rpc_cv = PTHREAD_COND_INITIALIZER;

rpc_t *rpc_begin(uint32_t id, int kind, uint16_t mask) {
    rpc_t *r = calloc(1, sizeof *r);
    r->alloc_id = id; r->kind = kind; r->waiting = mask;
    pthread_mutex_lock(&rpc_mu); r->next = rpcs; rpcs = r; pthread_mutex_unlock(&rpc_mu);
    return r;
}
int rpc_wait_end(rpc_t *r) {
    uint64_t end = now_ms() + g_n.cfg.request_timeout_ms;
    int rc = 0;
    pthread_mutex_lock(&rpc_mu);
    while (r->waiting) {
        for (int n = 0; n < DSM_MAX_NODES; n++)
            if ((r->waiting & BIT(n)) && membership_status((node_id_t)n) == NODE_OFFLINE) r->waiting &= (uint16_t)~BIT(n);
        if (!r->waiting) break;
        struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 50 * 1000000L; if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
        pthread_cond_timedwait(&rpc_cv, &rpc_mu, &ts);
        if (r->waiting && now_ms() >= end) { rc = -1; break; }
    }
    for (rpc_t **pp = &rpcs; *pp; pp = &(*pp)->next) if (*pp == r) { *pp = r->next; break; }
    pthread_mutex_unlock(&rpc_mu);
    return rc;
}
void rpc_reply(uint32_t id, int kind, node_id_t src, uint32_t granted) {
    pthread_mutex_lock(&rpc_mu);
    for (rpc_t *r = rpcs; r; r = r->next)
        if (r->alloc_id == id && r->kind == kind && (r->waiting & BIT(src))) {
            r->waiting &= (uint16_t)~BIT(src);
            if (src < DSM_MAX_NODES) r->granted[src] += granted;
            pthread_cond_broadcast(&rpc_cv);
            break;
        }
    pthread_mutex_unlock(&rpc_mu);
}

/* ========================================================================= */
/* ALLOC_ANNOUNCE: formato propio del cuerpo (el documento lo deja abierto)   */
/*   u32 alloc_id, u16 origin, u32 owner_pid, u32 vpn_start, u32 n_pages,     */
/*   u16 n_extents, n_extents x {u32 vpn_start, u32 n_pages, u16 home,        */
/*   u16 backup}, u8 name_len, name                                           */
/* ========================================================================= */
static size_t build_announce(const arec_t *rec, uint8_t **out) {
    size_t nl = strlen(rec->name);
    size_t len = 20 + (size_t)rec->a.n_extents * 12 + 1 + nl;
    uint8_t *b = malloc(len), *p = b;
    w32(&p, rec->a.alloc_id); w16(&p, rec->a.origin); w32(&p, rec->a.owner_pid);
    w32(&p, rec->a.vpn_start); w32(&p, rec->a.n_pages); w16(&p, rec->a.n_extents);
    for (int i = 0; i < rec->a.n_extents; i++) {
        const extent_t *e = &rec->a.extents[i];
        w32(&p, e->vpn_start); w32(&p, e->n_pages); w16(&p, e->home); w16(&p, e->backup);
    }
    w8(&p, (uint8_t)nl); memcpy(p, rec->name, nl);
    *out = b;
    return len;
}

static arec_t *parse_announce(const uint8_t *b, uint32_t len) {
    if (len < 21) return NULL;
    const uint8_t *p = b;
    arec_t *rec = calloc(1, sizeof *rec);
    rec->a.alloc_id = r32(&p); rec->a.origin = r16(&p); rec->a.owner_pid = r32(&p);
    rec->a.vpn_start = r32(&p); rec->a.n_pages = r32(&p); rec->a.n_extents = r16(&p);
    if (len < 21u + (uint32_t)rec->a.n_extents * 12u) { free(rec); return NULL; }
    rec->a.extents = calloc(rec->a.n_extents ? rec->a.n_extents : 1, sizeof(extent_t));
    for (int i = 0; i < rec->a.n_extents; i++) {
        rec->a.extents[i].vpn_start = r32(&p); rec->a.extents[i].n_pages = r32(&p);
        rec->a.extents[i].home = r16(&p); rec->a.extents[i].backup = r16(&p);
    }
    uint8_t nl = r8(&p);
    if ((size_t)(p - b) + nl > len || nl >= DSM_NAME_MAX) { free(rec->a.extents); free(rec); return NULL; }
    memcpy(rec->name, p, nl);
    return rec;
}

size_t dsm_alloc_table_serialize(uint8_t **out) {
    pthread_rwlock_rdlock(&alloc_rw);
    size_t total_len = 4;
    for (size_t i = 0; i < n_allocs; i++) {
        arec_t *rec = allocs[i];
        size_t nl = strlen(rec->name);
        total_len += 20 + (size_t)rec->a.n_extents * 12 + 1 + nl;
    }
    uint8_t *b = malloc(total_len ? total_len : 1);
    if (!b) { pthread_rwlock_unlock(&alloc_rw); *out = NULL; return 0; }
    uint8_t *p = b;
    w32(&p, (uint32_t)n_allocs);
    for (size_t i = 0; i < n_allocs; i++) {
        arec_t *rec = allocs[i];
        w32(&p, rec->a.alloc_id);
        w16(&p, rec->a.origin);
        w32(&p, rec->a.owner_pid);
        w32(&p, rec->a.vpn_start);
        w32(&p, rec->a.n_pages);
        w16(&p, rec->a.n_extents);
        for (int k = 0; k < rec->a.n_extents; k++) {
            extent_t *e = &rec->a.extents[k];
            w32(&p, e->vpn_start);
            w32(&p, e->n_pages);
            w16(&p, e->home);
            w16(&p, e->backup);
        }
        size_t nl = strlen(rec->name);
        w8(&p, (uint8_t)nl);
        memcpy(p, rec->name, nl);
        p += nl;
    }
    pthread_rwlock_unlock(&alloc_rw);
    *out = b;
    return (size_t)(p - b);
}

int dsm_alloc_table_deserialize(const uint8_t *b, size_t len) {
    if (!b || len < 4) return -1;
    const uint8_t *p = b;
    uint32_t count = r32(&p);
    for (uint32_t i = 0; i < count; i++) {
        if ((size_t)(p - b) + 20 > len) return -1;
        arec_t *rec = calloc(1, sizeof *rec);
        if (!rec) return -1;
        rec->a.alloc_id = r32(&p);
        rec->a.origin = r16(&p);
        rec->a.owner_pid = r32(&p);
        rec->a.vpn_start = r32(&p);
        rec->a.n_pages = r32(&p);
        rec->a.n_extents = r16(&p);
        if ((size_t)(p - b) + (size_t)rec->a.n_extents * 12 > len) {
            free(rec); return -1;
        }
        rec->a.extents = calloc(rec->a.n_extents ? rec->a.n_extents : 1, sizeof(extent_t));
        for (int k = 0; k < rec->a.n_extents; k++) {
            rec->a.extents[k].vpn_start = r32(&p);
            rec->a.extents[k].n_pages = r32(&p);
            rec->a.extents[k].home = r16(&p);
            rec->a.extents[k].backup = r16(&p);
        }
        if ((size_t)(p - b) + 1 > len) { free(rec->a.extents); free(rec); return -1; }
        uint8_t nl = r8(&p);
        if ((size_t)(p - b) + nl > len || nl >= DSM_NAME_MAX) { free(rec->a.extents); free(rec); return -1; }
        memcpy(rec->name, p, nl);
        rec->name[nl] = '\0';
        p += nl;
        apply_announce(rec);
    }
    return 0;
}

/* Aplica un anuncio (origen o receptor). Idempotente: si ya existia, solo actualiza el nombre. */
static void apply_announce(arec_t *rec) {
    char name[DSM_NAME_MAX];
    memcpy(name, rec->name, sizeof name);
    arec_t *in = table_insert(rec);
    if (in != rec) {                                        /* ya conocida: publicacion de nombre */
        pthread_rwlock_wrlock(&alloc_rw);
        memcpy(in->name, name, sizeof name);
        pthread_rwlock_unlock(&alloc_rw);
        return;
    }
    for (int i = 0; i < rec->a.n_extents; i++) {
        extent_t *e = &rec->a.extents[i];
        if (e->home != (node_id_t)g_n.self) continue;
        for (uint32_t k = 0; k < e->n_pages; k++) dir_page_create(e->vpn_start + k);
        pager_init_home_range(e->vpn_start, e->n_pages);     /* estado M, RW, contenido en ceros */
    }
    resv_take(rec->a.alloc_id);                              /* la reserva pasa a ser uso real */
}

static void apply_free(uint32_t id) {
    arec_t *rec = table_remove(id);
    if (!rec) { uint64_t n = resv_take(id); if (n) release_frames(n); return; }
    for (int i = 0; i < rec->a.n_extents; i++) {
        extent_t *e = &rec->a.extents[i];
        if (e->home == (node_id_t)g_n.self) { dir_range_destroy(e->vpn_start, e->n_pages); release_frames(e->n_pages); }
        pager_reset_range(e->vpn_start, e->n_pages);
    }
    if (rec->a.origin == (node_id_t)g_n.self) vpn_free(rec->a.vpn_start, rec->a.n_pages);
    free(rec->a.extents); free(rec);
}

/* ========================================================================= */
/* dsm_malloc / dsm_malloc_striped                                           */
/* ========================================================================= */
int online_others(node_id_t *ids) {
    node_id_t on[DSM_MAX_NODES];
    int n = membership_online_ids(on, DSM_MAX_NODES), k = 0;
    for (int i = 0; i < n; i++) if (on[i] != (node_id_t)g_n.self) ids[k++] = on[i];
    return k;
}

static int send_alloc_request(node_id_t n, uint32_t alloc_id, uint32_t pages) {
    uint8_t b[8], *p = b;
    w32(&p, pages); w32(&p, alloc_id);
    struct iovec iov = { b, 8 };
    return net_sendv(n, MSG_ALLOC_REQUEST, 0, net_new_req_id(), &iov, 1);
}

/* pide 'want[n]' paginas a cada nodo; devuelve por 'got[n]' lo concedido */
static void request_round(uint32_t alloc_id, const uint32_t *want, uint32_t *got) {
    uint16_t mask = 0;
    rpc_t *r = NULL;
    pthread_mutex_lock(&rpc_mu);                              /* se registra antes de enviar */
    pthread_mutex_unlock(&rpc_mu);
    for (int n = 0; n < DSM_MAX_NODES; n++) if (want[n]) mask |= BIT(n);
    r = rpc_begin(alloc_id, 1, mask);
    for (int n = 0; n < DSM_MAX_NODES; n++)
        if (want[n] && send_alloc_request((node_id_t)n, alloc_id, want[n]) < 0) {
            pthread_mutex_lock(&rpc_mu); r->waiting &= (uint16_t)~BIT(n); pthread_mutex_unlock(&rpc_mu);
        }
    rpc_wait_end(r);
    for (int n = 0; n < DSM_MAX_NODES; n++) got[n] = r->granted[n];
    free(r);
}

static void abort_alloc(uint32_t alloc_id, uint64_t local, const uint32_t *got, vpn_t v0, uint32_t np) {
    if (local) release_frames(local);
    for (int n = 0; n < DSM_MAX_NODES; n++) {
        if (!got[n]) continue;
        uint8_t b[4], *p = b; w32(&p, alloc_id);
        struct iovec iov = { b, 4 };
        net_sendv((node_id_t)n, MSG_FREE, 0, net_new_req_id(), &iov, 1);   /* libera su reserva */
    }
    vpn_free(v0, np);
}

static _Atomic uint32_t alloc_ctr;

static void *do_alloc(dsm_ctx_t *ctx, size_t size, size_t stripe, int striped) {
    if (!g_n.ready) return fail_ptr(DSM_ERR_NOT_INIT);
    uint32_t ps = g_n.page_size;
    if (!size || (striped && (!stripe || stripe % ps))) return fail_ptr(DSM_ERR_INVAL);
    uint64_t np64 = ((uint64_t)size + ps - 1) / ps;
    if (np64 > g_n.slab_pages) return fail_ptr(DSM_ERR_INVAL);       /* limite de una asignacion (sec. 3.1) */
    uint32_t np = (uint32_t)np64;
    node_id_t self = (node_id_t)g_n.self;

    node_id_t on[DSM_MAX_NODES];
    int non = membership_online_ids(on, DSM_MAX_NODES);
    if (non <= 0) { on[0] = self; non = 1; }

    vpn_t v0;
    if (!vpn_alloc(np, &v0)) return fail_ptr(DSM_ERR_NOMEM);
    uint32_t alloc_id = ((uint32_t)self << 24) | (++alloc_ctr & 0xFFFFFFu);

    uint32_t need[DSM_MAX_NODES] = {0}, got[DSM_MAX_NODES] = {0};
    uint64_t local = 0;
    size_t maxext = DSM_MAX_NODES + 1;
    uint32_t sp = striped ? (uint32_t)(stripe / ps) : 0;
    if (striped) { uint64_t nb = (np + sp - 1) / sp; if (nb > 60000) { vpn_free(v0, np); return fail_ptr(DSM_ERR_INVAL); } maxext = nb; }
    extent_t *ext = calloc(maxext, sizeof *ext);
    int nx = 0;

    if (striped) {
        /* un bloque por nodo ONLINE en orden de node_id; si hay mas bloques que nodos, se reparte en ronda */
        vpn_t cur = v0;
        for (uint32_t k = 0, left = np; left; k++) {
            uint32_t pages = left < sp ? left : sp;
            node_id_t h = on[k % (uint32_t)non];
            if (nx && ext[nx - 1].home == h && ext[nx - 1].vpn_start + ext[nx - 1].n_pages == cur) ext[nx - 1].n_pages += pages;
            else ext[nx++] = (extent_t){ cur, pages, h, DSM_NODE_NONE };
            need[h] += pages; cur += pages; left -= pages;
        }
        if (reserve_upto(need[self]) != need[self]) { release_frames(0); goto nomem_striped; }
        local = need[self];
        uint32_t want[DSM_MAX_NODES] = {0};
        for (int n = 0; n < DSM_MAX_NODES; n++) if (n != self) want[n] = need[n];
        request_round(alloc_id, want, got);
        for (int n = 0; n < DSM_MAX_NODES; n++) if (n != self && got[n] != need[n]) goto nomem_striped;
        goto announce;
    nomem_striped:
        if (local) { /* ya reservado */ }
        abort_alloc(alloc_id, local, got, v0, np);
        free(ext);
        return fail_ptr(DSM_ERR_NOMEM);
    } else {
        local = reserve_upto(np);
        uint32_t rem = np - (uint32_t)local;
        if (rem) {
            node_id_t oth[DSM_MAX_NODES];
            int no = online_others(oth);
            uint64_t freep[DSM_MAX_NODES] = {0}, total = 0;
            for (int i = 0; i < no; i++) {
                node_info_t ni;
                if (membership_get(oth[i], &ni) == 0 && ni.mem_bytes > ni.mem_used) { freep[oth[i]] = (ni.mem_bytes - ni.mem_used) / ps; total += freep[oth[i]]; }
            }
            if (total < rem) { abort_alloc(alloc_id, local, got, v0, np); free(ext); return fail_ptr(DSM_ERR_NOMEM); }
            uint32_t want[DSM_MAX_NODES] = {0}, assigned = 0;
            for (int i = 0; i < no; i++) { want[oth[i]] = (uint32_t)((uint64_t)rem * freep[oth[i]] / total); assigned += want[oth[i]]; }
            for (uint32_t left = rem - assigned; left;)           /* el resto, de uno en uno */
                for (int i = 0; i < no && left; i++) if (want[oth[i]] < freep[oth[i]]) { want[oth[i]]++; left--; }
            request_round(alloc_id, want, got);
            uint32_t sum = 0; for (int n = 0; n < DSM_MAX_NODES; n++) sum += got[n];
            for (int i = 0; i < no && sum < rem; i++) {            /* 2a pasada: lo que falto */
                node_id_t n = oth[i];
                if (freep[n] <= got[n]) continue;
                uint32_t ask = rem - sum; if (ask > freep[n] - got[n]) ask = (uint32_t)(freep[n] - got[n]);
                uint32_t w2[DSM_MAX_NODES] = {0}, g2[DSM_MAX_NODES] = {0};
                w2[n] = ask; request_round(alloc_id, w2, g2);
                got[n] += g2[n]; sum += g2[n];
            }
            if (sum < rem) { abort_alloc(alloc_id, local, got, v0, np); free(ext); return fail_ptr(DSM_ERR_NOMEM); }
        }
        vpn_t cur = v0;
        if (local) { ext[nx++] = (extent_t){ cur, (uint32_t)local, self, DSM_NODE_NONE }; cur += (uint32_t)local; }
        for (int n = 0; n < DSM_MAX_NODES; n++)
            if (n != self && got[n]) { ext[nx++] = (extent_t){ cur, got[n], (node_id_t)n, DSM_NODE_NONE }; cur += got[n]; }
    }

announce:;
    for (int k = 0; k < nx; k++) {
        if (g_n.cfg.replication_factor >= 2 && ext[k].backup == DSM_NODE_NONE)
            ext[k].backup = select_backup_node(ext[k].home);
    }
    arec_t *rec = calloc(1, sizeof *rec);
    rec->a = (alloc_t){ alloc_id, self, ctx ? ctx->owner_pid : 0, v0, np, (uint16_t)nx, ext };
    uint8_t *msg; size_t ml = build_announce(rec, &msg);
    uint32_t aid = alloc_id;
    apply_announce(rec);                                     /* rec pertenece a la tabla desde aqui */

    node_id_t oth[DSM_MAX_NODES];
    int no = online_others(oth);
    uint16_t mask = 0;
    for (int i = 0; i < no; i++) mask |= BIT(oth[i]);
    rpc_t *r = rpc_begin(aid, 2, mask);
    for (int i = 0; i < no; i++) {
        struct iovec iov = { msg, ml };
        if (net_sendv(oth[i], MSG_ALLOC_ANNOUNCE, 0, net_new_req_id(), &iov, 1) < 0) {
            pthread_mutex_lock(&rpc_mu); r->waiting &= (uint16_t)~BIT(oth[i]); pthread_mutex_unlock(&rpc_mu);
        }
    }
    int rc = rpc_wait_end(r);
    free(r); free(msg);
    if (rc < 0) {                                            /* alguien no confirmo: se deshace */
        uint8_t b[4], *p = b; w32(&p, aid);
        struct iovec iov = { b, 4 };
        for (int i = 0; i < no; i++) net_sendv(oth[i], MSG_FREE, 0, net_new_req_id(), &iov, 1);
        apply_free(aid);
        return fail_ptr(DSM_ERR_NET);
    }
    return VPN_APP(v0);
}

void *dsm_malloc(dsm_ctx_t *ctx, size_t size) { return do_alloc(ctx, size, 0, 0); }
void *dsm_malloc_striped(dsm_ctx_t *ctx, size_t size, size_t stripe) { return do_alloc(ctx, size, stripe, 1); }

static arec_t *rec_by_ptr(void *ptr) {                       /* lock tomado por el llamador */
    if ((char *)ptr < g_n.app || (char *)ptr >= g_n.app + g_n.win_size) return NULL;
    if ((size_t)((char *)ptr - g_n.app) % g_n.page_size) return NULL;
    vpn_t v = (vpn_t)((size_t)((char *)ptr - g_n.app) >> g_n.page_shift);
    for (size_t i = 0; i < n_allocs; i++) if (allocs[i]->a.vpn_start == v) return allocs[i];
    return NULL;
}

int dsm_free(dsm_ctx_t *ctx, void *ptr) {
    (void)ctx;
    if (!g_n.ready) return DSM_ERR_NOT_INIT;
    pthread_rwlock_rdlock(&alloc_rw);
    arec_t *rec = rec_by_ptr(ptr);
    uint32_t id = rec ? rec->a.alloc_id : 0;
    pthread_rwlock_unlock(&alloc_rw);
    if (!rec) return DSM_ERR_INVAL;
    node_id_t oth[DSM_MAX_NODES];
    int no = online_others(oth);
    uint8_t b[4], *p = b; w32(&p, id);
    struct iovec iov = { b, 4 };
    for (int i = 0; i < no; i++) net_sendv(oth[i], MSG_FREE, 0, net_new_req_id(), &iov, 1);
    apply_free(id);
    return DSM_OK;
}

int dsm_publish(dsm_ctx_t *ctx, const char *name, void *ptr) {
    (void)ctx;
    if (!g_n.ready) return DSM_ERR_NOT_INIT;
    if (!name || strlen(name) >= DSM_NAME_MAX || !*name) return DSM_ERR_INVAL;
    pthread_rwlock_wrlock(&alloc_rw);
    arec_t *rec = rec_by_ptr(ptr);
    if (!rec) { pthread_rwlock_unlock(&alloc_rw); return DSM_ERR_INVAL; }
    for (size_t i = 0; i < n_allocs; i++)
        if (!strcmp(allocs[i]->name, name)) { pthread_rwlock_unlock(&alloc_rw); return DSM_ERR_EXISTS; }
    strcpy(rec->name, name);
    uint8_t *msg; size_t ml = build_announce(rec, &msg);
    uint32_t aid = rec->a.alloc_id;
    pthread_rwlock_unlock(&alloc_rw);

    node_id_t oth[DSM_MAX_NODES];
    int no = online_others(oth);
    uint16_t mask = 0;
    for (int i = 0; i < no; i++) mask |= BIT(oth[i]);
    rpc_t *r = rpc_begin(aid, 2, mask);
    for (int i = 0; i < no; i++) {
        struct iovec iov = { msg, ml };
        if (net_sendv(oth[i], MSG_ALLOC_ANNOUNCE, 0, net_new_req_id(), &iov, 1) < 0) {
            pthread_mutex_lock(&rpc_mu); r->waiting &= (uint16_t)~BIT(oth[i]); pthread_mutex_unlock(&rpc_mu);
        }
    }
    int rc = rpc_wait_end(r);
    free(r); free(msg);
    return rc < 0 ? DSM_ERR_NET : DSM_OK;
}

void *dsm_lookup(dsm_ctx_t *ctx, const char *name) {
    (void)ctx;
    if (!g_n.ready) return fail_ptr(DSM_ERR_NOT_INIT);
    void *res = NULL;
    pthread_rwlock_rdlock(&alloc_rw);
    for (size_t i = 0; name && i < n_allocs; i++)
        if (allocs[i]->name[0] && !strcmp(allocs[i]->name, name)) { res = VPN_APP(allocs[i]->a.vpn_start); break; }
    pthread_rwlock_unlock(&alloc_rw);
    return res ? res : fail_ptr(DSM_ERR_NOTFOUND);
}

/* ========================================================================= */
/* Hilos, identidad, contexto                                                 */
/* ========================================================================= */
int dsm_thread_register(dsm_ctx_t *ctx) { (void)ctx; return g_n.ready ? pager_thread_register() : DSM_ERR_NOT_INIT; }
int dsm_thread_unregister(dsm_ctx_t *ctx) { (void)ctx; return pager_thread_unregister(); }
int dsm_node_id(dsm_ctx_t *ctx) { (void)ctx; return g_n.self; }
int dsm_num_nodes(dsm_ctx_t *ctx) { (void)ctx; node_id_t on[DSM_MAX_NODES]; return membership_online_ids(on, DSM_MAX_NODES); }
size_t dsm_page_size(dsm_ctx_t *ctx) { (void)ctx; return g_n.page_size ? (size_t)g_n.page_size : 4096u; }

dsm_ctx_t *dsm_ctx_for_app(dsm_ctx_t *node_ctx_, uint32_t owner_pid) {
    (void)node_ctx_;
    dsm_ctx_t *c = calloc(1, sizeof *c);
    if (c) c->owner_pid = owner_pid;
    return c;
}

/* ========================================================================= */
/* Arranque y parada                                                         */
/* ========================================================================= */
static void *map_sparse(size_t bytes) {
    void *p = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    return p == MAP_FAILED ? NULL : p;
}

static int mem_init(void) {
    dsm_config_t *c = &g_n.cfg;
    long sp = sysconf(_SC_PAGESIZE);
    if (!c->page_size) c->page_size = 4096; /* valor predeterminado compatible */
    if ((c->page_size & (c->page_size - 1)) || c->page_size % (uint32_t)sp || c->page_size < (uint32_t)sp)
        return DSM_ERR_CONFIG;
    g_n.page_size = c->page_size;
    g_n.page_shift = (uint32_t)__builtin_ctz(c->page_size);
    g_n.base = c->base_addr;
    g_n.win_size = (size_t)c->max_nodes * c->va_slab_bytes;
    g_n.slab_pages = c->va_slab_bytes / c->page_size;
    g_n.n_vpages = g_n.win_size / c->page_size;
    g_n.node_mem_pages = c->node_mem_bytes / c->page_size;
    if (g_n.n_vpages > 0xFFFFFFF0ull || c->max_nodes > DSM_MAX_NODES) return DSM_ERR_CONFIG;

    g_n.memfd = memfd_create("dsm", MFD_CLOEXEC);
    if (g_n.memfd < 0 || ftruncate(g_n.memfd, (off_t)g_n.win_size) < 0) return DSM_ERR_NOMEM;
    g_n.app = mmap((void *)g_n.base, g_n.win_size, PROT_NONE, MAP_SHARED | MAP_FIXED_NOREPLACE, g_n.memfd, 0);
    if (g_n.app != (char *)g_n.base) {
        LOG_ERR("no se pudo mapear la ventana DSM en 0x%lx (cambie base_addr en dsm.conf)\n", (unsigned long)g_n.base);
        return DSM_ERR_CONFIG;
    }
    g_n.sys = mmap(NULL, g_n.win_size, PROT_READ | PROT_WRITE, MAP_SHARED, g_n.memfd, 0);
    if (g_n.sys == MAP_FAILED) return DSM_ERR_NOMEM;
    g_n.lp = map_sparse(g_n.n_vpages * sizeof(local_page_t));
    g_n.dir = map_sparse(g_n.n_vpages * sizeof(dir_entry_t));
    g_n.dir_sem = map_sparse(g_n.n_vpages * sizeof(sem_t));
    g_n.transfers = map_sparse(g_n.n_vpages * sizeof(uint32_t));
    if (!g_n.lp || !g_n.dir || !g_n.dir_sem || !g_n.transfers) return DSM_ERR_NOMEM;

    FILE *f = fopen("/proc/sys/vm/max_map_count", "r");           /* sec. 3.4 punto 2 */
    long maxmap = 0;
    if (f) { if (fscanf(f, " %ld", &maxmap) != 1) maxmap = 0; fclose(f); }
    if (maxmap && 2L * c->cache_max_pages + 1024 > maxmap)
        LOG_ERR("AVISO: cache_max_pages=%u puede no caber en vm.max_map_count=%ld\n", c->cache_max_pages, maxmap);
    return DSM_OK;
}

int dsm_init_with(const dsm_config_t *cfg, int self_id, dsm_ctx_t **ctx) {
    memset(&g_n, 0, sizeof g_n);
    g_n.cfg = *cfg;
    g_n.self = self_id;
    int rc = mem_init();
    if (rc) return rc;
    if ((rc = dir_init())) return rc;
    if ((rc = pager_init())) return rc;
    vpn_init();

    /* Inicializa la red y el membership */
    net_init(&g_n.cfg, (node_id_t)self_id);
    if ((rc = membership_start(&g_n.cfg, self_id))) return rc;
    sync_start(&g_n.cfg, (node_id_t)self_id);
    puente_start();
    if (net_start() < 0) return DSM_ERR_NET; /* inicia el hilo de recepcion de mensajes */

    g_n.ready = 1;
    stats_init();
    node_ctx.owner_pid = (uint32_t)getpid();
    if (ctx) *ctx = &node_ctx;
    return DSM_OK;
}

int dsm_init(const char *conf_path, int id, dsm_ctx_t **ctx) {
    dsm_config_t cfg; int self;
    int rc = config_load(conf_path, id, &cfg, &self);
    if (rc) return rc;
    return dsm_init_with(&cfg, self, ctx);
}

int dsm_shutdown(dsm_ctx_t *ctx) {
    (void)ctx;
    if (!g_n.ready) return DSM_ERR_NOT_INIT;
    g_n.ready = 0;
    pager_writeback_all();           /* LEAVE ordenado: devolver las M ajenas (sec. 7) */
    membership_stop();
    net_stop();
    pager_stop();
    if (g_n.app) { munmap(g_n.app, g_n.win_size); g_n.app = NULL; }
    if (g_n.sys) { munmap(g_n.sys, g_n.win_size); g_n.sys = NULL; }
    if (g_n.lp)  { munmap(g_n.lp, g_n.n_vpages * sizeof(local_page_t)); g_n.lp = NULL; }
    if (g_n.dir) { munmap(g_n.dir, g_n.n_vpages * sizeof(dir_entry_t)); g_n.dir = NULL; }
    if (g_n.dir_sem) { munmap(g_n.dir_sem, g_n.n_vpages * sizeof(sem_t)); g_n.dir_sem = NULL; }
    if (g_n.transfers) { munmap(g_n.transfers, g_n.n_vpages * sizeof(uint32_t)); g_n.transfers = NULL; }
    if (g_n.memfd >= 0) { close(g_n.memfd); g_n.memfd = -1; }
    return DSM_OK;
}

_Atomic int g_task_failed = 0;
_Atomic int g_task_failed_err = 0;

void dsm_task_notify_failure(int err, node_id_t err_node, vpn_t vpn) {
    (void)err_node;
    (void)vpn;
    atomic_store(&g_task_failed, 1);
    atomic_store(&g_task_failed_err, err);
}

size_t alloc_extent_count(void) {
    pthread_rwlock_rdlock(&alloc_rw);
    size_t cnt = n_xs;
    pthread_rwlock_unlock(&alloc_rw);
    return cnt;
}

int alloc_get_extent(size_t index, vpn_t *out_start, uint32_t *out_n, node_id_t *out_home, node_id_t *out_backup) {
    pthread_rwlock_rdlock(&alloc_rw);
    if (index >= n_xs) {
        pthread_rwlock_unlock(&alloc_rw);
        return -1;
    }
    if (out_start) *out_start = xs[index].start;
    if (out_n) *out_n = xs[index].n;
    if (out_home) *out_home = xs[index].home;
    if (out_backup) *out_backup = xs[index].backup;
    pthread_rwlock_unlock(&alloc_rw);
    return 0;
}

int alloc_update_extent_nodes(vpn_t vstart, uint32_t n_pages, node_id_t new_home, node_id_t new_backup) {
    pthread_rwlock_wrlock(&alloc_rw);
    for (size_t i = 0; i < n_xs; i++) {
        if (xs[i].start >= vstart && xs[i].start < vstart + n_pages) {
            xs[i].home = new_home;
            xs[i].backup = new_backup;
            if (xs[i].rec) {
                for (int k = 0; k < xs[i].rec->a.n_extents; k++) {
                    if (xs[i].rec->a.extents[k].vpn_start == xs[i].start) {
                        xs[i].rec->a.extents[k].home = new_home;
                        xs[i].rec->a.extents[k].backup = new_backup;
                    }
                }
            }
        }
    }
    pthread_rwlock_unlock(&alloc_rw);
    return 0;
}

void dsm_on_node_down(node_id_t id) {
    if (!g_n.ready) return;
    dir_on_node_down(id);
    pager_on_node_down(id);
    replica_on_node_down(id);
}

/* ========================================================================= */
/* Consultas del sistema (sec. 10.6) y dsm_checkpoint (sec. 10.3 / 14.4)     */
/* ========================================================================= */
int dsm_checkpoint(dsm_ctx_t *ctx) {
    return replica_checkpoint(ctx);
}

int dsm_dump_allocs(dsm_ctx_t *ctx, FILE *out) {
    (void)ctx;
    if (!out) out = stdout;
    pthread_rwlock_rdlock(&alloc_rw);
    fprintf(out, "%-12s %-10s %-11s %-8s %s\n",
            "ALLOC", "PROCESO", "VPN INICIO", "PAGINAS", "TRAMOS (home:paginas)");
    for (size_t i = 0; i < n_allocs; i++) {
        arec_t *r = allocs[i];
        fprintf(out, "0x%08x   %u:%-6u %-11u %-8u ",
                r->a.alloc_id, r->a.origin, r->a.owner_pid,
                r->a.vpn_start, r->a.n_pages);
        for (int k = 0; k < r->a.n_extents; k++) {
            extent_t *e = &r->a.extents[k];
            if (e->backup != DSM_NODE_NONE) {
                fprintf(out, "%s%u:%u[bk:%u]", k ? ", " : "", e->home, e->n_pages, e->backup);
            } else {
                fprintf(out, "%s%u:%u", k ? ", " : "", e->home, e->n_pages);
            }
        }
        if (r->name[0]) fprintf(out, " \"%s\"", r->name);
        fprintf(out, "\n");
    }
    pthread_rwlock_unlock(&alloc_rw);
    return DSM_OK;
}

int dsm_dump_pages(dsm_ctx_t *ctx, FILE *out, int node) {
    (void)ctx;
    if (!out) out = stdout;
    fprintf(out, "%-5s %-5s %-6s %-12s %-10s %-7s %-7s %-8s %-12s %-8s\n",
            "VPN", "HOME", "ESTADO", "PROPIETARIO", "COPIAS", "VERSION", "TRANSF", "PROCESO", "ALLOC", "ORDEN");

    vpn_t limit = (vpn_t)g_n.n_vpages;
    for (vpn_t v = 0; v < limit; v++) {
        node_id_t h = alloc_home_of(v);
        if (h == DSM_NODE_NONE) continue;
        if (node != -1 && h != (node_id_t)node) continue;

        char st_c = 'I';
        char owner_str[16] = "-";
        char sharers_str[32] = "{}";
        uint32_t ver = 0;
        uint32_t transf = 0;

        if (h == (node_id_t)g_n.self) {
            dir_entry_t *d = &g_n.dir[v];
            if (d->state == PG_SHARED) st_c = 'S';
            else if (d->state == PG_MODIFIED) {
                st_c = 'M';
                if (d->owner != DSM_NODE_NONE) snprintf(owner_str, sizeof owner_str, "%u", d->owner);
            }
            ver = d->version;
            transf = g_n.transfers ? g_n.transfers[v] : 0;

            int pos = 0;
            sharers_str[pos++] = '{';
            int first = 1;
            for (int k = 0; k < DSM_MAX_NODES; k++) {
                if (d->sharers & (1 << k)) {
                    pos += snprintf(sharers_str + pos, sizeof(sharers_str) - pos, "%s%d", first ? "" : ",", k);
                    first = 0;
                }
            }
            if (pos < (int)sizeof(sharers_str) - 1) {
                sharers_str[pos++] = '}';
                sharers_str[pos] = '\0';
            }
        } else {
            local_page_t *lp = &g_n.lp[v];
            if (lp->state == PG_SHARED) st_c = 'S';
            else if (lp->state == PG_MODIFIED) { st_c = 'M'; snprintf(owner_str, sizeof owner_str, "%d", g_n.self); }
            ver = lp->version;
        }

        uint32_t alloc_id = 0, owner_pid = 0, page_order = 0, n_pages = 0;
        node_id_t origin = 0;
        dsm_alloc_find_vpn(v, &alloc_id, &origin, &owner_pid, &page_order, &n_pages);

        char proc_str[16];
        snprintf(proc_str, sizeof proc_str, "%u:%u", origin, owner_pid);
        char alloc_str[16];
        snprintf(alloc_str, sizeof alloc_str, "0x%08x", alloc_id);
        char order_str[16];
        snprintf(order_str, sizeof order_str, "%u/%u", page_order, n_pages);

        fprintf(out, "%-5u %-5u %-6c %-12s %-10s %-7u %-7u %-8s %-12s %-8s\n",
                v, h, st_c, owner_str, sharers_str, ver, transf, proc_str, alloc_str, order_str);
    }
    return DSM_OK;
}

int dsm_get_nodes(dsm_ctx_t *ctx, dsm_node_status_t *out, int max) {
    (void)ctx;
    if (!out || max <= 0) return 0;
    int count = 0;
    int n_peers = (int)g_n.cfg.max_nodes;
    if (n_peers > max) n_peers = max;
    for (int i = 0; i < n_peers; i++) {
        out[count].id = i;
        if (g_n.cfg.peers[i].host[0])
            strncpy(out[count].host, g_n.cfg.peers[i].host, sizeof(out[count].host) - 1);
        else
            snprintf(out[count].host, sizeof(out[count].host), "127.0.0.1");
        out[count].port_base = g_n.cfg.peers[i].port_base;
        out[count].status = (uint8_t)membership_status((node_id_t)i);
        out[count].mem_bytes = g_n.cfg.node_mem_bytes;
        out[count].mem_used = (i == g_n.self) ? atomic_load(&g_n.used_pages) * g_n.page_size : 0;
        out[count].mem_free = (out[count].mem_bytes > out[count].mem_used) ? (out[count].mem_bytes - out[count].mem_used) : 0;
        out[count].last_hb_age_ms = 0;
        count++;
    }
    return count;
}

int dsm_dump_nodes(dsm_ctx_t *ctx, FILE *out) {
    if (!out) out = stdout;
    dsm_node_status_t st[DSM_MAX_NODES];
    int n = dsm_get_nodes(ctx, st, DSM_MAX_NODES);
    fprintf(out, "%-3s %-16s %-10s %-10s %-10s %-10s\n",
            "ID", "HOST", "ESTADO", "APORTE", "USADO", "LIBRE");
    for (int i = 0; i < n; i++) {
        const char *s = (st[i].status == NODE_ONLINE) ? "ONLINE" :
                        (st[i].status == NODE_SUSPECT) ? "SUSPECT" : "OFFLINE";
        fprintf(out, "%-3d %-16s %-10s %llu MiB    %llu MiB    %llu MiB\n",
                st[i].id, st[i].host, s,
                (unsigned long long)(st[i].mem_bytes / (1024 * 1024)),
                (unsigned long long)(st[i].mem_used / (1024 * 1024)),
                (unsigned long long)(st[i].mem_free / (1024 * 1024)));
    }
    return DSM_OK;
}

/* ========================================================================= */
/* Despacho de mensajes (lo llama el pool de servicio de network.c)           */
/* ========================================================================= */
void dsm_msg_dispatch(const msg_hdr_t *h, const uint8_t *body) {
    if (!g_n.ready && h->type != MSG_ALLOC_REPLY) { /* aun arrancando: nada que atender */ }
    const uint8_t *p = body;
    switch (h->type) {
    case MSG_PAGE_REQUEST:   dir_on_page_request(h, body); break;
    case MSG_PAGE_DATA:
        if (h->length >= 9 && alloc_home_of(r32(&p)) == (node_id_t)g_n.self) dir_on_page_data(h, body);   /* respuesta a FETCH */
        else pager_on_page_data(h, body);
        break;
    case MSG_FETCH:          pager_on_fetch(h, body); break;
    case MSG_INVALIDATE:     pager_on_invalidate(h, body); break;
    case MSG_INVALIDATE_ACK: dir_on_invalidate_ack(h, body); break;
    case MSG_EVICT_NOTICE:   dir_on_evict_notice(h, body); break;
    case MSG_WRITEBACK:      dir_on_writeback(h, body); break;
    case MSG_WRITEBACK_ACK:  pager_on_writeback_ack(h, body); break;
    case MSG_ERROR:          pager_on_error(h, body); break;
    case MSG_ALLOC_REQUEST: {
        uint32_t n = r32(&p), id = r32(&p);
        uint64_t g = reserve_upto(n);
        if (g) resv_add(id, g);
        uint8_t b[8], *q = b; w32(&q, id); w32(&q, (uint32_t)g);
        struct iovec iov = { b, 8 };
        net_sendv(h->src, MSG_ALLOC_REPLY, MSG_F_RESP, h->req_id, &iov, 1);
        break;
    }
    case MSG_ALLOC_REPLY:    { uint32_t id = r32(&p), g = r32(&p); rpc_reply(id, 1, h->src, g); break; }
    case MSG_ALLOC_ANNOUNCE: {
        arec_t *rec = parse_announce(body, h->length);
        if (!rec) break;
        uint32_t id = rec->a.alloc_id;
        apply_announce(rec);
        uint8_t b[4], *q = b; w32(&q, id);
        struct iovec iov = { b, 4 };
        net_sendv(h->src, MSG_ALLOC_ANNOUNCE_ACK, MSG_F_RESP, h->req_id, &iov, 1);
        break;
    }
    case MSG_ALLOC_ANNOUNCE_ACK: { uint32_t id = r32(&p); rpc_reply(id, 2, h->src, 0); break; }
    case MSG_FREE:           apply_free(r32(&p)); break;

    /* Extra de tolerancia a fallos con replica (sec. 7.1 y 14.4) */
    case MSG_REPLICA_UPDATE:     replica_handle_update(h, body); break;
    case MSG_REPLICA_ACK:        replica_handle_ack(h, body); break;
    case MSG_HOME_CHANGE:        replica_handle_home_change(h, body); break;
    case MSG_HOME_CHANGE_ACK:    replica_handle_home_change_ack(h, body); break;
    case MSG_RECOVER_DONE:       replica_handle_recover_done(h, body); break;

    default: break;
    }
}
