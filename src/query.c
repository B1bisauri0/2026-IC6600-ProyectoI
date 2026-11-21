/* query.c - atiende a dsm_query y dsm_run: recibe una línea ("nodes",
 * "pages 1", "run ...") y responde texto. Una conexión por consulta, cada una
 * en su propio hilo.
 *
 * Consultas:
 *   nodes            tabla de nodos (local, sin red)
 *   allocs           asignaciones y orden de sus páginas (local, sin red)
 *   pages [N]        tabla de páginas del nodo N, o de todo el sistema. Cada
 *                    home manda su directorio con PAGETABLE_REQUEST/REPLY
 *   stats [N]        estadísticas del nodo N, o de cada nodo y el total
 *                    (STATS_REQUEST/REPLY)
 *   run ...          la usa dsm_run (task.c)
 */
#define _GNU_SOURCE
#include "query.h"
#include "membership.h"
#include "dsm_internal.h"
#include "directory.h"
#include "espera.h"
#include "task.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdlib.h>
#include <stddef.h>

#define MAX_QUERIES 16

static struct { const char *name; query_fn_t fn; } table[MAX_QUERIES];
static int n_queries;
static int listen_fd = -1;

void query_register(const char *name, query_fn_t fn) {
    if (n_queries < MAX_QUERIES) {
        table[n_queries].name = name;
        table[n_queries].fn = fn;
        n_queries++;
    }
}

static void fmt_mib(char *buf, size_t n, uint64_t bytes) {
    snprintf(buf, n, "%llu MiB", (unsigned long long)(bytes >> 20));
}

/* dsm_query nodes */
static void q_nodes(FILE *out, const char *args) {
    (void)args;
    dsm_node_status_t st[DSM_MAX_NODES];
    int n = membership_snapshot(st, DSM_MAX_NODES);
    fprintf(out, "%-3s %-15s %-6s %-8s %-9s %-9s %-9s %s\n",
            "ID", "HOST", "PUERTO", "ESTADO", "APORTE", "USADO", "LIBRE", "ULTIMO LATIDO");
    for (int i = 0; i < n; i++) {
        char a[16], u[16], l[16], hb[24];
        int known = st[i].mem_bytes != 0;
        fmt_mib(a, sizeof a, st[i].mem_bytes);
        fmt_mib(u, sizeof u, st[i].mem_used);
        fmt_mib(l, sizeof l, st[i].mem_free);
        if (st[i].last_hb_age_ms == UINT64_MAX) snprintf(hb, sizeof hb, "nunca");
        else snprintf(hb, sizeof hb, "%.1f s", st[i].last_hb_age_ms / 1000.0);
        fprintf(out, "%-3d %-15s %-6u %-8s %-9s %-9s %-9s %s\n",
                st[i].id, st[i].host, st[i].port_base, node_status_str(st[i].status),
                known ? a : "-", known ? u : "-", known ? l : "-", hb);
    }
}

/* ================= estadísticas (STATS_REQUEST / STATS_REPLY) ================= */

/* dsm_stats_t es un int y después puros uint64_t: viajan en orden de red */
#define N_CAMPOS ((sizeof(dsm_stats_t) - offsetof(dsm_stats_t, read_faults)) / sizeof(uint64_t))

static void stats_a_red(const dsm_stats_t *s, uint8_t *b) {
    uint8_t *p = b;
    const uint64_t *c = &s->read_faults;
    w32(&p, (uint32_t)s->node);
    for (size_t i = 0; i < N_CAMPOS; i++) w64(&p, c[i]);
}

static void stats_de_red(const uint8_t *b, dsm_stats_t *s) {
    const uint8_t *p = b;
    uint64_t *c = &s->read_faults;
    s->node = (int)r32(&p);
    for (size_t i = 0; i < N_CAMPOS; i++) c[i] = r64(&p);
}

static void on_stats_request(const msg_hdr_t *h, const void *body) {
    (void)body;
    dsm_stats_t s;
    memset(&s, 0, sizeof s);
    stats_snapshot(&s);
    s.node = g_n.self;
    uint8_t b[4 + N_CAMPOS * 8];
    stats_a_red(&s, b);
    net_send(h->src, MSG_STATS_REPLY, F_REPLY, h->req_id, b, sizeof b);
}

static int stats_de(int nodo, dsm_stats_t *out) {
    memset(out, 0, sizeof *out);
    if (nodo == g_n.self) { stats_snapshot(out); out->node = nodo; return DSM_OK; }
    if (membership_status((node_id_t)nodo) == NODE_OFFLINE) return DSM_ERR_NODE_DOWN;
    uint64_t req = espera_nueva();
    if (net_send((node_id_t)nodo, MSG_STATS_REQUEST, 0, req, NULL, 0) < 0) { espera_cancelar(req); return DSM_ERR_NODE_DOWN; }
    void *r; uint32_t len;
    int rc = espera_esperar(req, (node_id_t)nodo, (int)g_n.cfg.request_timeout_ms, &r, &len);
    if (rc) return rc;
    if (len >= 4 + N_CAMPOS * 8) stats_de_red(r, out); else rc = DSM_ERR_NET;
    free(r);
    return rc;
}

/* node = -1: suma de todos los nodos ONLINE (sec. 10.7) */
int dsm_get_stats(dsm_ctx_t *ctx, dsm_stats_t *out, int node) {
    (void)ctx;
    if (!out) return DSM_ERR_INVAL;
    if (node >= 0) return node < g_n.cfg.n_peers ? stats_de(node, out) : DSM_ERR_INVAL;

    memset(out, 0, sizeof *out);
    node_id_t on[DSM_MAX_NODES];
    int n = membership_online_ids(on, DSM_MAX_NODES);
    for (int i = 0; i < n; i++) {
        dsm_stats_t s;
        if (stats_de(on[i], &s) != DSM_OK) continue;
        uint64_t *a = &out->read_faults, *b = &s.read_faults;
        for (size_t k = 0; k < N_CAMPOS; k++) a[k] += b[k];
    }
    out->node = -1;
    return DSM_OK;
}

static void fila_stats(FILE *out, const char *quien, const dsm_stats_t *s) {
    double lat = s->fault_count ? (double)s->fault_time_us_total / (double)s->fault_count : 0;
    fprintf(out, "%-6s %8llu %8llu %8llu %7llu %7llu %8llu %8llu %10llu %10llu %9.1f\n", quien,
            (unsigned long long)s->read_faults, (unsigned long long)s->write_faults,
            (unsigned long long)s->upgrade_faults, (unsigned long long)s->pages_home,
            (unsigned long long)s->pages_cached, (unsigned long long)s->invalidations_sent,
            (unsigned long long)s->evictions, (unsigned long long)s->msgs_sent,
            (unsigned long long)s->bytes_sent, lat);
}

/* dsm_query stats [N] */
static void q_stats(FILE *out, const char *args) {
    fprintf(out, "%-6s %8s %8s %8s %7s %7s %8s %8s %10s %10s %9s\n", "NODO", "F_LECT", "F_ESCR",
            "F_UPGR", "HOME", "CACHE", "INVALID", "DESALOJ", "MSGS_ENV", "BYTES_ENV", "LAT_us");
    if (*args) {
        int n = atoi(args);
        dsm_stats_t s;
        int rc = (n >= 0 && n < g_n.cfg.n_peers) ? stats_de(n, &s) : DSM_ERR_INVAL;
        if (rc) { fprintf(out, "nodo %d: %s\n", n, dsm_strerror((dsm_status_t)rc)); return; }
        char q[16]; snprintf(q, sizeof q, "%d", n);
        fila_stats(out, q, &s);
        return;
    }
    dsm_stats_t total;
    memset(&total, 0, sizeof total);
    for (int i = 0; i < g_n.cfg.n_peers; i++) {
        dsm_stats_t s;
        char q[16]; snprintf(q, sizeof q, "%d", i);
        if (stats_de(i, &s) != DSM_OK) { fprintf(out, "%-6s (OFFLINE)\n", q); continue; }
        fila_stats(out, q, &s);
        uint64_t *a = &total.read_faults, *b = &s.read_faults;
        for (size_t k = 0; k < N_CAMPOS; k++) a[k] += b[k];
    }
    fila_stats(out, "TOTAL", &total);
}

/* ================= tabla de páginas (PAGETABLE_REQUEST / REPLY) ================= */

#define PT_MAX   2048          /* entradas por respuesta */
#define PT_BYTES 18            /* pt_entry_t en la red */

/* cuerpo de la pregunta: vpn desde donde seguir (4).
 * respuesta: cuantas (4) | hay_mas (1) | entradas */
static void on_pagetable_request(const msg_hdr_t *h, const void *body) {
    const uint8_t *p = body;
    vpn_t desde = h->length >= 4 ? r32(&p) : 0;
    pt_entry_t *e = malloc(PT_MAX * sizeof *e);
    int more = 0;
    uint32_t n = e ? dir_snapshot(desde, PT_MAX, e, &more) : 0;

    uint8_t *b = malloc(5 + (size_t)n * PT_BYTES), *q = b;
    w32(&q, n);
    w8(&q, (uint8_t)more);
    for (uint32_t i = 0; i < n; i++) {
        w32(&q, e[i].vpn); w8(&q, e[i].state); w8(&q, e[i].flags); w16(&q, e[i].owner);
        w16(&q, e[i].sharers); w32(&q, e[i].version); w32(&q, e[i].transfers);
    }
    net_send(h->src, MSG_PAGETABLE_REPLY, F_REPLY, h->req_id, b, (uint32_t)(q - b));
    free(b);
    free(e);
}

static void fila_pagina(FILE *out, node_id_t home, const pt_entry_t *e) {
    char est = e->state == PG_MODIFIED ? 'M' : e->state == PG_SHARED ? 'S' : 'I';
    char dueno[8] = "-", copias[48] = "{";
    if (e->owner != DSM_NODE_NONE && e->state == PG_MODIFIED) snprintf(dueno, sizeof dueno, "%u", e->owner);
    int pos = 1, primero = 1;
    for (int k = 0; k < DSM_MAX_NODES; k++)
        if (e->sharers & (1u << k)) { pos += snprintf(copias + pos, sizeof copias - pos, "%s%d", primero ? "" : ",", k); primero = 0; }
    snprintf(copias + pos, sizeof copias - pos, "}");

    uint32_t aid = 0, pid = 0, orden = 0, np = 0;
    node_id_t origen = 0;
    char proc[24] = "-", alloc[16] = "-", ord[24] = "-";
    if (dsm_alloc_find_vpn(e->vpn, &aid, &origen, &pid, &orden, &np) == 0) {
        snprintf(proc, sizeof proc, "%u:%u", origen, pid);
        snprintf(alloc, sizeof alloc, "0x%08x", aid);
        snprintf(ord, sizeof ord, "%u/%u", orden, np);
    }
    fprintf(out, "%-7u %-5u %c%-5s %-11s %-10s %-7u %-7u %-10s %-11s %s\n", e->vpn, home, est,
            (e->flags & 1) ? "*" : "", dueno, copias, e->version, e->transfers, proc, alloc, ord);
}

/* pide la tabla de un home (o la lee directo si es este nodo) y la imprime */
static int paginas_de(FILE *out, node_id_t nodo, uint64_t *total) {
    vpn_t desde = 0;
    for (;;) {
        pt_entry_t *e = malloc(PT_MAX * sizeof *e);
        uint32_t n = 0;
        int more = 0;
        if (nodo == (node_id_t)g_n.self) {
            n = dir_snapshot(desde, PT_MAX, e, &more);
        } else {
            uint8_t b[4], *q = b;
            w32(&q, desde);
            uint64_t req = espera_nueva();
            if (net_send(nodo, MSG_PAGETABLE_REQUEST, 0, req, b, 4) < 0) { espera_cancelar(req); free(e); return DSM_ERR_NODE_DOWN; }
            void *r; uint32_t len;
            int rc = espera_esperar(req, nodo, (int)g_n.cfg.request_timeout_ms, &r, &len);
            if (rc) { free(e); return rc; }
            const uint8_t *p = r;
            if (len >= 5) {
                n = r32(&p);
                more = r8(&p);
                if (n > PT_MAX || 5 + (size_t)n * PT_BYTES > len) n = 0;
                for (uint32_t i = 0; i < n; i++) {
                    e[i].vpn = r32(&p); e[i].state = r8(&p); e[i].flags = r8(&p); e[i].owner = r16(&p);
                    e[i].sharers = r16(&p); e[i].version = r32(&p); e[i].transfers = r32(&p);
                }
            }
            free(r);
        }
        for (uint32_t i = 0; i < n; i++) fila_pagina(out, nodo, &e[i]);
        *total += n;
        vpn_t ultimo = n ? e[n - 1].vpn : 0;
        free(e);
        if (!more || !n) return DSM_OK;
        desde = ultimo + 1;
    }
}

/* dsm_query pages [N] */
static void q_pages(FILE *out, const char *args) {
    fprintf(out, "%-7s %-5s %-6s %-11s %-10s %-7s %-7s %-10s %-11s %s\n", "VPN", "HOME", "ESTADO",
            "PROPIETARIO", "COPIAS", "VERSION", "TRANSF", "PROCESO", "ALLOC", "ORDEN");
    int solo = *args ? atoi(args) : -1;
    uint64_t total = 0;
    for (int i = 0; i < g_n.cfg.n_peers; i++) {
        if (solo >= 0 && i != solo) continue;
        if (i != g_n.self && membership_status((node_id_t)i) == NODE_OFFLINE) {
            fprintf(out, "(nodo %d OFFLINE: no se pueden leer sus páginas)\n", i);
            continue;
        }
        int rc = paginas_de(out, (node_id_t)i, &total);
        if (rc) fprintf(out, "(nodo %d: %s)\n", i, dsm_strerror((dsm_status_t)rc));
    }
    fprintf(out, "%llu página(s). * = transacción en curso (BUSY)\n", (unsigned long long)total);
}

/* dsm_query allocs */
static void q_allocs(FILE *out, const char *args) {
    (void)args;
    dsm_dump_allocs(NULL, out);
}

/* handlers de red de las consultas: se registran antes de net_start (puente.c) */
void query_registrar_red(void) {
    net_register_handler(MSG_STATS_REQUEST, on_stats_request);
    net_register_handler(MSG_STATS_REPLY, espera_respuesta);
    net_register_handler(MSG_PAGETABLE_REQUEST, on_pagetable_request);
    net_register_handler(MSG_PAGETABLE_REPLY, espera_respuesta);
}

static void q_help(FILE *out, const char *args) {
    (void)args;
    fprintf(out, "Consultas disponibles:");
    for (int i = 0; i < n_queries; i++) fprintf(out, " %s", table[i].name);
    fprintf(out, "\n");
}

static void *client_main(void *arg) {
    int fd = (int)(intptr_t)arg;
    FILE *in = fdopen(dup(fd), "r"), *out = fdopen(fd, "w");
    char line[2048];
    if (in && out && fgets(line, sizeof line, in)) {
        line[strcspn(line, "\r\n")] = '\0';
        char *args = strchr(line, ' ');
        if (args) *args++ = '\0'; else args = "";
        int found = 0;
        for (int i = 0; i < n_queries; i++)
            if (!strcmp(line, table[i].name)) { table[i].fn(out, args); found = 1; break; }
        if (!found) {
            fprintf(out, "Consulta desconocida o todavía no implementada: '%s'\n", line);
            q_help(out, "");
        }
    }
    if (in) fclose(in);
    if (out) fclose(out); else close(fd);
    return NULL;
}

static void *listener_main(void *arg) {
    (void)arg;
    for (;;) {
        int fd = accept(listen_fd, NULL, NULL);
        if (fd < 0) continue;
        pthread_t t;
        pthread_create(&t, NULL, client_main, (void *)(intptr_t)fd);
        pthread_detach(t);
    }
    return NULL;
}

int query_start(const dsm_config_t *cfg, node_id_t self) {
    query_register("nodes", q_nodes);
    query_register("pages", q_pages);
    query_register("allocs", q_allocs);
    query_register("stats", q_stats);
    query_register("help", q_help);
    task_registrar_consulta();

    uint16_t port = (uint16_t)(cfg->peers[self].port_base + 2);
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK),   /* solo desde la misma máquina */
    };
    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof addr) < 0 || listen(listen_fd, 8) < 0) {
        perror("query: bind");
        return -1;
    }
    pthread_t t;
    pthread_create(&t, NULL, listener_main, NULL);
    pthread_detach(t);
    return 0;
}