/* query.c - atiende a dsm_query: recibe una línea ("nodes", "pages ...") y
 * responde texto. Una conexión por consulta. */
#define _GNU_SOURCE
#include "query.h"
#include "membership.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

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

static void q_help(FILE *out, const char *args) {
    (void)args;
    fprintf(out, "Consultas disponibles:");
    for (int i = 0; i < n_queries; i++) fprintf(out, " %s", table[i].name);
    fprintf(out, "\n");
}

static void *client_main(void *arg) {
    int fd = (int)(intptr_t)arg;
    FILE *in = fdopen(dup(fd), "r"), *out = fdopen(fd, "w");
    char line[256];
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
    query_register("help", q_help);

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
