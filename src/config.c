#define _GNU_SOURCE
#include "config.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* quita espacios al inicio y al final */
static char *trim(char *s) {
    while (isspace((unsigned char)*s)) s++;
    char *e = s + strlen(s);
    while (e > s && isspace((unsigned char)e[-1])) *--e = '\0';
    return s;
}

/* "10.0.0.11:27000,10.0.0.12:27000" -> arreglo de peers */
static int parse_peers(char *v, dsm_config_t *cfg) {
    cfg->n_peers = 0;
    for (char *tok = strtok(v, ","); tok; tok = strtok(NULL, ",")) {
        tok = trim(tok);
        char *colon = strrchr(tok, ':');
        if (!colon) {
            fprintf(stderr, "config: el peer '%s' no tiene puerto (formato host:puerto)\n", tok);
            return -1;
        }
        if (cfg->n_peers >= DSM_MAX_NODES) {
            fprintf(stderr, "config: demasiados peers (máximo %d)\n", DSM_MAX_NODES);
            return -1;
        }
        *colon = '\0';
        dsm_peer_t *p = &cfg->peers[cfg->n_peers++];
        snprintf(p->host, sizeof p->host, "%s", tok);
        p->port_base = (uint16_t)atoi(colon + 1);
    }
    return cfg->n_peers > 0 ? 0 : -1;
}

/* la dirección "host" es una de las IP de esta máquina? */
static int host_is_local(const char *host) {
    struct addrinfo hints = { .ai_family = AF_INET }, *res = NULL;
    if (getaddrinfo(host, NULL, &hints, &res) != 0) return 0;
    struct in_addr target = ((struct sockaddr_in *)res->ai_addr)->sin_addr;
    freeaddrinfo(res);

    struct ifaddrs *ifs, *it;
    int found = 0;
    if (getifaddrs(&ifs) < 0) return 0;
    for (it = ifs; it; it = it->ifa_next) {
        if (it->ifa_addr && it->ifa_addr->sa_family == AF_INET &&
            ((struct sockaddr_in *)it->ifa_addr)->sin_addr.s_addr == target.s_addr) {
            found = 1;
            break;
        }
    }
    freeifaddrs(ifs);
    return found;
}

/* Dado un ID forzado o la IP local, resuelve el ID del nodo:
 * si se usa --id N, ese; si no, pregunta a Linux las IP de la máquina */
static int config_resolve_id(const dsm_config_t *cfg, int forced_id) {
    if (forced_id >= 0) {
        if (forced_id >= cfg->n_peers) {
            fprintf(stderr, "config: --id %d no existe (hay %d peers)\n", forced_id, cfg->n_peers);
            return -1;
        }
        return forced_id;
    }
    int match = -1, count = 0;
    for (int i = 0; i < cfg->n_peers; i++)
        if (host_is_local(cfg->peers[i].host)) { match = i; count++; }
    if (count == 1) return match;
    fprintf(stderr, "config: %s; use --id N\n",
            count == 0 ? "ninguna entrada de peers es una IP de esta máquina"
                       : "varias entradas de peers son de esta máquina");
    return -1;
}

/* es la función que carga la configuración desde un archivo y decide el id */
int config_load(const char *path, int id_arg, dsm_config_t *cfg, int *self_id) {
    memset(cfg, 0, sizeof *cfg);
    /* valores por defecto */
    cfg->page_size = 4096;
    cfg->node_mem_bytes = 1024ULL << 20;
    cfg->max_nodes = 8;
    cfg->va_slab_bytes = 4096ULL << 20;
    cfg->base_addr = 0x200000000000ULL;
    cfg->heartbeat_ms = 1000;
    cfg->heartbeat_miss_max = 3;
    cfg->connect_timeout_ms = 2000;
    cfg->request_timeout_ms = 5000;
    cfg->cache_max_pages = 24000;
    cfg->worker_threads = 4;
    cfg->max_user_threads = 64;
    cfg->replication_factor = 1;
    cfg->log_level = 2;
    cfg->replacement_policy = 0;

    FILE *f = fopen(path, "r");
    if (!f) { perror(path); return DSM_ERR_CONFIG; }

    char line[1024];
    int lineno = 0, ok = 0;
    while (fgets(line, sizeof line, f)) {
        lineno++;
        char *hash = strchr(line, '#');
        if (hash) *hash = '\0';
        char *s = trim(line);
        if (*s == '\0') continue;

        char *eq = strchr(s, '=');
        if (!eq) { fprintf(stderr, "config: línea %d sin '='\n", lineno); goto out; }
        *eq = '\0';
        char *k = trim(s), *v = trim(eq + 1);

        /* asigna el valor a la estructura */
        if (!strcmp(k, "page_size"))          cfg->page_size = strtoul(v, NULL, 0);
        else if (!strcmp(k, "node_mem_mb"))        cfg->node_mem_bytes = strtoull(v, NULL, 0) << 20;
        else if (!strcmp(k, "max_nodes"))          cfg->max_nodes = atoi(v);
        else if (!strcmp(k, "va_slab_mb"))         cfg->va_slab_bytes = strtoull(v, NULL, 0) << 20;
        else if (!strcmp(k, "base_addr"))          cfg->base_addr = strtoull(v, NULL, 0);
        else if (!strcmp(k, "peers"))            { if (parse_peers(v, cfg) < 0) goto out; }
        else if (!strcmp(k, "port_range")) {
            if (sscanf(v, "%hu-%hu", &cfg->port_min, &cfg->port_max) != 2) {
                fprintf(stderr, "config: port_range debe ser inicio-fin\n"); goto out;
            }
        }
        else if (!strcmp(k, "heartbeat_ms"))       cfg->heartbeat_ms = atoi(v);
        else if (!strcmp(k, "heartbeat_miss_max")) cfg->heartbeat_miss_max = atoi(v);
        else if (!strcmp(k, "connect_timeout_ms")) cfg->connect_timeout_ms = atoi(v);
        else if (!strcmp(k, "request_timeout_ms")) cfg->request_timeout_ms = atoi(v);
        else if (!strcmp(k, "cache_max_pages"))    cfg->cache_max_pages = atoi(v);
        else if (!strcmp(k, "worker_threads"))     cfg->worker_threads = atoi(v);
        else if (!strcmp(k, "max_user_threads"))   cfg->max_user_threads = atoi(v);
        else if (!strcmp(k, "replication_factor")) cfg->replication_factor = atoi(v);
        else if (!strcmp(k, "log_level"))
            cfg->log_level = !strcmp(v, "error") ? 0 : !strcmp(v, "warn") ? 1 : !strcmp(v, "debug") ? 3 : 2;
        else if (!strcmp(k, "replacement")) {
            if      (!strcmp(v, "fifo"))  cfg->replacement_policy = 0;
            else if (!strcmp(v, "clock")) cfg->replacement_policy = 1;
            else { fprintf(stderr, "config: replacement debe ser fifo o clock\n"); goto out; }
        }
        else fprintf(stderr, "config: clave desconocida '%s' (se ignora)\n", k);
    }

    /* validaciones */
    if (cfg->n_peers == 0) { fprintf(stderr, "config: falta 'peers'\n"); goto out; }
    if ((uint32_t)cfg->n_peers > cfg->max_nodes) { fprintf(stderr, "config: hay más peers que max_nodes\n"); goto out; }
    if (cfg->page_size < 4096 || (cfg->page_size & (cfg->page_size - 1))) {
        fprintf(stderr, "config: page_size debe ser potencia de 2 y al menos 4096\n"); goto out;
    }
    for (int i = 0; i < cfg->n_peers; i++) {
        uint16_t b = cfg->peers[i].port_base;
        if (cfg->port_max && (b < cfg->port_min || b + 2 > cfg->port_max)) {
            fprintf(stderr, "config: los puertos %u-%u del nodo %d quedan fuera de port_range %u-%u\n",
                    b, b + 2, i, cfg->port_min, cfg->port_max);
            goto out;
        }
        /* nodos en la misma máquina deben separar sus puertos al menos 3 */
        for (int j = 0; j < i; j++) {
            int d = (int)b - (int)cfg->peers[j].port_base;
            if (!strcmp(cfg->peers[i].host, cfg->peers[j].host) && d > -3 && d < 3) {
                fprintf(stderr, "config: los nodos %d y %d comparten host y sus puertos distan menos de 3\n", j, i);
                goto out;
            }
        }
    }
    ok = 1;
out:
    fclose(f);
    if (!ok) return DSM_ERR_CONFIG;
    int id = config_resolve_id(cfg, id_arg);
    if (id < 0) return DSM_ERR_CONFIG;
    *self_id = id;
    return DSM_OK;
}

uint32_t config_hash(const dsm_config_t *cfg) {
    /* FNV-1a sobre los parámetros globales (no incluye el id):
     * es como una contraseña de la configuración, para que los nodos detecten si no coinciden */
    uint32_t h = 2166136261u;
#define MIX(x) do { const unsigned char *b = (const void *)&(x); \
                    for (size_t _i = 0; _i < sizeof(x); _i++) { h ^= b[_i]; h *= 16777619u; } } while (0)
    MIX(cfg->page_size); MIX(cfg->node_mem_bytes); MIX(cfg->max_nodes);
    MIX(cfg->va_slab_bytes); MIX(cfg->base_addr); MIX(cfg->replication_factor);
    for (int i = 0; i < cfg->n_peers; i++) {
        for (const char *c = cfg->peers[i].host; *c; c++) { h ^= (unsigned char)*c; h *= 16777619u; }
        MIX(cfg->peers[i].port_base);
    }
#undef MIX
    return h;
}