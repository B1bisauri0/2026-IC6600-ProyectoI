/*
 * config.h -- CONTRATO con el modulo B (config.c).
 * Campos = claves de dsm.conf del documento de diseño, sec. 6.
 * El modulo A solo LEE esta estructura; config.c (B) la llena.
 */
#ifndef DSM_CONFIG_H
#define DSM_CONFIG_H
#include <stddef.h>
#include <stdint.h>
#include "dsm_types.h"

typedef struct {
    char     host[64];
    uint16_t port_base;
} dsm_peer_t;

typedef struct {
    uint32_t   page_size;            /* page_size */
    uint64_t   node_mem_bytes;       /* node_mem_mb * 1 MiB */
    uint32_t   max_nodes;            /* max_nodes */
    uint64_t   va_slab_bytes;        /* va_slab_mb * 1 MiB */
    uintptr_t  base_addr;            /* base_addr */
    int        n_peers;
    dsm_peer_t peers[DSM_MAX_NODES]; /* la posicion es el node_id (D6) */
    uint32_t   heartbeat_ms, heartbeat_miss_max;
    uint32_t   connect_timeout_ms, request_timeout_ms;
    uint32_t   cache_max_pages;
    uint32_t   worker_threads;
    uint32_t   max_user_threads;
    int        log_level;
    int        replication_factor;
    int        replacement_policy;   /* 0 = FIFO, 1 = CLOCK */
} dsm_config_t;

/* Lee y valida dsm.conf. id_arg = -1: autodeteccion (sec. 6.1 regla 4).
 * Devuelve DSM_OK o DSM_ERR_CONFIG. */
int config_load(const char *path, int id_arg, dsm_config_t *cfg, int *self_id);

#endif
