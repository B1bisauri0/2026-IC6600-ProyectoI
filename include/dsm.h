/*
 * dsm.h -- API publica del DSM (sec. 10 del documento de diseño).
 * Es lo unico que incluyen las aplicaciones.
 */
#ifndef DSM_H
#define DSM_H

#include <stdint.h>
#include <stdio.h>
#include <stddef.h>
#include "dsm_types.h"

typedef struct dsm_ctx dsm_ctx_t;

typedef struct {
    int      id;
    char     host[64];
    uint16_t port_base;
    uint8_t  status;
    uint64_t mem_bytes, mem_used, mem_free, last_hb_age_ms;
} dsm_node_status_t;

/* La escribe cada aplicacion (punto de entrada que llama el nodo). */
int   dsm_app_main(dsm_ctx_t *ctx, int argc, char **argv);

/* ---- implementadas por el modulo A (dsm.c) ---- */
int   dsm_init(const char *conf_path, int id, dsm_ctx_t **ctx);
int   dsm_shutdown(dsm_ctx_t *ctx);
void *dsm_malloc(dsm_ctx_t *ctx, size_t size);
void *dsm_malloc_striped(dsm_ctx_t *ctx, size_t size, size_t stripe);
int   dsm_free(dsm_ctx_t *ctx, void *ptr);
int   dsm_publish(dsm_ctx_t *ctx, const char *name, void *ptr);
void *dsm_lookup(dsm_ctx_t *ctx, const char *name);
int   dsm_thread_register(dsm_ctx_t *ctx);
int   dsm_thread_unregister(dsm_ctx_t *ctx);
int   dsm_node_id(dsm_ctx_t *ctx);
int   dsm_num_nodes(dsm_ctx_t *ctx);
dsm_status_t dsm_errno(void);
const char  *dsm_strerror(dsm_status_t s);
size_t       dsm_page_size(dsm_ctx_t *ctx);

/* Necesaria para task.c (no esta en el borrador de la sec. 10.3): crea el
 * contexto de una aplicacion lanzada por dsm_run (owner_pid = PID de dsm_run). */
dsm_ctx_t *dsm_ctx_for_app(dsm_ctx_t *node_ctx, uint32_t owner_pid);

/* ---- implementadas por el modulo B (sync.c, task.c, query.c, replica.c) ---- */
int   dsm_barrier(dsm_ctx_t *ctx, int id, int n);
int   dsm_lock(dsm_ctx_t *ctx, int lock_id);
int   dsm_unlock(dsm_ctx_t *ctx, int lock_id);
int   dsm_checkpoint(dsm_ctx_t *ctx);
int   dsm_printf(dsm_ctx_t *ctx, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
int   dsm_get_stats(dsm_ctx_t *ctx, dsm_stats_t *out, int node);
int   dsm_dump_pages(dsm_ctx_t *ctx, FILE *out, int node);
int   dsm_get_nodes(dsm_ctx_t *ctx, dsm_node_status_t *out, int max);
int   dsm_dump_nodes(dsm_ctx_t *ctx, FILE *out);
int   dsm_dump_allocs(dsm_ctx_t *ctx, FILE *out);

#endif
