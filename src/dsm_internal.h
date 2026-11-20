/*
 * dsm_internal.h -- estado del nodo compartido entre pager.c, directory.c y dsm.c.
 */
#ifndef DSM_INTERNAL_H
#define DSM_INTERNAL_H

#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>
#include "dsm_types.h"
#include "config.h"
#include "network.h"
#include "membership.h"
#include "stats.h"

typedef struct {
    dsm_config_t cfg;
    int          self;
    uint32_t     page_size, page_shift;
    uintptr_t    base;
    size_t       win_size;
    uint64_t     slab_pages, n_vpages;
    char        *app;                 /* vista de aplicacion (permisos MSI)   */
    char        *sys;                 /* vista de sistema (siempre RW)        */
    int          memfd;
    local_page_t *lp;                 /* tabla local de paginas, por vpn      */
    dir_entry_t  *dir;                /* directorio, por vpn (solo home)      */
    sem_t        *dir_sem;            /* semaforo binario por pagina del home */
    uint32_t     *transfers;          /* transferencias por pagina (home)     */
    uint64_t      node_mem_pages;     /* marcos que aporta este nodo          */
    _Atomic uint64_t used_pages;      /* marcos home en uso                   */
    int           ready;
} dsm_node_t;

extern dsm_node_t g_n;

#define VPN_APP(v) (g_n.app + ((size_t)(v) << g_n.page_shift))
#define VPN_SYS(v) (g_n.sys + ((size_t)(v) << g_n.page_shift))
#define BIT(n)     ((uint16_t)(1u << (n)))

#define LOG_ERR(...) do { fprintf(stderr, "[dsm %d] ", g_n.self); fprintf(stderr, __VA_ARGS__); } while (0)

#include "wire.h"

/* ---- tabla de asignaciones (dsm.c) ---- */
typedef struct { vpn_t start; uint32_t n; } vpn_range_t;
node_id_t alloc_home_of(vpn_t vpn);                       /* DSM_NODE_NONE si no esta asignada */
node_id_t alloc_backup_of(vpn_t vpn);
/* rangos de paginas cuyo home es 'home'; el llamador libera *out con free() */
int       alloc_home_ranges(node_id_t home, vpn_range_t **out);

/* Funciones para consultar y actualizar tramos de asignacion (extents) */
size_t    alloc_extent_count(void);
int       alloc_get_extent(size_t index, vpn_t *out_start, uint32_t *out_n, node_id_t *out_home, node_id_t *out_backup);
int       alloc_update_extent_nodes(vpn_t vstart, uint32_t n_pages, node_id_t new_home, node_id_t new_backup);
int       online_others(node_id_t *out);

/* RPC helpers sincronos compartidos (dsm.c y replica.c) */
typedef struct rpc {
    uint32_t alloc_id;
    int kind;
    uint16_t waiting;
    uint32_t granted[DSM_MAX_NODES];
    struct rpc *next;
} rpc_t;

rpc_t *rpc_begin(uint32_t id, int kind, uint16_t mask);
int    rpc_wait_end(rpc_t *r);
void   rpc_reply(uint32_t id, int kind, node_id_t from, uint32_t val);

/* Funciones para consultar y recorrer la tabla de asignaciones (sec. 5.3 y 10.6) */
size_t    dsm_alloc_count(void);
int       dsm_alloc_get_info(size_t index, alloc_t *out_a, char *out_name, size_t name_max);
void      dsm_alloc_free_info(alloc_t *a);
int       dsm_alloc_find_vpn(vpn_t vpn, uint32_t *out_alloc_id, node_id_t *out_origin,
                             uint32_t *out_owner_pid, uint32_t *out_order, uint32_t *out_n_pages);
size_t    dsm_alloc_table_serialize(uint8_t **out);
int       dsm_alloc_table_deserialize(const uint8_t *b, size_t len);

/* Notificacion de fallo segun politica P3 (sec. 7) */
extern _Atomic int g_task_failed;
extern _Atomic int g_task_failed_err;
void dsm_task_notify_failure(int err, node_id_t err_node, vpn_t vpn);

uint64_t  now_ms(void);
/* Igual que dsm_init() pero con la configuracion ya cargada (la usan las pruebas). */
struct dsm_ctx;
int       dsm_init_with(const dsm_config_t *cfg, int self_id, struct dsm_ctx **ctx);
uint64_t  now_us(void);

#endif
