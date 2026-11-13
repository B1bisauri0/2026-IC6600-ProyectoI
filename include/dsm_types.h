/*
 * dsm_types.h -- Tipos compartidos del sistema DSM.
 *
 * Todo lo que esta aqui sale del Documento de Diseño (Entregable 1):
 *   - sec. 5   estructuras de datos y tamaños
 *   - sec. 8   encabezado y catalogo de mensajes
 *   - sec. 10  codigos de error y estadisticas
 * Lo comparten el modulo A (pager, directory, dsm) y el modulo B (network,
 * membership, sync, config, query, task).
 */
#ifndef DSM_TYPES_H
#define DSM_TYPES_H

#include <stdatomic.h>
#include <stdint.h>
#include <sys/types.h>

#define DSM_MAX_NODES 8          /* MAX_NODES (sec. 3.1) */
#define DSM_NODE_NONE 0xFFFF     /* "ninguno" (sec. 5.1) */
#define DSM_NAME_MAX  64         /* nombre publicado, incluye el NUL */

/* ---- 5.1 Tipos base ---------------------------------------------------- */
typedef uint16_t node_id_t;
typedef uint32_t vpn_t;
typedef uint64_t req_id_t;
typedef enum { PG_INVALID = 0, PG_SHARED = 1, PG_MODIFIED = 2 } pg_state_t;
typedef enum { NODE_OFFLINE = 0, NODE_ONLINE = 1, NODE_SUSPECT = 2 } node_status_t;

/* ---- 5.2 Tabla de nodos (la mantiene membership.c) ---------------------- */
typedef struct {
    node_id_t id;          /* 2 B */
    uint8_t   status;      /* 1 B node_status_t */
    uint8_t   missed_hb;   /* 1 B */
    char      host[64];    /* 64 B */
    uint16_t  port_base;   /* 2 B */
    uint64_t  mem_bytes;   /* 8 B */
    uint64_t  mem_used;    /* 8 B */
    uint64_t  last_hb_ms;  /* 8 B */
    int       fd;          /* 4 B */
} node_info_t;             /* 104 B */

/* ---- 5.3 Asignaciones --------------------------------------------------- */
typedef struct {
    vpn_t     vpn_start;   /* 4 B */
    uint32_t  n_pages;     /* 4 B */
    node_id_t home;        /* 2 B */
    node_id_t backup;      /* 2 B  0xFFFF si factor = 1 */
} extent_t;                /* 12 B */

typedef struct {
    uint32_t  alloc_id;    /* (node_id << 24) | contador local */
    node_id_t origin;
    uint32_t  owner_pid;   /* PID del dsm_run que lanzo la aplicacion */
    vpn_t     vpn_start;
    uint32_t  n_pages;
    uint16_t  n_extents;
    extent_t *extents;     /* no viaja por la red */
} alloc_t;                 /* 32 B */

/* ---- 5.4 Tabla local de paginas ----------------------------------------- */
#define PF_PENDING 0x01    /* bit0: pendiente de red */
#define PF_ISHOME  0x02    /* bit1: es home */
#define PF_DIRTY   0x04    /* bit2: sucia (reservado para dsm_checkpoint) */
#define PF_CACHED  0x08    /* bit3: ocupa la cache remota (extension del modulo A) */
#define PF_REF     0x10    /* bit4: bit de referencia (reloj / segunda oportunidad) */

typedef struct {
    uint8_t  state;        /* pg_state_t */
    uint8_t  flags;
    uint16_t _pad;
    uint32_t version;
} local_page_t;            /* 8 B */

/* ---- 5.5 Directorio del home -------------------------------------------- */
#define DIR_BUSY 0x01      /* transaccion en curso */
#define DIR_LOST 0x02      /* pagina perdida (propietario caido, sec. 4.4 caso 6) */

typedef struct {
    uint8_t   state;       /* estado global I/S/M */
    uint8_t   busy;        /* mascara DIR_BUSY | DIR_LOST */
    node_id_t owner;       /* valido si state == M */
    uint16_t  sharers;     /* bitmap de copias */
    uint16_t  acks_pending;
    uint32_t  version;
    uint32_t  waitq_head;  /* indice (base 1) en el pool de solicitudes; 0 = vacia */
} dir_entry_t;             /* 16 B */

/* ---- 5.6 Estructura de fallo (manejador -> Pager) ----------------------- */
typedef struct {
    vpn_t     vpn;         /* 4 B */
    uint8_t   is_write;    /* 1 B */
    uint8_t   _pad;        /* 1 B */
    node_id_t err_node;    /* 2 B (usa el relleno de alineacion: nodo caido) */
    atomic_int state;      /* 4 B 0=libre, 1=pendiente, 2=resuelto */
    int       result;      /* 4 B 0 = ok, <0 = dsm_status_t */
    int       wake_fd;     /* 4 B */
    pid_t     tid;         /* 4 B */
} fault_slot_t;            /* 24 B */

/* ---- 8.1 Encabezado de mensajes ----------------------------------------- */
#define DSM_MAGIC       0x44534D31u   /* "DSM1" */
#define DSM_PROTO_VER   1

#define MSG_F_RESP      0x0001        /* bit0 respuesta */
#define MSG_F_ERR       0x0002        /* bit1 error */
#define MSG_F_PAYLOAD   0x0004        /* bit2 lleva payload */
#define MSG_F_MORE      0x0008        /* bit3 hay mas */
#define MSG_F_KEEP      0x0010        /* bit4 conservar estado */

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint8_t  version;
    uint8_t  type;
    uint16_t flags;
    uint16_t src;
    uint16_t dst;
    uint32_t length;
    uint64_t req_id;
    uint32_t epoch;
    uint32_t reserved;
} msg_hdr_t;                          /* 32 B */

typedef enum {
    MSG_JOIN_REQUEST = 1, MSG_JOIN_ACK = 2, MSG_NODE_ANNOUNCE = 3,
    MSG_LEAVE = 4, MSG_HEARTBEAT = 5,
    MSG_ALLOC_REQUEST = 10, MSG_ALLOC_REPLY = 11,
    MSG_ALLOC_ANNOUNCE = 12, MSG_FREE = 13, MSG_ALLOC_ANNOUNCE_ACK = 14,
    MSG_PAGE_REQUEST = 20, MSG_PAGE_DATA = 21, MSG_FETCH = 22,
    MSG_INVALIDATE = 23, MSG_INVALIDATE_ACK = 24,
    MSG_EVICT_NOTICE = 25, MSG_WRITEBACK = 26, MSG_WRITEBACK_ACK = 27,
    MSG_LOCK_REQ = 30, MSG_LOCK_GRANT = 31, MSG_LOCK_REL = 32,
    MSG_BARRIER_ARRIVE = 33, MSG_BARRIER_GO = 34,
    MSG_STATS_REQUEST = 40, MSG_STATS_REPLY = 41,
    MSG_PAGETABLE_REQUEST = 42, MSG_PAGETABLE_REPLY = 43,
    MSG_TASK_START = 50, MSG_TASK_DONE = 51,
    MSG_REPLICA_UPDATE = 60, MSG_REPLICA_ACK = 61, MSG_HOME_CHANGE = 62,
    MSG_HOME_CHANGE_ACK = 63, MSG_RECOVER_DONE = 64,
    MSG_ERROR = 99
} msg_type_t;

/* 8.3 codigos de MSG_ERROR */
enum {
    E_NOT_HOME = 1, E_NO_MEM = 2, E_BAD_VPN = 3, E_BUSY_TIMEOUT = 4,
    E_NODE_DOWN = 5, E_CONFIG_MISMATCH = 6, E_VERSION = 7
};

/* acceso pedido en PAGE_REQUEST (sec. 8.4) */
enum { ACC_R = 0, ACC_W = 1, ACC_UPG = 2 };

/* ---- 10.1 Estados de retorno --------------------------------------------- */
typedef enum {
    DSM_OK = 0,
    DSM_ERR_CONFIG = -1,
    DSM_ERR_NOMEM = -2,
    DSM_ERR_NET = -3,
    DSM_ERR_NODE_DOWN = -4,
    DSM_ERR_INVAL = -5,
    DSM_ERR_NOT_INIT = -6,
    DSM_ERR_EXISTS = -7,
    DSM_ERR_NOTFOUND = -8,
    DSM_ERR_THREAD = -9
} dsm_status_t;

/* ---- 10.2 Estadisticas ---------------------------------------------------- */
typedef struct {
    int      node;
    uint64_t read_faults, write_faults, upgrade_faults;
    uint64_t pages_invalid, pages_shared, pages_modified;
    uint64_t pages_home, pages_cached;
    uint64_t invalidations_sent, invalidations_recv;
    uint64_t evictions, writebacks;
    uint64_t msgs_sent, msgs_recv, bytes_sent, bytes_recv;
    uint64_t fault_time_us_total, fault_count;
    uint64_t mem_contributed, mem_used;
    uint64_t checkpoints, replica_pages_sent, recoveries;
} dsm_stats_t;

/* ---- 10.6 entrada de la tabla de paginas del sistema ---------------------- */
typedef struct __attribute__((packed)) {
    uint32_t vpn;
    uint8_t  state;
    uint8_t  flags;        /* bit0 = BUSY */
    uint16_t owner;
    uint16_t sharers;
    uint32_t version;
    uint32_t transfers;
} pt_entry_t;              /* 18 B */

#endif
