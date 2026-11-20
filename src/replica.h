/*
 * replica.h -- Tolerancia a fallos mediante replicacion y recuperacion (sec. 7.1 y 14.4).
 * Maneja el punto de control (checkpoint), actualizacion de replicas y el protocolo
 * de recuperacion ante caida de un nodo home o backup cuando replication_factor >= 2.
 */
#ifndef DSM_REPLICA_H
#define DSM_REPLICA_H

#include "dsm_types.h"
#include "dsm_internal.h"

/* Selecciona el siguiente nodo ONLINE en el orden circular de peers para ser backup */
node_id_t select_backup_node(node_id_t home);

/* Punto de control: envia paginas M a su backup y degrada permisos a PROT_READ (sec. 10.3) */
int replica_checkpoint(dsm_ctx_t *ctx);

/* Recuperacion ante la caida de un nodo (sec. 7.1): asume home, actualiza backup, RECOVER_DONE */
void replica_on_node_down(node_id_t failed_node);

/* Manejadores de mensajes del protocolo de replica (despachados desde dsm_msg_dispatch) */
void replica_handle_update(const msg_hdr_t *h, const uint8_t *body);
void replica_handle_ack(const msg_hdr_t *h, const uint8_t *body);
void replica_handle_home_change(const msg_hdr_t *h, const uint8_t *body);
void replica_handle_home_change_ack(const msg_hdr_t *h, const uint8_t *body);
void replica_handle_recover_done(const msg_hdr_t *h, const uint8_t *body);

/* Espera a que termine una recuperacion en curso */
int replica_wait_recover_done(uint32_t timeout_ms);

/* Consulta si hay un proceso de recuperacion activo */
int replica_is_recovering(void);

/* Modifica el flag de recuperacion activa (usado por pruebas unitarias de segunda caida) */
void replica_set_recovering(int active);

#endif /* DSM_REPLICA_H */
