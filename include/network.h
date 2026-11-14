#ifndef DSM_NETWORK_H
#define DSM_NETWORK_H

#include <stddef.h>
#include <sys/uio.h>
#include "config.h"
#include "protocol.h"

/* Orden de arranque: net_init, luego cada módulo registra sus handlers,
 * y al final net_start abre el puerto y arranca los hilos. Así ningún
 * mensaje llega antes de que exista quién lo atienda. */
void net_init(const dsm_config_t *cfg, node_id_t self);
int  net_start(void);
void net_stop(void);

/* Hay conexión abierta con ese nodo? */
int  net_is_connected(node_id_t id);

/* Lo llama network.c cuando una conexión se abre o se cierra. */
typedef void (*net_conn_cb_t)(node_id_t id, int up);
void net_on_connection(net_conn_cb_t cb);

/* Funciones de envío y recepción completas (manejan envíos parciales). */
int send_all(int fd, const void *buf, size_t n);
int recv_all(int fd, void *buf, size_t n);

/* Época (encarnación) de este nodo: se fija al arrancar. */
uint32_t net_epoch(void);


/* Envía un mensaje cuyo cuerpo viene en pedazos (iov). Devuelve 0 o -1. */
int net_sendv(node_id_t dst, msg_type_t type, uint16_t flags, uint64_t req_id,
              const struct iovec *iov, int iovcnt);

/* Id de solicitud único en el clúster: (node_id << 48) | contador. */
uint64_t net_new_req_id(void);

/* Registra dsm_msg_dispatch como handler de los mensajes de la parte de memoria.
 * Va entre sync_start y net_start. */
void puente_start(void);

/* La implementa la parte de memoria (dsm.c): atiende sus mensajes
 * (PAGE_*, FETCH, INVALIDATE*, ALLOC_*, WRITEBACK*...). puente.c la registra. */
void dsm_msg_dispatch(const msg_hdr_t *h, const uint8_t *body);

#endif