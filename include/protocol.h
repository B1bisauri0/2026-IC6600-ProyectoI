#ifndef DSM_PROTOCOL_H
#define DSM_PROTOCOL_H

#include "dsm_types.h"

/* Nombres cortos de los bits de flags */
#define F_REPLY   MSG_F_RESP
#define F_ERROR   MSG_F_ERR
#define F_PAYLOAD MSG_F_PAYLOAD

/* Tamaño de página en bytes. Debe ser potencia de 2 y <= 65536. */
#define MAX_PEERS DSM_MAX_NODES

/* Este es el cuerpo de un mensaje de error */
typedef struct __attribute__((packed)) { uint16_t code; uint32_t vpn; } body_error_t;

/* ---- Interfaz de red (network.c) ---- */\

/* Envía un mensaje completo. body puede ser NULL si len == 0.
 * Devuelve 0 o -1 si el nodo no está conectado. No bloquea esperando respuesta. */
int net_send(node_id_t dst, uint8_t type, uint16_t flags, req_id_t req_id,
             const void *body, uint32_t len);

/* Igual, pero agrega una página de PAGE_SIZE bytes después del cuerpo. */
int net_send_page(node_id_t dst, uint8_t type, uint16_t flags, req_id_t req_id,
                  const void *body, uint32_t len, const void *page);

/* Registra quién atiende cada tipo de mensaje recibido. Lo llama el pool
 * de servicio, nunca el manejador de señal. El body se libera al retornar. */
typedef void (*msg_handler_t)(const msg_hdr_t *h, const void *body);
void net_register_handler(uint8_t type, msg_handler_t fn);

/* Datos del nodo local y del clúster. */
node_id_t net_self(void);
int       net_num_peers(void);

#endif
