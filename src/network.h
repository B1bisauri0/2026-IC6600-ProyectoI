/*
 * network.h -- CONTRATO con el modulo B (network.c).
 *
 * Lo que el modulo A NECESITA de la red. Si network.c ya tiene otros nombres,
 * basta con adaptar estas funciones (el modulo A solo las usa aqui).
 *
 * REGLAS QUE B DEBE CUMPLIR (el protocolo del modulo A depende de ellas):
 *
 *  R1. Los mensajes del MISMO nodo origen se entregan a dsm_msg_dispatch()
 *      EN ORDEN DE LLEGADA y de uno en uno: el siguiente mensaje de ese
 *      origen no se despacha hasta que dsm_msg_dispatch() del anterior
 *      retorno. Mensajes de origenes distintos si pueden correr en paralelo.
 *      (Cola por origen servida por el pool: sigue siendo "lector -> cola ->
 *      pool" de la sec. 9.1. Sin esto, un EVICT_NOTICE o INVALIDATE podria
 *      adelantarse a la respuesta que le precede en el mismo socket.)
 *  R2. dsm_msg_dispatch() nunca espera respuestas de red, asi que el pool
 *      puede llamarla sin riesgo de interbloqueo (sec. 9.4).
 *  R3. net_sendv() es seguro entre hilos (mutex de envio por conexion) y
 *      completa envios parciales. Devuelve 0 o -1.
 *  R4. El encabezado msg_hdr_t se convierte a orden de red dentro de
 *      net_sendv()/lector; el modulo A siempre ve valores en orden del host.
 *      Los cuerpos (body) YA van serializados en orden de red por el modulo A.
 */
#ifndef DSM_NETWORK_H
#define DSM_NETWORK_H
#include <stddef.h>
#include <sys/uio.h>
#include "dsm_types.h"
#include "config.h"

/* Envia type/flags/req_id a dst con el cuerpo dado por iov. src, dst, magic,
 * version, epoch y length los completa B. */
int net_sendv(node_id_t dst, msg_type_t type, uint16_t flags, uint64_t req_id,
              const struct iovec *iov, int iovcnt);

/* Id de solicitud unico en el cluster (p. ej. node_id en los 16 bits altos). */
uint64_t net_new_req_id(void);

int  net_start(const dsm_config_t *cfg, int self_id);  /* listener, lectores, pool */
void net_stop(void);

/* Lo implementa el modulo A (dsm.c). B la llama por cada mensaje de tipo:
 * ALLOC_*, FREE, PAGE_*, FETCH, INVALIDATE*, EVICT_NOTICE, WRITEBACK*, ERROR. */
void dsm_msg_dispatch(const msg_hdr_t *h, const uint8_t *body);

#endif
