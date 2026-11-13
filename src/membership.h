/*
 * membership.h -- CONTRATO con el modulo B (membership.c).
 * Solo lectura sobre la tabla de nodos (sec. 5.2) mas el arranque/parada.
 */
#ifndef DSM_MEMBERSHIP_H
#define DSM_MEMBERSHIP_H
#include "dsm_types.h"
#include "config.h"

int           membership_start(const dsm_config_t *cfg, int self_id); /* JOIN + heartbeat */
void          membership_stop(void);                                  /* envia LEAVE */
node_status_t membership_status(node_id_t id);
/* ids ONLINE en orden ascendente (incluye al propio nodo); devuelve cuantos. */
int           membership_online_ids(node_id_t *out, int max);
/* copia de la entrada de la tabla (mem_bytes, mem_used...). 0 = ok. */
int           membership_get(node_id_t id, node_info_t *out);

/* Lo implementa el modulo A (dsm.c). membership.c la llama al pasar un nodo
 * a OFFLINE (heartbeat_miss_max latidos perdidos o conexion cerrada). */
void          dsm_on_node_down(node_id_t id);

/* Lo implementa el modulo A: bytes de marcos home en uso (va en HEARTBEAT). */
uint64_t      dsm_local_mem_used_bytes(void);

#endif
