/*
 * directory.h -- directorio de paginas del nodo home (sec. 4 y 5.5).
 */
#ifndef DSM_DIRECTORY_H
#define DSM_DIRECTORY_H
#include "dsm_internal.h"

int  dir_init(void);                                   /* cola de solicitudes */
void dir_page_create(vpn_t vpn);                       /* pagina nueva respaldada por este nodo */
void dir_range_destroy(vpn_t start, uint32_t n);

void dir_request_local(vpn_t vpn, int access, uint64_t req_id);       /* solicitante = este nodo */
void dir_on_page_request(const msg_hdr_t *h, const uint8_t *body);
void dir_on_page_data(const msg_hdr_t *h, const uint8_t *body);       /* respuesta a un FETCH */
void dir_on_invalidate_ack(const msg_hdr_t *h, const uint8_t *body);
void dir_on_evict_notice(const msg_hdr_t *h, const uint8_t *body);
void dir_on_writeback(const msg_hdr_t *h, const uint8_t *body);
void dir_on_error(const msg_hdr_t *h, const uint8_t *body);           /* ERROR a un FETCH */

void dir_on_node_down(node_id_t n);
void dir_check_timeouts(uint64_t now_ms);

/* PAGETABLE_REQUEST (sec. 10.6): copia hasta 'max' entradas en uso desde vpn_start.
 * *more = 1 si quedan mas. Devuelve cuantas copio. */
uint32_t dir_snapshot(vpn_t vpn_start, uint32_t max, pt_entry_t *out, int *more);

/* Conteo acumulado de transferencias de paginas por la red */
uint64_t dir_get_total_transfers(void);
void     dir_reset_total_transfers(void);

#endif
