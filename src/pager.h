/*
 * pager.h -- manejador de SIGSEGV, hilo Pager y lado "nodo" del protocolo MSI.
 */
#ifndef DSM_PAGER_H
#define DSM_PAGER_H
#include "dsm_internal.h"

int  pager_init(void);                 /* reserva tablas, instala sigaction, lanza el hilo Pager */
void pager_stop(void);

int  pager_thread_register(void);      /* dsm_thread_register  (sec. 10.4) */
int  pager_thread_unregister(void);

/* Mensajes que atiende el nodo (los llama dsm_msg_dispatch) */
void pager_on_page_data(const msg_hdr_t *h, const uint8_t *body);
void pager_on_fetch(const msg_hdr_t *h, const uint8_t *body);
void pager_on_invalidate(const msg_hdr_t *h, const uint8_t *body);
void pager_on_writeback_ack(const msg_hdr_t *h, const uint8_t *body);
void pager_on_error(const msg_hdr_t *h, const uint8_t *body);

/* Usado por el directorio cuando el solicitante es el propio home */
void pager_local_complete(vpn_t vpn, pg_state_t perm, uint32_t version, uint64_t req_id);
void pager_local_fail(uint64_t req_id, int status, node_id_t node);

/* Transiciones locales del home (el llamador tiene el semaforo del directorio) */
int  pager_home_downgrade(vpn_t vpn);  /* M -> S. Devuelve 0 en exito, <0 si falla por ENOMEM */
int  pager_home_invalidate(vpn_t vpn); /* S|M -> I. Devuelve 0 en exito, <0 si falla por ENOMEM */

/* Politica de reemplazo de la cache remota */
enum { PAGER_POLICY_FIFO = 0, PAGER_POLICY_CLOCK = 1 };
void pager_set_policy(int policy);
int  pager_get_policy(void);
void pager_touch_page(vpn_t vpn);      /* Marca el bit de referencia para reloj */

/* Mutex por franja (sec. 9.2) */
void pager_page_lock(vpn_t vpn);
void pager_page_unlock(vpn_t vpn);

/* Asignacion / liberacion de rangos */
void pager_init_home_range(vpn_t start, uint32_t n);   /* estado M, RW, version 0 */
void pager_reset_range(vpn_t start, uint32_t n);       /* estado I, PROT_NONE, libera memoria */

/* Caida de nodos y cierre ordenado */
void pager_on_node_down(node_id_t n);
int  pager_writeback_all(void);        /* devuelve las paginas M ajenas a su home (LEAVE) */

/* Consultas / depuracion */
pg_state_t pager_local_state(vpn_t vpn);
uint64_t   pager_cache_count(void);

#endif
