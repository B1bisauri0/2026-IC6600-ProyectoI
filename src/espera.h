#ifndef DSM_ESPERA_H
#define DSM_ESPERA_H

#include "network.h"

/* devuelve un id de petición único para usar con espera_esperar() */
uint64_t espera_nueva(void);
/* 0 = llegó (el llamador libera *body con free), DSM_ERR_NODE_DOWN si el nodo
 * se cayó o la respuesta no llegó a tiempo, DSM_ERR_NET si vino con error */
int  espera_esperar(uint64_t req, node_id_t de, int timeout_ms, void **body, uint32_t *len);
/* cancela una espera que no se va a usar (por ejemplo si falló el envío) */
void espera_cancelar(uint64_t req);
void espera_respuesta(const msg_hdr_t *h, const void *body);

#endif
