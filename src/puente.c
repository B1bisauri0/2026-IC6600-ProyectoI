/* puente.c - une la parte de memoria (dsm.c, pager.c, directory.c) con la red.
 *
 * La parte de memoria espera tres cosas que network.c no tenía:
 *   - net_sendv: enviar un mensaje armado en pedazos (struct iovec)
 *   - net_new_req_id: un req_id único en todo el clúster
 *   - que todos sus tipos de mensaje lleguen a dsm_msg_dispatch
 */
#define _GNU_SOURCE
#include "network.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

/* junta los pedazos en un solo buffer y lo manda con net_send.
 * Si el último pedazo es una página igual se copia; es más lento pero simple */
int net_sendv(node_id_t dst, msg_type_t type, uint16_t flags, uint64_t req_id,
              const struct iovec *iov, int iovcnt) {
    size_t total = 0;
    for (int i = 0; i < iovcnt; i++) total += iov[i].iov_len;

    uint8_t pila[256], *buf = total <= sizeof pila ? pila : malloc(total);
    if (!buf) return -1;
    size_t off = 0;
    for (int i = 0; i < iovcnt; i++) {
        if (iov[i].iov_len) memcpy(buf + off, iov[i].iov_base, iov[i].iov_len);
        off += iov[i].iov_len;
    }
    int rc = net_send(dst, (uint8_t)type, flags, req_id, total ? buf : NULL, (uint32_t)total);
    if (buf != pila) free(buf);
    return rc;
}

/* id del nodo en los 16 bits de arriba + contador: nunca se repite entre nodos */
uint64_t net_new_req_id(void) {
    static uint64_t cont = 0;
    static pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&m);
    uint64_t r = ((uint64_t)net_self() << 48) | ++cont;
    pthread_mutex_unlock(&m);
    return r;
}

/* network.c llama handlers con (h, void*), ella los quiere con (h, uint8_t*) */
static void a_memoria(const msg_hdr_t *h, const void *body) {
    dsm_msg_dispatch(h, (const uint8_t *)body);
}

/* registrar todos los mensajes que atiende la parte de memoria.
 * Hay que llamarlo antes de net_start */
void puente_start(void) {
    static const uint8_t tipos[] = {
        MSG_ALLOC_REQUEST, MSG_ALLOC_REPLY, MSG_ALLOC_ANNOUNCE, MSG_FREE, MSG_ALLOC_ANNOUNCE_ACK,
        MSG_PAGE_REQUEST, MSG_PAGE_DATA, MSG_FETCH, MSG_INVALIDATE, MSG_INVALIDATE_ACK,
        MSG_EVICT_NOTICE, MSG_WRITEBACK, MSG_WRITEBACK_ACK, MSG_ERROR,
        MSG_REPLICA_UPDATE, MSG_REPLICA_ACK, MSG_HOME_CHANGE, MSG_HOME_CHANGE_ACK, MSG_RECOVER_DONE,
    };
    for (size_t i = 0; i < sizeof tipos; i++)
        net_register_handler(tipos[i], a_memoria);
}