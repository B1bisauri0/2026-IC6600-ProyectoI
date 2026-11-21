/* Respuestas pendientes, buscadas por req_id. */
#define _GNU_SOURCE
#include "espera.h"
#include "membership.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct pend {
    uint64_t     req;
    int          listo, error;
    void        *body;
    uint32_t     len;
    struct pend *next;
} pend_t;

static pend_t         *lista;
static pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  cv = PTHREAD_COND_INITIALIZER;

uint64_t espera_nueva(void) {
    pend_t *p = calloc(1, sizeof *p);
    p->req = net_new_req_id();
    pthread_mutex_lock(&m);
    p->next = lista;
    lista = p;
    pthread_mutex_unlock(&m);
    return p->req;
}

/* saca de la lista y devuelve el pendiente (llamar con m tomado) */
static pend_t *sacar(uint64_t req) {
    for (pend_t **pp = &lista; *pp; pp = &(*pp)->next)
        if ((*pp)->req == req) { pend_t *p = *pp; *pp = p->next; return p; }
    return NULL;
}

void espera_cancelar(uint64_t req) {
    pthread_mutex_lock(&m);
    pend_t *p = sacar(req);
    pthread_mutex_unlock(&m);
    if (p) { free(p->body); free(p); }
}

int espera_esperar(uint64_t req, node_id_t de, int timeout_ms, void **body, uint32_t *len) {
    struct timespec fin;
    clock_gettime(CLOCK_REALTIME, &fin);
    fin.tv_sec += timeout_ms / 1000;
    fin.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (fin.tv_nsec >= 1000000000L) { fin.tv_sec++; fin.tv_nsec -= 1000000000L; }

    pthread_mutex_lock(&m);
    pend_t *p = NULL;
    for (pend_t *q = lista; q; q = q->next) if (q->req == req) p = q;
    int rc = 0;
    while (p && !p->listo) {
        /* despierta cada 200 ms para ver si el nodo se cayó */
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 200 * 1000000L;
        if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
        if (ts.tv_sec > fin.tv_sec || (ts.tv_sec == fin.tv_sec && ts.tv_nsec > fin.tv_nsec)) ts = fin;
        int r = pthread_cond_timedwait(&cv, &m, &ts);
        if (p->listo) break;
        if (membership_status(de) == NODE_OFFLINE) { rc = DSM_ERR_NODE_DOWN; break; }
        if (r != 0 && ts.tv_sec == fin.tv_sec && ts.tv_nsec == fin.tv_nsec) { rc = DSM_ERR_NODE_DOWN; break; }
    }
    if (p) sacar(req);
    pthread_mutex_unlock(&m);
    if (!p) return DSM_ERR_INVAL;

    if (rc == 0 && p->error) rc = DSM_ERR_NET;
    if (rc == 0) { *body = p->body; if (len) *len = p->len; p->body = NULL; }
    free(p->body);
    free(p);
    return rc;
}

/* handler de red para todas las respuestas: guarda una copia del cuerpo */
void espera_respuesta(const msg_hdr_t *h, const void *body) {
    pthread_mutex_lock(&m);
    for (pend_t *p = lista; p; p = p->next) {
        if (p->req != h->req_id || p->listo) continue;
        p->len = h->length;
        p->body = malloc(h->length ? h->length : 1);
        if (h->length) memcpy(p->body, body, h->length);
        p->error = (h->flags & F_ERROR) != 0;
        p->listo = 1;
        pthread_cond_broadcast(&cv);
        break;
    }
    pthread_mutex_unlock(&m);
}
