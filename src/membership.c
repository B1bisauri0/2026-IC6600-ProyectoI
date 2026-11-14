/* membership.c - quién está en el clúster y quién sigue vivo.
 *
 *  - Al conectarse, el que llama envía JOIN_REQUEST (lo hace network.c) y el
 *    que acepta responde JOIN_ACK con su memoria aportada y usada.
 *  - Cada heartbeat_ms, un hilo envía HEARTBEAT a todos con la memoria usada,
 *    y revisa cuánto hace que no oye a cada nodo:
 *      1 latido perdido                -> SUSPECT
 *      heartbeat_miss_max perdidos     -> OFFLINE
 *    Si la conexión TCP se cierra, el nodo pasa a OFFLINE de inmediato.
 *  - Cuando un nodo pasa a OFFLINE se avisa a la parte de memoria con
 *    dsm_on_node_down (siempre FUERA del lock, porque ella llama a
 *    membership_status y si no se queda trabado).
 *  - La memoria usada la da la parte de memoria: dsm_local_mem_used_bytes().
 */
#define _GNU_SOURCE
#include "membership.h"
#include "network.h"

#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    uint8_t  status;
    uint8_t  missed_hb;
    uint64_t mem_bytes;
    uint64_t mem_used;
    uint64_t last_hb_ms;
} node_state_t;

/* ---------------- mensajes ---------------- */
typedef struct __attribute__((packed)) { uint64_t mem_bytes; uint64_t mem_used; } body_mem_t;
typedef struct __attribute__((packed)) { uint64_t ts; uint64_t mem_used; } body_hb_t;

/* ---------------- estado global ---------------- */
static const dsm_config_t *cfg;
static node_id_t          self_id;
static node_state_t       nodes[MAX_PEERS];
static pthread_mutex_t    lock = PTHREAD_MUTEX_INITIALIZER;

static uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

const char *node_status_str(int s) {
    return s == NODE_ONLINE ? "ONLINE" : s == NODE_SUSPECT ? "SUSPECT" : "OFFLINE";
}

/* cambia el estado; devuelve 1 si el nodo se acaba de caer (para avisar luego) */
static int set_status(node_id_t id, uint8_t st) {
    int cayo = nodes[id].status != NODE_OFFLINE && st == NODE_OFFLINE;
    if (nodes[id].status != st)
        fprintf(stderr, "[nodo %u] el nodo %u pasa a %s\n", self_id, id, node_status_str(st));
    nodes[id].status = st;
    return cayo;
}

/* Cualquier mensaje de un nodo es prueba de que está vivo */
static void heard_from(node_id_t id) {
    nodes[id].last_hb_ms = now_ms();
    nodes[id].missed_hb = 0;
    set_status(id, NODE_ONLINE);
}

static void send_mem(node_id_t dst, uint8_t type, uint16_t flags) {
    body_mem_t b = { htobe64(cfg->node_mem_bytes), htobe64(dsm_local_mem_used_bytes()) };
    net_send(dst, type, flags, 0, &b, sizeof b);
}

/* ---------------- handlers ---------------- */

static void on_join_request(const msg_hdr_t *h, const void *body) {
    (void)body;
    pthread_mutex_lock(&lock);
    heard_from(h->src);
    pthread_mutex_unlock(&lock);
    /* respondemos con nuestra memoria; el otro contesta con NODE_ANNOUNCE */
    send_mem(h->src, MSG_JOIN_ACK, F_REPLY);
}

static void on_join_ack(const msg_hdr_t *h, const void *body) {
    const body_mem_t *b = body;
    pthread_mutex_lock(&lock);
    if (h->length >= sizeof *b) {
        nodes[h->src].mem_bytes = be64toh(b->mem_bytes);
        nodes[h->src].mem_used = be64toh(b->mem_used);
    }
    heard_from(h->src);
    pthread_mutex_unlock(&lock);
    /* respondemos con nuestros datos para que el otro también los tenga */
    send_mem(h->src, MSG_NODE_ANNOUNCE, 0);
}

static void on_node_announce(const msg_hdr_t *h, const void *body) {
    const body_mem_t *b = body;
    pthread_mutex_lock(&lock);
    if (h->length >= sizeof *b) {
        nodes[h->src].mem_bytes = be64toh(b->mem_bytes);
        nodes[h->src].mem_used = be64toh(b->mem_used);
    }
    heard_from(h->src);
    pthread_mutex_unlock(&lock);
}

static void on_heartbeat(const msg_hdr_t *h, const void *body) {
    const body_hb_t *b = body;
    pthread_mutex_lock(&lock);
    if (h->length >= sizeof *b) nodes[h->src].mem_used = be64toh(b->mem_used);
    heard_from(h->src);
    pthread_mutex_unlock(&lock);
}

static void on_leave(const msg_hdr_t *h, const void *body) {
    (void)body;
    pthread_mutex_lock(&lock);
    int cayo = set_status(h->src, NODE_OFFLINE);
    pthread_mutex_unlock(&lock);
    if (cayo) dsm_on_node_down(h->src);
}

static void on_connection(node_id_t id, int up) {
    if (up) return;                      /* ONLINE se marca al recibir su primer mensaje */
    pthread_mutex_lock(&lock);
    int cayo = set_status(id, NODE_OFFLINE);
    pthread_mutex_unlock(&lock);
    if (cayo) dsm_on_node_down(id);
}

/* ---------------- hilo de heartbeat ---------------- */

static void *heartbeat_main(void *arg) {
    (void)arg;
    for (;;) {
        usleep((useconds_t)cfg->heartbeat_ms * 1000);
        body_hb_t b = { htobe64(now_ms()), htobe64(dsm_local_mem_used_bytes()) };
        uint64_t now = now_ms();

        pthread_mutex_lock(&lock);
        nodes[self_id].mem_used = dsm_local_mem_used_bytes();
        pthread_mutex_unlock(&lock);

        for (int i = 0; i < cfg->n_peers; i++) {
            if (i == self_id) continue;
            net_send((node_id_t)i, MSG_HEARTBEAT, 0, 0, &b, sizeof b);

            int cayo = 0;
            pthread_mutex_lock(&lock);
            node_state_t *n = &nodes[i];
            if (n->status != NODE_OFFLINE && n->last_hb_ms) {
                /* latidos perdidos, con medio intervalo de tolerancia para que
                 * un latido que llega un poco tarde no cuente como perdido */
                uint64_t hb = (uint64_t)cfg->heartbeat_ms, age = now - n->last_hb_ms;
                uint64_t missed = age > hb / 2 ? (age - hb / 2) / hb : 0;
                n->missed_hb = missed > 255 ? 255 : (uint8_t)missed;
                if (n->missed_hb >= cfg->heartbeat_miss_max) cayo = set_status((node_id_t)i, NODE_OFFLINE);
                else if (n->missed_hb >= 1) set_status((node_id_t)i, NODE_SUSPECT);
            }
            pthread_mutex_unlock(&lock);
            if (cayo) dsm_on_node_down((node_id_t)i);   /* afuera del lock */
        }
    }
    return NULL;
}

/* ---------------- interfaz ---------------- */

int membership_start(const dsm_config_t *c, int self) {
    cfg = c;
    self_id = (node_id_t)self;
    for (int i = 0; i < cfg->n_peers; i++) nodes[i].status = NODE_OFFLINE;
    nodes[self].status = NODE_ONLINE;
    nodes[self].mem_bytes = cfg->node_mem_bytes;

    net_register_handler(MSG_JOIN_REQUEST, on_join_request);
    net_register_handler(MSG_JOIN_ACK, on_join_ack);
    net_register_handler(MSG_NODE_ANNOUNCE, on_node_announce);
    net_register_handler(MSG_HEARTBEAT, on_heartbeat);
    net_register_handler(MSG_LEAVE, on_leave);
    net_on_connection(on_connection);

    pthread_t t;
    pthread_create(&t, NULL, heartbeat_main, NULL);
    pthread_detach(t);
    return 0;
}

int membership_snapshot(dsm_node_status_t *out, int max) {
    uint64_t now = now_ms();
    int n = cfg->n_peers < max ? cfg->n_peers : max;
    pthread_mutex_lock(&lock);
    nodes[self_id].mem_used = dsm_local_mem_used_bytes();
    for (int i = 0; i < n; i++) {
        node_state_t *s = &nodes[i];
        dsm_node_status_t *o = &out[i];
        o->id = i;
        snprintf(o->host, sizeof o->host, "%s", cfg->peers[i].host);
        o->port_base = cfg->peers[i].port_base;
        o->status = s->status;
        o->mem_bytes = s->mem_bytes;
        o->mem_used = s->mem_used;
        o->mem_free = s->mem_bytes > s->mem_used ? s->mem_bytes - s->mem_used : 0;
        o->last_hb_age_ms = i == self_id ? 0 : s->last_hb_ms ? now - s->last_hb_ms : UINT64_MAX;
    }
    pthread_mutex_unlock(&lock);
    return n;
}

/* lo que pide la parte de memoria: estado de un nodo */
node_status_t membership_status(node_id_t id) {
    if (id >= cfg->n_peers) return NODE_OFFLINE;
    pthread_mutex_lock(&lock);
    node_status_t st = (node_status_t)nodes[id].status;
    pthread_mutex_unlock(&lock);
    return st;
}

/* ids que están ONLINE, de menor a mayor (incluye este nodo) */
int membership_online_ids(node_id_t *out, int max) {
    int k = 0;
    pthread_mutex_lock(&lock);
    for (int i = 0; i < cfg->n_peers && k < max; i++)
        if (nodes[i].status == NODE_ONLINE) out[k++] = (node_id_t)i;
    pthread_mutex_unlock(&lock);
    return k;
}

/* copia la entrada de un nodo en el formato de ella (node_info_t) */
int membership_get(node_id_t id, node_info_t *out) {
    if (id >= cfg->n_peers) return -1;
    memset(out, 0, sizeof *out);
    pthread_mutex_lock(&lock);
    if (id == self_id) nodes[id].mem_used = dsm_local_mem_used_bytes();
    node_state_t *s = &nodes[id];
    out->id = id;
    out->status = s->status;
    out->missed_hb = s->missed_hb;
    snprintf(out->host, sizeof out->host, "%s", cfg->peers[id].host);
    out->port_base = cfg->peers[id].port_base;
    out->mem_bytes = s->mem_bytes;
    out->mem_used = s->mem_used;
    out->last_hb_ms = s->last_hb_ms;
    out->fd = -1;                        /* el socket lo maneja network.c */
    pthread_mutex_unlock(&lock);
    return 0;
}

/* antes se llamaba membership_leave: avisa a todos que nos vamos */
void membership_stop(void) {
    uint16_t id = htons(self_id);
    for (int i = 0; i < cfg->n_peers; i++)
        if (i != self_id) net_send((node_id_t)i, MSG_LEAVE, 0, 0, &id, sizeof id);
}