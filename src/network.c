/* network.c - conexiones TCP persistentes entre cada par de nodos.
 *
 * Cómo funciona:
 *  - Cada nodo escucha en su puerto_base (hilo listener).
 *  - Para no abrir dos conexiones por par, el nodo de id MAYOR llama al de id
 *    menor (hilo connector, reintenta cada 500 ms hasta que el otro arranque).
 *  - El primer mensaje de toda conexión es JOIN_REQUEST con el hash de la
 *    configuración: así el que acepta sabe quién llamó y si usa el mismo archivo.
 *  - Por cada conexión hay un hilo lector que solo lee mensajes completos y los
 *    pone en una cola. Los hilos del pool de servicio los sacan y llaman al
 *    handler registrado para ese tipo. Así un lector nunca se bloquea procesando
 *    (regla 4 de la estrategia anti-deadlock).
 */
#define _GNU_SOURCE
#include "network.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* ---------------- estado ---------------- */

typedef struct {
    int             fd;          /* -1 = sin conexión */
    pthread_mutex_t send_lock;   /* para que dos hilos no mezclen sus bytes */
} conn_t;

typedef struct work {
    msg_hdr_t    hdr;            /* ya en orden de la máquina */
    void        *body;
    struct work *next;
} work_t;

static const dsm_config_t *cfg;
static node_id_t          self_id;
static uint32_t           my_epoch;
static conn_t             conns[MAX_PEERS];
static pthread_mutex_t    conns_lock = PTHREAD_MUTEX_INITIALIZER;
static msg_handler_t      handlers[256];
static net_conn_cb_t      conn_cb;
static volatile int       running;
static int                listen_fd = -1;

/* cola de mensajes recibidos (lectores -> pool) */
static work_t          *q_head, *q_tail;
static pthread_mutex_t  q_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t   q_cond = PTHREAD_COND_INITIALIZER;

/* ---------------- envío y recepción completos ---------------- */

int send_all(int fd, const void *buf, size_t n) {
    const char *p = buf;
    while (n > 0) {
        ssize_t w = send(fd, p, n, MSG_NOSIGNAL);   /* sin SIGPIPE si el otro cerró */
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) return -1;
        p += w; n -= (size_t)w;
    }
    return 0;
}

int recv_all(int fd, void *buf, size_t n) {
    char *p = buf;
    while (n > 0) {
        ssize_t r = recv(fd, p, n, 0);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return -1;                      /* 0 = el otro cerró */
        p += r; n -= (size_t)r;
    }
    return 0;
}

/* ---------------- encabezado: máquina <-> red ---------------- */

static void hdr_to_net(msg_hdr_t *h) {
    h->magic = htonl(h->magic);
    h->flags = htons(h->flags);
    h->src = htons(h->src);
    h->dst = htons(h->dst);
    h->length = htonl(h->length);
    h->req_id = htobe64(h->req_id);
    h->epoch = htonl(h->epoch);
}

static void hdr_to_host(msg_hdr_t *h) {
    h->magic = ntohl(h->magic);
    h->flags = ntohs(h->flags);
    h->src = ntohs(h->src);
    h->dst = ntohs(h->dst);
    h->length = ntohl(h->length);
    h->req_id = be64toh(h->req_id);
    h->epoch = ntohl(h->epoch);
}

/* ---------------- interfaz pública (protocol.h) ---------------- */

node_id_t net_self(void)      { return self_id; }
int       net_num_peers(void) { return cfg->n_peers; }
uint32_t  net_epoch(void)     { return my_epoch; }

void net_register_handler(uint8_t type, msg_handler_t fn) { handlers[type] = fn; }
void net_on_connection(net_conn_cb_t cb) { conn_cb = cb; }

int net_is_connected(node_id_t id) {
    if (id >= cfg->n_peers) return 0;
    pthread_mutex_lock(&conns_lock);
    int up = conns[id].fd >= 0;
    pthread_mutex_unlock(&conns_lock);
    return up;
}

/* Envía encabezado + cuerpo + (opcional) una página, todo seguido. */
static int send_msg(node_id_t dst, uint8_t type, uint16_t flags, req_id_t req_id,
                    const void *body, uint32_t len, const void *page, uint32_t page_len) {
    if (dst >= cfg->n_peers || dst == self_id) return -1;

    msg_hdr_t h = {
        .magic = DSM_MAGIC, .version = DSM_PROTO_VER, .type = type,
        .flags = flags, .src = self_id, .dst = dst,
        .length = len + page_len, .req_id = req_id, .epoch = my_epoch,
    };
    hdr_to_net(&h);

    conn_t *c = &conns[dst];
    pthread_mutex_lock(&c->send_lock);
    int fd = c->fd, rc = -1;
    if (fd >= 0 &&
        send_all(fd, &h, sizeof h) == 0 &&
        (len == 0 || send_all(fd, body, len) == 0) &&
        (page_len == 0 || send_all(fd, page, page_len) == 0))
        rc = 0;
    pthread_mutex_unlock(&c->send_lock);
    return rc;
}

int net_send(node_id_t dst, uint8_t type, uint16_t flags, req_id_t req_id,
             const void *body, uint32_t len) {
    return send_msg(dst, type, flags, req_id, body, len, NULL, 0);
}

int net_send_page(node_id_t dst, uint8_t type, uint16_t flags, req_id_t req_id,
                  const void *body, uint32_t len, const void *page) {
    return send_msg(dst, type, flags | F_PAYLOAD, req_id, body, len, page, cfg->page_size);
}

/* ---------------- pool de servicio ---------------- */

static void enqueue(const msg_hdr_t *h, void *body) {
    work_t *w = malloc(sizeof *w);
    w->hdr = *h;
    w->body = body;
    w->next = NULL;
    pthread_mutex_lock(&q_lock);
    if (q_tail) q_tail->next = w; else q_head = w;
    q_tail = w;
    pthread_cond_signal(&q_cond);
    pthread_mutex_unlock(&q_lock);
}

static void *worker_main(void *arg) {
    (void)arg;
    for (;;) {
        pthread_mutex_lock(&q_lock);
        while (!q_head && running) pthread_cond_wait(&q_cond, &q_lock);
        if (!q_head) { pthread_mutex_unlock(&q_lock); break; }
        work_t *w = q_head;
        q_head = w->next;
        if (!q_head) q_tail = NULL;
        pthread_mutex_unlock(&q_lock);

        msg_handler_t fn = handlers[w->hdr.type];
        if (fn) fn(&w->hdr, w->body);
        else fprintf(stderr, "net: mensaje tipo %u de nodo %u sin handler\n", w->hdr.type, w->hdr.src);
        free(w->body);
        free(w);
    }
    return NULL;
}

/* ---------------- conexiones ---------------- */

typedef struct { int fd; int peer; } reader_arg_t;

/* Registra fd como la conexión con "peer". Cierra una anterior si existía. */
static void conn_set(node_id_t peer, int fd) {
    pthread_mutex_lock(&conns_lock);
    pthread_mutex_lock(&conns[peer].send_lock);
    int old = conns[peer].fd;
    conns[peer].fd = fd;
    pthread_mutex_unlock(&conns[peer].send_lock);
    pthread_mutex_unlock(&conns_lock);
    if (old >= 0 && old != fd) shutdown(old, SHUT_RDWR);
}

static void conn_drop(node_id_t peer, int fd) {
    int dropped = 0;
    pthread_mutex_lock(&conns_lock);
    pthread_mutex_lock(&conns[peer].send_lock);
    if (conns[peer].fd == fd) { conns[peer].fd = -1; dropped = 1; }
    pthread_mutex_unlock(&conns[peer].send_lock);
    pthread_mutex_unlock(&conns_lock);
    close(fd);
    if (dropped && conn_cb) conn_cb(peer, 0);
}

/* Cuerpo del primer mensaje de cada conexión */
typedef struct __attribute__((packed)) { uint32_t cfg_hash; uint16_t port_base; } body_hello_t;

static void send_hello(int fd, node_id_t dst) {
    body_hello_t b = { htonl(config_hash(cfg)), htons(cfg->peers[self_id].port_base) };
    msg_hdr_t h = {
        .magic = DSM_MAGIC, .version = DSM_PROTO_VER, .type = MSG_JOIN_REQUEST,
        .src = self_id, .dst = dst, .length = sizeof b, .epoch = my_epoch,
    };
    hdr_to_net(&h);
    send_all(fd, &h, sizeof h);
    send_all(fd, &b, sizeof b);
}

/* Lee un mensaje completo. Devuelve 0 o -1 si la conexión se cayó o es inválida. */
static int read_msg(int fd, msg_hdr_t *h, void **body) {
    if (recv_all(fd, h, sizeof *h) < 0) return -1;
    hdr_to_host(h);
    if (h->magic != DSM_MAGIC) { fprintf(stderr, "net: magic inválido, cierro la conexión\n"); return -1; }
    if (h->length > (16u << 20)) { fprintf(stderr, "net: mensaje demasiado grande\n"); return -1; }
    *body = NULL;
    if (h->length) {
        *body = malloc(h->length);
        if (recv_all(fd, *body, h->length) < 0) { free(*body); return -1; }
    }
    return 0;
}

static void *reader_main(void *arg) {
    reader_arg_t a = *(reader_arg_t *)arg;
    free(arg);
    int peer = a.peer;   /* -1 si todavía no sabemos quién es (conexión aceptada) */

    for (;;) {
        msg_hdr_t h;
        void *body;
        if (read_msg(a.fd, &h, &body) < 0) break;

        if (peer < 0) {
            /* primer mensaje de una conexión aceptada: debe ser el saludo */
            if (h.type != MSG_JOIN_REQUEST || h.src >= cfg->n_peers || h.src == self_id ||
                h.length < sizeof(body_hello_t)) {
                fprintf(stderr, "net: conexión sin saludo válido, la cierro\n");
                free(body);
                close(a.fd);
                return NULL;
            }
            body_hello_t *b = body;
            if (ntohl(b->cfg_hash) != config_hash(cfg)) {
                fprintf(stderr, "net: el nodo %u usa otra configuración (E_CONFIG_MISMATCH)\n", h.src);
                body_error_t e = { htons(E_CONFIG_MISMATCH), 0 };
                msg_hdr_t eh = { .magic = DSM_MAGIC, .version = DSM_PROTO_VER, .type = MSG_ERROR,
                                 .flags = F_REPLY | F_ERROR, .src = self_id, .dst = h.src,
                                 .length = sizeof e, .epoch = my_epoch };
                hdr_to_net(&eh);
                send_all(a.fd, &eh, sizeof eh);
                send_all(a.fd, &e, sizeof e);
                free(body);
                close(a.fd);
                return NULL;
            }
            peer = h.src;
            conn_set((node_id_t)peer, a.fd);
            if (conn_cb) conn_cb((node_id_t)peer, 1);
        }
        enqueue(&h, body);
    }
    if (peer >= 0) conn_drop((node_id_t)peer, a.fd);
    else close(a.fd);
    return NULL;
}

static void start_reader(int fd, int peer) {
    reader_arg_t *a = malloc(sizeof *a);
    a->fd = fd;
    a->peer = peer;
    pthread_t t;
    pthread_create(&t, NULL, reader_main, a);
    pthread_detach(t);
}

static void *listener_main(void *arg) {
    (void)arg;
    while (running) {
        int fd = accept(listen_fd, NULL, NULL);
        if (fd < 0) { if (errno == EINTR) continue; if (!running) break; continue; }
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        start_reader(fd, -1);
    }
    return NULL;
}

/* connect() con timeout. Devuelve el fd o -1. */
static int connect_to(const peer_t *p, int timeout_ms) {
    char port[8];
    snprintf(port, sizeof port, "%u", p->port_base);
    struct addrinfo hints = { .ai_family = AF_INET, .ai_socktype = SOCK_STREAM }, *res;
    if (getaddrinfo(p->host, port, &hints, &res) != 0) return -1;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    fcntl(fd, F_SETFL, O_NONBLOCK);
    int rc = connect(fd, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    if (rc < 0 && errno == EINPROGRESS) {
        struct pollfd pfd = { .fd = fd, .events = POLLOUT };
        int err = 0;
        socklen_t len = sizeof err;
        if (poll(&pfd, 1, timeout_ms) == 1 &&
            getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) == 0 && err == 0)
            rc = 0;
    }
    if (rc < 0) { close(fd); return -1; }
    fcntl(fd, F_SETFL, 0);   /* de vuelta a bloqueante */
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    return fd;
}

static void *connector_main(void *arg) {
    (void)arg;
    while (running) {
        for (int j = 0; j < self_id; j++) {          /* solo llamo a los de id menor */
            if (net_is_connected((node_id_t)j)) continue;
            int fd = connect_to(&cfg->peers[j], cfg->connect_timeout_ms);
            if (fd < 0) continue;                    /* todavía no arranca; reintento */
            send_hello(fd, (node_id_t)j);
            conn_set((node_id_t)j, fd);
            if (conn_cb) conn_cb((node_id_t)j, 1);
            start_reader(fd, j);
        }
        usleep(500 * 1000);
    }
    return NULL;
}

/* ---------------- arranque ---------------- */

void net_init(const dsm_config_t *c, node_id_t self) {
    cfg = c;
    self_id = self;
    my_epoch = (uint32_t)time(NULL);
    for (int i = 0; i < MAX_PEERS; i++) {
        conns[i].fd = -1;
        pthread_mutex_init(&conns[i].send_lock, NULL);
    }
}

int net_start(void) {

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(cfg->peers[self_id].port_base),
        .sin_addr.s_addr = htonl(INADDR_ANY),
    };
    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof addr) < 0 || listen(listen_fd, 16) < 0) {
        fprintf(stderr, "net: no pude escuchar en el puerto %u: %s\n",
                cfg->peers[self_id].port_base, strerror(errno));
        return -1;
    }

    running = 1;
    pthread_t t;
    pthread_create(&t, NULL, listener_main, NULL);  pthread_detach(t);
    pthread_create(&t, NULL, connector_main, NULL); pthread_detach(t);
    for (int i = 0; i < cfg->worker_threads; i++) {
        pthread_create(&t, NULL, worker_main, NULL);
        pthread_detach(t);
    }
    return 0;
}

void net_stop(void) {
    running = 0;
    if (listen_fd >= 0) shutdown(listen_fd, SHUT_RDWR);
    pthread_mutex_lock(&q_lock);
    pthread_cond_broadcast(&q_cond);
    pthread_mutex_unlock(&q_lock);
    for (int i = 0; i < cfg->n_peers; i++)
        if (conns[i].fd >= 0) shutdown(conns[i].fd, SHUT_RDWR);
}
