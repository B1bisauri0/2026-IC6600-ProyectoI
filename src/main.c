#define _GNU_SOURCE
#include "config.h"
#include "membership.h"
#include "network.h"
#include "query.h"
#include "sync.h"
#include "dsm.h"

#include <getopt.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static dsm_config_t cfg;
static node_id_t    self;


/* Cada nodo toma el lock 0 K veces. Nunca deben verse dos "DENTRO" seguidos
 * de nodos distintos sin un "SALE" en medio. */
static void *demo_lock(void *arg) {
    int k = (int)(intptr_t)arg;
    sleep(3);   /* dar tiempo a que los demás nodos se conecten */
    for (int i = 0; i < k; i++) {
        if (sync_lock(0) != 0) { printf("[nodo %u] el gestor del lock no responde\n", self); return NULL; }
        printf("[nodo %u] DENTRO  (vuelta %d)\n", self, i + 1); fflush(stdout);
        usleep(300 * 1000);
        printf("[nodo %u] SALE\n", self); fflush(stdout);
        sync_unlock(0);
        usleep(100 * 1000);
    }
    printf("[nodo %u] terminé la demostración del lock\n", self);
    return NULL;
}

/* Todos los nodos pasan K barreras juntos */
static void *demo_barrier(void *arg) {
    int k = (int)(intptr_t)arg;
    sleep(3);
    for (int i = 0; i < k; i++) {
        usleep((useconds_t)(rand() % 1000) * 1000);   /* cada uno llega a otra hora */
        printf("[nodo %u] llego a la barrera %d\n", self, i + 1); fflush(stdout);
        if (sync_barrier(1, cfg.n_peers) != 0) { printf("[nodo %u] el coordinador no responde\n", self); return NULL; }
        printf("[nodo %u] paso la barrera %d\n", self, i + 1); fflush(stdout);
    }
    return NULL;
}

static void usage(const char *prog) {
    fprintf(stderr, "Uso: %s --conf ARCHIVO [--id N] [--demo-lock K] [--demo-barrier K]\n", prog);
    exit(2);
}

int main(int argc, char **argv) {
    const char *conf = NULL;
    int id = -1, demo_l = 0, demo_b = 0;
    static struct option opts[] = {
        { "conf", required_argument, 0, 'c' },
        { "id", required_argument, 0, 'i' },
        { "demo-lock", required_argument, 0, 'l' },
        { "demo-barrier", required_argument, 0, 'b' },
        { 0, 0, 0, 0 },
    };
    int o;
    while ((o = getopt_long(argc, argv, "c:i:l:b:", opts, NULL)) != -1) {
        switch (o) {
        case 'c': conf = optarg; break;
        case 'i': id = atoi(optarg); break;
        case 'l': demo_l = atoi(optarg); break;
        case 'b': demo_b = atoi(optarg); break;
        default: usage(argv[0]);
        }
    }
    if (!conf) usage(argv[0]);

    int self_i;
    if (config_load(conf, id, &cfg, &self_i) != DSM_OK) return 1;
    self = (node_id_t)self_i;

    /* Bloquear SIGINT/SIGTERM en todos los hilos.main los espera con sigwait */
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &set, NULL);

    dsm_ctx_t *ctx;
    if (dsm_init(conf, id, &ctx) != DSM_OK) return 1;
    if (query_start(&cfg, self) < 0) return 1;

    printf("[nodo %u] escuchando en %s:%u (datos), consultas en el puerto %u. %d nodos configurados.\n",
           self, cfg.peers[self].host, cfg.peers[self].port_base,
           cfg.peers[self].port_base + 2, cfg.n_peers);
    fflush(stdout);

    srand((unsigned)(getpid() ^ self));
    pthread_t t;
    if (demo_l) { pthread_create(&t, NULL, demo_lock, (void *)(intptr_t)demo_l); pthread_detach(t); }
    if (demo_b) { pthread_create(&t, NULL, demo_barrier, (void *)(intptr_t)demo_b); pthread_detach(t); }

    int sig;
    sigwait(&set, &sig);
    printf("\n[nodo %u] saliendo...\n", self);
    dsm_shutdown(ctx);
    return 0;
}
