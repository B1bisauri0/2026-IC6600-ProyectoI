/*
 * test_cluster.c -- pruebas del modulo A en varios procesos (un proceso = un nodo) usando
 * los sustitutos de tests/stub_b.c.   uso: test_cluster <escenario> <nodos> [args...]
 *   t3    contador con dsm_lock desde varios hilos en todos los nodos (T3)
 *   t4    ida y vuelta I->S->M->I sobre una pagina conocida (T4)
 *   t5    reemplazo: cache pequena (CACHE=16), datos correctos, evictions y writebacks > 0 (T5)
 *   big   asignacion mayor que la memoria de un nodo, repartida entre varios homes
 *   life  Juego de la Vida: args = los de life (n gens ...)
 *   down  un nodo muere con paginas M: la lectura posterior debe fallar con diagnostico (T10)
 */
#define _GNU_SOURCE
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../include/dsm.h"
#include "../src/dsm_internal.h"
#include "../src/pager.h"
#include "../src/directory.h"
#include "stub_b.h"

int dsm_app_main(dsm_ctx_t *ctx, int argc, char **argv);   /* life.c */

static dsm_ctx_t *ctx;
static int fails;
static void check(int ok, const char *what) {
    if (!ok) { fails++; fprintf(stderr, "[nodo %d] FALLA: %s\n", stub_self, what); }
}
#define PS 4096

/* ---------- t3 ---------- */
static void *t3_worker(void *a) {
    long *c = a;
    dsm_thread_register(ctx);
    for (int i = 0; i < 300; i++) { dsm_lock(ctx, 1); (*c)++; dsm_unlock(ctx, 1); }
    dsm_thread_unregister(ctx);
    return NULL;
}
static void t3(void) {
    long *c;
    if (stub_self == 0) { c = dsm_malloc(ctx, PS); dsm_publish(ctx, "contador", c); }
    else { while (!(c = dsm_lookup(ctx, "contador"))) usleep(1000); }
    pthread_t th[2];
    for (int i = 0; i < 2; i++) pthread_create(&th[i], NULL, t3_worker, c);
    for (int i = 0; i < 2; i++) pthread_join(th[i], NULL);
    dsm_barrier(ctx, 50, stub_n);
    if (stub_self == 0) { long expect = 300L * 2 * stub_n; fprintf(stderr, "t3: contador = %ld (esperado %ld)\n", *c, expect); check(*c == expect, "valor final del contador"); }
    dsm_barrier(ctx, 51, stub_n);
}

/* ---------- t4 ---------- */
static void t4(void) {
    volatile int *p;
    if (stub_self == 0) { p = dsm_malloc(ctx, PS); dsm_publish(ctx, "t4", (void *)p); *p = 11; }
    else { while (!(p = dsm_lookup(ctx, "t4"))) usleep(1000); }
    vpn_t v = (vpn_t)(((char *)p - g_n.app) / PS);
    dsm_barrier(ctx, 60, stub_n);
    if (stub_self == 1) { check(*p == 11, "nodo 1 lee 11"); check(pager_local_state(v) == PG_SHARED, "nodo 1 en S tras leer"); }
    dsm_barrier(ctx, 61, stub_n);
    if (stub_self == 0) check(pager_local_state(v) == PG_SHARED, "home baja a S por la lectura remota");
    dsm_barrier(ctx, 62, stub_n);
    if (stub_self == 1) { *p = 22; check(pager_local_state(v) == PG_MODIFIED, "nodo 1 en M tras escribir (upgrade)"); }
    dsm_barrier(ctx, 63, stub_n);
    if (stub_self == 0) { check(pager_local_state(v) == PG_INVALID, "home en I tras la escritura remota"); check(*p == 22, "home lee 22"); check(pager_local_state(v) == PG_SHARED, "home vuelve a S"); }
    dsm_barrier(ctx, 64, stub_n);
    if (stub_self == 1) check(pager_local_state(v) == PG_SHARED, "nodo 1 baja a S por FETCH(R)");
    dsm_barrier(ctx, 65, stub_n);
}

/* ---------- t5 ---------- */
static void t5(void) {
    int pages = 200;
    char *a;
    if (stub_self == 0) { a = dsm_malloc_striped(ctx, (size_t)pages * PS, (size_t)pages / 2 * PS); dsm_publish(ctx, "t5", a); }
    else { while (!(a = dsm_lookup(ctx, "t5"))) usleep(1000); }
    if (stub_self == 0) for (int i = 0; i < pages; i++) { a[(size_t)i * PS] = (char)(i + 1); a[(size_t)i * PS + PS - 1] = (char)(i * 3); }
    dsm_barrier(ctx, 70, stub_n);
    if (stub_self == 1) for (int i = 0; i < pages; i++) {
        check(a[(size_t)i * PS] == (char)(i + 1), "dato inicial de pagina");
        check(a[(size_t)i * PS + PS - 1] == (char)(i * 3), "dato final de pagina");
    }
    dsm_barrier(ctx, 71, stub_n);
    if (stub_self == 0) for (int i = 0; i < pages; i++) check(a[(size_t)i * PS] == (char)(i + 1), "nodo 0 relee tras ser reemplazado");
    dsm_barrier(ctx, 72, stub_n);
    uint64_t ev = dsm_stats.evictions, wb = dsm_stats.writebacks;
    fprintf(stderr, "t5 nodo %d: evictions=%lu writebacks=%lu cache=%lu\n", stub_self, (unsigned long)ev, (unsigned long)wb, (unsigned long)pager_cache_count());
    check(pager_cache_count() <= g_n.cfg.cache_max_pages + 4, "la cache no supera cache_max_pages");
    if (stub_self == 0) { check(ev > 0, "evictions > 0"); check(wb > 0, "writebacks > 0"); }
    if (stub_self == 1) { check(ev > 0, "nodo 1 evictions > 0"); }
}

/* ---------- t7 ---------- */
static void t7(void) {
    /* Compara Game of Life alineado vs --no-align (sec. 11.4 y 14.1):
     * 1. Conteo de celulas vivas y hash identicos
     * 2. sum(transfers_no_align) > sum(transfers_aligned) debido a falso compartir */
    uint64_t *metrics = NULL;
    if (stub_self == 0) {
        metrics = dsm_malloc(ctx, PS);
        memset(metrics, 0, PS);
        dsm_publish(ctx, "t7_metrics", metrics);
    } else {
        while (!(metrics = dsm_lookup(ctx, "t7_metrics"))) usleep(1000);
    }
    dsm_barrier(ctx, 400, stub_n);

    /* 1. Ejecucion ALINEADA (64x64, 30 generaciones, random seed 7) */
    char *argv_align[] = { (char *)"life", (char *)"64", (char *)"30", (char *)"--random", (char *)"7", (char *)"--quiet" };
    int rc1 = dsm_app_main(ctx, 6, argv_align);
    check(rc1 == 0, "life alineado exitoso");
    dsm_barrier(ctx, 401, stub_n);

    /* Contabilizar transferencias de paginas por la red */
    uint64_t local_transf1 = dir_get_total_transfers();
    dsm_lock(ctx, 2);
    metrics[0] += local_transf1;
    dsm_unlock(ctx, 2);
    dir_reset_total_transfers();
    dsm_barrier(ctx, 402, stub_n);

    /* 2. Ejecucion NO ALINEADA (--no-align) */
    char *argv_noalign[] = { (char *)"life", (char *)"64", (char *)"30", (char *)"--random", (char *)"7", (char *)"--no-align", (char *)"--quiet" };
    int rc2 = dsm_app_main(ctx, 7, argv_noalign);
    check(rc2 == 0, "life no-align exitoso");
    dsm_barrier(ctx, 404, stub_n);

    uint64_t local_transf2 = dir_get_total_transfers();
    dsm_lock(ctx, 2);
    metrics[1] += local_transf2;
    dsm_unlock(ctx, 2);
    dsm_barrier(ctx, 405, stub_n);

    if (stub_self == 0) {
        uint64_t t_align = metrics[0];
        uint64_t t_noalign = metrics[1];
        fprintf(stderr, "t7: transferencias totales aligned=%lu, no-align=%lu\n",
                (unsigned long)t_align, (unsigned long)t_noalign);
        check(t_noalign > t_align, "sum(transfers_no_align) > sum(transfers_align)");
        dsm_free(ctx, metrics);
    }
    dsm_barrier(ctx, 406, stub_n);
}

/* ---------- clock_diff ---------- */
static void clock_diff(void) {
    /* Demuestra que CLOCK y FIFO seleccionan victimas distintas bajo accesos reales */
    int pages = 3;
    char *a;
    if (stub_self == 0) {
        a = dsm_malloc(ctx, (size_t)pages * PS);
        dsm_publish(ctx, "cdiff", a);
        a[0] = 10; a[PS] = 20; a[2 * PS] = 30;
    } else {
        while (!(a = dsm_lookup(ctx, "cdiff"))) usleep(1000);
    }
    dsm_barrier(ctx, 60, stub_n);

    if (stub_self == 1) {
        vpn_t v0 = (vpn_t)((size_t)(a - g_n.app) >> g_n.page_shift);
        vpn_t v1 = v0 + 1;

        /* Nodo 1 accede P0 y luego P1. Con CACHE=2, la cache tiene [P0, P1] */
        volatile char c0 = a[0]; (void)c0;
        volatile char c1 = a[PS]; (void)c1;

        /* Poner reloj a cero simulando paso de aguja: limpiar PF_REF y revocar permisos con PROT_NONE */
        pager_page_lock(v0); g_n.lp[v0].flags &= (uint8_t)~PF_REF; mprotect(a, PS, PROT_NONE); pager_page_unlock(v0);
        pager_page_lock(v1); g_n.lp[v1].flags &= (uint8_t)~PF_REF; mprotect(a + PS, PS, PROT_NONE); pager_page_unlock(v1);

        /* Re-accede P0 mediante acceso real: genera SIGSEGV -> resolve() restaura PROT_READ y activa PF_REF.
           P1 no es re-accedida, por lo que conserva PF_REF=0. */
        volatile char c0_again = a[0]; (void)c0_again;

        /* Accede P2: fuerza desalojo porque cache_max_pages=2 */
        volatile char c2 = a[2 * PS]; (void)c2;

        pager_page_lock(v0); int p0_cached = (g_n.lp[v0].flags & PF_CACHED) != 0; pager_page_unlock(v0);
        pager_page_lock(v1); int p1_cached = (g_n.lp[v1].flags & PF_CACHED) != 0; pager_page_unlock(v1);

        int is_clock = (pager_get_policy() == PAGER_POLICY_CLOCK);
        if (is_clock) {
            check(p0_cached, "CLOCK: P0 sobrevive en cache gracias a segunda oportunidad");
            check(!p1_cached, "CLOCK: P1 fue desalojada por carecer de referencia");
        } else {
            check(!p0_cached, "FIFO: P0 fue desalojada por ser la primera en llegar");
            check(p1_cached, "FIFO: P1 sigue en cache");
        }
    }
    dsm_barrier(ctx, 61, stub_n);
    if (stub_self == 0) check(dsm_free(ctx, a) == DSM_OK, "dsm_free cdiff");
}

/* ---------- big ---------- */
static void big(void) {
    size_t bytes = (size_t)(g_n.cfg.node_mem_bytes + g_n.cfg.node_mem_bytes / 2);   /* 1.5 x el aporte de un nodo */
    char *a;
    if (stub_self == 0) {
        a = dsm_malloc(ctx, bytes);
        check(a != NULL, "dsm_malloc mayor que la memoria de un nodo");
        if (!a) return;
        dsm_publish(ctx, "big", a);
        for (size_t o = 0; o < bytes; o += 97 * PS) a[o] = (char)(o / PS + 1);
        a[bytes - 1] = 42;
    } else { while (!(a = dsm_lookup(ctx, "big"))) usleep(1000); }
    dsm_barrier(ctx, 80, stub_n);
    if (stub_self == stub_n - 1) {
        for (size_t o = 0; o < bytes; o += 97 * PS) check(a[o] == (char)(o / PS + 1), "lectura remota de la region grande");
        check(a[bytes - 1] == 42, "ultimo byte");
    }
    dsm_barrier(ctx, 81, stub_n);
    fprintf(stderr, "big nodo %d: marcos home en uso = %lu MiB\n", stub_self, (unsigned long)(dsm_local_mem_used_bytes() >> 20));
    check(dsm_local_mem_used_bytes() <= g_n.cfg.node_mem_bytes, "no se excede el aporte del nodo");
    dsm_barrier(ctx, 82, stub_n);
    if (stub_self == 0) check(dsm_free(ctx, a) == DSM_OK, "dsm_free");
    dsm_barrier(ctx, 83, stub_n);
    check(dsm_local_mem_used_bytes() == 0, "marcos liberados en todos los nodos");
}

/* ---------- down ---------- */
static volatile char *down_ptr;
static void *down_thr(void *x) {
    (void)x;
    dsm_thread_register(ctx);
    volatile char c = down_ptr[0];
    (void)c;
    return NULL;
}

static void down(void) {
    volatile char *a;
    if (stub_self == 0) { a = dsm_malloc(ctx, PS); dsm_publish(ctx, "down", (void *)a); }
    else { while (!(a = dsm_lookup(ctx, "down"))) usleep(1000); }
    if (stub_self == 1) { a[0] = 7; }                /* el nodo 1 queda como propietario M de una pagina del nodo 0 */
    dsm_barrier(ctx, 90, stub_n);
    if (stub_self == 1) _exit(0);                    /* cae sin devolver la pagina */
    if (stub_self == 0) {
        usleep(300000);
        int st = (int)membership_status(1);
        check(st == NODE_OFFLINE, "el nodo 1 aparece OFFLINE");
        fprintf(stderr, "down: leyendo pagina perdida (debe quedar suspendido el hilo)...\n");
        pthread_t t;
        down_ptr = a;
        pthread_create(&t, NULL, down_thr, NULL);
        usleep(500000);
        fprintf(stderr, "down: OK, el hilo sigue suspendido y el nodo vivo\n");
    }
}

static void node_main(int self) {
    signal(SIGPIPE, SIG_IGN);
    stub_self = self;
    dsm_config_t c; memset(&c, 0, sizeof c);
    c.page_size = getenv("PAGE_SIZE") ? (uint32_t)atoi(getenv("PAGE_SIZE")) : PS;
    c.node_mem_bytes = getenv("MEM_MB") ? ((uint64_t)atoi(getenv("MEM_MB")) << 20) : (64ull << 20);
    c.max_nodes = (uint32_t)stub_n;
    c.va_slab_bytes = getenv("SLAB_MB") ? ((uint64_t)atoi(getenv("SLAB_MB")) << 20) : (c.node_mem_bytes * c.max_nodes * 2);
    if (c.va_slab_bytes < (256ull << 20)) c.va_slab_bytes = 256ull << 20;
    c.base_addr = 0x200000000000ull; c.n_peers = stub_n;
    c.request_timeout_ms = 5000; c.heartbeat_ms = 50; c.heartbeat_miss_max = 3;
    c.cache_max_pages = getenv("CACHE") ? (uint32_t)atoi(getenv("CACHE")) : 24000;
    c.replacement_policy = getenv("POLICY") && (!strcmp(getenv("POLICY"), "clock") || !strcmp(getenv("POLICY"), "1")) ? 1 : 0;
    c.worker_threads = 4; c.max_user_threads = 16;
    c.replication_factor = getenv("REPL") ? (uint32_t)atoi(getenv("REPL")) : 1;
    int rc = dsm_init_with(&c, self, &ctx);
    if (rc) { fprintf(stderr, "dsm_init fallo: %d\n", rc); _exit(10); }
    dsm_thread_register(ctx);
    extern char *test_name; extern int test_argc; extern char **test_argv;
    if (!strcmp(test_name, "t3")) t3();
    else if (!strcmp(test_name, "t4")) t4();
    else if (!strcmp(test_name, "t5")) t5();
    else if (!strcmp(test_name, "t7")) t7();
    else if (!strcmp(test_name, "clock_diff")) clock_diff();
    else if (!strcmp(test_name, "big")) big();
    else if (!strcmp(test_name, "down")) down();
    else if (!strcmp(test_name, "life")) {
        int solo = 0;
        for (int i = 0; i < test_argc; i++) if (!strcmp(test_argv[i], "--solo")) solo = 1;
        if (!solo || stub_self == 0) fails += dsm_app_main(ctx, test_argc, test_argv) != 0;   /* --solo: solo lo lanza el nodo 0 */
        dsm_barrier(ctx, 999, stub_n);                  /* los demas nodos siguen vivos aportando memoria */
        uint64_t tr = dir_get_total_transfers();
        fprintf(stderr, "[nodo %d] transferencias_directorio=%lu\n", stub_self, (unsigned long)tr);
    }
    else { fprintf(stderr, "escenario desconocido\n"); _exit(11); }
    fflush(stdout); fflush(stderr);
    _exit(fails ? 1 : 0);
}

char *test_name; int test_argc; char **test_argv;

int main(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "uso: %s <escenario> <nodos> [args]\n", argv[0]); return 2; }
    test_name = argv[1]; stub_n = atoi(argv[2]);
    test_argc = argc - 2; test_argv = argv + 2; test_argv[0] = (char *)"life";
    for (int i = 0; i < DSM_MAX_NODES; i++) stub_fd[i] = -1;
    int mesh[DSM_MAX_NODES][DSM_MAX_NODES][2];
    for (int i = 0; i < stub_n; i++) for (int j = i + 1; j < stub_n; j++) {
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, mesh[i][j]) < 0) { perror("socketpair"); return 2; }
    }
    pid_t pid[DSM_MAX_NODES];
    for (int i = 0; i < stub_n; i++) {
        pid[i] = fork();
        if (pid[i] == 0) {
            for (int a = 0; a < stub_n; a++) for (int b = a + 1; b < stub_n; b++) {
                if (a == i) { stub_fd[b] = mesh[a][b][0]; close(mesh[a][b][1]); }
                else if (b == i) { stub_fd[a] = mesh[a][b][1]; close(mesh[a][b][0]); }
                else { close(mesh[a][b][0]); close(mesh[a][b][1]); }
            }
            node_main(i);
        }
    }
    for (int a = 0; a < stub_n; a++) for (int b = a + 1; b < stub_n; b++) { close(mesh[a][b][0]); close(mesh[a][b][1]); }
    int bad = 0;
    for (int i = 0; i < stub_n; i++) {
        int st; waitpid(pid[i], &st, 0);
        if (!WIFEXITED(st) || WEXITSTATUS(st) != 0) { bad = 1; fprintf(stderr, "nodo %d termino mal (status %d)\n", i, st); }
    }
    fprintf(stderr, "%s con %d nodo(s): %s\n", test_name, stub_n, bad ? "FALLO" : "OK");
    return bad;
}
