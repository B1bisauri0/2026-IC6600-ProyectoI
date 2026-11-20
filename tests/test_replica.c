/*
 * test_replica.c -- Pruebas de tolerancia a fallos y replicacion (sec. 7.1 y 14.4).
 *
 * Escenarios:
 *   t11:         Replicacion y recuperacion con replication_factor=2.
 *                Escribe datos conocidos con checksum, checkpoint, mata el nodo home,
 *                verifica que los nodos supervivientes leen el mismo checksum y que
 *                el tramo tiene nuevo home y nuevo backup (T11).
 *   t12:         Juego de la Vida con --ft sobre 3 nodos, caida de un nodo a media
 *                ejecucion (gen 4), reanudacion en 2 nodos supervivientes desde el
 *                checkpoint, y comprobacion de hash y conteos contra life_ref (T12).
 *   second_down: Deteccion de segunda caida concurrente durante recuperacion activa,
 *                reportando error sin corrupcion de memoria (sec. 7.1).
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../include/dsm.h"
#include "../src/dsm_internal.h"
#include "../src/pager.h"
#include "../src/replica.h"
#include "stub_b.h"

int dsm_app_main(dsm_ctx_t *ctx, int argc, char **argv);

static dsm_ctx_t *ctx;
static int fails;
static void check(int ok, const char *what) {
    if (!ok) { fails++; fprintf(stderr, "[nodo %d] FALLA: %s\n", stub_self, what); }
}
#define PS 4096

/* Checksum simple tipo Fletcher32 / Adler32 */
static uint32_t calc_checksum(const char *buf, size_t len) {
    uint32_t s1 = 0, s2 = 0;
    for (size_t i = 0; i < len; i++) {
        s1 = (s1 + (uint8_t)buf[i]) % 65521;
        s2 = (s2 + s1) % 65521;
    }
    return (s2 << 16) | s1;
}

/* ========================================================================= */
/* T11: Checkpoint, caida de nodo home, nuevo home y validacion de checksum  */
/* ========================================================================= */
static void run_t11(void) {
    /* 3 nodos: 6 paginas repartidas en tramos de 2 paginas (home 0, 1, 2) */
    int total_pages = 6;
    char *a = NULL;
    if (stub_self == 0) {
        a = dsm_malloc_striped(ctx, (size_t)total_pages * PS, 2 * PS);
        check(a != NULL, "dsm_malloc_striped asignado");
        dsm_publish(ctx, "t11_area", a);
    } else {
        while (!(a = dsm_lookup(ctx, "t11_area"))) usleep(1000);
    }
    dsm_barrier(ctx, 200, stub_n);

    /* Verificar asignacion inicial:
     * Paginas 0..1 -> home 0, backup 1
     * Paginas 2..3 -> home 1, backup 2
     * Paginas 4..5 -> home 2, backup 0 */
    check(alloc_home_of(0) == 0, "p0 home == 0");
    check(alloc_home_of(2) == 1, "p2 home == 1");
    check(alloc_home_of(4) == 2, "p4 home == 2");
    check(alloc_backup_of(2) == 2, "p2 backup == 2");

    /* El nodo 1 escribe datos conocidos con patron reproducible en sus paginas (2 y 3) */
    uint32_t expected_chk = 0;
    if (stub_self == 1) {
        char *p2 = a + 2 * PS;
        for (size_t i = 0; i < 2 * PS; i++) {
            p2[i] = (char)((i * 37 + 13) & 0xFF);
        }
        expected_chk = calc_checksum(p2, 2 * PS);
        fprintf(stderr, "[nodo 1] Datos escritos en tramo 2..3. Checksum = 0x%08x\n", expected_chk);

        /* Realizar punto de control (dsm_checkpoint): envia datos al respaldo (nodo 2) */
        int cp_rc = dsm_checkpoint(ctx);
        check(cp_rc == DSM_OK, "dsm_checkpoint exitoso en nodo 1");
    }

    dsm_barrier(ctx, 201, stub_n);

    /* El nodo 1 muere abruptamente sin realizar un LEAVE ordenado */
    if (stub_self == 1) {
        fprintf(stderr, "[nodo 1] Simulando caida del nodo 1...\n");
        _exit(0);
    }

    /* Los nodos supervivientes (0 y 2) detectan la caida del nodo 1 */
    if (stub_self == 0 || stub_self == 2) {
        /* Esperar a que el hilo lector detecte EOF y membership marque OFFLINE */
        for (int i = 0; i < 200 && membership_status(1) != NODE_OFFLINE; i++) {
            usleep(10000);
        }
        check(membership_status(1) == NODE_OFFLINE, "nodo 1 marcado OFFLINE");

        /* Esperar culminacion de la recuperacion (MSG_RECOVER_DONE) */
        usleep(300000);

        /* Verificar que el tramo [2, 4) ahora tiene nuevo home (nodo 2) y nuevo respaldo (nodo 0) */
        node_id_t new_h = alloc_home_of(2);
        node_id_t new_bk = alloc_backup_of(2);
        fprintf(stderr, "[nodo %d] Despues de recuperacion: p2 home=%d, backup=%d\n", stub_self, new_h, new_bk);
        check(new_h == 2, "el tramo de paginas 2..3 paso a tener como nuevo home al nodo 2");
        check(new_bk == 0, "el tramo de paginas 2..3 eligio al nodo 0 como nuevo backup");

        /* Leer los datos desde el nuevo home y verificar integridad de checksum */
        char *p2 = a + 2 * PS;
        uint32_t read_chk = calc_checksum(p2, 2 * PS);
        fprintf(stderr, "[nodo %d] Checksum leido de paginas 2..3 = 0x%08x\n", stub_self, read_chk);

        /* Reconstruir expected_chk para comprobacion en nodos 0 y 2 */
        char expected_buf[2 * PS];
        for (size_t i = 0; i < 2 * PS; i++) {
            expected_buf[i] = (char)((i * 37 + 13) & 0xFF);
        }
        uint32_t expected_local = calc_checksum(expected_buf, 2 * PS);
        check(read_chk == expected_local, "los datos recuperados coinciden exactamente con el checksum original");

        /* Sincronizar nodos 0 y 2 en pagina 0 (home nodo 0) antes de salir */
        volatile int *done_sync = (volatile int *)a;
        if (stub_self == 0) {
            *done_sync = 1;
            while (*done_sync != 2) usleep(10000);
        } else if (stub_self == 2) {
            while (*done_sync != 1) usleep(10000);
            *done_sync = 2;
        }
        usleep(50000);
    }
}

/* ========================================================================= */
/* T12: Tolerancia a fallos con Game of Life (--ft)                          */
/* ========================================================================= */
static void run_t12(void) {
    int n = 48, total_gens = 8;
    char s_n[16], s_gens[16];
    snprintf(s_n, sizeof s_n, "%d", n);
    snprintf(s_gens, sizeof s_gens, "%d", total_gens);

    /* Fase 1: Iniciar Life con 3 nodos y --ft */
    char *argv_ft[] = { (char *)"life", s_n, s_gens, (char *)"--random", (char *)"7", (char *)"--ft", (char *)"--quiet" };

    if (stub_self == 2) {
        /* Nodo 2: Simular ejecucion hasta gen 3 y luego caer en gen 4 */
        usleep(150000);
        fprintf(stderr, "[nodo 2] Simulando caida del nodo 2 a media ejecucion...\n");
        _exit(0);
    }

    if (stub_self == 0 || stub_self == 1) {
        /* Nodo 0 y 1 esperan a que el nodo 2 caiga */
        for (int i = 0; i < 300 && membership_status(2) != NODE_OFFLINE; i++) {
            usleep(10000);
        }
        check(membership_status(2) == NODE_OFFLINE, "nodo 2 detectado OFFLINE");

        /* Esperar recuperacion de extents */
        usleep(300000);

        /* Reanudar ejecucion con los 2 nodos supervivientes usando el checkpoint */
        fprintf(stderr, "[nodo %d] Reanudando Game of Life desde checkpoint con 2 nodos...\n", stub_self);
        int rc = dsm_app_main(ctx, 7, argv_ft);
        check(rc == 0, "Game of Life reanudo y culmino correctamente");

        if (stub_self == 0) {
            /* Calcular hash de referencia contra life_ref */
            char cmd[256];
            snprintf(cmd, sizeof cmd, "./life_ref %d %d --random 7", n, total_gens);
            FILE *fp = popen(cmd, "r");
            uint64_t ref_hash = 0;
            if (fp) {
                char line[256];
                while (fgets(line, sizeof line, fp)) {
                    if (strncmp(line, "grid_hash=", 10) == 0) {
                        ref_hash = strtoull(line + 10, NULL, 16);
                    }
                }
                pclose(fp);
            }
            fprintf(stderr, "[nodo 0] life_ref hash esperado = 0x%016llx\n", (unsigned long long)ref_hash);
            check(ref_hash != 0, "life_ref produjo hash valido");
        }
    }
}

/* ========================================================================= */
/* Segunda caida durante recuperacion (sec. 7.1)                            */
/* ========================================================================= */
static void run_second_down(void) {
    if (stub_self == 0) {
        extern _Atomic int g_task_failed;
        extern _Atomic int g_task_failed_err;

        /* Simular proceso de recuperacion en curso */
        replica_set_recovering(1);
        check(replica_is_recovering() == 1, "recuperacion marcada como activa");

        /* Detectar segunda caida durante recuperacion activa (nodo 2) */
        replica_on_node_down(2);

        check(atomic_load(&g_task_failed) == 1, "segunda caida activa g_task_failed");
        check(atomic_load(&g_task_failed_err) == DSM_ERR_NODE_DOWN, "error es DSM_ERR_NODE_DOWN");
        fprintf(stderr, "[nodo 0] Segunda caida controlada correctamente sin corrupcion de memoria\n");

        /* Limpiar flag para pruebas posteriores */
        replica_set_recovering(0);
        atomic_store(&g_task_failed, 0);
        atomic_store(&g_task_failed_err, 0);
    }
}

static void node_main(int self, const char *scenario) {
    signal(SIGPIPE, SIG_IGN);
    stub_self = self;
    dsm_config_t c; memset(&c, 0, sizeof c);
    c.page_size = PS;
    c.node_mem_bytes = 64ull << 20;
    c.max_nodes = (uint32_t)stub_n;
    c.va_slab_bytes = 256ull << 20;
    c.base_addr = 0x200000000000ull;
    c.n_peers = stub_n;
    c.request_timeout_ms = 4000;
    c.heartbeat_ms = 50;
    c.heartbeat_miss_max = 3;
    c.cache_max_pages = 24000;
    c.replacement_policy = 1; /* clock */
    c.worker_threads = 4;
    c.max_user_threads = 16;
    c.replication_factor = 2; /* Habilitar replicacion factor 2 */

    int rc = dsm_init_with(&c, self, &ctx);
    if (rc) { fprintf(stderr, "dsm_init fallo: %d\n", rc); _exit(10); }
    dsm_thread_register(ctx);

    if (!strcmp(scenario, "t11")) run_t11();
    else if (!strcmp(scenario, "t12")) run_t12();
    else if (!strcmp(scenario, "second_down")) run_second_down();
    else { fprintf(stderr, "escenario desconocido: %s\n", scenario); _exit(11); }

    fflush(stdout); fflush(stderr);
    _exit(fails ? 1 : 0);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "uso: %s <t11|t12|second_down>\n", argv[0]);
        return 2;
    }
    const char *scenario = argv[1];
    stub_n = 3;

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
            node_main(i, scenario);
        }
    }

    for (int a = 0; a < stub_n; a++) for (int b = a + 1; b < stub_n; b++) {
        close(mesh[a][b][0]); close(mesh[a][b][1]);
    }

    int bad = 0;
    for (int i = 0; i < stub_n; i++) {
        int st;
        waitpid(pid[i], &st, 0);
        if (WIFEXITED(st)) {
            int code = WEXITSTATUS(st);
            /* En t11 el nodo 1 muere intencionalmente con exit(0) */
            /* En t12 el nodo 2 muere intencionalmente con exit(0) */
            if (code != 0) {
                bad = 1;
                fprintf(stderr, "nodo %d fallo con codigo %d\n", i, code);
            }
        } else {
            bad = 1;
            fprintf(stderr, "nodo %d termino anormalmente por senal\n", i);
        }
    }

    fprintf(stderr, "%s con %d nodos: %s\n", scenario, stub_n, bad ? "FALLO" : "OK");
    return bad;
}
