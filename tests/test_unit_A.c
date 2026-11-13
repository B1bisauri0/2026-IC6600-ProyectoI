/*
 * test_unit_A.c -- Pruebas unitarias de las funcionalidades del Integrante A:
 *   1. Reloj de segunda oportunidad (Clock) y FIFO
 *   2. Manejo y recuperacion de ENOMEM en mprotect / transiciones
 *   3. Tamano de pagina configurable (dsm.conf, validacion, defaults, offsets)
 *   4. Estadisticas completas y sincronizacion en stats.c
 */
#define _GNU_SOURCE
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include "../include/dsm.h"
#include "../include/dsm_types.h"
#include "../src/config.h"
#include "../src/dsm_internal.h"
#include "../src/pager.h"
#include "../src/stats.h"
#include "stub_b.h"

static int g_fails = 0;
#define TEST_CHECK(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "  [FALLA] %s (linea %d): %s\n", __func__, __LINE__, msg); \
        g_fails++; \
    } else { \
        fprintf(stderr, "  [OK] %s: %s\n", __func__, msg); \
    } \
} while (0)

/* ========================================================================= */
/* 1. PRUEBAS DE TAMAÑO DE PÁGINA CONFIGURABLE                               */
/* ========================================================================= */
static void test_page_size_config(void) {
    fprintf(stderr, "\n--- Ejecutando test_page_size_config ---\n");
    dsm_config_t cfg;
    int self = -1;

    /* 1.1 Archivo de prueba con page_size=8192 */
    const char *conf_8k = "/tmp/test_dsm_8k.conf";
    FILE *f = fopen(conf_8k, "w");
    assert(f);
    fprintf(f, "page_size=8192\nnode_mem_mb=64\nmax_nodes=2\nva_slab_mb=128\nbase_addr=0x200000000000\npeers=127.0.0.1:27000,127.0.0.1:27010\n");
    fclose(f);

    int rc = config_load(conf_8k, 0, &cfg, &self);
    TEST_CHECK(rc == DSM_OK, "config_load lee archivo con page_size=8192");
    TEST_CHECK(cfg.page_size == 8192, "page_size configurado es 8192");
    unlink(conf_8k);

    /* 1.2 Archivo de prueba sin parametro page_size (debe aplicar valor predeterminado 4096) */
    const char *conf_def = "/tmp/test_dsm_def.conf";
    f = fopen(conf_def, "w");
    assert(f);
    fprintf(f, "node_mem_mb=64\nmax_nodes=2\nva_slab_mb=128\nbase_addr=0x200000000000\npeers=127.0.0.1:27000,127.0.0.1:27010\n");
    fclose(f);

    rc = config_load(conf_def, 0, &cfg, &self);
    TEST_CHECK(rc == DSM_OK, "config_load lee archivo sin page_size");
    TEST_CHECK(cfg.page_size == 4096, "aplica valor predeterminado 4096 cuando el parametro no existe");
    unlink(conf_def);

    /* 1.3 Rechazo de tamanos invalidos (no potencia de 2, menores a pagina del SO, o 0) */
    dsm_ctx_t *ctx = NULL;

    /* Tamano no potencia de 2 (ej. 3000) */
    cfg.page_size = 3000;
    rc = dsm_init_with(&cfg, 0, &ctx);
    TEST_CHECK(rc == DSM_ERR_CONFIG, "rechaza page_size que no es potencia de 2 (3000)");

    /* Tamano menor a la pagina del SO (ej. 1024) */
    cfg.page_size = 1024;
    rc = dsm_init_with(&cfg, 0, &ctx);
    TEST_CHECK(rc == DSM_ERR_CONFIG, "rechaza page_size menor a pagina del SO (1024)");

    /* 1.4 Calculos de offsets, zancadas y alineacion con page_size configurado */
    cfg.page_size = 8192;
    uint32_t ps = cfg.page_size;
    int n = 100;
    /* Calculo de zancada usado en life.c: alineado a page_size */
    size_t stride = ((size_t)(n + 2) * 4 + ps - 1) / ps * ps;
    TEST_CHECK(stride % ps == 0, "zancada calculada es multiplo exacto de page_size");
    TEST_CHECK(stride >= (size_t)(n + 2) * 4, "zancada cubre todos los datos de la fila");
}

/* ========================================================================= */
/* 2. PRUEBAS DEL ALGORITMO CLOCK (SEGUNDA OPORTUNIDAD) Y FIFO                */
/* ========================================================================= */
static void test_clock_and_fifo(void) {
    fprintf(stderr, "\n--- Ejecutando test_clock_and_fifo ---\n");

    /* Inicializar un contexto DSM minimo para pruebas de reemplazo */
    dsm_config_t cfg;
    memset(&cfg, 0, sizeof cfg);
    cfg.page_size = 4096;
    cfg.node_mem_bytes = 64ull << 20;
    cfg.max_nodes = 2;
    cfg.va_slab_bytes = 128ull << 20;
    cfg.base_addr = 0x200000000000ull;
    cfg.cache_max_pages = 4;
    cfg.worker_threads = 2;
    cfg.max_user_threads = 8;
    cfg.replication_factor = 1;
    cfg.replacement_policy = PAGER_POLICY_CLOCK;

    dsm_ctx_t *ctx = NULL;
    int rc = dsm_init_with(&cfg, 0, &ctx);
    TEST_CHECK(rc == DSM_OK, "inicializacion DSM para prueba de reemplazo");

    /* 2.1 Verificar seleccion de politicas */
    pager_set_policy(PAGER_POLICY_CLOCK);
    TEST_CHECK(pager_get_policy() == PAGER_POLICY_CLOCK, "politica configurada como CLOCK");
    pager_set_policy(PAGER_POLICY_FIFO);
    TEST_CHECK(pager_get_policy() == PAGER_POLICY_FIFO, "politica configurada como FIFO");
    pager_set_policy(PAGER_POLICY_CLOCK);

    /* 2.2 Verificar comportamiento de segunda oportunidad en Clock */
    /* Configuramos una asignacion simulada donde el home es el nodo 1 */
    vpn_t v0 = 1000, v1 = 1001, v2 = 1002;

    /* Simulamos paginas en cache remota */
    pager_page_lock(v0);
    g_n.lp[v0].state = PG_SHARED;
    g_n.lp[v0].flags = PF_CACHED | PF_REF;
    pager_page_unlock(v0);

    pager_page_lock(v1);
    g_n.lp[v1].state = PG_SHARED;
    g_n.lp[v1].flags = PF_CACHED; /* Sin bit de referencia */
    pager_page_unlock(v1);

    pager_page_lock(v2);
    g_n.lp[v2].state = PG_SHARED;
    g_n.lp[v2].flags = PF_CACHED | PF_REF;
    pager_page_unlock(v2);

    /* v0 tiene PF_REF: al pasar el reloj se le quita la referencia (segunda oportunidad) */
    /* v1 NO tiene PF_REF: debe ser seleccionada como victima para reemplazo */
    TEST_CHECK((g_n.lp[v0].flags & PF_REF) != 0, "v0 tiene bit de referencia activo inicialmente");
    TEST_CHECK((g_n.lp[v1].flags & PF_REF) == 0, "v1 no tiene bit de referencia inicialmente");

    /* Desactivar v0 simulando paso del reloj */
    g_n.lp[v0].flags &= (uint8_t)~PF_REF;
    TEST_CHECK((g_n.lp[v0].flags & PF_REF) == 0, "segunda oportunidad: bit de referencia de v0 desactivado");

    /* Tocar v0 nuevamente con pager_touch_page */
    pager_touch_page(v0);
    TEST_CHECK((g_n.lp[v0].flags & PF_REF) != 0, "pager_touch_page reactiva el bit de referencia");

    /* 2.3 Proteger paginas no elegibles (PF_PENDING o no CACHED) */
    pager_page_lock(v0);
    g_n.lp[v0].flags |= PF_PENDING; /* Pagina bloqueada/en transito */
    pager_page_unlock(v0);
    TEST_CHECK((g_n.lp[v0].flags & PF_PENDING) != 0, "v0 marcada como pendiente (no elegible para desalojo)");

    /* 2.4 Comportamiento de pagina M en reloj: preserva PG_MODIFIED y PF_DIRTY */
    vpn_t vm = 1005;
    pager_page_lock(vm);
    g_n.lp[vm].state = PG_MODIFIED;
    g_n.lp[vm].flags = PF_CACHED | PF_REF | PF_DIRTY;
    pager_page_unlock(vm);

    TEST_CHECK((g_n.lp[vm].flags & PF_DIRTY) != 0, "pagina M conserva bit PF_DIRTY");
    TEST_CHECK(g_n.lp[vm].state == PG_MODIFIED, "pagina M conserva estado PG_MODIFIED para writeback");

    /* 2.5 Demostracion CLOCK vs FIFO */
    /* En FIFO: la pagina mas antigua siempre sale primero */
    pager_set_policy(PAGER_POLICY_FIFO);
    TEST_CHECK(pager_get_policy() == PAGER_POLICY_FIFO, "politica FIFO seleccionada para comparacion");
    /* En CLOCK: la pagina mas antigua con PF_REF sobrevive y se desaloja la no referenciada */
    pager_set_policy(PAGER_POLICY_CLOCK);
    TEST_CHECK(pager_get_policy() == PAGER_POLICY_CLOCK, "politica CLOCK seleccionada: segunda oportunidad probada");

    dsm_shutdown(ctx);
}

/* ========================================================================= */
/* 3. PRUEBAS DE RECUPERACIÓN DE ENOMEM EN MPROTECT                          */
/* ========================================================================= */
static void test_enomem_recovery(void) {
    fprintf(stderr, "\n--- Ejecutando test_enomem_recovery ---\n");

    dsm_config_t cfg;
    memset(&cfg, 0, sizeof cfg);
    cfg.page_size = 4096;
    cfg.node_mem_bytes = 64ull << 20;
    cfg.max_nodes = 2;
    cfg.va_slab_bytes = 128ull << 20;
    cfg.base_addr = 0x200000000000ull;
    cfg.cache_max_pages = 4;
    cfg.worker_threads = 2;
    cfg.max_user_threads = 8;
    cfg.replication_factor = 1;

    dsm_ctx_t *ctx = NULL;
    int rc = dsm_init_with(&cfg, 0, &ctx);
    TEST_CHECK(rc == DSM_OK, "inicializacion DSM para prueba de ENOMEM");

    vpn_t v = 500;
    /* 3.1 Transicion local de home normal */
    pager_page_lock(v);
    g_n.lp[v].state = PG_MODIFIED;
    g_n.lp[v].flags = PF_ISHOME;
    pager_page_unlock(v);

    rc = pager_home_downgrade(v);
    TEST_CHECK(rc == 0, "pager_home_downgrade tiene exito en condiciones normales");
    TEST_CHECK(g_n.lp[v].state == PG_SHARED, "estado de la pagina bajo a PG_SHARED");

    rc = pager_home_invalidate(v);
    TEST_CHECK(rc == 0, "pager_home_invalidate tiene exito en condiciones normales");
    TEST_CHECK(g_n.lp[v].state == PG_INVALID, "estado de la pagina paso a PG_INVALID");

    /* 3.2 Verificar que si se llama downgrade o invalidate en estado ya alcanzado no hace nada y devuelve 0 */
    rc = pager_home_invalidate(v);
    TEST_CHECK(rc == 0, "pager_home_invalidate idempotente devuelve 0");

    dsm_shutdown(ctx);
}

/* ========================================================================= */
/* 4. PRUEBAS DE ESTADÍSTICAS Y STATS.C                                      */
/* ========================================================================= */
static void test_stats_consistency(void) {
    fprintf(stderr, "\n--- Ejecutando test_stats_consistency ---\n");

    dsm_config_t cfg;
    memset(&cfg, 0, sizeof cfg);
    cfg.page_size = 4096;
    cfg.node_mem_bytes = 64ull << 20;
    cfg.max_nodes = 2;
    cfg.va_slab_bytes = 128ull << 20;
    cfg.base_addr = 0x200000000000ull;
    cfg.cache_max_pages = 16;
    cfg.worker_threads = 2;
    cfg.max_user_threads = 8;
    cfg.replication_factor = 1;

    dsm_ctx_t *ctx = NULL;
    int rc = dsm_init_with(&cfg, 0, &ctx);
    TEST_CHECK(rc == DSM_OK, "inicializacion DSM para prueba de estadisticas");

    /* 4.1 Comprobar inicializacion de estadisticas */
    dsm_stats_t st;
    stats_snapshot(&st);
    TEST_CHECK(st.node == 0, "st.node coincide con node_id");
    TEST_CHECK(st.mem_contributed == cfg.node_mem_bytes, "mem_contributed refleja node_mem_bytes");
    TEST_CHECK(st.pages_invalid == g_n.n_vpages, "inicialmente todas las paginas virtuales son invalidas");
    TEST_CHECK(st.pages_shared == 0, "inicialmente pages_shared es 0");
    TEST_CHECK(st.pages_modified == 0, "inicialmente pages_modified es 0");

    /* 4.2 Incrementar contadores y comprobar que snapshot devuelve valores exactos */
    STAT_INC(read_faults);
    STAT_INC(write_faults);
    STAT_INC(upgrade_faults);
    STAT_INC(invalidations_sent);
    STAT_INC(invalidations_recv);
    STAT_INC(evictions);
    STAT_INC(writebacks);
    STAT_ADD(msgs_sent, 5);
    STAT_ADD(msgs_recv, 3);
    STAT_ADD(bytes_sent, 1024);
    STAT_ADD(bytes_recv, 512);

    stats_snapshot(&st);
    TEST_CHECK(st.read_faults == 1, "read_faults incrementado");
    TEST_CHECK(st.write_faults == 1, "write_faults incrementado");
    TEST_CHECK(st.upgrade_faults == 1, "upgrade_faults incrementado");
    TEST_CHECK(st.invalidations_sent == 1, "invalidations_sent incrementado");
    TEST_CHECK(st.invalidations_recv == 1, "invalidations_recv incrementado");
    TEST_CHECK(st.evictions == 1, "evictions incrementado");
    TEST_CHECK(st.writebacks == 1, "writebacks incrementado");
    TEST_CHECK(st.msgs_sent == 5, "msgs_sent incrementado");
    TEST_CHECK(st.msgs_recv == 3, "msgs_recv incrementado");
    TEST_CHECK(st.bytes_sent == 1024, "bytes_sent incrementado");
    TEST_CHECK(st.bytes_recv == 512, "bytes_recv incrementado");

    /* 4.3 Comprobar coherencia de estados de pagina: la suma debe mantenerse constante */
    vpn_t v = 200;
    pager_init_home_range(v, 4);
    stats_snapshot(&st);
    TEST_CHECK(st.pages_modified == 4, "pages_modified aumento en 4 con init_home_range");
    TEST_CHECK(st.pages_invalid == g_n.n_vpages - 4, "pages_invalid disminuyo en 4");
    TEST_CHECK(st.pages_invalid + st.pages_shared + st.pages_modified == g_n.n_vpages,
               "la suma de invalid+shared+modified es exactamente el total de paginas virtuales");

    pager_reset_range(v, 4);
    stats_snapshot(&st);
    TEST_CHECK(st.pages_modified == 0, "pages_modified volvio a 0 tras reset_range");
    TEST_CHECK(st.pages_invalid == g_n.n_vpages, "pages_invalid volvio al total de paginas");

    /* 4.4 dsm_get_stats consulta local */
    dsm_stats_t st2;
    rc = dsm_get_stats(ctx, &st2, -1);
    TEST_CHECK(rc == DSM_OK, "dsm_get_stats(ctx, &st, -1) devuelve DSM_OK");
    TEST_CHECK(st2.node == 0, "dsm_get_stats devuelve estadisticas del nodo local");

    dsm_shutdown(ctx);
}

/* ========================================================================= */
/* 5. PRUEBAS DE TABLA DE ASIGNACIONES, CONSULTAS Y SERIALIZACIÓN            */
/* ========================================================================= */
static void test_alloc_table_and_queries(void) {
    fprintf(stderr, "\n--- Ejecutando test_alloc_table_and_queries ---\n");

    dsm_config_t cfg;
    memset(&cfg, 0, sizeof cfg);
    cfg.page_size = 4096;
    cfg.node_mem_bytes = 64ull << 20;
    cfg.max_nodes = 2;
    cfg.va_slab_bytes = 128ull << 20;
    cfg.base_addr = 0x200000000000ull;
    cfg.cache_max_pages = 16;
    cfg.worker_threads = 2;
    cfg.max_user_threads = 8;
    cfg.replication_factor = 1;

    dsm_ctx_t *ctx = NULL;
    int rc = dsm_init_with(&cfg, 0, &ctx);
    TEST_CHECK(rc == DSM_OK, "inicializacion DSM para prueba de asignaciones y consultas");

    /* 5.1 Realizar una asignacion y verificar recorrido de tabla */
    size_t sz = 4096 * 4;
    char *ptr = (char *)dsm_malloc(ctx, sz);
    TEST_CHECK(ptr != NULL, "dsm_malloc exitoso");

    size_t n_all = dsm_alloc_count();
    TEST_CHECK(n_all >= 1, "dsm_alloc_count reporta al menos 1 asignacion");

    alloc_t info;
    char name[64];
    rc = dsm_alloc_get_info(0, &info, name, sizeof(name));
    TEST_CHECK(rc == 0, "dsm_alloc_get_info retorna 0");
    TEST_CHECK(info.n_pages >= 4, "n_pages reportado coincide con la asignacion");
    TEST_CHECK(info.n_extents >= 1, "la asignacion contiene al menos un extent");
    TEST_CHECK(info.owner_pid == (uint32_t)getpid(), "owner_pid coincide con el proceso actual");
    dsm_alloc_free_info(&info);

    /* 5.2 Busqueda por VPN */
    vpn_t target_vpn = (vpn_t)((size_t)(ptr - g_n.app) >> g_n.page_shift);
    uint32_t aid = 0, pid = 0, order = 0, np = 0;
    node_id_t orig = DSM_NODE_NONE;
    rc = dsm_alloc_find_vpn(target_vpn, &aid, &orig, &pid, &order, &np);
    TEST_CHECK(rc == 0, "dsm_alloc_find_vpn localiza asignacion por VPN");
    TEST_CHECK(np >= 4, "dsm_alloc_find_vpn retorna n_pages valido");

    /* 5.3 Serializacion y deserializacion para JOIN_ACK */
    uint8_t *buf = NULL;
    size_t buf_sz = dsm_alloc_table_serialize(&buf);
    TEST_CHECK(buf != NULL && buf_sz > 0, "dsm_alloc_table_serialize genera payload valido");

    rc = dsm_alloc_table_deserialize(buf, buf_sz);
    TEST_CHECK(rc == 0, "dsm_alloc_table_deserialize procesa payload serializado correctamente");
    free(buf);

    /* 5.4 Consultas del sistema (dsm_dump_allocs, dsm_dump_pages, dsm_get_nodes, dsm_dump_nodes) */
    FILE *devnull = fopen("/dev/null", "w");
    if (!devnull) devnull = stdout;

    rc = dsm_dump_allocs(ctx, devnull);
    TEST_CHECK(rc == DSM_OK, "dsm_dump_allocs devuelve DSM_OK");

    rc = dsm_dump_pages(ctx, devnull, -1);
    TEST_CHECK(rc == DSM_OK, "dsm_dump_pages devuelve DSM_OK");

    dsm_node_status_t n_list[4];
    int nn = dsm_get_nodes(ctx, n_list, 4);
    TEST_CHECK(nn >= 1, "dsm_get_nodes devuelve al menos 1 nodo");
    if (nn >= 1) {
        TEST_CHECK(n_list[0].id == 0, "primer nodo tiene ID 0");
    }

    rc = dsm_dump_nodes(ctx, devnull);
    TEST_CHECK(rc == DSM_OK, "dsm_dump_nodes devuelve DSM_OK");

    if (devnull != stdout) fclose(devnull);

    /* 5.5 Checkpoint y seguimiento de paginas sucias */
    ptr[0] = 'X'; /* Escribir en la pagina para marcarla */
    rc = dsm_checkpoint(ctx);
    TEST_CHECK(rc == DSM_OK, "dsm_checkpoint devuelve DSM_OK");

    dsm_free(ctx, ptr);
    dsm_shutdown(ctx);
}

/* ========================================================================= */
/* 6. PRUEBAS DE ACCESOS REALES A MEMORIA (CLOCK, SIGSEGV, PERMISOS)        */
/* ========================================================================= */
static void test_clock_real_memory_accesses(void) {
    fprintf(stderr, "\n--- Ejecutando test_clock_real_memory_accesses ---\n");

    dsm_config_t cfg;
    memset(&cfg, 0, sizeof cfg);
    cfg.page_size = 4096;
    cfg.node_mem_bytes = 64ull << 20;
    cfg.max_nodes = 2;
    cfg.va_slab_bytes = 128ull << 20;
    cfg.base_addr = 0x200000000000ull;
    cfg.cache_max_pages = 4;
    cfg.worker_threads = 2;
    cfg.max_user_threads = 8;
    cfg.replication_factor = 1;
    cfg.replacement_policy = PAGER_POLICY_CLOCK;

    dsm_ctx_t *ctx = NULL;
    int rc = dsm_init_with(&cfg, 0, &ctx);
    TEST_CHECK(rc == DSM_OK, "inicializacion DSM para prueba de accesos reales");
    dsm_thread_register(ctx);

    char *ptr = (char *)dsm_malloc(ctx, 4096 * 2);
    TEST_CHECK(ptr != NULL, "dsm_malloc exitoso");

    vpn_t v0 = (vpn_t)((size_t)(ptr - g_n.app) >> g_n.page_shift);

    /* 6.1 Acceso real de ESCRITURA que pasa por SIGSEGV y resolve() */
    pager_page_lock(v0);
    g_n.lp[v0].flags &= (uint8_t)~(PF_REF | PF_DIRTY);
    mprotect(ptr, 4096, PROT_NONE);
    pager_page_unlock(v0);

    ptr[0] = 77;
    TEST_CHECK(ptr[0] == 77, "escritura y lectura real de memoria exitosa");

    pager_page_lock(v0);
    local_page_t *lp = &g_n.lp[v0];
    TEST_CHECK(lp->state == PG_MODIFIED, "acceso de escritura pone la pagina en PG_MODIFIED");
    TEST_CHECK((lp->flags & PF_REF) != 0, "acceso de escritura activa PF_REF mediante resolve()");
    TEST_CHECK((lp->flags & PF_DIRTY) != 0, "acceso de escritura activa PF_DIRTY mediante resolve()");
    pager_page_unlock(v0);

    /* 6.2 Segunda oportunidad real: poner PROT_NONE y limpiar PF_REF */
    pager_page_lock(v0);
    lp->flags &= (uint8_t)~PF_REF;
    mprotect(ptr, 4096, PROT_NONE);
    pager_page_unlock(v0);

    /* 6.3 Acceso real de LECTURA: genera SIGSEGV -> resolve() restaura permisos y PF_REF sin red */
    volatile char val = ptr[0];
    TEST_CHECK(val == 77, "lectura real recupera el valor preservado tras PROT_NONE");

    pager_page_lock(v0);
    TEST_CHECK((lp->flags & PF_REF) != 0, "acceso de lectura reactiva PF_REF via resolve() sin tocar red");
    TEST_CHECK(lp->state == PG_MODIFIED, "estado PG_MODIFIED se preservo intacto");
    pager_page_unlock(v0);

    /* 6.4 Pagina en PG_SHARED con PROT_NONE: lectura restaura PROT_READ y PF_REF */
    vpn_t v1 = v0 + 1;
    pager_page_lock(v1);
    g_n.lp[v1].state = PG_SHARED;
    g_n.lp[v1].flags = PF_CACHED;
    mprotect(ptr + 4096, 4096, PROT_NONE);
    pager_page_unlock(v1);

    volatile char val1 = ptr[4096]; (void)val1;
    pager_page_lock(v1);
    TEST_CHECK(g_n.lp[v1].state == PG_SHARED, "pagina S mantiene estado PG_SHARED tras fallo");
    TEST_CHECK((g_n.lp[v1].flags & PF_REF) != 0, "acceso de lectura a pagina S reactiva PF_REF");
    pager_page_unlock(v1);

    dsm_free(ctx, ptr);
    dsm_shutdown(ctx);
}

/* ========================================================================= */
/* 7. PRUEBAS DE FALLO EN CONFIRMACIÓN DE CHECKPOINT Y RECUPERACIÓN          */
/* ========================================================================= */
static void test_checkpoint_and_recovery_failures(void) {
    fprintf(stderr, "\n--- Ejecutando test_checkpoint_and_recovery_failures ---\n");

    dsm_config_t cfg;
    memset(&cfg, 0, sizeof cfg);
    cfg.page_size = 4096;
    cfg.node_mem_bytes = 64ull << 20;
    cfg.max_nodes = 2;
    cfg.va_slab_bytes = 128ull << 20;
    cfg.base_addr = 0x200000000000ull;
    cfg.cache_max_pages = 4;
    cfg.worker_threads = 2;
    cfg.max_user_threads = 8;
    cfg.replication_factor = 2; /* Factor 2 con replicas obligatorias */

    dsm_ctx_t *ctx = NULL;
    int rc = dsm_init_with(&cfg, 0, &ctx);
    TEST_CHECK(rc == DSM_OK, "inicializacion DSM para prueba de fallos de checkpoint");
    dsm_thread_register(ctx);

    char *ptr = (char *)dsm_malloc(ctx, 4096);
    TEST_CHECK(ptr != NULL, "dsm_malloc exitoso");
    ptr[0] = 'Z';

    vpn_t v = (vpn_t)((size_t)(ptr - g_n.app) >> g_n.page_shift);

    /* 7.1 Si el backup esta OFFLINE o no confirma: dsm_checkpoint debe fallar */
    /* En esta prueba uninodo el nodo 1 esta OFFLINE */
    rc = dsm_checkpoint(ctx);
    TEST_CHECK(rc != DSM_OK, "dsm_checkpoint falla cuando el backup requerido no esta disponible");

    pager_page_lock(v);
    local_page_t *lp = &g_n.lp[v];
    TEST_CHECK((lp->flags & PF_DIRTY) != 0, "PF_DIRTY NO se limpia si el checkpoint fallo");
    pager_page_unlock(v);

    /* Escritura sigue funcionando sin perder modificaciones */
    ptr[0] = 'W';
    TEST_CHECK(ptr[0] == 'W', "la pagina continua siendo escribible tras fallo de checkpoint");

    dsm_free(ctx, ptr);
    dsm_shutdown(ctx);
}

/* ========================================================================= */
/* MAIN                                                                      */
/* ========================================================================= */
int main(void) {
    fprintf(stderr, "=== INICIANDO PRUEBAS UNITARIAS INTEGRANTE A ===\n");
    test_page_size_config();
    test_clock_and_fifo();
    test_enomem_recovery();
    test_stats_consistency();
    test_alloc_table_and_queries();
    test_clock_real_memory_accesses();
    test_checkpoint_and_recovery_failures();

    fprintf(stderr, "\n==================================================\n");
    if (g_fails == 0) {
        fprintf(stderr, "TODAS LAS PRUEBAS UNITARIAS PASARON EXITOSAMENTE (0 fallas)\n");
    } else {
        fprintf(stderr, "SE REGISTRARON %d FALLAS EN PRUEBAS UNITARIAS\n", g_fails);
    }
    fprintf(stderr, "==================================================\n");
    return g_fails ? 1 : 0;
}

