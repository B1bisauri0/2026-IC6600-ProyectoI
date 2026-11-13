/*
 * life.c -- Juego de la Vida distribuido sobre el DSM (aplicacion 1, sec. 14.1).
 *
 * Basado en la arquitectura y diseño de pthreadslife del curso de procesamiento
 * paralelo de Siena College (C con pthreads, licencia MIT) [2], adaptado a la API
 * del sistema de memoria compartida distribuida DSM:
 *   - dsm_malloc_striped para repartir las bandas entre los nodos home
 *   - dsm_barrier para sincronizar generaciones y fases de hilos
 *   - dsm_lock para la actualizacion atomica de contadores
 *   - dsm_publish / dsm_lookup para compartir cuadriculas entre nodos
 *   - dsm_checkpoint para persistencia y tolerancia a fallos (--ft)
 *   - dsm_printf para salida distribuida
 *
 *   uso:  life <n> <generaciones> [--threads T] [--random SEMILLA | --rle ARCHIVO]
 *                                 [--solo] [--quiet] [--ft] [--no-align]
 *
 * Disposicion (sec. 14.1): dos cuadriculas de (n + 2) filas; cada celda ocupa 4 B; cada
 * fila se alinea a pagina (salvo con --no-align para probar thrashing T7). Las filas 0 y n + 1
 * y las columnas 0 y n + 1 son bordes muertos que nunca se escriben. Para que cada nodo sea
 * home de su propia banda con dsm_malloc_striped, las n filas reales van primero (fisicas 0..n-1)
 * y los dos bordes al final (fisicas n y n + 1).
 *
 * Modo normal: se lanza en TODOS los nodos (dsm_run --all, extra de ejecucion distribuida);
 * cada nodo calcula la banda de la que es home. Con --solo solo el nodo que lo lanza
 * calcula, y los demas nodos solo aportan memoria.
 */
#include <pthread.h>
#include <string.h>
#include <unistd.h>

#include "dsm.h"
#include "life_common.h"

typedef struct {
    dsm_ctx_t *ctx;
    int n, gens, tid, nthreads, nprocs;
    int lo, hi;                    /* filas 0-based [lo, hi) de este hilo */
    int is_leader_thread;
    size_t stride;
    char *grid[2];
    uint64_t *cnt;                 /* vivas, nacidas, muertas */
    const rle_t *pat; uint64_t seed; int quiet;
    int ft;
    uint64_t *ft_meta;             /* [0]=completed_gen, [1]=active_grid */
} work_t;

static inline int32_t *row_ptr(const work_t *w, int g, int r) {   /* r = 0 .. n+1 */
    int p = (r == 0) ? w->n : (r == w->n + 1) ? w->n + 1 : r - 1;
    return (int32_t *)(w->grid[g] + (size_t)p * w->stride);
}

static void *worker(void *arg) {
    work_t *w = arg;
    dsm_ctx_t *ctx = w->ctx;
    int n = w->n, total = w->nprocs * w->nthreads;
    if (dsm_thread_register(ctx) != DSM_OK) return (void *)1;

    int start_g = 0;
    if (w->ft && w->ft_meta) {
        start_g = (int)w->ft_meta[0];
    }

    if (start_g == 0) {
        for (int i = w->lo; i < w->hi; i++) {                      /* estado inicial de mi banda */
            int32_t *row = row_ptr(w, 0, i + 1);
            for (int c = 1; c <= n; c++) row[c] = life_init_cell(w->pat, w->seed, i + 1, c);
        }
    }
    dsm_barrier(ctx, 10, total);

    for (int g = start_g; g < w->gens; g++) {
        int cur = g & 1, nxt = cur ^ 1;
        uint64_t alive = 0, born = 0, died = 0;
        for (int i = w->lo; i < w->hi; i++) {
            int r = i + 1;
            const int32_t *up = row_ptr(w, cur, r - 1), *mid = row_ptr(w, cur, r), *dn = row_ptr(w, cur, r + 1);
            int32_t *out = row_ptr(w, nxt, r);
            for (int c = 1; c <= n; c++) {
                int nb = up[c-1] + up[c] + up[c+1] + mid[c-1] + mid[c+1] + dn[c-1] + dn[c] + dn[c+1];
                int old = mid[c], nw = (nb == 3) || (old && nb == 2);
                out[c] = nw;
                alive += (uint64_t)nw; born += (uint64_t)(nw && !old); died += (uint64_t)(!nw && old);
            }
        }
        dsm_lock(ctx, 1);                                          /* contadores compartidos */
        w->cnt[0] += alive; w->cnt[1] += born; w->cnt[2] += died;
        dsm_unlock(ctx, 1);
        dsm_barrier(ctx, 100 + 2 * g, total);
        if (w->is_leader_thread) {
            if (!w->quiet)
                dsm_printf(ctx, "gen %d vivas=%llu nacidas=%llu muertas=%llu\n", g + 1,
                           (unsigned long long)w->cnt[0], (unsigned long long)w->cnt[1], (unsigned long long)w->cnt[2]);
            w->cnt[0] = w->cnt[1] = w->cnt[2] = 0;
        }
        dsm_barrier(ctx, 101 + 2 * g, total);

        /* Tolerancia a fallos (--ft, sec. 14.4): checkpoint y barrera por generacion */
        if (w->ft && w->ft_meta) {
            if (w->is_leader_thread) {
                w->ft_meta[0] = (uint64_t)(g + 1);
                w->ft_meta[1] = (uint64_t)nxt;
                dsm_checkpoint(ctx);
            }
            dsm_barrier(ctx, 300 + 2 * g, total);
        }
    }
    dsm_barrier(ctx, 500, total);
    if (w->is_leader_thread && !w->quiet) {
        int final_g = (w->gens & 1);
        uint64_t gh = 0;
        for (int r = 1; r <= n; r++) {
            const int32_t *row = row_ptr(w, final_g, r);
            for (int c = 1; c <= n; c++) {
                if (row[c]) gh ^= ((uint64_t)r * 1000003ULL + (uint64_t)c) * 11400714819323198485ULL;
            }
        }
        dsm_printf(ctx, "grid_hash=0x%016llx\n", (unsigned long long)gh);
    }
    dsm_thread_unregister(ctx);
    return NULL;
}

int dsm_app_main(dsm_ctx_t *ctx, int argc, char **argv) {
    if (argc < 3) {
        dsm_printf(ctx, "uso: life <n> <generaciones> [--threads T] [--random S|--rle F] [--solo] [--quiet] [--ft] [--no-align]\n");
        return 2;
    }
    int n = atoi(argv[1]), gens = atoi(argv[2]), T = 1, solo = 0, quiet = 0;
    int ft = 0, no_align = 0;
    uint64_t seed = 12345; const char *rlef = NULL;
    for (int i = 3; i < argc; i++) {
        if (!strcmp(argv[i], "--threads") && i + 1 < argc) T = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--random") && i + 1 < argc) seed = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--rle") && i + 1 < argc) rlef = argv[++i];
        else if (!strcmp(argv[i], "--solo")) solo = 1;
        else if (!strcmp(argv[i], "--quiet")) quiet = 1;
        else if (!strcmp(argv[i], "--ft")) ft = 1;
        else if (!strcmp(argv[i], "--no-align") || !strcmp(argv[i], "--unaligned")) no_align = 1;
    }
    if (n < 3 || gens < 1 || T < 1) { dsm_printf(ctx, "parametros invalidos\n"); return 2; }
    rle_t pat = {0, 0, NULL};
    if (rlef && rle_load(rlef, &pat) < 0) { dsm_printf(ctx, "no se pudo leer el patron %s\n", rlef); return 2; }

    int homes = dsm_num_nodes(ctx);                    /* nodos que aportan memoria */
    int me_id = dsm_node_id(ctx);
    int nprocs = solo ? 1 : homes;
    int part = solo ? 0 : me_id;                       /* posicion de este nodo entre los que calculan */
    int leader = solo ? me_id : 0;
    size_t page_sz = dsm_page_size(ctx);
    if (!page_sz) page_sz = 4096u;
    size_t row_bytes = (size_t)(n + 2) * sizeof(int32_t);
    size_t stride = no_align ? row_bytes : (((row_bytes + page_sz - 1) / page_sz) * page_sz);
    int B = (n + homes - 1) / homes;                   /* filas por home */

    char *g[2]; uint64_t *cnt; uint64_t *ft_meta = NULL;
    size_t stripe_bytes = (((size_t)B * stride + page_sz - 1) / page_sz) * page_sz;
    int is_resume = 0;
    if (ft) {
        void *pft = dsm_lookup(ctx, "life_ft");
        if (pft) {
            ft_meta = (uint64_t *)pft;
            if (ft_meta[0] > 0) {
                is_resume = 1;
                g[0] = dsm_lookup(ctx, "life_g0");
                g[1] = dsm_lookup(ctx, "life_g1");
                cnt = dsm_lookup(ctx, "life_cnt");
            }
        }
    }

    if (!is_resume) {
        if (me_id == leader) {
            g[0] = dsm_malloc_striped(ctx, (size_t)(n + 2) * stride, stripe_bytes);
            g[1] = dsm_malloc_striped(ctx, (size_t)(n + 2) * stride, stripe_bytes);
            cnt = dsm_malloc(ctx, page_sz);
            if (ft) {
                ft_meta = dsm_malloc(ctx, page_sz);
                if (ft_meta) { ft_meta[0] = 0; ft_meta[1] = 0; dsm_publish(ctx, "life_ft", ft_meta); }
            }
            if (!g[0] || !g[1] || !cnt || (ft && !ft_meta)) {
                dsm_printf(ctx, "dsm_malloc: %s\n", dsm_strerror(dsm_errno()));
                return 1;
            }
            dsm_publish(ctx, "life_g0", g[0]); dsm_publish(ctx, "life_g1", g[1]); dsm_publish(ctx, "life_cnt", cnt);
        } else {
            void *p = NULL;
            for (int i = 0; i < 2000 && !(p = dsm_lookup(ctx, "life_cnt")); i++) usleep(5000);
            g[0] = dsm_lookup(ctx, "life_g0"); g[1] = dsm_lookup(ctx, "life_g1"); cnt = p;
            if (ft) {
                void *pft = NULL;
                for (int i = 0; i < 2000 && !(pft = dsm_lookup(ctx, "life_ft")); i++) usleep(5000);
                ft_meta = pft;
            }
            if (!g[0] || !g[1] || !cnt || (ft && !ft_meta)) {
                dsm_printf(ctx, "no se encontro la cuadricula publicada\n");
                return 1;
            }
        }
    }

    int lo = solo ? 0 : part * B, hi = solo ? n : ((part + 1) * B < n ? (part + 1) * B : n);
    if (lo > hi) lo = hi;
    pthread_t th[64]; work_t w[64];
    if (T > 64) T = 64;
    for (int t = 0; t < T; t++) {
        w[t] = (work_t){ ctx, n, gens, t, T, nprocs, lo + (hi - lo) * t / T, lo + (hi - lo) * (t + 1) / T,
                         me_id == leader && t == 0, stride, { g[0], g[1] }, cnt, rlef ? &pat : NULL, seed, quiet,
                         ft, ft_meta };
        pthread_create(&th[t], NULL, worker, &w[t]);
    }
    int rc = 0;
    for (int t = 0; t < T; t++) { void *r; pthread_join(th[t], &r); if (r) rc = 1; }
    if (me_id == leader) {                             /* los demas ya pasaron la ultima barrera */
        usleep(200000);
        dsm_free(ctx, g[0]); dsm_free(ctx, g[1]); dsm_free(ctx, cnt);
        if (ft && ft_meta) dsm_free(ctx, ft_meta);
    }
    free(pat.cells);
    return rc;
}
