/*
 * life_ref.c -- Referencia en UNA sola maquina (sin DSM) para comparar contadores.
 * Mismo estado inicial (life_common.h) y mismo formato de salida que life.c.
 *   uso: life_ref <n> <generaciones> [--random S | --rle F]
 */
#include "life_common.h"

int main(int argc, char **argv) {
    if (argc < 3) return 2;
    int n = atoi(argv[1]), gens = atoi(argv[2]);
    uint64_t seed = 12345; const char *rlef = NULL;
    for (int i = 3; i < argc; i++) {
        if (!strcmp(argv[i], "--random") && i + 1 < argc) seed = strtoull(argv[++i], NULL, 10);
        else if (!strcmp(argv[i], "--rle") && i + 1 < argc) rlef = argv[++i];
    }
    rle_t pat = {0, 0, NULL};
    if (rlef && rle_load(rlef, &pat) < 0) { fprintf(stderr, "no se pudo leer %s\n", rlef); return 2; }
    size_t W = (size_t)n + 2;
    int *a = calloc(W * W, sizeof *a), *b = calloc(W * W, sizeof *b);
    for (int r = 1; r <= n; r++) for (int c = 1; c <= n; c++) a[r * W + c] = life_init_cell(rlef ? &pat : NULL, seed, r, c);
    for (int g = 0; g < gens; g++) {
        unsigned long long alive = 0, born = 0, died = 0;
        for (int r = 1; r <= n; r++) for (int c = 1; c <= n; c++) {
            int nb = 0;
            for (int dr = -1; dr <= 1; dr++) for (int dc = -1; dc <= 1; dc++) if (dr || dc) nb += a[(r + dr) * W + c + dc];
            int old = a[r * W + c], nw = (nb == 3) || (old && nb == 2);
            b[r * W + c] = nw; alive += nw; born += (nw && !old); died += (!nw && old);
        }
        printf("gen %d vivas=%llu nacidas=%llu muertas=%llu\n", g + 1, alive, born, died);
        int *t = a; a = b; b = t;
    }
    uint64_t gh = 0;
    for (int r = 1; r <= n; r++) {
        for (int c = 1; c <= n; c++) {
            if (a[r * W + c]) gh ^= ((uint64_t)r * 1000003ULL + (uint64_t)c) * 11400714819323198485ULL;
        }
    }
    printf("grid_hash=0x%016llx\n", (unsigned long long)gh);
    return 0;
}
