/*
 * life_common.h -- Parte comun al Juego de la Vida DSM (life.c) y al programa de
 * referencia en una sola maquina (life_ref.c). Ambos DEBEN arrancar del mismo estado
 * inicial para que sus contadores se puedan comparar (sec. 14.1).
 */
#ifndef LIFE_COMMON_H
#define LIFE_COMMON_H
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct { int w, h; uint8_t *cells; } rle_t;

static inline uint64_t life_mix(uint64_t x) {               /* splitmix64 */
    x += 0x9E3779B97F4A7C15ull;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
    return x ^ (x >> 31);
}

/* Lee un patron RLE (formato de LifeWiki): cabecera "x = W, y = H", luego b/o/$/! con repeticiones. */
static inline int rle_load(const char *path, rle_t *r) {
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    char line[4096];
    r->w = r->h = 0; r->cells = NULL;
    int x = 0, y = 0, count = 0;
    while (fgets(line, sizeof line, f)) {
        if (line[0] == '#') continue;
        if (!r->cells) {
            if (sscanf(line, " x = %d , y = %d", &r->w, &r->h) != 2 || r->w <= 0 || r->h <= 0) { fclose(f); return -1; }
            r->cells = calloc((size_t)r->w * (size_t)r->h, 1);
            continue;
        }
        for (char *p = line; *p; p++) {
            if (*p >= '0' && *p <= '9') { count = count * 10 + (*p - '0'); continue; }
            int k = count ? count : 1;
            if (*p == 'b') x += k;
            else if (*p == 'o') { for (int i = 0; i < k; i++, x++) if (x < r->w && y < r->h) r->cells[y * r->w + x] = 1; }
            else if (*p == '$') { y += k; x = 0; }
            else if (*p == '!') { fclose(f); return 0; }
            count = 0;
        }
    }
    fclose(f);
    return r->cells ? 0 : -1;
}

/* Estado inicial de la celda (r, c), 1 <= r, c <= n.
 * Con patron: se repite en mosaico con 3 celdas de separacion. Sin patron: aleatorio de semilla fija (30 %). */
static inline int life_init_cell(const rle_t *pat, uint64_t seed, int r, int c) {
    if (pat && pat->cells) {
        int rr = (r - 1) % (pat->h + 3), cc = (c - 1) % (pat->w + 3);
        return rr < pat->h && cc < pat->w && pat->cells[rr * pat->w + cc];
    }
    return (life_mix(seed ^ ((uint64_t)(unsigned)r << 32) ^ (uint64_t)(unsigned)c) % 100) < 30;
}
#endif
