#ifndef DSM_STATS_H
#define DSM_STATS_H
#include "dsm_types.h"
/* Contadores con operaciones atomicas (sec. 9.2). */
extern dsm_stats_t dsm_stats;
#define STAT_ADD(field, n) __atomic_fetch_add(&dsm_stats.field, (uint64_t)(n), __ATOMIC_RELAXED)
#define STAT_INC(field)    STAT_ADD(field, 1)
#define STAT_SUB(field, n) __atomic_fetch_sub(&dsm_stats.field, (uint64_t)(n), __ATOMIC_RELAXED)
void stats_init(void);
void stats_snapshot(dsm_stats_t *out);   /* copia local (node = -1 lo pone quien consulta) */
#endif
