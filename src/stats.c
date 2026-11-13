#include <string.h>
#include "stats.h"
#include "dsm_internal.h"

dsm_stats_t dsm_stats;

void stats_init(void) {
    memset(&dsm_stats, 0, sizeof dsm_stats);
    dsm_stats.node = g_n.ready ? g_n.self : -1;
    if (g_n.ready) {
        dsm_stats.pages_invalid = g_n.n_vpages;
        dsm_stats.mem_contributed = g_n.cfg.node_mem_bytes;
        dsm_stats.mem_used = (uint64_t)atomic_load(&g_n.used_pages) * g_n.page_size;
    }
}

void stats_snapshot(dsm_stats_t *out) {
    if (!out) return;
    out->node = g_n.ready ? g_n.self : -1;
    out->read_faults = __atomic_load_n(&dsm_stats.read_faults, __ATOMIC_RELAXED);
    out->write_faults = __atomic_load_n(&dsm_stats.write_faults, __ATOMIC_RELAXED);
    out->upgrade_faults = __atomic_load_n(&dsm_stats.upgrade_faults, __ATOMIC_RELAXED);
    out->pages_invalid = __atomic_load_n(&dsm_stats.pages_invalid, __ATOMIC_RELAXED);
    out->pages_shared = __atomic_load_n(&dsm_stats.pages_shared, __ATOMIC_RELAXED);
    out->pages_modified = __atomic_load_n(&dsm_stats.pages_modified, __ATOMIC_RELAXED);
    out->pages_home = __atomic_load_n(&dsm_stats.pages_home, __ATOMIC_RELAXED);
    out->pages_cached = __atomic_load_n(&dsm_stats.pages_cached, __ATOMIC_RELAXED);
    out->invalidations_sent = __atomic_load_n(&dsm_stats.invalidations_sent, __ATOMIC_RELAXED);
    out->invalidations_recv = __atomic_load_n(&dsm_stats.invalidations_recv, __ATOMIC_RELAXED);
    out->evictions = __atomic_load_n(&dsm_stats.evictions, __ATOMIC_RELAXED);
    out->writebacks = __atomic_load_n(&dsm_stats.writebacks, __ATOMIC_RELAXED);
    out->msgs_sent = __atomic_load_n(&dsm_stats.msgs_sent, __ATOMIC_RELAXED);
    out->msgs_recv = __atomic_load_n(&dsm_stats.msgs_recv, __ATOMIC_RELAXED);
    out->bytes_sent = __atomic_load_n(&dsm_stats.bytes_sent, __ATOMIC_RELAXED);
    out->bytes_recv = __atomic_load_n(&dsm_stats.bytes_recv, __ATOMIC_RELAXED);
    out->fault_time_us_total = __atomic_load_n(&dsm_stats.fault_time_us_total, __ATOMIC_RELAXED);
    out->fault_count = __atomic_load_n(&dsm_stats.fault_count, __ATOMIC_RELAXED);
    out->mem_contributed = g_n.ready ? g_n.cfg.node_mem_bytes : __atomic_load_n(&dsm_stats.mem_contributed, __ATOMIC_RELAXED);
    out->mem_used = g_n.ready ? ((uint64_t)atomic_load(&g_n.used_pages) * g_n.page_size) : __atomic_load_n(&dsm_stats.mem_used, __ATOMIC_RELAXED);
    out->checkpoints = __atomic_load_n(&dsm_stats.checkpoints, __ATOMIC_RELAXED);
    out->replica_pages_sent = __atomic_load_n(&dsm_stats.replica_pages_sent, __ATOMIC_RELAXED);
    out->recoveries = __atomic_load_n(&dsm_stats.recoveries, __ATOMIC_RELAXED);
}
