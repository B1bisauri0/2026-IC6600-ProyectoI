/* sync.h - locks y barreras distribuidos (sección 9.3 del documento).
 * dsm.c los envuelve como dsm_lock, dsm_unlock y dsm_barrier. */
#ifndef DSM_SYNC_H
#define DSM_SYNC_H

#include "config.h"

int sync_start(const dsm_config_t *cfg, node_id_t self);

/* Devuelven 0, -4 (DSM_ERR_NODE_DOWN) o -5 (DSM_ERR_INVAL).
 * El gestor de un lock es lock_id % n_peers; el coordinador de una barrera,
 * barrier_id % n_peers. */
int sync_lock(int lock_id);
int sync_unlock(int lock_id);
int sync_barrier(int barrier_id, int n);

#endif
