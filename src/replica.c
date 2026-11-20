/*
 * replica.c -- Tolerancia a fallos mediante replicacion y recuperacion (sec. 7.1 y 14.4).
 *
 * Implementa el punto de control periodico (dsm_checkpoint / replica_checkpoint),
 * el despacho y atencion de mensajes de replica (MSG_REPLICA_UPDATE / ACK),
 * la difusion y atencion de cambio de home (MSG_HOME_CHANGE / ACK) devolviendo
 * paginas modificadas con MSG_WRITEBACK, y la culminacion de la recuperacion
 * con MSG_RECOVER_DONE. Maneja caidas multiples detectando una segunda caida
 * durante recuperacion para evitar corrupcion de datos.
 */
#define _GNU_SOURCE
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <unistd.h>

#include "replica.h"
#include "pager.h"
#include "directory.h"

static _Atomic int s_recovering = 0;
static pthread_mutex_t s_recover_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t s_recover_cv = PTHREAD_COND_INITIALIZER;

node_id_t select_backup_node(node_id_t home) {
    if (g_n.cfg.replication_factor < 2) return DSM_NODE_NONE;
    int n_nodes = g_n.cfg.max_nodes ? (int)g_n.cfg.max_nodes : DSM_MAX_NODES;
    for (int step = 1; step < n_nodes; step++) {
        node_id_t cand = (node_id_t)((home + step) % n_nodes);
        if (membership_status(cand) == NODE_ONLINE) return cand;
    }
    return DSM_NODE_NONE;
}

int replica_is_recovering(void) {
    return atomic_load(&s_recovering);
}

void replica_set_recovering(int active) {
    atomic_store(&s_recovering, active);
}

int replica_wait_recover_done(uint32_t timeout_ms) {
    uint64_t deadline = now_ms() + timeout_ms;
    pthread_mutex_lock(&s_recover_mu);
    while (atomic_load(&s_recovering)) {
        uint64_t now = now_ms();
        if (now >= deadline) {
            pthread_mutex_unlock(&s_recover_mu);
            return -1;
        }
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        uint64_t rem = deadline - now;
        ts.tv_sec += (time_t)(rem / 1000);
        ts.tv_nsec += (long)((rem % 1000) * 1000000);
        if (ts.tv_nsec >= 1000000000) { ts.tv_sec++; ts.tv_nsec -= 1000000000; }
        pthread_cond_timedwait(&s_recover_cv, &s_recover_mu, &ts);
    }
    pthread_mutex_unlock(&s_recover_mu);
    return 0;
}

int replica_checkpoint(dsm_ctx_t *ctx) {
    (void)ctx;
    if (!g_n.ready) return DSM_ERR_NOT_INIT;

    uint64_t n_replicated = 0;
    int checkpoint_failed = 0;

    /* Registrar paginas modificadas a persistir */
    vpn_t mod_vpns[1024];
    uint32_t mod_vers[1024];
    node_id_t mod_bks[1024];
    int n_mods = 0;

    for (vpn_t v = 0; v < (vpn_t)g_n.n_vpages && n_mods < 1024; v++) {
        pager_page_lock(v);
        local_page_t *lp = &g_n.lp[v];
        if (lp->state == PG_MODIFIED) {
            mod_vpns[n_mods] = v;
            mod_vers[n_mods] = lp->version;
            node_id_t bk = DSM_NODE_NONE;
            if (lp->flags & PF_ISHOME) {
                if (g_n.cfg.replication_factor >= 2) {
                    bk = alloc_backup_of(v);
                }
            } else {
                bk = alloc_home_of(v);
            }
            mod_bks[n_mods] = bk;
            n_mods++;
        }
        pager_page_unlock(v);
    }

    /* Enviar actualizaciones de replica y esperar confirmaciones */
    for (int i = 0; i < n_mods; i++) {
        vpn_t v = mod_vpns[i];
        node_id_t dst = mod_bks[i];
        uint32_t expected_ver = mod_vers[i];
        size_t off = (size_t)v << g_n.page_shift;

        if (dst != DSM_NODE_NONE && membership_status(dst) == NODE_ONLINE) {
            uint8_t hd[8], *q = hd;
            w32(&q, v); w32(&q, expected_ver);
            struct iovec iov[2] = { { hd, 8 }, { g_n.sys + off, g_n.page_size } };

            rpc_t *r = rpc_begin(v, 3, BIT(dst));
            int send_rc = net_sendv(dst, MSG_REPLICA_UPDATE, MSG_F_PAYLOAD, net_new_req_id(), iov, 2);
            if (send_rc >= 0) {
                int wait_rc = rpc_wait_end(r);
                if (wait_rc < 0 || r->granted[dst] != expected_ver) {
                    checkpoint_failed = 1;
                } else {
                    n_replicated++;
                }
            } else {
                checkpoint_failed = 1;
            }
            free(r);
        } else if (g_n.cfg.replication_factor >= 2) {
            checkpoint_failed = 1;
        }

        if (checkpoint_failed) break;
    }

    if (checkpoint_failed) {
        return DSM_ERR_NET;
    }

    /* Degradacion a PROT_READ para atrapar escrituras futuras (sec. 7.1) */
    for (int i = 0; i < n_mods; i++) {
        vpn_t v = mod_vpns[i];
        pager_page_lock(v);
        local_page_t *lp = &g_n.lp[v];
        if (lp->state == PG_MODIFIED) {
            size_t off = (size_t)v << g_n.page_shift;
            mprotect(g_n.app + off, g_n.page_size, PROT_READ);
            lp->flags &= (uint8_t)~PF_DIRTY;
        }
        pager_page_unlock(v);
    }

    STAT_INC(checkpoints);
    STAT_ADD(replica_pages_sent, n_replicated);
    return DSM_OK;
}

void replica_on_node_down(node_id_t failed_node) {
    if (!g_n.ready || g_n.cfg.replication_factor < 2) return;

    /* Deteccion de segunda caida antes de finalizar recuperacion previa (sec. 7.1) */
    if (atomic_exchange(&s_recovering, 1) != 0) {
        LOG_ERR("Segunda caida detectada (nodo %u) durante recuperacion activa. "
                "Abortando recuperacion para evitar corrupcion de datos (sec. 7.1).\n", failed_node);
        atomic_store(&g_task_failed, 1);
        atomic_store(&g_task_failed_err, DSM_ERR_NODE_DOWN);
        return;
    }

    size_t count = alloc_extent_count();

    for (size_t i = 0; i < count; i++) {
        vpn_t vstart = 0;
        uint32_t np = 0;
        node_id_t home = DSM_NODE_NONE, backup = DSM_NODE_NONE;
        if (alloc_get_extent(i, &vstart, &np, &home, &backup) < 0) continue;

        /* Paso 1: Si el nodo caido era HOME de este tramo y este nodo es el BACKUP, asume como HOME */
        if (home == failed_node && backup == (node_id_t)g_n.self) {
            node_id_t new_home = (node_id_t)g_n.self;
            node_id_t new_backup = select_backup_node(new_home);

            /* Actualizar tabla de tramos local */
            alloc_update_extent_nodes(vstart, np, new_home, new_backup);

            /* Paso 3: Crear entradas del directorio en estado I, sin copias (sec. 7.1) */
            for (uint32_t pg = 0; pg < np; pg++) {
                vpn_t v = vstart + pg;
                memset(&g_n.dir[v], 0, sizeof(dir_entry_t));
                sem_init(&g_n.dir_sem[v], 0, 1);
                g_n.dir[v].state = PG_INVALID;
                g_n.dir[v].owner = DSM_NODE_NONE;
                g_n.dir[v].sharers = 0;
                g_n.dir[v].version = 1;
                if (g_n.transfers) g_n.transfers[v] = 0;
                pager_page_lock(v);
                g_n.lp[v].flags |= PF_ISHOME;
                g_n.lp[v].flags &= (uint8_t)~PF_CACHED;
                g_n.lp[v].state = PG_INVALID;
                size_t off = (size_t)v << g_n.page_shift;
                mprotect(g_n.app + off, g_n.page_size, PROT_NONE);
                pager_page_unlock(v);
            }

            /* Paso 1: Difundir HOME_CHANGE a todos los demas nodos y esperar HOME_CHANGE_ACK */
            uint8_t b[14], *p = b;
            w32(&p, vstart); w32(&p, np); w16(&p, failed_node); w16(&p, new_home); w16(&p, new_backup);
            struct iovec iov = { b, 14 };

            node_id_t oth[DSM_MAX_NODES];
            int no = online_others(oth);
            uint16_t mask = 0;
            for (int k = 0; k < no; k++) mask |= BIT(oth[k]);

            int hc_ok = 1;
            if (mask) {
                rpc_t *r = rpc_begin(vstart, 4, mask);
                for (int k = 0; k < no; k++) {
                    net_sendv(oth[k], MSG_HOME_CHANGE, 0, net_new_req_id(), &iov, 1);
                }
                if (rpc_wait_end(r) < 0) {
                    LOG_ERR("recuperacion: timeout en HOME_CHANGE_ACK para tramo %u\n", vstart);
                    hc_ok = 0;
                }
                free(r);
            }

            /* Paso 4: Copiar el tramo al nuevo respaldo con REPLICA_UPDATE (sec. 7.1) */
            int repl_ok = 1;
            if (hc_ok && new_backup != DSM_NODE_NONE && membership_status(new_backup) == NODE_ONLINE) {
                for (uint32_t pg = 0; pg < np; pg++) {
                    vpn_t v = vstart + pg;
                    size_t off = (size_t)v << g_n.page_shift;
                    uint8_t hd[8], *q = hd;
                    w32(&q, v); w32(&q, 1);
                    struct iovec r_iov[2] = { { hd, 8 }, { g_n.sys + off, g_n.page_size } };
                    rpc_t *rr = rpc_begin(v, 3, BIT(new_backup));
                    if (net_sendv(new_backup, MSG_REPLICA_UPDATE, MSG_F_PAYLOAD, net_new_req_id(), r_iov, 2) >= 0) {
                        if (rpc_wait_end(rr) < 0 || rr->granted[new_backup] != 1) repl_ok = 0;
                    } else {
                        repl_ok = 0;
                    }
                    free(rr);
                    if (!repl_ok) break;
                }
            }

            if (hc_ok && repl_ok) {
                /* Paso 7: Enviar MSG_RECOVER_DONE a todos al culminar (sec. 7.1) */
                uint8_t rdb[6], *rdp = rdb;
                w16(&rdp, failed_node); w32(&rdp, 1);
                struct iovec rdiov = { rdb, 6 };
                for (int k = 0; k < no; k++) {
                    net_sendv(oth[k], MSG_RECOVER_DONE, 0, net_new_req_id(), &rdiov, 1);
                }
                STAT_INC(recoveries);
            }
        }
        /* Paso 5: Si este nodo era HOME y el caido era su BACKUP, asignar nuevo backup */
        else if (home == (node_id_t)g_n.self && backup == failed_node) {
            node_id_t new_backup = select_backup_node((node_id_t)g_n.self);
            alloc_update_extent_nodes(vstart, np, (node_id_t)g_n.self, new_backup);

            uint8_t b[14], *p = b;
            w32(&p, vstart); w32(&p, np); w16(&p, failed_node); w16(&p, (node_id_t)g_n.self); w16(&p, new_backup);
            struct iovec iov = { b, 14 };
            node_id_t oth[DSM_MAX_NODES];
            int no = online_others(oth);
            uint16_t mask = 0;
            for (int k = 0; k < no; k++) mask |= BIT(oth[k]);
            if (mask) {
                rpc_t *r = rpc_begin(vstart, 4, mask);
                for (int k = 0; k < no; k++) {
                    net_sendv(oth[k], MSG_HOME_CHANGE, 0, net_new_req_id(), &iov, 1);
                }
                rpc_wait_end(r);
                free(r);
            }

            if (new_backup != DSM_NODE_NONE && membership_status(new_backup) == NODE_ONLINE) {
                for (uint32_t pg = 0; pg < np; pg++) {
                    vpn_t v = vstart + pg;
                    size_t off = (size_t)v << g_n.page_shift;
                    uint8_t hd[8], *q = hd;
                    w32(&q, v); w32(&q, 1);
                    struct iovec r_iov[2] = { { hd, 8 }, { g_n.sys + off, g_n.page_size } };
                    rpc_t *rr = rpc_begin(v, 3, BIT(new_backup));
                    if (net_sendv(new_backup, MSG_REPLICA_UPDATE, MSG_F_PAYLOAD, net_new_req_id(), r_iov, 2) >= 0) {
                        rpc_wait_end(rr);
                    }
                    free(rr);
                }
            }
        }
    }

    atomic_store(&s_recovering, 0);
    pthread_mutex_lock(&s_recover_mu);
    pthread_cond_broadcast(&s_recover_cv);
    pthread_mutex_unlock(&s_recover_mu);
}

void replica_handle_update(const msg_hdr_t *h, const uint8_t *body) {
    if (h->length >= 8 + g_n.page_size) {
        const uint8_t *p = body;
        uint32_t vpn = r32(&p);
        uint32_t ver = r32(&p);
        size_t off = (size_t)vpn << g_n.page_shift;
        memcpy(g_n.sys + off, p, g_n.page_size);
        uint8_t b[8], *q = b;
        w32(&q, vpn); w32(&q, ver);
        struct iovec iov = { b, 8 };
        net_sendv(h->src, MSG_REPLICA_ACK, MSG_F_RESP, h->req_id, &iov, 1);
    }
}

void replica_handle_ack(const msg_hdr_t *h, const uint8_t *body) {
    if (h->length >= 8) {
        const uint8_t *p = body;
        uint32_t vpn = r32(&p);
        uint32_t ver = r32(&p);
        rpc_reply(vpn, 3, h->src, ver);
    }
}

void replica_handle_home_change(const msg_hdr_t *h, const uint8_t *body) {
    if (h->length >= 14) {
        const uint8_t *p = body;
        vpn_t vstart = r32(&p);
        uint32_t np = r32(&p);
        (void)r16(&p);
        node_id_t new_h = r16(&p);
        node_id_t new_bk = r16(&p);

        alloc_update_extent_nodes(vstart, np, new_h, new_bk);

        /* Paso 2: Si las tiene en M, las envia a B con WRITEBACK; si en S, las descarta */
        for (uint32_t i = 0; i < np; i++) {
            vpn_t v = vstart + i;
            pager_page_lock(v);
            local_page_t *lp = &g_n.lp[v];
            if (lp->flags & PF_CACHED) {
                size_t off = (size_t)v << g_n.page_shift;
                if (lp->state == PG_MODIFIED) {
                    uint8_t hd[8], *q = hd;
                    w32(&q, v); w32(&q, lp->version);
                    struct iovec wb_iov[2] = { { hd, 8 }, { g_n.sys + off, g_n.page_size } };
                    net_sendv(new_h, MSG_WRITEBACK, MSG_F_PAYLOAD, net_new_req_id(), wb_iov, 2);
                }
                mprotect(g_n.app + off, g_n.page_size, PROT_NONE);
                lp->state = PG_INVALID;
                lp->flags &= (uint8_t)~(PF_CACHED | PF_REF | PF_DIRTY);
            }
            pager_page_unlock(v);
        }

        uint8_t b[4], *q = b;
        w32(&q, vstart);
        struct iovec iov = { b, 4 };
        net_sendv(h->src, MSG_HOME_CHANGE_ACK, MSG_F_RESP, h->req_id, &iov, 1);
    }
}

void replica_handle_home_change_ack(const msg_hdr_t *h, const uint8_t *body) {
    if (h->length >= 4) {
        const uint8_t *p = body;
        uint32_t vstart = r32(&p);
        rpc_reply(vstart, 4, h->src, 0);
    }
}

void replica_handle_recover_done(const msg_hdr_t *h, const uint8_t *body) {
    (void)h;
    if (h->length >= 6) {
        const uint8_t *p = body;
        node_id_t failed_node = r16(&p);
        uint32_t epoch = r32(&p);
        (void)failed_node; (void)epoch;
        atomic_store(&s_recovering, 0);
        pthread_mutex_lock(&s_recover_mu);
        pthread_cond_broadcast(&s_recover_cv);
        pthread_mutex_unlock(&s_recover_mu);
        fprintf(stderr, "[dsm %d] MSG_RECOVER_DONE recibido: nodo caido %u, epoca %u\n",
                g_n.self, failed_node, epoch);
    }
}
