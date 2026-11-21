/* query.h - servidor de consultas en puerto_base + 2 (sección 9.6) */
#ifndef DSM_QUERY_H
#define DSM_QUERY_H

#include <stdio.h>
#include "config.h"

/* Una consulta recibe el resto de la línea (argumentos) y escribe texto en out. */
typedef void (*query_fn_t)(FILE *out, const char *args);

int  query_start(const dsm_config_t *cfg, node_id_t self);

/* Para que otras partes agreguen consultas, por ejemplo "pages" desde pager.c */
void query_register(const char *name, query_fn_t fn);

/* Handlers de red de STATS_* y PAGETABLE_* (los llama puente_start) */
void query_registrar_red(void);

#endif
