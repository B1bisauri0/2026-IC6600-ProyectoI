/* task.h - correr aplicaciones (.so) dentro del nodo (D8 del diseño) y el
 * extra de ejecución distribuida (dsm_run --all). */
#ifndef DSM_TASK_H
#define DSM_TASK_H

#include <stdio.h>
#include "dsm.h"

/* registra los handlers de TASK_START/TASK_DONE (antes de net_start) */
void  task_registrar(void);
/* registra la consulta "run" en el servidor de consultas */
void  task_registrar_consulta(void);
/* manda texto de dsm_printf a dsm_run, si esa app la lanzó dsm_run */
void  task_escribir(dsm_ctx_t *ctx, const char *texto);

#endif
