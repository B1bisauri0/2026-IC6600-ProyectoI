/* task.c - ejecución de aplicaciones dentro del nodo.
 *
 * dsm_run le manda al nodo local, por el puerto de consultas (puerto_base + 2):
 *     run PID TODOS RUTA_ABS RUTA arg0 arg1 ...
 * El nodo carga la .so con dlopen, busca dsm_app_main y la corre en un hilo
 * registrado. Lo que la app imprime con dsm_printf le llega a dsm_run.
 *
 * Con --all (TODOS = 1), además manda TASK_START a cada nodo ONLINE. Cada uno
 * corre la misma app en su propio hilo y al terminar contesta TASK_DONE con
 * su código de salida y lo que imprimió. El nodo que lanzó espera a todos
 */
#define _GNU_SOURCE
#include "task.h"
#include "dsm_internal.h"
#include "espera.h"
#include "query.h"

#include <dlfcn.h>
#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MAX_ARGS   32
#define MAX_SALIDA (1u << 20)      /* lo que un nodo remoto devuelve impreso, máx 1 MiB */

typedef int (*app_main_t)(dsm_ctx_t *, int, char **);

/* ---------------- a dónde va dsm_printf ---------------- */

static struct { dsm_ctx_t *ctx; FILE *f; } salidas[16];
static pthread_mutex_t salidas_m = PTHREAD_MUTEX_INITIALIZER;

static void salida_poner(dsm_ctx_t *ctx, FILE *f) {
    pthread_mutex_lock(&salidas_m);
    for (int i = 0; i < 16; i++) if (!salidas[i].ctx) { salidas[i].ctx = ctx; salidas[i].f = f; break; }
    pthread_mutex_unlock(&salidas_m);
}

static void salida_quitar(dsm_ctx_t *ctx) {
    pthread_mutex_lock(&salidas_m);
    for (int i = 0; i < 16; i++) if (salidas[i].ctx == ctx) salidas[i].ctx = NULL;
    pthread_mutex_unlock(&salidas_m);
}

/* escribe con el lock tomado, así nadie cierra la salida mientras se escribe */
void task_escribir(dsm_ctx_t *ctx, const char *texto) {
    pthread_mutex_lock(&salidas_m);
    for (int i = 0; i < 16; i++)
        if (ctx && salidas[i].ctx == ctx) { fputs(texto, salidas[i].f); fflush(salidas[i].f); }
    pthread_mutex_unlock(&salidas_m);
}

/* ---------------- correr una app en este nodo ---------------- */

/* carga la .so (primero la ruta absoluta; si en esta máquina no existe,
 * la ruta tal como la escribió el usuario, relativa a donde corre dsm_node) */
static app_main_t cargar(const char *abs, const char *ruta, FILE *err) {
    void *h = dlopen(abs, RTLD_NOW | RTLD_LOCAL);
    if (!h) {
        char rel[PATH_MAX];
        snprintf(rel, sizeof rel, "%s%s", strchr(ruta, '/') ? "" : "./", ruta);
        h = dlopen(rel, RTLD_NOW | RTLD_LOCAL);
    }
    if (!h) { if (err) fprintf(err, "no se pudo cargar %s: %s\n", ruta, dlerror()); return NULL; }
    app_main_t fn = (app_main_t)dlsym(h, "dsm_app_main");
    if (!fn) { if (err) fprintf(err, "%s no tiene dsm_app_main\n", ruta); return NULL; }
    return fn;
}

static int correr(const char *abs, const char *ruta, int argc, char **argv, uint32_t pid, FILE *out,
                  dsm_ctx_t **ctx_out) {
    app_main_t fn = cargar(abs, ruta, out);
    if (!fn) return DSM_ERR_NOTFOUND;

    dsm_ctx_t *ctx = dsm_ctx_for_app(NULL, pid);
    if (!ctx) return DSM_ERR_NOMEM;
    if (out) salida_poner(ctx, out);
    if (ctx_out) *ctx_out = ctx;
    atomic_store(&g_task_failed, 0);

    printf("[nodo %d] corriendo %s (pid de dsm_run %u)\n", g_n.self, argv[0], pid);
    fflush(stdout);
    int rc = dsm_thread_register(ctx);
    if (rc == DSM_OK) {
        rc = fn(ctx, argc, argv);
        dsm_thread_unregister(ctx);
    }
    if (atomic_load(&g_task_failed)) {
        char aviso[160];
        snprintf(aviso, sizeof aviso, "aviso: hubo un fallo de página sin resolver (%s)\n",
                 dsm_strerror((dsm_status_t)atomic_load(&g_task_failed_err)));
        task_escribir(ctx, aviso);
    }
    printf("[nodo %d] %s terminó con %d\n", g_n.self, argv[0], rc);
    fflush(stdout);

    if (out) salida_quitar(ctx);
    free(ctx);
    return rc;
}

/* está en el heap (no en la pila de q_run) porque si la app queda detenida,
 * q_run termina y el hilo sigue vivo: el último de los dos lo libera */
typedef struct {
    char  linea[2048];             /* copia propia de los argumentos */
    char *tok[MAX_ARGS + 4];
    int   argc;
    uint32_t pid;
    FILE *out;
    dsm_ctx_t *ctx;
    int rc, listo;
    int abandonada;
} local_t;
static pthread_mutex_t loc_m = PTHREAD_MUTEX_INITIALIZER;

static void *local_main(void *arg) {
    local_t *l = arg;
    int rc = correr(l->tok[2], l->tok[3], l->argc, l->tok + 4, l->pid, l->out, &l->ctx);
    pthread_mutex_lock(&loc_m);
    l->rc = rc;
    l->listo = 1;
    int libre = l->abandonada;
    pthread_mutex_unlock(&loc_m);
    if (libre) free(l);
    return NULL;
}

/* Corre la app en otro hilo y la vigila cada 200 ms:
 *  - avisa en out si se cae alguno de los nodos "lanzados"
 *  - si hay un fallo de página sin resolver por 2 s (política P3: el hilo
 *    queda suspendido), deja de esperar y devuelve DSM_ERR_NODE_DOWN.
 * El loc pasa a ser del hilo si se abandona (lo libera él), si no lo libera
 * quien llamó. */
static int correr_vigilado(local_t *loc, const node_id_t *lanzados, int nl, int *avisado, int *abandonada) {
    *abandonada = 0;
    pthread_t th;
    if (pthread_create(&th, NULL, local_main, loc) != 0) return DSM_ERR_THREAD;
    int detenida = 0;
    for (;;) {
        pthread_mutex_lock(&loc_m);
        int listo = loc->listo;
        pthread_mutex_unlock(&loc_m);
        if (listo) break;
        for (int i = 0; i < nl; i++)
            if (!avisado[i] && membership_status(lanzados[i]) == NODE_OFFLINE) {
                fprintf(loc->out, "[nodo %u] se cayó durante la ejecución\n", lanzados[i]);
                fflush(loc->out);
                avisado[i] = 1;
            }
        if (atomic_load(&g_task_failed) && ++detenida == 10) {
            fprintf(loc->out, "[nodo %d] la aplicación quedó detenida: %s (sus hilos quedan suspendidos)\n",
                    g_n.self, dsm_strerror((dsm_status_t)atomic_load(&g_task_failed_err)));
            fflush(loc->out);
            pthread_mutex_lock(&loc_m);
            loc->abandonada = 1;                       /* el hilo lo libera si algún día termina */
            if (loc->ctx) salida_quitar(loc->ctx);     /* ya no escribe en out */
            pthread_mutex_unlock(&loc_m);
            pthread_detach(th);
            *abandonada = 1;
            return DSM_ERR_NODE_DOWN;
        }
        usleep(200 * 1000);
    }
    pthread_join(th, NULL);
    return loc->rc;
}

/* ---------------- mensaje TASK_START ----------------
 * cuerpo: pid (4) | argc (2) | "ruta_abs\0ruta\0arg0\0arg1\0..." */

static uint32_t armar_start(uint8_t *buf, size_t max, uint32_t pid, const char *abs, const char *ruta,
                            int argc, char **argv) {
    uint8_t *p = buf;
    w32(&p, pid);
    w16(&p, (uint16_t)argc);
    const char *partes[MAX_ARGS + 2] = { abs, ruta };
    for (int i = 0; i < argc; i++) partes[i + 2] = argv[i];
    for (int i = 0; i < argc + 2; i++) {
        size_t l = strlen(partes[i]) + 1;
        if ((size_t)(p - buf) + l > max) return 0;
        memcpy(p, partes[i], l);
        p += l;
    }
    return (uint32_t)(p - buf);
}

typedef struct {
    node_id_t origen;
    uint64_t  req;
    uint8_t  *body;
    uint32_t  len;
} remota_t;

/* hilo que corre la app que pidió otro nodo y le contesta TASK_DONE */
static void *remota_main(void *arg) {
    remota_t *r = arg;
    const uint8_t *p = r->body, *fin = r->body + r->len;
    local_t *loc = calloc(1, sizeof *loc);
    loc->pid = r32(&p);
    int argc = r16(&p);
    int n = 0;
    char *q = loc->linea;                       /* copiar las partes al loc */
    while (p < fin && n < argc + 2 && n < MAX_ARGS + 4) {   /* cada parte termina en \0 */
        size_t l = strnlen((const char *)p, (size_t)(fin - p));
        if ((size_t)(q - loc->linea) + l + 1 > sizeof loc->linea) break;
        memcpy(q, p, l); q[l] = '\0';
        loc->tok[n + 2] = q;                    /* tok[2] = ruta_abs, tok[3] = ruta, tok[4..] = args */
        q += l + 1; p += l + 1; n++;
    }
    loc->argc = argc;
    int rc = DSM_ERR_INVAL;

    char *texto = NULL; size_t tlen = 0;
    FILE *out = open_memstream(&texto, &tlen);  /* lo que imprima se le devuelve al origen */
    loc->out = out;
    int abandonada = 0;
    if (out && n == argc + 2 && argc >= 1) rc = correr_vigilado(loc, NULL, 0, NULL, &abandonada);
    else if (out) fprintf(out, "TASK_START mal formado\n");
    if (!abandonada) free(loc);
    if (out) fclose(out);

    uint32_t tl = texto ? (uint32_t)(tlen < MAX_SALIDA ? tlen : MAX_SALIDA) : 0;
    uint8_t hdr[4], *hp = hdr;
    w32(&hp, (uint32_t)rc);
    struct iovec iov[2] = { { hdr, 4 }, { texto, tl } };
    net_sendv(r->origen, MSG_TASK_DONE, F_REPLY, r->req, iov, tl ? 2 : 1);

    free(texto);
    free(r->body);
    free(r);
    return NULL;
}

static void on_task_start(const msg_hdr_t *h, const void *body) {
    remota_t *r = malloc(sizeof *r);
    r->origen = h->src;
    r->req = h->req_id;
    r->len = h->length;
    r->body = malloc(h->length ? h->length : 1);
    memcpy(r->body, body, h->length);
    pthread_t t;
    if (pthread_create(&t, NULL, remota_main, r) == 0) pthread_detach(t);
    else { free(r->body); free(r); }
}

void task_registrar(void) {
    net_register_handler(MSG_TASK_START, on_task_start);
    net_register_handler(MSG_TASK_DONE, espera_respuesta);
}

/* ---------------- consulta "run" (la manda dsm_run) ---------------- */



static void q_run(FILE *out, const char *args) {
    local_t *loc = calloc(1, sizeof *loc);
    if (!loc) return;
    snprintf(loc->linea, sizeof loc->linea, "%s", args);
    char **tok = loc->tok;
    int nt = 0;
    char *guarda;
    for (char *s = strtok_r(loc->linea, " ", &guarda); s && nt < MAX_ARGS + 4; s = strtok_r(NULL, " ", &guarda))
        tok[nt++] = s;
    if (nt < 5) { fprintf(out, "uso interno: run PID TODOS RUTA_ABS RUTA [args]\n"); free(loc); return; }

    uint32_t pid = (uint32_t)strtoul(tok[0], NULL, 10);
    int todos = atoi(tok[1]);
    const char *abs = tok[2], *ruta = tok[3];
    int argc = nt - 4;
    char **argv = tok + 4;

    /* --all: avisar a los demás nodos ONLINE antes de arrancar aquí */
    node_id_t on[DSM_MAX_NODES], lanzados[DSM_MAX_NODES] = {0};
    uint64_t reqs[DSM_MAX_NODES];
    int nl = 0;
    if (todos) {
        uint8_t body[4096];
        uint32_t len = armar_start(body, sizeof body, pid, abs, ruta, argc, argv);
        int non = membership_online_ids(on, DSM_MAX_NODES);
        for (int i = 0; i < non; i++) {
            if (on[i] == (node_id_t)g_n.self) continue;
            uint64_t req = espera_nueva();
            if (!len || net_send(on[i], MSG_TASK_START, 0, req, body, len) < 0) {
                fprintf(out, "[nodo %u] no se le pudo mandar la tarea\n", on[i]);
                espera_cancelar(req);
                continue;
            }
            lanzados[nl] = on[i]; reqs[nl] = req; nl++;
        }
        fprintf(out, "lanzada en %d nodo(s)\n", nl + 1);
        fflush(out);
    }

    /* la app de este nodo corre en otro hilo, para poder avisar mientras tanto
     * si se cae un nodo o si la app quedó detenida por una página perdida */
    loc->argc = argc;
    loc->pid = pid;
    loc->out = out;
    int avisado[DSM_MAX_NODES] = {0}, abandonada;
    int rc = correr_vigilado(loc, lanzados, nl, avisado, &abandonada);
    if (!abandonada) free(loc);
    fprintf(out, "[nodo %d] terminó con %d\n", g_n.self, rc);
    fflush(out);

    /* esperar a los demás; cada uno manda lo que imprimió */
    for (int i = 0; i < nl; i++) {
        if (avisado[i]) { espera_cancelar(reqs[i]); if (rc == 0) rc = DSM_ERR_NODE_DOWN; continue; }
        void *resp; uint32_t len;
        int e = espera_esperar(reqs[i], lanzados[i], INT_MAX, &resp, &len);
        if (e != 0) { fprintf(out, "[nodo %u] se cayó antes de terminar\n", lanzados[i]); continue; }
        const uint8_t *p = resp;
        int rr = len >= 4 ? (int)r32(&p) : DSM_ERR_NET;
        if (len > 4) fwrite((char *)resp + 4, 1, len - 4, out);
        fprintf(out, "[nodo %u] terminó con %d\n", lanzados[i], rr);
        if (rr != 0 && rc == 0) rc = rr;
        free(resp);
    }
    fprintf(out, "fin: %s\n", rc == 0 ? "ok" : "con errores");
}

void task_registrar_consulta(void) {
    query_register("run", q_run);
}
