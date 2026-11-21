#define _GNU_SOURCE
/* dsm_run - le pide al dsm_node de esta máquina que corra una aplicación.
 *
 * Uso: ./dsm_run [--port P] [--all] APP.so [argumentos de la app]
 *      ./dsm_run apps/life/life.so 512 50 --threads 2          (solo este nodo)
 *      ./dsm_run --all apps/life/life.so 4000 100 --threads 4  (todos los nodos)
 *
 * El nodo carga la .so y la corre en sus hilos; lo que imprime con dsm_printf
 * sale aquí. Termina cuando la app termina en todos los nodos.
 * Puerto: el de consultas del nodo (puerto_base + 2), 27002 por defecto.
 */
#include <arpa/inet.h>
#include <limits.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static void uso(const char *p) {
    fprintf(stderr, "Uso: %s [--port P] [--all] APP.so [argumentos]\n", p);
    exit(2);
}

int main(int argc, char **argv) {
    int port = 27002, todos = 0, i = 1;
    for (; i < argc && argv[i][0] == '-' && argv[i][1] == '-'; i++) {
        if (!strcmp(argv[i], "--port") && i + 1 < argc) port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--all")) todos = 1;
        else uso(argv[0]);
    }
    if (i >= argc) uso(argv[0]);

    const char *ruta = argv[i++];
    char abs[PATH_MAX];
    if (!realpath(ruta, abs)) { perror(ruta); return 1; }

    /* nombre de la app para argv[0]: "apps/life/life.so" -> "life" */
    const char *base = strrchr(ruta, '/') ? strrchr(ruta, '/') + 1 : ruta;
    char nombre[64];
    snprintf(nombre, sizeof nombre, "%s", base);
    char *punto = strstr(nombre, ".so");
    if (punto) *punto = '\0';

    char linea[2048];
    int n = snprintf(linea, sizeof linea, "run %d %d %s %s %s", (int)getpid(), todos, abs, ruta, nombre);
    for (; i < argc; i++) {
        if (strchr(argv[i], ' ')) { fprintf(stderr, "los argumentos no pueden tener espacios: '%s'\n", argv[i]); return 2; }
        n += snprintf(linea + n, sizeof linea - (size_t)n, " %s", argv[i]);
    }
    if (n >= (int)sizeof linea - 2) { fprintf(stderr, "demasiados argumentos\n"); return 2; }
    if (strchr(abs, ' ')) { fprintf(stderr, "la ruta no puede tener espacios\n"); return 2; }
    strcat(linea, "\n");

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in a = { .sin_family = AF_INET, .sin_port = htons((uint16_t)port) };
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&a, sizeof a) < 0) {
        fprintf(stderr, "No hay un dsm_node escuchando en el puerto %d\n", port);
        return 1;
    }
    if (write(fd, linea, strlen(linea)) < 0) return 1;

    /* mostrar todo lo que mande el nodo hasta que cierre */
    char buf[4096];
    ssize_t r;
    int ok = 0;
    while ((r = read(fd, buf, sizeof buf)) > 0) {
        fwrite(buf, 1, (size_t)r, stdout);
        fflush(stdout);
        if (memmem(buf, (size_t)r, "fin: ok", 7)) ok = 1;
    }
    close(fd);
    return ok ? 0 : 1;
}
