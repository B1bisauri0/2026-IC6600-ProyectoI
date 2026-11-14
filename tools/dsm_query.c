/* dsm_query - consulta a un nodo local.
 * Uso: ./dsm_query [--port P] CONSULTA [argumentos]
 *      ./dsm_query nodes              (nodo en el puerto 27002 por defecto)
 *      ./dsm_query --port 27012 nodes (el nodo 1 en pruebas locales)
 */
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

int main(int argc, char **argv) {
    int port = 27002, i = 1;
    if (argc > 2 && !strcmp(argv[1], "--port")) { port = atoi(argv[2]); i = 3; }
    if (i >= argc) {
        fprintf(stderr, "Uso: %s [--port P] nodes|help\n", argv[0]);
        return 2;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in a = { .sin_family = AF_INET, .sin_port = htons((uint16_t)port) };
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&a, sizeof a) < 0) {
        fprintf(stderr, "No hay un dsm_node escuchando en el puerto %d\n", port);
        return 1;
    }

    char line[512] = "";
    for (; i < argc; i++) {
        strncat(line, argv[i], sizeof line - strlen(line) - 2);
        if (i + 1 < argc) strcat(line, " ");
    }
    strcat(line, "\n");
    if (write(fd, line, strlen(line)) < 0) return 1;

    char buf[4096];
    ssize_t n;
    while ((n = read(fd, buf, sizeof buf)) > 0) fwrite(buf, 1, (size_t)n, stdout);
    close(fd);
    return 0;
}
