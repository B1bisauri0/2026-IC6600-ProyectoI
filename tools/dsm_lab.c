#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/utsname.h>
#include <ucontext.h>
#include <unistd.h>

#ifndef MAP_FIXED_NOREPLACE
#define MAP_FIXED_NOREPLACE 0x100000
#endif

static char *app, *sys_view;
static size_t pg, win;
static sem_t pager_sem;
static int wake_fd;
static void *volatile fault_addr;
static volatile int fault_write, n_faults;
static int fails;

static void report(const char *name, int ok, const char *detail) {
    printf("[%s] %-34s %s\n", ok ? " OK " : "FALLA", name, detail);
    if (!ok) fails++;
}

static void handler(int sig, siginfo_t *si, void *uc_) {
    ucontext_t *uc = uc_;
    int saved = errno;
    uint64_t v;
    char *a = si->si_addr;
    if (a < app || a >= app + win) { signal(sig, SIG_DFL); raise(sig); return; }
    fault_addr = a;
    fault_write = (uc->uc_mcontext.gregs[REG_ERR] & 2) != 0;
    n_faults++;
    sem_post(&pager_sem);
    while (read(wake_fd, &v, sizeof v) < 0 && errno == EINTR) {}
    errno = saved;
}

static void *pager(void *arg) {
    (void)arg;
    uint64_t one = 1;
    for (;;) {
        while (sem_wait(&pager_sem) < 0 && errno == EINTR) {}
        size_t off = ((char *)fault_addr - app) / pg * pg;
        if (fault_write) {
            mprotect(app + off, pg, PROT_READ | PROT_WRITE);
        } else {
            memset(sys_view + off, 0x5A, pg);   /* "llega" el contenido */
            mprotect(app + off, pg, PROT_READ);
        }
        if (write(wake_fd, &one, sizeof one) < 0) _exit(3);
    }
    return NULL;
}

static void *worker(void *arg) {
    volatile char *p = arg;
    char r = p[10];              /* I -> S  */
    int ok_read = (r == 0x5A);
    p[10] = 0x11;                /* S -> M  */
    return (void *)(intptr_t)(ok_read && p[10] == 0x11 && n_faults == 2);
}

int main(int argc, char **argv) {
    uintptr_t base = argc > 1 ? strtoull(argv[1], NULL, 16) : 0x200000000000ULL;
    win = (size_t)(argc > 2 ? atoi(argv[2]) : 32) << 30;
    pg = sysconf(_SC_PAGESIZE);
    struct utsname u; uname(&u);
    char buf[256];

    snprintf(buf, sizeof buf, "kernel %s, %s, pagina %zu B", u.release, u.machine, pg);
    report("plataforma", strcmp(u.machine, "x86_64") == 0 && pg == 4096, buf);

    int fd = memfd_create("dsm", MFD_CLOEXEC);
    report("memfd_create", fd >= 0, fd >= 0 ? "" : strerror(errno));
    if (fd < 0 || ftruncate(fd, win) < 0) { report("ftruncate", 0, strerror(errno)); return 1; }

    app = mmap((void *)base, win, PROT_NONE, MAP_SHARED | MAP_FIXED_NOREPLACE, fd, 0);
    snprintf(buf, sizeof buf, "0x%lx + %zu GiB -> %s", (unsigned long)base, win >> 30,
             app == MAP_FAILED ? strerror(errno) : "libre");
    report("direccion base (P1)", app == (char *)base, buf);
    if (app != (char *)base) return 1;

    sys_view = mmap(NULL, win, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    report("vista de sistema", sys_view != MAP_FAILED, "");

    struct sigaction sa; memset(&sa, 0, sizeof sa);
    sa.sa_sigaction = handler; sa.sa_flags = SA_SIGINFO;
    sigaction(SIGSEGV, &sa, NULL);
    sem_init(&pager_sem, 0, 0);
    wake_fd = eventfd(0, EFD_CLOEXEC);
    pthread_t pt, wt; void *res;
    pthread_create(&pt, NULL, pager, NULL);
    pthread_create(&wt, NULL, worker, app + 7 * pg);
    pthread_join(wt, &res);
    report("SIGSEGV -> Pager -> eventfd", res != NULL, "lectura I->S y escritura S->M con REG_ERR");

    /* Una llamada al sistema sobre PROT_NONE NO produce SIGSEGV. */
    int before = n_faults, pfd[2]; ssize_t n;
    if (pipe(pfd) < 0) return 1;
    if (write(pfd[1], "x", 1) != 1) return 1;
    n = read(pfd[0], app + 20 * pg, 1);
    int e = errno;
    snprintf(buf, sizeof buf, "read() = %zd, errno = %s, fallos nuevos = %d", n, strerror(e), n_faults - before);
    report("syscall sobre PROT_NONE", n < 0 && e == EFAULT && n_faults == before, buf);

    /* Limite de VMA: paginas alternas con permisos distintos (NOVMA=1 lo omite). */
    if (getenv("NOVMA")) { printf("\nResultado parcial: %d fallas\n", fails); return fails != 0; }
    long maxmap = 0; FILE *f = fopen("/proc/sys/vm/max_map_count", "r");
    if (f) { if (fscanf(f, "%ld", &maxmap) != 1) maxmap = 0; fclose(f); }
    size_t i, limit = win / pg / 2, done = 0;
    for (i = 0; i < limit; i++) {
        if (mprotect(app + (1000 + 2 * i) * pg, pg, PROT_READ) < 0) break;
        done++;
    }
    snprintf(buf, sizeof buf, "max_map_count=%ld; %zu paginas aisladas antes de %s (%.0f MiB)",
             maxmap, done, done < limit ? strerror(errno) : "terminar", done * (double)pg / 1048576.0);
    report("limite de VMA", 1, buf);
 
    printf("\nResultado: %s\n", fails ? "HAY FALLAS, ver arriba" : "todo correcto en esta computadora");
    return fails != 0;
}
