#include <stddef.h>
#include <stdatomic.h>
#include <stdio.h>
#include <sys/resource.h>
extern void *__libc_malloc(size_t);
extern void *__libc_calloc(size_t, size_t);
extern void *__libc_realloc(void *, size_t);
extern void *__libc_memalign(size_t, size_t);
static _Atomic unsigned long calls;
void *malloc(size_t n) { atomic_fetch_add(&calls, 1); return __libc_malloc(n); }
void *calloc(size_t n, size_t size) { atomic_fetch_add(&calls, 1); return __libc_calloc(n, size); }
void *realloc(void *p, size_t n) { atomic_fetch_add(&calls, 1); return __libc_realloc(p, n); }
void *aligned_alloc(size_t a, size_t n) { atomic_fetch_add(&calls, 1); return __libc_memalign(a, n); }
int posix_memalign(void **p, size_t a, size_t n) { atomic_fetch_add(&calls, 1); void *v=__libc_memalign(a,n); if (!v) return 12; *p=v; return 0; }
__attribute__((destructor)) static void report(void) {
    struct rusage r; getrusage(RUSAGE_SELF, &r);
    fprintf(stderr, "allocation_calls=%lu peak_rss_kib=%ld\n", atomic_load(&calls), r.ru_maxrss);
}
