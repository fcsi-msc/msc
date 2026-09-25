/* storage_fault_shim.c -- LD_PRELOAD destination-storage fault injection.
 *
 * Makes the receiver's writes start failing with ENOSPC once it has written
 * MSC_TEST_ENOSPC_AFTER_BYTES bytes, so the resume suites can assert that a
 * disk filling up mid-transfer is reported as the stable exit 4 (destination)
 * and leaves a REUSABLE checkpoint behind -- the routine production failure
 * that a transfer tool has to get right.
 *
 * Used by udp_resume_test.sh and udp_recursive_resume_test.sh (make
 * udp_resume_suite).
 *
 * Env:
 *   MSC_TEST_ENOSPC_AFTER_BYTES=N   fail once N bytes have been written
 *                                   (unset or 0 = never fail; the shim is then
 *                                   a pure pass-through)
 *   MSC_TEST_ENOSPC_ERRNO=E         errno to report (default ENOSPC)
 *
 * Only the destination write path is intercepted -- pwrite/pwritev/write/writev
 * (udp_io.c uses pwrite and pwritev for the non-mmap path, which is why the
 * suites pass MSC_UDP_NO_MMAP=1; write/writev are covered so the shim stays
 * useful if that path changes). Reads, opens and everything else are untouched,
 * so a preloaded process behaves normally right up to the injected failure.
 *
 * The byte counter is process-wide and atomic: the receiver writes from several
 * flow threads at once, and the threshold has to mean the same thing whichever
 * of them crosses it.
 */
#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

static atomic_ullong g_written;      /* bytes let through so far */
static unsigned long long g_limit;   /* 0 = never fail */
static int g_errno = ENOSPC;
static int g_ready;

static ssize_t (*real_write)(int, const void *, size_t);
static ssize_t (*real_pwrite)(int, const void *, size_t, off_t);
static ssize_t (*real_writev)(int, const struct iovec *, int);
static ssize_t (*real_pwritev)(int, const struct iovec *, int, off_t);
static ssize_t (*real_pwrite64)(int, const void *, size_t, off64_t);
static ssize_t (*real_pwritev64)(int, const struct iovec *, int, off64_t);

static void shim_init(void)
{
   const char *v;
   if (g_ready)
      return;
   real_write   = dlsym(RTLD_NEXT, "write");
   real_pwrite  = dlsym(RTLD_NEXT, "pwrite");
   real_writev  = dlsym(RTLD_NEXT, "writev");
   real_pwritev = dlsym(RTLD_NEXT, "pwritev");
   v = getenv("MSC_TEST_ENOSPC_AFTER_BYTES");
   g_limit = v != NULL ? strtoull(v, NULL, 10) : 0;
   v = getenv("MSC_TEST_ENOSPC_ERRNO");
   if (v != NULL && *v != '\0')
      g_errno = atoi(v);
   g_ready = 1;
}

/* Charge `n` bytes against the budget. Returns 0 to let the call through, or
 * -1 when the budget is spent (errno already set).
 *
 * A call that would STRADDLE the limit is failed outright rather than
 * short-written: a real ENOSPC can do either, and failing whole keeps the
 * suites' post-conditions exact (the checkpoint's durable byte count stays on
 * the chunk boundary the test computed). */
static int charge(size_t n)
{
   unsigned long long before;
   if (g_limit == 0)
      return 0;
   before = atomic_fetch_add(&g_written, (unsigned long long)n);
   if (before + (unsigned long long)n > g_limit)
   {
      atomic_fetch_sub(&g_written, (unsigned long long)n);   /* nothing written */
      errno = g_errno;
      return -1;
   }
   return 0;
}

static size_t iov_bytes(const struct iovec *iov, int iovcnt)
{
   size_t total = 0;
   int i;
   for (i = 0; i < iovcnt; i++)
      total += iov[i].iov_len;
   return total;
}

ssize_t write(int fd, const void *buf, size_t n)
{
   shim_init();
   if (charge(n) != 0)
      return -1;
   return real_write(fd, buf, n);
}

ssize_t pwrite(int fd, const void *buf, size_t n, off_t off)
{
   shim_init();
   if (charge(n) != 0)
      return -1;
   return real_pwrite(fd, buf, n, off);
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt)
{
   shim_init();
   if (charge(iov_bytes(iov, iovcnt)) != 0)
      return -1;
   return real_writev(fd, iov, iovcnt);
}

ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t off)
{
   shim_init();
   if (charge(iov_bytes(iov, iovcnt)) != 0)
      return -1;
   return real_pwritev(fd, iov, iovcnt, off);
}

/* The large-file entry points are the ones that actually matter here, and
 * missing them is a silent no-op rather than an error: msc is built with
 * -D_FILE_OFFSET_BITS=64 semantics, so `nm -D` on the binary shows it importing
 * pwrite64/pwritev64 and never pwrite/pwritev. Interposing only the unsuffixed
 * names let a 4 MiB transfer sail past a 1.25 MiB fault budget and the suite
 * saw exit 0 where it wanted exit 4. */
ssize_t pwrite64(int fd, const void *buf, size_t n, off64_t off)
{
   shim_init();
   if (charge(n) != 0)
      return -1;
   if (real_pwrite64 == NULL)
      real_pwrite64 = dlsym(RTLD_NEXT, "pwrite64");
   return real_pwrite64(fd, buf, n, off);
}

ssize_t pwritev64(int fd, const struct iovec *iov, int iovcnt, off64_t off)
{
   shim_init();
   if (charge(iov_bytes(iov, iovcnt)) != 0)
      return -1;
   if (real_pwritev64 == NULL)
      real_pwritev64 = dlsym(RTLD_NEXT, "pwritev64");
   return real_pwritev64(fd, iov, iovcnt, off);
}
