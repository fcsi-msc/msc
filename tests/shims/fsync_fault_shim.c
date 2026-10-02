/* Test-only worker failure injection and fsync recorder. Never link into MSC. */
#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int (*next_create)(pthread_t *, const pthread_attr_t *, void *(*)(void *), void *);
static int (*next_fsync)(int);
static int (*next_fallocate)(int, int, off_t, off_t);
static int (*next_fallocate64)(int, int, off64_t, off64_t);
static atomic_int calls;
static int fail_at, record_fd = -1;
static const char *fail_kind;
static const char *allocation_error;

__attribute__((constructor)) static void init(void)
{
   const char *value = getenv("MSC_TEST_FAIL_THREAD_AT");
   const char *path = getenv("MSC_TEST_FSYNC_LOG");
   next_create = dlsym(RTLD_NEXT, "pthread_create");
   next_fsync = dlsym(RTLD_NEXT, "fsync");
   next_fallocate = dlsym(RTLD_NEXT, "fallocate");
   next_fallocate64 = dlsym(RTLD_NEXT, "fallocate64");
   allocation_error = getenv("MSC_TEST_FALLOCATE_ERROR");
   fail_at = value ? atoi(value) : 0;
   fail_kind = getenv("MSC_TEST_FAIL_FSYNC");
   if (path) record_fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
}

static int fail_allocation(void)
{
   if (allocation_error == NULL) return 0;
   fprintf(stderr, "storage test: injected allocation %s\n", allocation_error);
   errno = strcmp(allocation_error, "unsupported") == 0 ? EOPNOTSUPP : ENOSPC;
   return 1;
}

int fallocate(int fd, int mode, off_t offset, off_t length)
{
   if (fail_allocation()) return -1;
   return next_fallocate(fd, mode, offset, length);
}

int fallocate64(int fd, int mode, off64_t offset, off64_t length)
{
   if (fail_allocation()) return -1;
   return next_fallocate64(fd, mode, offset, length);
}

int pthread_create(pthread_t *t, const pthread_attr_t *a, void *(*fn)(void *), void *arg)
{
   int call = atomic_fetch_add(&calls, 1) + 1;
   if (call == fail_at)
   {
      fprintf(stderr, "fsync test: injected thread failure at %d\n", call);
      return EAGAIN;
   }
   return next_create(t, a, fn, arg);
}

int fsync(int fd)
{
   struct stat st;
   if (fstat(fd, &st) == 0)
   {
      if (record_fd >= 0)
      {
         char proc[64], path[PATH_MAX + 4];
         ssize_t n;
         snprintf(proc, sizeof(proc), "/proc/self/fd/%d", fd);
         path[0] = S_ISDIR(st.st_mode) ? 'D' : 'F'; path[1] = ' ';
         n = readlink(proc, path + 2, PATH_MAX);
         if (n > 0)
         {
            ssize_t written;
            path[n + 2] = '\n';
            written = write(record_fd, path, (size_t)n + 3);
            if (written != n + 3) _exit(99);
         }
      }
      if (fail_kind &&
          ((strcmp(fail_kind, "files") == 0 && S_ISREG(st.st_mode)) ||
           (strcmp(fail_kind, "directories") == 0 && S_ISDIR(st.st_mode))))
      {
         fprintf(stderr, "fsync test: injected %s EIO\n", fail_kind);
         errno = EIO; return -1;
      }
   }
   return next_fsync(fd);
}
