/*
 * udp_io.c - shared byte-order, socket I/O, allocation, and clock helpers.
 *
 * These mirror msc's netutil.c so MSC UDP is self-contained: it shares no object
 * files with msc, only the proven shapes.
 */
#include "udp_session.h"
#include "msc.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <time.h>
#include <unistd.h>

/* Lustre's superblock magic, so detection works even without the Lustre headers. */
#ifndef LL_SUPER_MAGIC
#define LL_SUPER_MAGIC 0x0BD00BD0
#endif

#ifdef HAVE_LUSTRE
#include <lustre/lustreapi.h>
/* lov_user_md plus room for the per-OST object array the query fills in. */
#define MSC_UDP_LUM_BUFSIZE 65536
/* RAID0 (plain striping) is pattern 0; guard in case an older header omits it. */
#ifndef LOV_PATTERN_RAID0
#define LOV_PATTERN_RAID0 0
#endif
#endif

void *msc_udp_alloc(size_t size, const char *what)
{
   void *t = malloc(size);
   if (t == NULL)
   {
      fprintf(stderr, "MSC UDP: out of memory allocating %s\n", what);
      exit(MSC_EXIT_INTERNAL);
   }
   return t;
}

uint64_t msc_udp_hton64(uint64_t v)
{
   uint32_t hi = htonl((uint32_t)(v >> 32));
   uint32_t lo = htonl((uint32_t)(v & 0xffffffffU));
   return ((uint64_t)lo << 32) | hi;
}

uint64_t msc_udp_ntoh64(uint64_t v)
{
   uint32_t hi = ntohl((uint32_t)(v & 0xffffffffU));
   uint32_t lo = ntohl((uint32_t)(v >> 32));
   return ((uint64_t)hi << 32) | lo;
}

int msc_udp_send_all(int fd, const void *buf, size_t n)
{
   const char *p = buf;
   size_t sent = 0;
   /* UDP control modes (MSC_UDP_CTL=many|one) register the fd with the control_channel
    * shim; msc_udp_send_all/msc_udp_recv_all are used ONLY for the control channel, so
    * this dispatch reroutes every control call site without touching them.
    * TCP mode never registers, so the lookup fails and nothing changes. */
   {
      struct msc_udp_control_channel *channel = msc_udp_control_channel_lookup(fd);
      if (channel != NULL)
         return msc_udp_control_channel_send(channel, buf, n);
   }
   while (sent < n)
   {
      ssize_t w = write(fd, p + sent, n - sent);
      if (w < 0 && errno == EINTR)
         continue;
      if (w <= 0)
      {
         if (w < 0)
            fprintf(stderr, "MSC UDP control channel send failed: %s\n", strerror(errno));
         return -1;
      }
      sent += (size_t)w;
   }
   return 0;
}

int msc_udp_recv_all(int fd, void *buf, size_t n)
{
   char *p = buf;
   size_t got = 0;
   {
      struct msc_udp_control_channel *channel = msc_udp_control_channel_lookup(fd);
      if (channel != NULL)
         return msc_udp_control_channel_recv(channel, buf, n);
   }
   while (got < n)
   {
      ssize_t r = recv(fd, p + got, n - got, 0);
      if (r < 0 && errno == EINTR)
         continue;
      if (r <= 0)
      {
         if (r < 0)
            fprintf(stderr, "MSC UDP control channel receive failed: %s\n", strerror(errno));
         else
            fprintf(stderr, "MSC UDP control channel closed mid-record\n");
         return -1;
      }
      got += (size_t)r;
   }
   return 0;
}

int msc_udp_pread_all(int fd, void *buf, size_t n, off_t off)
{
   char *p = buf;
   size_t total = 0;
   while (total < n)
   {
      ssize_t r = pread(fd, p + total, n - total, off + total);
      if (r < 0 && errno == EINTR)
         continue;
      if (r <= 0)
         return -1;
      total += (size_t)r;
   }
   return 0;
}

int msc_udp_pwrite_all(int fd, const void *buf, size_t n, off_t off)
{
   const char *p = buf;
   size_t total = 0;
   while (total < n)
   {
      ssize_t w = pwrite(fd, p + total, n - total, off + total);
      if (w < 0 && errno == EINTR)
         continue;
      if (w <= 0)
         return -1;
      total += (size_t)w;
   }
   return 0;
}

/* Write iovcnt buffers to consecutive bytes starting at off, retrying short
 * writes and EINTR. The iov array is consumed in place, so the caller must not
 * reuse it afterward. */
int msc_udp_pwritev_all(int fd, struct iovec *iov, int iovcnt, off_t off)
{
   off_t pos = off;
   while (iovcnt > 0)
   {
      ssize_t w = pwritev(fd, iov, iovcnt, pos);
      if (w < 0 && errno == EINTR)
         continue;
      if (w <= 0)
         return -1;
      pos += w;
      while (iovcnt > 0 && (size_t)w >= iov->iov_len)
      {
         w -= (ssize_t)iov->iov_len;
         iov++;
         iovcnt--;
      }
      if (iovcnt > 0 && w > 0)
      {
         iov->iov_base = (char *)iov->iov_base + w;
         iov->iov_len -= (size_t)w;
      }
   }
   return 0;
}

uint64_t msc_udp_now_ns(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ----- Lustre layout helpers --------------------------------------------- */
int msc_udp_is_lustre(const char *path)
{
   struct statfs sfs;
   if (path == NULL)
      return 0;
   if (statfs(path, &sfs) != 0)
      return 0;
   return sfs.f_type == (__typeof__(sfs.f_type))LL_SUPER_MAGIC;
}

/* Same test on an already-open fd (the receiver has fds, not paths). Used to pick
 * the dest write path: mmap for non-Lustre, pwritev for Lustre (mmap writeback on
 * Lustre is ~7x slower than large coalesced pwritev). */
int msc_udp_fd_is_lustre(int fd)
{
   struct statfs sfs;
   if (fd < 0 || fstatfs(fd, &sfs) != 0)
      return 0;
   return sfs.f_type == (__typeof__(sfs.f_type))LL_SUPER_MAGIC;
}

int msc_udp_query_stripe(const char *path, uint64_t *stripe_size, uint32_t *stripe_count)
{
#ifdef HAVE_LUSTRE
   struct lov_user_md *lum;

   if (!msc_udp_is_lustre(path))
      return -1;
   lum = malloc(MSC_UDP_LUM_BUFSIZE);
   if (lum == NULL)
      return -1;
   if (llapi_file_get_stripe(path, lum) != 0)
   {
      free(lum);   /* no explicit layout (e.g. default-striped dir): caller treats as 1 */
      return -1;
   }
   if (stripe_size != NULL)
      *stripe_size = (uint64_t)lum->lmm_stripe_size;
   if (stripe_count != NULL)
      *stripe_count = (uint32_t)lum->lmm_stripe_count;
   free(lum);
   return 0;
#else
   (void)path;
   (void)stripe_size;
   (void)stripe_count;
   return -1;
#endif
}

int msc_lustre_create_striped(const char *path, int flags, mode_t mode,
                              uint64_t stripe_size, long stripe_count)
{
#ifdef HAVE_LUSTRE
   int fd;

   if (path == NULL || stripe_count == 0 || stripe_count < -1)
   {
      errno = EINVAL;
      return -1;
   }
   fd = llapi_file_open(path, flags, (int)mode, (unsigned long long)stripe_size,
                        -1, (int)stripe_count, LOV_PATTERN_RAID0);
   if (fd >= 0)
      return fd;
   /* llapi reports failure as -errno, and a failed SETSTRIPE ioctl can leave a
    * layoutless file behind. Remove it: an explicit request must never survive
    * as a silently default-striped destination. */
   if (fd < -1)
      errno = -fd;
   unlink(path);
   return -1;
#else
   (void)path;
   (void)flags;
   (void)mode;
   (void)stripe_size;
   (void)stripe_count;
   errno = ENOTSUP;
   return -1;
#endif
}

int msc_lustre_check_stripe_count(const char *path, long want, uint32_t *actual)
{
   uint32_t have = 0;

   if (actual != NULL)
      *actual = 0;
   if (want == 0)
      return 0;
   if (msc_udp_query_stripe(path, NULL, &have) != 0)
      return -1;
   if (actual != NULL)
      *actual = have;
   /* -1 resolves to "every OST" only at creation time, and the filesystem's OST
    * total is not part of a file's layout, so all that can be confirmed after
    * the fact is that the file carries a real striped layout. */
   if (want == -1)
      return have >= 1 ? 0 : -1;
   return (long)have == want ? 0 : -1;
}

int msc_udp_open_dest(const char *path, uint64_t stripe_size, uint32_t stripe_count,
                      long dest_stripe_count, int force)
{
   int flags = O_RDWR | O_CREAT | (force ? O_TRUNC : 0);

   if (path == NULL)
      return -1;

   /* An explicit --dest-stripe-count is a request, not a hint: it overrides the
    * source layout, and a Lustre failure is reported rather than degraded into a
    * default-striped file (the caller turns -1 into MSC_EXIT_DESTINATION). */
   if (dest_stripe_count != 0)
   {
      struct stat st;
      /* The layout is fixed at creation, so an existing inode -- normally the
       * 0600 O_EXCL temp make_temp_destination just created -- has to go first.
       * Recreate it with the mode it already had so publication cannot widen the
       * destination's permissions, and with O_EXCL so the unlink/create window
       * cannot be filled by someone else. */
      mode_t mode = stat(path, &st) == 0 ? (st.st_mode & 07777) : 0644;
      unlink(path);
      return msc_lustre_create_striped(path,
                                       (flags & ~O_TRUNC) | O_EXCL | O_NOFOLLOW,
                                       mode, stripe_size, dest_stripe_count);
   }
#ifdef HAVE_LUSTRE
   if (stripe_count > 1)
   {
      int fd;
      /* The Lustre layout can only be set at creation time, so an existing inode
       * (e.g. a pre-created temp) must go first; we are about to overwrite it
       * anyway. unlink-ENOENT is fine. */
      unlink(path);
      fd = llapi_file_open(path, flags, 0644, (unsigned long long)stripe_size,
                           -1, (int)stripe_count, LOV_PATTERN_RAID0);
      if (fd >= 0)
         return fd;
      /* any layout error (old kernel, quota, race): this is only a source-layout
       * heuristic, so fall back to a plain open */
   }
#else
   (void)stripe_size;
   (void)stripe_count;
#endif
   return open(path, flags, 0644);
}

int msc_udp_checksum_fd(int fd, off_t off, uint64_t len, uint64_t *sum)
{
   const size_t bufsz = 1024 * 1024;
   unsigned char *buf = msc_udp_alloc(bufsz, "checksum buffer");
   uint64_t h = 1469598103934665603ULL;   /* FNV-1a 64-bit offset basis */
   uint64_t done = 0;
   int rc = 0;

   while (done < len)
   {
      size_t want = (size_t)((len - done) > (uint64_t)bufsz ? bufsz : (len - done));
      size_t i;
      if (msc_udp_pread_all(fd, buf, want, off + (off_t)done) != 0)
      {
         rc = -1;
         goto out;
      }
      for (i = 0; i < want; i++)
      {
         h ^= (uint64_t)buf[i];
         h *= 1099511628211ULL;
      }
      done += want;
   }

   *sum = h;
out:
   free(buf);
   return rc;
}
