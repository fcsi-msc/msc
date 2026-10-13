/*
 * udp_offload_shim.c - LD_PRELOAD shim for MSC UDP's segmentation-offload
 * error handling.
 *
 * Default mode simulates a kernel without UDP offloads: any sendmsg carrying a
 * UDP_SEGMENT cmsg fails with EINVAL and setsockopt(SOL_UDP, UDP_GRO) fails
 * with ENOPROTOOPT. The parity suite uses it to pin MSC UDP's
 * attempt-and-degrade behavior -- GSO must fall back to sendmmsg after one
 * logged failure, GRO must silently no-op -- with the transferred bytes still
 * correct.
 *
 * Env:
 *   MSC_TEST_GSO_FAIL_AT=N     single-failure mode: simulate a kernel that has
 *                              both offloads, and fail only the Nth sendmsg
 *                              carrying UDP_SEGMENT (1-based, counted across
 *                              all threads of the process). Every other call,
 *                              and UDP_GRO, passes through.
 *   MSC_TEST_GSO_FAIL_ERRNO=E  errno for that failure: EINVAL, EIO, EMSGSIZE,
 *                              EOPNOTSUPP, EPERM, ENOBUFS or EAGAIN, or a
 *                              number (default EINVAL)
 *
 * In single-failure mode the shim reports the injection on stderr when it
 * happens, and when the preloading process exits it prints
 *   udp_offload_shim: S UDP_SEGMENT sends, D data datagrams sent
 * where S counts every UDP_SEGMENT sendmsg (the failed one included) and D
 * counts the MSC DATA datagrams that sendmsg/sendmmsg actually handed the
 * kernel, a UDP_SEGMENT send contributing one per segment. Both counters live
 * in shared memory so a forked sender is included; it may leave with _exit().
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dlfcn.h>
#include <errno.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netinet/in.h>
#include <netinet/udp.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

#ifndef SOL_UDP
#define SOL_UDP IPPROTO_UDP
#endif
#ifndef UDP_SEGMENT
#define UDP_SEGMENT 103
#endif
#ifndef UDP_GRO
#define UDP_GRO 104
#endif

/* MSC UDP's common packet header begins with network-order magic and type.
 * Kept independent of udp_session.h so the shim builds on its own. */
#define MSC_UDP_WIRE_MAGIC 0x4d534331U
#define MSC_UDP_WIRE_DATA  1U

static ssize_t (*real_sendmsg)(int, const struct msghdr *, int);
static int (*real_sendmmsg)(int, struct mmsghdr *, unsigned int, int);
static int (*real_setsockopt)(int, int, int, const void *, socklen_t);
static int fail_at;              /* 0 = default mode: every UDP_SEGMENT send fails */
static int fail_errno = EINVAL;
static const char *fail_name = "EINVAL";
static pid_t owner;              /* the process that loaded the shim reports */

struct counters
{
   atomic_int segment_sends;     /* UDP_SEGMENT sendmsg calls, failed or not */
   atomic_ullong data_sent;      /* DATA datagrams handed to the kernel */
};
static struct counters local_counters;
static struct counters *counters = &local_counters;

static const struct { const char *name; int value; } errnos[] =
{
   { "EINVAL", EINVAL }, { "EIO", EIO }, { "EMSGSIZE", EMSGSIZE },
   { "EOPNOTSUPP", EOPNOTSUPP }, { "EPERM", EPERM }, { "ENOBUFS", ENOBUFS },
   { "EAGAIN", EAGAIN },
};

/* Resolve before workers start; lazy writes to static function pointers race
 * when several flows encounter unsupported offloads at the same time. */
__attribute__((constructor)) static void init(void)
{
   const char *v;
   size_t i;
   real_sendmsg = dlsym(RTLD_NEXT, "sendmsg");
   real_sendmmsg = dlsym(RTLD_NEXT, "sendmmsg");
   real_setsockopt = dlsym(RTLD_NEXT, "setsockopt");
   v = getenv("MSC_TEST_GSO_FAIL_AT");
   fail_at = v != NULL ? atoi(v) : 0;
   if (fail_at < 0) fail_at = 0;
   v = getenv("MSC_TEST_GSO_FAIL_ERRNO");
   if (v != NULL && *v != '\0')
   {
      fail_name = v;
      fail_errno = atoi(v);
      for (i = 0; i < sizeof(errnos) / sizeof(errnos[0]); i++)
         if (strcmp(v, errnos[i].name) == 0)
            fail_errno = errnos[i].value;
   }
   if (fail_at != 0)
   {
      void *p = mmap(NULL, sizeof(*counters), PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_ANONYMOUS, -1, 0);
      if (p != MAP_FAILED)
         counters = p;   /* zero-filled */
      owner = getpid();
   }
}

__attribute__((destructor)) static void fini(void)
{
   if (fail_at != 0 && getpid() == owner)
      fprintf(stderr, "udp_offload_shim: %d UDP_SEGMENT sends, %llu data "
              "datagrams sent\n", atomic_load(&counters->segment_sends),
              (unsigned long long)atomic_load(&counters->data_sent));
}

/* UDP_SEGMENT size carried by msg, or 0 */
static unsigned segment_size(const struct msghdr *msg)
{
   struct cmsghdr *c;
   for (c = CMSG_FIRSTHDR((struct msghdr *)msg); c != NULL;
        c = CMSG_NXTHDR((struct msghdr *)msg, c))
      if (c->cmsg_level == SOL_UDP && c->cmsg_type == UDP_SEGMENT)
      {
         uint16_t gso;
         memcpy(&gso, CMSG_DATA(c), sizeof(gso));
         return gso != 0 ? gso : 1;
      }
   return 0;
}

/* MSC DATA datagrams a successfully sent msg put on the wire */
static unsigned data_datagrams(const struct msghdr *msg)
{
   unsigned char header[8];
   uint32_t magic, type;
   size_t copied = 0, total = 0, i;
   unsigned gso;
   for (i = 0; i < msg->msg_iovlen; i++)
   {
      size_t take = msg->msg_iov[i].iov_len;
      if (copied < sizeof(header))
      {
         if (take > sizeof(header) - copied) take = sizeof(header) - copied;
         memcpy(header + copied, msg->msg_iov[i].iov_base, take);
         copied += take;
      }
      total += msg->msg_iov[i].iov_len;
   }
   if (copied != sizeof(header)) return 0;
   memcpy(&magic, header, sizeof(magic));
   memcpy(&type, header + sizeof(magic), sizeof(type));
   if (ntohl(magic) != MSC_UDP_WIRE_MAGIC || ntohl(type) != MSC_UDP_WIRE_DATA)
      return 0;
   gso = segment_size(msg);
   return gso != 0 ? (unsigned)((total + gso - 1) / gso) : 1;
}

ssize_t sendmsg(int fd, const struct msghdr *msg, int flags)
{
   ssize_t s;
   if (segment_size(msg) != 0)
   {
      int call;
      if (fail_at == 0)
      {
         errno = EINVAL;
         return -1;
      }
      call = atomic_fetch_add(&counters->segment_sends, 1) + 1;
      if (call == fail_at)
      {
         fprintf(stderr, "udp_offload_shim: injected %s into UDP_SEGMENT "
                 "sendmsg %d\n", fail_name, call);
         errno = fail_errno;
         return -1;
      }
   }
   s = real_sendmsg(fd, msg, flags);
   if (fail_at != 0 && s >= 0)
      atomic_fetch_add(&counters->data_sent, data_datagrams(msg));
   return s;
}

int sendmmsg(int fd, struct mmsghdr *msgvec, unsigned int vlen, int flags)
{
   int sent = real_sendmmsg(fd, msgvec, vlen, flags);
   int i;
   if (fail_at != 0)
      for (i = 0; i < sent; i++)
         atomic_fetch_add(&counters->data_sent, data_datagrams(&msgvec[i].msg_hdr));
   return sent;
}

int setsockopt(int fd, int level, int optname, const void *optval,
               socklen_t optlen)
{
   if (fail_at == 0 && level == SOL_UDP && optname == UDP_GRO)
   {
      errno = ENOPROTOOPT;
      return -1;
   }
   return real_setsockopt(fd, level, optname, optval, optlen);
}
