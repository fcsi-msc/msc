/*
 * udp_offload_shim.c - LD_PRELOAD shim simulating a kernel without UDP
 * offloads: any sendmsg carrying a UDP_SEGMENT cmsg fails with EINVAL and
 * setsockopt(SOL_UDP, UDP_GRO) fails with ENOPROTOOPT. The parity suite uses
 * it to pin MSC UDP's attempt-and-degrade behavior -- GSO must fall back to
 * sendmmsg after one logged failure, GRO must silently no-op -- with the
 * transferred bytes still correct.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stddef.h>
#include <netinet/in.h>
#include <netinet/udp.h>
#include <sys/socket.h>

#ifndef SOL_UDP
#define SOL_UDP IPPROTO_UDP
#endif
#ifndef UDP_SEGMENT
#define UDP_SEGMENT 103
#endif
#ifndef UDP_GRO
#define UDP_GRO 104
#endif

static ssize_t (*real_sendmsg)(int, const struct msghdr *, int);
static int (*real_setsockopt)(int, int, int, const void *, socklen_t);

/* Resolve before workers start; lazy writes to static function pointers race
 * when several flows encounter unsupported offloads at the same time. */
__attribute__((constructor)) static void init(void)
{
   real_sendmsg = dlsym(RTLD_NEXT, "sendmsg");
   real_setsockopt = dlsym(RTLD_NEXT, "setsockopt");
}

ssize_t sendmsg(int fd, const struct msghdr *msg, int flags)
{
   struct cmsghdr *c;
   for (c = CMSG_FIRSTHDR((struct msghdr *)msg); c != NULL;
        c = CMSG_NXTHDR((struct msghdr *)msg, c))
      if (c->cmsg_level == SOL_UDP && c->cmsg_type == UDP_SEGMENT)
      {
         errno = EINVAL;
         return -1;
      }
   return real_sendmsg(fd, msg, flags);
}

int setsockopt(int fd, int level, int optname, const void *optval,
               socklen_t optlen)
{
   if (level == SOL_UDP && optname == UDP_GRO)
   {
      errno = ENOPROTOOPT;
      return -1;
   }
   return real_setsockopt(fd, level, optname, optval, optlen);
}
