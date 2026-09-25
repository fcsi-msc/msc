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

ssize_t sendmsg(int fd, const struct msghdr *msg, int flags)
{
   static ssize_t (*real)(int, const struct msghdr *, int);
   struct cmsghdr *c;
   if (real == NULL)
      real = (ssize_t (*)(int, const struct msghdr *, int))
             dlsym(RTLD_NEXT, "sendmsg");
   for (c = CMSG_FIRSTHDR((struct msghdr *)msg); c != NULL;
        c = CMSG_NXTHDR((struct msghdr *)msg, c))
      if (c->cmsg_level == SOL_UDP && c->cmsg_type == UDP_SEGMENT)
      {
         errno = EINVAL;
         return -1;
      }
   return real(fd, msg, flags);
}

int setsockopt(int fd, int level, int optname, const void *optval,
               socklen_t optlen)
{
   static int (*real)(int, int, int, const void *, socklen_t);
   if (real == NULL)
      real = (int (*)(int, int, int, const void *, socklen_t))
             dlsym(RTLD_NEXT, "setsockopt");
   if (level == SOL_UDP && optname == UDP_GRO)
   {
      errno = ENOPROTOOPT;
      return -1;
   }
   return real(fd, level, optname, optval, optlen);
}
