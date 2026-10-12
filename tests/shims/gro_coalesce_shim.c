/*
 * gro_coalesce_shim.c - LD_PRELOAD shim reproducing UDP GRO coalescing of
 * back-to-back PMTUD probes.
 *
 * With UDP_GRO set, the kernel may hand one receive buffer holding a datagram
 * followed by a no-larger datagram from the same peer, and reports the segment
 * size only in a UDP_GRO cmsg. A reader that ignores the cmsg sees the two
 * sizes summed. Real coalescing depends on arrival timing, so this shim makes
 * it deterministic for MSC_UDP_PROBE datagrams read with recvfrom(): after
 * returning a probe it waits briefly for the next queued datagram and, if that
 * is also a probe no larger than the first, appends it to the same buffer.
 * Every other datagram, socket type and call passes straight through.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dlfcn.h>
#include <poll.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>

#include "../../src/udp_session.h"

#define COALESCE_WAIT_MS 50   /* longer than the sender's 10 ms inter-round pause */

static ssize_t (*real_recvfrom)(int, void *, size_t, int, struct sockaddr *, socklen_t *);

__attribute__((constructor)) static void init(void)
{
   real_recvfrom = dlsym(RTLD_NEXT, "recvfrom");
}

static int is_probe(const void *buf, ssize_t n)
{
   struct msc_udp_packet_header h;
   if (n < (ssize_t)sizeof(h)) return 0;
   memcpy(&h, buf, sizeof(h));
   return ntohl(h.magic) == MSC_UDP_MAGIC && ntohl(h.type) == MSC_UDP_PROBE;
}

static int is_datagram_socket(int fd)
{
   int type = 0;
   socklen_t len = sizeof(type);
   return getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &len) == 0 && type == SOCK_DGRAM;
}

ssize_t recvfrom(int fd, void *buf, size_t len, int flags,
                 struct sockaddr *addr, socklen_t *addrlen)
{
   ssize_t n = real_recvfrom(fd, buf, len, flags, addr, addrlen);
   char next[MSC_UDP_MAX_DATAGRAM];
   struct pollfd pfd;
   ssize_t m;

   if (n <= 0 || (flags & MSG_PEEK) || !is_probe(buf, n) || !is_datagram_socket(fd))
      return n;
   pfd.fd = fd;
   pfd.events = POLLIN;
   if (poll(&pfd, 1, COALESCE_WAIT_MS) <= 0)
      return n;
   m = real_recvfrom(fd, next, sizeof(next), MSG_PEEK | MSG_DONTWAIT, NULL, NULL);
   if (m <= 0 || m > n || (size_t)(n + m) > len || !is_probe(next, m))
      return n;
   m = real_recvfrom(fd, (char *)buf + n, (size_t)m, MSG_DONTWAIT, NULL, NULL);
   return m > 0 ? n + m : n;
}
