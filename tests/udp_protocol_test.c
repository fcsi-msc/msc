/*
 * Protocol-negotiation regression: a fake receiver advertises an incompatible
 * protocol version and the MSC UDP engine must reject it before data transfer.
 *
 * The rendezvous follows MSC_UDP_CTL, because the control mode decides how the
 * sender reaches a receiver at all: the stdio modes (the default) are handed a
 * socketpair, everything else dials the TCP listener.  Getting this wrong is
 * silent rather than loud -- a sender that refuses to start for the WRONG
 * reason also returns NULL, which is exactly what this test asserts, so the
 * mismatch would be declared rejected without a byte of protocol crossing.
 */
#include "msc.h"
#include "udp_session.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

int msc_debug = 0;
FILE *msc_debugout;

static int send_u32_test(int fd, uint32_t value)
{
   value = htonl(value);
   return sendall(fd, &value, sizeof(value));
}

static int recv_u32_test(int fd, uint32_t *value)
{
   uint32_t wire;
   if (recvall_exact(fd, &wire, sizeof(wire)) != 0) return -1;
   *value = ntohl(wire);
   return 0;
}

static void child_fail(const char *stage)
{
   fprintf(stderr, "udp_protocol_test fake receiver failed during %s\n", stage);
   _exit(1);
}

int main(void)
{
   int listenfd = -1, controlfd, udpfd, status;
   unsigned int udpport;
   struct sockaddr_in addr;
   socklen_t addrlen = sizeof(addr);
   pid_t child;
   struct msc_udp_config cfg;
   void *session;
   int stdio_control = msc_udp_ctl_is_stdio(msc_udp_ctl_mode());
   int ctlpair[2] = { -1, -1 };

   memset(&addr, 0, sizeof(addr));
   if (stdio_control)
   {
      /* No port to dial: stdio's control channel is whatever connected byte
       * stream the launcher owns (ssh's pipes in production). */
      if (socketpair(AF_UNIX, SOCK_STREAM, 0, ctlpair) != 0)
      {
         perror("udp_protocol_test socketpair");
         return 1;
      }
   }
   else
   {
      listenfd = socket(AF_INET, SOCK_STREAM, 0);
      if (listenfd < 0)
      {
         perror("udp_protocol_test socket");
         return 1;
      }
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
      if (bind(listenfd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
          listen(listenfd, 1) != 0 ||
          getsockname(listenfd, (struct sockaddr *)&addr, &addrlen) != 0)
      {
         perror("udp_protocol_test listener setup");
         return 1;
      }
   }

   /* The fake receiver's data port is bound here, before the fork, so the
    * parent can name it in MSC_UDP_PORT_LIST: a real sender verifies the
    * advertised port list against its own copy of the request, and an
    * unannounced ephemeral port is exactly what that check exists to reject. */
   {
      struct sockaddr_in udpaddr;
      socklen_t udplen = sizeof(udpaddr);
      char portbuf[16];
      udpfd = socket(AF_INET, SOCK_DGRAM, 0);
      memset(&udpaddr, 0, sizeof(udpaddr));
      udpaddr.sin_family = AF_INET;
      udpaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
      if (udpfd < 0 ||
          bind(udpfd, (struct sockaddr *)&udpaddr, sizeof(udpaddr)) != 0 ||
          getsockname(udpfd, (struct sockaddr *)&udpaddr, &udplen) != 0)
      {
         perror("udp_protocol_test UDP bind");
         return 1;
      }
      udpport = ntohs(udpaddr.sin_port);
      snprintf(portbuf, sizeof(portbuf), "%u", udpport);
      setenv("MSC_UDP_PORT_LIST", portbuf, 1);
   }

   child = fork();
   if (child < 0)
   {
      perror("udp_protocol_test fork");
      return 1;
   }
   if (child == 0)
   {
      uint32_t tag, version, features, id_hi, id_lo;
      if (stdio_control)
      {
         close(ctlpair[0]);
         controlfd = ctlpair[1];
      }
      else
         controlfd = accept(listenfd, NULL, NULL);
      if (controlfd < 0) child_fail("socket setup");
      if (send_u32_test(controlfd, MSC_UDP_CTL_PORTS) != 0 ||
          send_u32_test(controlfd, 1) != 0 ||
          send_u32_test(controlfd, udpport) != 0)
         child_fail("port-list send");
      if (recv_u32_test(controlfd, &tag) != 0 ||
          recv_u32_test(controlfd, &version) != 0 ||
          recv_u32_test(controlfd, &features) != 0 ||
          recv_u32_test(controlfd, &id_hi) != 0 ||
          recv_u32_test(controlfd, &id_lo) != 0)
         child_fail("HELLO receive");
      if (tag != MSC_UDP_CTL_HELLO || version != MSC_UDP_PROTO_VERSION ||
          (id_hi | id_lo) == 0 || (id_hi >> 31) != 0)
         child_fail("HELLO validation");
      if (send_u32_test(controlfd, MSC_UDP_CTL_HELLO) != 0 ||
          send_u32_test(controlfd, MSC_UDP_PROTO_VERSION + 1) != 0 ||
          send_u32_test(controlfd, features) != 0)
         child_fail("mismatched HELLO send");
      close(udpfd);
      close(controlfd);
      _exit(0);
   }

   memset(&cfg, 0, sizeof(cfg));
   cfg.receiver_host = "127.0.0.1";
   cfg.control_port = ntohs(addr.sin_port);
   cfg.flow_count = 1;
   cfg.source_fd = -1;
   cfg.dest_fd = -1;
   if (stdio_control)
   {
      close(ctlpair[1]);
      ctlpair[1] = -1;
      cfg.ctl_fd = msc_udp_control_channel_pipe(ctlpair[0], ctlpair[0]);
      if (cfg.ctl_fd < 0)
      {
         fprintf(stderr, "udp_protocol_test: could not wrap the control pipe\n");
         return 1;
      }
   }
   session = msc_udp_sender_open(&cfg);
   if (session != NULL)
   {
      fprintf(stderr, "udp_protocol_test: mismatched protocol was accepted\n");
      msc_udp_sender_close(session);
      return 1;
   }
   if (waitpid(child, &status, 0) != child)
   {
      perror("udp_protocol_test waitpid");
      return 1;
   }
   if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
   {
      fprintf(stderr, "udp_protocol_test: fake receiver exited abnormally\n");
      return 1;
   }
   return 0;
}
