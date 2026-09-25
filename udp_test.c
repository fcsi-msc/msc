/*
 * udp_test.c - loopback integration test for the reliable-UDP data path.
 *
 * Stands in for the SSH rendezvous, in whichever shape the selected control
 * mode wants: an AF_UNIX socketpair for the stdio modes (the default), a TCP
 * connection over 127.0.0.1 for tcp, the reliable UDP shim for many/one. Forks
 * a receiver and a sender and drives a transfer end to end. If the source is a
 * directory the recursive (-R) path is exercised, otherwise the single-file
 * path. Set MSC_UDP_DROP=<pct> to force packet loss and exercise MSC UDP's
 * retransmit path.
 *
 *   usage: udp_test source destination streams [drop_pct]
 */
#include "msc.h"
#include "udp_session.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

int msc_debug = 0;
FILE *msc_debugout;

static struct argdata *make_argdata(int streams, int recursive)
{
   struct argdata *AD = checkmalloc(sizeof(*AD), "argdata");
   const char *payload = getenv("MSC_UDP_PAYLOAD");
   memset(AD, 0, sizeof(*AD));
   /* Mirror msc.c: 0 is a real fd, so a bzero'd struct must not read as "the
    * stdio control channel is fd 0" -- that sails past udp_send()'s `< 0`
    * guard and hands the transport this process's stdin. */
   AD->udp_ctl_rfd = -1;
   AD->udp_ctl_wfd = -1;
   AD->numstreams = streams;
   AD->packetsize = DEFAULT_UDP_PAYLOAD;
   AD->udp = 1;
   /* mirror parse.c's real CLI defaults: GSO/GRO on (MSC owns MSC_UDP_GSO/MSC_UDP_GRO
    * via apply_transport_options, so plain env vars cannot enable them here),
    * -s implicit so MSC UDP's PMTUD auto-raise picks the payload. */
   AD->gso = getenv("MSC_TEST_NO_GSO") == NULL;
   AD->gro = getenv("MSC_TEST_NO_GRO") == NULL;
   AD->recursive = recursive;
   AD->checkpoint_path = getenv("MSC_TEST_CHECKPOINT");
   AD->stall_timeout_ms = getenv("MSC_TEST_STALL_TIMEOUT_MS") != NULL
      ? strtoull(getenv("MSC_TEST_STALL_TIMEOUT_MS"), NULL, 10)
      : DEFAULT_STALL_TIMEOUT_MS;
   AD->resume = getenv("MSC_TEST_RESUME") != NULL;
   AD->keep_checkpoint = getenv("MSC_TEST_KEEP_CHECKPOINT") != NULL;
   if (getenv("MSC_TEST_MULTI") != NULL)
      AD->parentsets = 2;
   if (getenv("MSC_TEST_SRC_OFFSET") != NULL)
      AD->src_offset = (off_t)atoll(getenv("MSC_TEST_SRC_OFFSET"));
   if (getenv("MSC_TEST_DST_OFFSET") != NULL)
      AD->dst_offset = (off_t)atoll(getenv("MSC_TEST_DST_OFFSET"));
   if (getenv("MSC_TEST_LENGTH") != NULL)
      AD->xferlen = atoll(getenv("MSC_TEST_LENGTH"));
   if (getenv("MSC_TEST_NO_CHECKSUM") != NULL)
      AD->no_internal_checksum = 1;
   /* MSC_UDP_PAYLOAD stands in for an explicit -s: it must set
    * packetsize_explicit too, or udp_payload() hands MSC UDP 0 and PMTUD
    * overrides the requested size. */
   if (payload != NULL)
   {
      AD->packetsize = (unsigned int)atoi(payload);
      AD->packetsize_explicit = 1;
   }
   AD->childinfo = checkmalloc(sizeof(struct jobinfo), "childinfo");
   memset(AD->childinfo, 0, sizeof(struct jobinfo));
   return AD;
}

static void free_argdata(struct argdata *AD)
{
   if (AD == NULL)
      return;
   free(AD->childinfo);
   free(AD);
}

int main(int argc, char **argv)
{
   const char *source;
   const char *dest;
   int streams;
   int recursive;
   int listenfd = -1;
   struct sockaddr_in addr;
   socklen_t len = sizeof(addr);
   unsigned int port = 0;
   int ctl_mode;
   int udp_control;
   int stdio_control;
   int ctlpair[2] = { -1, -1 };
   pid_t receiver, sender;
   pid_t killer = -1;
   struct stat srcstat;

   if (argc != 4 && argc != 5)
   {
      fprintf(stderr, "usage: %s source destination streams [drop_pct]\n", argv[0]);
      return 2;
   }
   source = argv[1];
   dest = argv[2];
   streams = atoi(argv[3]);
   if (argc == 5)
      setenv("MSC_UDP_DROP", argv[4], 1);
   if (stat(source, &srcstat) != 0)
   {
      perror("stat source");
      return 1;
   }
   recursive = S_ISDIR(srcstat.st_mode) ? 1 : 0;
   ctl_mode = msc_udp_ctl_mode();
   udp_control = ctl_mode == MSC_UDP_CTLMODE_MANY || ctl_mode == MSC_UDP_CTLMODE_ONE;
   stdio_control = msc_udp_ctl_is_stdio(ctl_mode);

   /* Control rendezvous mirrors the selected engine mode: an already-connected
    * byte stream for the stdio modes (the default), a TCP connection for tcp,
    * or the reliable UDP shim for the many/one integration arms.
    *
    * stdio has no port to dial and nothing to listen on -- in production its
    * channel is the stdin/stdout of the ssh session the CLI spawned. There is
    * no ssh here, but the mode never wanted ssh specifically: udp_session.c
    * describes what it takes as "an AF_UNIX socketpair end, a plain connected
    * byte stream", which is exactly what this hands the two children. */
   if (stdio_control)
   {
      if (socketpair(AF_UNIX, SOCK_STREAM, 0, ctlpair) < 0)
      {
         perror("socketpair");
         return 1;
      }
   }
   else if (udp_control)
   {
      listenfd = msc_udp_control_channel_bind_range(0, 1, &port);
      if (listenfd < 0)
         return 1;
   }
   else
   {
      listenfd = socket(AF_INET, SOCK_STREAM, 0);
      if (listenfd < 0)
      {
         perror("socket");
         return 1;
      }
      memset(&addr, 0, sizeof(addr));
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
      addr.sin_port = 0;
      if (bind(listenfd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
      {
         perror("bind");
         return 1;
      }
      if (getsockname(listenfd, (struct sockaddr *)&addr, &len) < 0)
      {
         perror("getsockname");
         return 1;
      }
      port = ntohs(addr.sin_port);
      if (listen(listenfd, 4) < 0)
      {
         perror("listen");
         return 1;
      }
   }

   receiver = fork();
   if (receiver < 0)
   {
      perror("fork");
      return 1;
   }

   if (receiver == 0)
   {
      /* receiver */
      struct argdata *AD = make_argdata(streams, recursive);
      int controlfd;
      if (stdio_control)
      {
         /* remote.c's receiver wraps its own stdin/stdout here; the socketpair
          * end is the same abstraction, so wrap it the same way. */
         close(ctlpair[0]);
         ctlpair[0] = -1;
         controlfd = msc_udp_control_channel_pipe(ctlpair[1], ctlpair[1]);
      }
      else
         controlfd = udp_control
            ? msc_udp_control_channel_accept(
                 listenfd, MSC_UDP_CONTROL_CONNECT_TIMEOUT_MS)
            : accept(listenfd, NULL, NULL);
      msc_install_signal_handlers();
      if (controlfd < 0)
      {
         perror(stdio_control ? "control channel" : "accept");
         _exit(1);
      }
      if (recursive)
      {
         AD->destfile = (char *)dest;
         AD->force = getenv("MSC_TEST_NO_FORCE") == NULL;
      }
      else
      {
         AD->destfile = (char *)dest;
         AD->force = getenv("MSC_TEST_NO_FORCE") == NULL;
         if (AD->parentsets > 1)
         {
            AD->outfileid = fopen(dest, "r+");
            if (AD->outfileid == NULL)
            {
               perror("fopen shared dest");
               _exit(1);
            }
         }
      }
      udp_receive(AD, controlfd);
      if (AD->outfileid != NULL)
         fclose(AD->outfileid);
      close(controlfd);           /* stdio: a dup of ctlpair[1], not the fd itself */
      if (ctlpair[1] >= 0)
         close(ctlpair[1]);
      free_argdata(AD);
      _exit(0);
   }

   sender = fork();
   if (sender < 0)
   {
      perror("fork sender");
      kill(receiver, SIGTERM);
      return 1;
   }
   if (sender == 0)
   {
      struct argdata *AD = make_argdata(streams, recursive);
      char *host = "127.0.0.1";
      if (listenfd >= 0)
         close(listenfd);
      if (stdio_control)
      {
         /* local_single.c hands udp_send() the ends of the pipes it spawned ssh
          * with; one socketpair end plays both parts here. */
         close(ctlpair[1]);
         ctlpair[1] = -1;
         AD->udp_ctl_rfd = ctlpair[0];
         AD->udp_ctl_wfd = ctlpair[0];
      }
      msc_install_signal_handlers();
      AD->remote_machines = &host;
      AD->finalport = port;
      AD->sourcefile = (char *)source;
      udp_send(AD);
      printf("udp transfer of %lld bytes complete\n",
             (long long)AD->childinfo->xfercount);
      fflush(stdout);
      free_argdata(AD);
      _exit(0);
   }
   else
   {
      int sender_status = 0, receiver_status = 0;
      int sender_code, receiver_code;
      const char *kill_marker = getenv("MSC_TEST_KILL_RECEIVER_AFTER_MARKER");
      if (listenfd >= 0)
         close(listenfd);
      /* Both ends, and before the killer forks: a copy left open here would
       * hold the socketpair alive after a SIGKILLed receiver, so the sender
       * would wait on a channel that can never reach EOF -- exactly the
       * interruption the resume suite depends on seeing. */
      if (ctlpair[0] >= 0) { close(ctlpair[0]); ctlpair[0] = -1; }
      if (ctlpair[1] >= 0) { close(ctlpair[1]); ctlpair[1] = -1; }
      if (kill_marker != NULL)
      {
         killer = fork();
         if (killer == 0)
         {
            unsigned int tries;
            for (tries = 0; tries < 10000 && access(kill_marker, F_OK) != 0; tries++)
               usleep(1000);
            if (access(kill_marker, F_OK) == 0)
            {
               if (getenv("MSC_TEST_KILL_RECEIVER_DELAY_MS") != NULL)
                  usleep((useconds_t)strtoul(getenv("MSC_TEST_KILL_RECEIVER_DELAY_MS"), NULL, 10) * 1000U);
               kill(receiver, SIGKILL);
            }
            _exit(0);
         }
      }
      waitpid(sender, &sender_status, 0);
      sender_code = WIFEXITED(sender_status) ? WEXITSTATUS(sender_status) : MSC_EXIT_INTERNAL;
      if (sender_code != 0 && sender_code != MSC_EXIT_NETWORK)
         kill(receiver, SIGTERM);
      waitpid(receiver, &receiver_status, 0);
      if (killer > 0) waitpid(killer, NULL, 0);
      receiver_code = WIFEXITED(receiver_status) ? WEXITSTATUS(receiver_status) :
         (kill_marker != NULL && WIFSIGNALED(receiver_status) &&
          WTERMSIG(receiver_status) == SIGKILL ? MSC_EXIT_NETWORK : MSC_EXIT_INTERNAL);
      if (sender_code != 0 && sender_code != MSC_EXIT_NETWORK)
         return sender_code;
      if (receiver_code == MSC_EXIT_SIGINT || receiver_code == MSC_EXIT_SIGTERM)
         return receiver_code;
      if (receiver_code != 0) return receiver_code;
      return sender_code;
   }
}
