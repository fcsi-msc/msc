/* listen_count_shim.c -- LD_PRELOAD probe that records every listening socket.
 *
 * stdio_ctl_test.sh must prove that the stdio control modes open no listening
 * TCP port. Polling `ss` for listeners cannot do that reliably: the TCP
 * control mode's listener lives only for the few milliseconds of the
 * rendezvous, so a poll misses it as often as not. This shim records each
 * successful listen() call instead, so even a listener that exists for a
 * microsecond is counted.
 *
 * Env:
 *   MSC_TEST_LISTEN_LOG=PATH   append one line per successful listen() on a
 *                              TCP socket (unset = pure pass-through)
 *
 * The log is opened O_APPEND per call, so the sender, the receiver and any
 * forked children can all share one file without coordinating.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

int listen(int fd, int backlog)
{
   static int (*real_listen)(int, int);
   const char *path;
   int type = 0;
   socklen_t len = sizeof(type);
   int rc;

   if (real_listen == NULL)
      real_listen = (int (*)(int, int))dlsym(RTLD_NEXT, "listen");
   rc = real_listen(fd, backlog);

   path = getenv("MSC_TEST_LISTEN_LOG");
   if (rc == 0 && path != NULL && path[0] != '\0' &&
       getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &len) == 0 &&
       type == SOCK_STREAM)
   {
      int log = open(path, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0600);
      if (log >= 0)
      {
         char line[64];
         int n = snprintf(line, sizeof(line), "%ld listen fd=%d\n",
                          (long)getpid(), fd);
         if (n > 0 && write(log, line, (size_t)n) < 0) { /* best effort */ }
         close(log);
      }
   }
   return rc;
}
