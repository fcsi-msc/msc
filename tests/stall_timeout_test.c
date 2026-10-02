#define _GNU_SOURCE
#include "msc.h"

#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static uint64_t now_ms(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * 1000U + (uint64_t)ts.tv_nsec / 1000000U;
}

int main(void)
{
   struct sockaddr_in address;
   socklen_t length = sizeof(address);
   int listener, sender, small = 4096;
   pid_t child;
   char buffer[65536];
   uint64_t started, elapsed;
   int status;

   listener = socket(AF_INET, SOCK_STREAM, 0);
   if (listener < 0) { perror("socket"); return 1; }
   memset(&address, 0, sizeof(address));
   address.sin_family = AF_INET;
   address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
   if (bind(listener, (struct sockaddr *)&address, sizeof(address)) != 0 ||
       getsockname(listener, (struct sockaddr *)&address, &length) != 0 ||
       listen(listener, 1) != 0)
   { perror("listen setup"); return 1; }

   child = fork();
   if (child < 0) { perror("fork"); return 1; }
   if (child == 0)
   {
      int peer = accept(listener, NULL, NULL);
      if (peer < 0) _exit(2);
      setsockopt(peer, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
      sleep(3);                    /* simulate a live peer that stopped reading */
      close(peer);
      _exit(0);
   }

   sender = socket(AF_INET, SOCK_STREAM, 0);
   if (sender < 0 ||
       setsockopt(sender, SOL_SOCKET, SO_SNDBUF, &small, sizeof(small)) != 0 ||
       msc_configure_tcp_socket(sender, 200) != 0 ||
       connect(sender, (struct sockaddr *)&address, sizeof(address)) != 0)
   { perror("sender setup"); kill(child, SIGKILL); waitpid(child, NULL, 0); return 1; }

   memset(buffer, 0x5a, sizeof(buffer));
   started = now_ms();
   for (;;)
   {
      ssize_t n = send(sender, buffer, sizeof(buffer), MSG_NOSIGNAL);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) break;
   }
   elapsed = now_ms() - started;
   close(sender);
   close(listener);
   kill(child, SIGKILL);
   waitpid(child, &status, 0);

   if (elapsed >= 2000)
   {
      fprintf(stderr, "TCP stall timeout took %llu ms (expected under 2000 ms)\n",
              (unsigned long long)elapsed);
      return 1;
   }
   printf("TCP stall timeout test passed (%llu ms)\n",
          (unsigned long long)elapsed);
   return 0;
}
