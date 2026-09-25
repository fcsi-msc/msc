/*
 * port_squat.c -- test helper: bind UDP ports and hold them.
 *
 * The shell has no portable way to occupy a UDP port, and the whole point of
 * the exact/scan port modes is what msc does when one is already taken. Prints
 * "ready" on stdout once every port is bound (so the caller can proceed without
 * racing), then sleeps until killed.
 *
 * Usage: port_squat <port> [port ...]
 * Exits 1 if any port cannot be bound -- the caller should treat that as "pick
 * a different test window", not as an msc failure.
 */
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

int main(int argc, char *argv[])
{
   int i;
   if (argc < 2)
   {
      fprintf(stderr, "usage: %s <port> [port ...]\n", argv[0]);
      return 2;
   }
   for (i = 1; i < argc; i++)
   {
      struct sockaddr_in addr;
      int fd = socket(AF_INET, SOCK_DGRAM, 0);
      if (fd < 0) { perror("socket"); return 1; }
      memset(&addr, 0, sizeof(addr));
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = INADDR_ANY;
      addr.sin_port = htons((uint16_t)atoi(argv[i]));
      if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
      {
         fprintf(stderr, "port_squat: cannot bind %s: ", argv[i]);
         perror("");
         return 1;
      }
   }
   printf("ready\n");
   fflush(stdout);
   for (;;)
      pause();
}
