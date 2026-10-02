#include "msc.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

extern int *sockfd;
extern void xferdata_segmented(FILE *, struct argdata *);
extern void readsocket_segmented(int, int *, FILE *, int, off_t);

int msc_debug = 0;
FILE *msc_debugout;

/* checkmalloc is provided by netutil.o */

static int child_succeeded(pid_t child)
{
   int status;
   if (waitpid(child, &status, 0) != child) return 0;
   if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
      fprintf(stderr, "segmented_test child %ld status %d\n", (long)child,
              WIFEXITED(status) ? WEXITSTATUS(status) : -1);
   return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

int main(int argc, char **argv)
{
   struct argdata sender;
   struct jobinfo job;
   int pairs[MAX_STREAMS][2];
   int sender_sockets[MAX_STREAMS];
   int receiver_sockets[MAX_STREAMS];
   int streams;
   int segment_size;
   int expect_success;
   int i;
   pid_t receiver;
   pid_t sender_child;

   if (argc != 6)
   {
      fprintf(stderr,
              "usage: segmented_test source destination streams segment_size "
              "expect_success\n");
      return 2;
   }
   streams = atoi(argv[3]);
   segment_size = atoi(argv[4]);
   expect_success = atoi(argv[5]);
   if (streams < 1 || streams > MAX_STREAMS || segment_size < 1)
      return 2;

   for (i = 0; i < streams; i++)
   {
      if (socketpair(AF_UNIX, SOCK_STREAM, 0, pairs[i]) != 0)
      {
         perror("socketpair");
         return 1;
      }
      sender_sockets[i] = pairs[i][0];
      receiver_sockets[i] = pairs[i][1];
   }

   receiver = fork();
   if (receiver == 0)
   {
      FILE *output;
      for (i = 0; i < streams; i++)
         close(sender_sockets[i]);
      output = fopen(argv[2], "w");
      if (output == NULL)
         return 1;
      readsocket_segmented(streams, receiver_sockets, output, segment_size, 0);
      fclose(output);
      return 0;
   }
   if (receiver < 0)
      return 1;

   sender_child = fork();
   if (sender_child == 0)
   {
      FILE *input;
      for (i = 0; i < streams; i++)
         close(receiver_sockets[i]);
      memset(&sender, 0, sizeof(sender));
      memset(&job, 0, sizeof(job));
      sender.numstreams = streams;
      sender.packetsize = segment_size;
      sender.childinfo = &job;
      sockfd = sender_sockets;
      input = fopen(argv[1], "r");
      if (input == NULL)
         return 1;
      xferdata_segmented(input, &sender);
      fclose(input);
      return 0;
   }
   if (sender_child < 0)
      return 1;

   for (i = 0; i < streams; i++)
   {
      close(sender_sockets[i]);
      close(receiver_sockets[i]);
   }

   if (expect_success)
      return child_succeeded(sender_child) && child_succeeded(receiver) ? 0 : 1;
   return !child_succeeded(sender_child) && !child_succeeded(receiver) ? 0 : 1;
}
