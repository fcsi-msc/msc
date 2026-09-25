#include "msc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

/* checkmalloc is provided by netutil.o */

int main(int argc, char **argv)
{
   struct argdata sender;
   struct argdata receiver;
   struct jobinfo job;
   int pairs[MAX_STREAMS][2];
   int sender_sockets[MAX_STREAMS];
   int receiver_sockets[MAX_STREAMS];
   int streams;
   int segment_size;
   int i;
   int status;
   pid_t child;

   if (argc != 5)
   {
      fprintf(stderr, "usage: recursive_test source destination streams segment_size\n");
      return 2;
   }
   streams = atoi(argv[3]);
   segment_size = atoi(argv[4]);
   if (streams < 1 || streams > MAX_STREAMS || segment_size < 1)
      return 2;
   memset(&sender, 0, sizeof(sender));
   memset(&receiver, 0, sizeof(receiver));
   memset(&job, 0, sizeof(job));
   sender.sourcefile = argv[1];
   sender.numstreams = streams;
   sender.packetsize = segment_size;
   sender.childinfo = &job;
   receiver.destfile = argv[2];
   receiver.numstreams = streams;
   receiver.packetsize = segment_size;
   sender.checkpoint_path = getenv("MSC_TEST_CHECKPOINT");
   receiver.checkpoint_path = getenv("MSC_TEST_CHECKPOINT");
   sender.resume = getenv("MSC_TEST_RESUME") != NULL;
   receiver.resume = sender.resume;
   sender.force = receiver.force = getenv("MSC_TEST_FORCE") != NULL;
   sender.keep_checkpoint = receiver.keep_checkpoint = getenv("MSC_TEST_KEEP_CHECKPOINT") != NULL;

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
   child = fork();
   if (child < 0)
   {
      perror("fork");
      return 1;
   }
   if (child == 0)
   {
      for (i = 0; i < streams; i++)
         close(sender_sockets[i]);
      receivedirectory(streams, receiver_sockets, &receiver);
      return 0;
   }
   for (i = 0; i < streams; i++)
      close(receiver_sockets[i]);
   xferdirectory(sender_sockets, &sender);
   for (i = 0; i < streams; i++)
      close(sender_sockets[i]);
   if (waitpid(child, &status, 0) < 0 || !WIFEXITED(status) ||
       WEXITSTATUS(status) != 0)
      return 1;
   printf("recursive transfer %lld bytes (%lld reused)\n", job.xfercount, job.reused);
   return 0;
}
