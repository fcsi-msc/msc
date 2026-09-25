#include "msc.h"

#define __USE_GNU
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <time.h>


int *sockfd;

struct sender_state
{
   struct argdata *AD;
   int inputfd;
   off_t source_base;
   off_t transfer_length;
   off_t next_offset;          /* shared cursor, claimed via __atomic_fetch_add */
   pthread_mutex_t error_lock;
   int error;
   struct msc_checkpoint *checkpoint;
   volatile uint64_t current_bytes;
   volatile uint64_t read_ns;
   volatile uint64_t send_ns;
   uint64_t destination_write_ns;
   uint64_t destination_flush_ns;
   int *sockets;
   unsigned int numstreams;
};

struct sender_worker
{
   struct sender_state *state;
   int socketfd;
   long long int bytes_sent;
};

#define NUMBUFFERS 3
long long int inputcounter[NUMBUFFERS];
char *inputbuffer[NUMBUFFERS];
pthread_mutex_t inputmutex[NUMBUFFERS];
pthread_mutex_t input_lock;

static uint64_t socket_now_ns(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

/* Legacy pipe mode uses the three buffer mutexes as ownership hand-offs, not
 * just exclusion: the reader keeps the buffer being filled locked, then
 * unlocks it for sendout_thread and blocks on the next buffer.  input_lock is
 * a completion latch held by the sender thread for its entire lifetime. */

/* Legacy stdin/command-pipe sender.  File-to-file transfers use the labeled
 * segment workers below, where short I/O and socket errors are handled. */
void *sendout_thread(void *arg)
{
   struct argdata *AD = arg;
   int current = 0;
   int didsomething = 1;
   unsigned int snum = 0;
   int written;
   int offset = 0;

   DEBUG0("start sendout_thread\n");
   DEBUGSYNC;
   pthread_mutex_lock(&input_lock);
   while(didsomething)
   {
      didsomething = 0;
      DEBUG1("sendout_thread: wait on lock buffer %d\n",current);
      pthread_mutex_lock(&inputmutex[current]);
      DEBUG1("sendout_thread: process buffer %d\n",current);
      while (inputcounter[current] > 0)
      {
         size_t towrite = inputcounter[current] < AD->packetsize
                        ? (size_t)inputcounter[current]  /* poss. last buffer */
                        : (size_t)AD->packetsize;
         written = write(sockfd[snum], inputbuffer[current]+offset, towrite);
         if (written < 0 && errno == EINTR)
            continue;
         if (written <= 0)
         {
            /* A failed socket write used to be subtracted as -1, growing the
             * counter and spinning forever.  The pipe path has no resume
             * state, so a dead stream is a fatal transport error. */
            fprintf(stderr, "MSC pipe-mode socket write failed: %s\n",
                    strerror(errno));
            exit(MSC_EXIT_NETWORK);
         }
         inputcounter[current] -= written;
         offset += written;
         snum++;
         if (snum >= AD->numstreams)
            snum = 0;
         didsomething = 1;
      }
      offset = 0;
      /* The loop above should drain this buffer completely. Reset it here so
       * a later pass cannot resend stale bytes if a short-write path changes. */
      inputcounter[current] = 0;
      pthread_mutex_unlock(&inputmutex[current]);
      current++;
      if (current >= NUMBUFFERS)
         current = 0;
   }
   pthread_mutex_unlock(&input_lock);
   return NULL;
}

void opensockets(struct argdata *AD)
{
   struct sockaddr_in serv_addr;
   struct hostent *server;
   unsigned int snum;
   int flag = 1;
   int buffer_size = TCP_SOCKET_BUFFER;

   sockfd = checkmalloc(sizeof(int) * AD->numstreams, "sockfd");

   DEBUG1("open sockets to host: %s\n", (AD->remote_machines)[0]);
   DEBUGSYNC;
   DEBUG1("open sockets to port: %d\n", AD->finalport);
   DEBUGSYNC;
   server = gethostbyname((AD->remote_machines)[0]);

   for (snum = 0; snum < AD->numstreams; snum++)
   {
      sockfd[snum] = socket(AF_INET, SOCK_STREAM, 0);
      if (sockfd[snum] < 0 ||
          msc_configure_tcp_socket(sockfd[snum], AD->stall_timeout_ms) != 0)
      {
         fprintf(stderr, "MSC could not configure TCP stall timeout: %s\n",
                 strerror(errno));
         exit(MSC_EXIT_INTERNAL);
      }
      setsockopt(sockfd[snum], IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(flag));
      setsockopt(sockfd[snum], SOL_SOCKET, SO_SNDBUF,
                 &buffer_size, sizeof(buffer_size));
      if (server == NULL)
      {
         fprintf(stderr,"ERROR, no such host\n");
         exit(MSC_EXIT_NETWORK);
      }
      bzero((char *) &serv_addr, sizeof(serv_addr));
      serv_addr.sin_family = AF_INET;
      bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, 
            server->h_length);
      serv_addr.sin_port = htons(AD->finalport);
      if (connect(sockfd[snum], (struct sockaddr *) 
                  &serv_addr, sizeof(serv_addr)) < 0)
      {
         fprintf(stderr,"ERROR connecting %d\n", errno);
         DEBUG1("ERROR connecting %d\n", errno);
         exit(MSC_EXIT_NETWORK);
      }
   }
}


void xferdata(FILE * in, struct argdata *AD)
{
   int n = 1;
   int lc;
   pthread_t threadid;
   int currentbuffer = 0;
   int oldbuffer;
   int buffersize = 1024*AD->packetsize;
   int toread;
   long long int xfcount = 0;

   DEBUG0("in xferdata\n");
   DEBUGSYNC;
       /* set up pthread and start */
   for (lc = 0; lc < NUMBUFFERS; lc++)
   {
      inputbuffer[lc] = checkmalloc(buffersize, "inputbuffer");
      inputcounter[lc] = 0;
      pthread_mutex_init(&inputmutex[lc], NULL);
      pthread_mutex_init(&input_lock, NULL);
   }
        /* grab lock on first buffer, then start output thread */
   pthread_mutex_lock(&inputmutex[currentbuffer]);
   pthread_create(&threadid, NULL, &sendout_thread, AD);


   DEBUG0("xferdata start xfer\n");
   DEBUGSYNC;
   while (n > 0)
   {
               /* read data from input stream */
      if (AD->xferlen != 0) /* limit on data transferred */
      {
         if ((AD->xferlen - xfcount) > buffersize)
            toread = buffersize;
         else
            toread = AD->xferlen - xfcount;
      }
      else
      {
         toread = buffersize;
      }

      inputcounter[currentbuffer] = fread(inputbuffer[currentbuffer], 1,
                                              toread, in);

      n = inputcounter[currentbuffer];
        /* track how much data we have read */
      xfcount += n;
      DEBUG1("xferdata read from file to buffer %d\n",currentbuffer);
      /* fread on a blocking stream returns short only at EOF or error, so no
       * separate EOF probe is needed.  The old one-byte probe read could
       * consume and silently discard a byte of pipe data. */
      if (n == 0 && ferror(in) != 0)
      {
         fprintf(stderr, "MSC pipe-mode input read failed\n");
         exit(MSC_EXIT_SOURCE);
      }

      if (n > 0)
      {
                 /* change buffers and give full one to output thread */
         oldbuffer = currentbuffer;
         currentbuffer++;
         if (currentbuffer >= NUMBUFFERS)
            currentbuffer = 0;
         DEBUG1("xferdata lock mutex %d\n",currentbuffer);
         pthread_mutex_lock(&inputmutex[currentbuffer]);
         DEBUG1("xferdata unlock mutex %d\n",oldbuffer);
         pthread_mutex_unlock(&inputmutex[oldbuffer]);
      }
      else
      {
         /* read returned zero, shut down */
         /* in robust program, check for EAGAIN */
         
         DEBUG1("xferdata shutdown unlock mutex %d\n",currentbuffer);
         pthread_mutex_unlock(&inputmutex[currentbuffer]);
         DEBUG0("xferdata shutdown lock input_lock\n");
         pthread_mutex_lock(&input_lock); /* wait for thread to exit */
         DEBUG0("xferdata shutdown post input_lock\n");
      }
   }
   AD->childinfo->xfercount = xfcount;
}

void closesockets(struct argdata *AD)
{
   unsigned int snum;

   for (snum = 0; snum < AD->numstreams; snum++)
   {
      close(sockfd[snum]);
   }
}

static void set_sender_error(struct sender_state *state)
{
   unsigned int i;
   pthread_mutex_lock(&state->error_lock);
   if (!state->error)
   {
      state->error = 1;
      for (i = 0; i < state->numstreams; i++)
         shutdown(state->sockets[i], SHUT_RDWR);
   }
   pthread_mutex_unlock(&state->error_lock);
}

static int sender_has_error(struct sender_state *state)
{
   int error;

   pthread_mutex_lock(&state->error_lock);
   error = state->error;
   pthread_mutex_unlock(&state->error_lock);
   return error;
}

static int send_iov_all(int fd, struct iovec *iov, int iovcnt)
{
   while (iovcnt > 0)
   {
      ssize_t written = writev(fd, iov, iovcnt);

      if (written < 0 && errno == EINTR)
         continue;
      if (written <= 0)
      {
         if (written < 0)
            fprintf(stderr, "MSC socket writev failed: %s\n", strerror(errno));
         return -1;
      }

      while (iovcnt > 0 && (size_t)written >= iov[0].iov_len)
      {
         written -= iov[0].iov_len;
         iov++;
         iovcnt--;
      }
      if (iovcnt > 0 && written > 0)
      {
         iov[0].iov_base = (char *)iov[0].iov_base + written;
         iov[0].iov_len -= written;
      }
   }
   return 0;
}

static void *segment_sender_thread(void *arg)
{
   struct sender_worker *worker = arg;
   struct sender_state *state = worker->state;
   size_t segment_size = state->AD->packetsize;
   char *buffer = checkmalloc(segment_size, "sender worker buffer");

   while (!sender_has_error(state) && !msc_cancelled())
   {
      off_t offset;
      size_t length;
      struct segment_header header;
      struct iovec iov[2];

      /* Claim the next segment-aligned chunk. Dynamic balancing: whichever
       * worker is free grabs the next segment, so a slow stream cannot stall
       * the transfer the way fixed striping does. */
      offset = __atomic_fetch_add(&state->next_offset, (off_t)segment_size,
                                  __ATOMIC_RELAXED);
      if (offset >= state->transfer_length)
         break;
      length = state->transfer_length - offset;
      if (length > segment_size)
         length = segment_size;

      if (state->checkpoint != NULL &&
          msc_checkpoint_contains(state->checkpoint, (uint64_t)offset,
                                  (uint64_t)offset + length))
         continue;

      {
         uint64_t started = socket_now_ns();
         if (preadall(state->inputfd, buffer, length, state->source_base + offset) != 0)
         {
            set_sender_error(state);
            break;
         }
         __atomic_fetch_add(&state->read_ns, socket_now_ns() - started, __ATOMIC_RELAXED);
      }

      header.magic = htonl(MSC_SEGMENT_MAGIC);
      header.reserved = 0;
      header.offset = host_to_network_64(offset);
      header.length = host_to_network_64(length);
      iov[0].iov_base = &header;
      iov[0].iov_len = sizeof(header);
      iov[1].iov_base = buffer;
      iov[1].iov_len = length;
      {
         uint64_t started = socket_now_ns();
         if (send_iov_all(worker->socketfd, iov, 2) != 0)
         {
            set_sender_error(state);
            break;
         }
         __atomic_fetch_add(&state->send_ns, socket_now_ns() - started, __ATOMIC_RELAXED);
      }
      worker->bytes_sent += length;
      __atomic_fetch_add(&state->current_bytes, length, __ATOMIC_RELAXED);
   }

   if (msc_cancelled())
      set_sender_error(state);

   if (!sender_has_error(state))
   {
      struct segment_header done;
      bzero(&done, sizeof(done));
      done.magic = htonl(MSC_SEGMENT_MAGIC);
      sendall(worker->socketfd, &done, sizeof(done));
   }
   free(buffer);
   return NULL;
}

void xferdata_segmented(FILE *in, struct argdata *AD)
{
   struct stat statbuf;
   struct sender_state state;
   struct sender_worker *workers;
   pthread_t *threads;
   off_t available;
   unsigned int lc;
   unsigned int created = 0;
   struct msc_checkpoint resumed;
   int have_checkpoint = 0;
   struct msc_progress progress;

   bzero(&state, sizeof(state));
   state.AD = AD;
   state.sockets = sockfd;
   state.numstreams = AD->numstreams;
   state.inputfd = fileno(in);
   state.source_base = AD->src_offset != 0 ? AD->src_offset : AD->lastgood;
   if (fstat(state.inputfd, &statbuf) != 0)
   {
      perror("msc fstat source");
      exit(MSC_EXIT_SOURCE);
   }
   available = statbuf.st_size - state.source_base;
   if (available < 0)
      available = 0;
   state.transfer_length = AD->xferlen != 0 ? AD->xferlen : available;
   if (state.transfer_length > available)
      state.transfer_length = available;
   pthread_mutex_init(&state.error_lock, NULL);

   msc_checkpoint_init(&resumed);
   if (AD->checkpoint_path != NULL)
   {
      struct msc_resume_hello hello;
      struct msc_resume_reply reply;
      const char *source_path = AD->sourcefile;
      size_t path_length = strlen(source_path);
      uint32_t i;
      if (path_length == 0 || path_length > 1024U * 1024U)
      {
         fprintf(stderr, "MSC source path is too long for resume negotiation\n");
         exit(MSC_EXIT_CLI);
      }
      memset(&hello, 0, sizeof(hello));
      hello.magic = htonl(MSC_RESUME_MAGIC);
      hello.version = htonl(MSC_CHECKPOINT_VERSION);
      hello.flags = htonl(AD->resume ? 1U : 0U);
      hello.path_length = htonl((uint32_t)path_length);
      hello.segment_size = host_to_network_64((uint64_t)AD->packetsize);
      hello.transfer_size = host_to_network_64((uint64_t)state.transfer_length);
      hello.source_dev = host_to_network_64((uint64_t)statbuf.st_dev);
      hello.source_ino = host_to_network_64((uint64_t)statbuf.st_ino);
      hello.source_size = host_to_network_64((uint64_t)statbuf.st_size);
      hello.source_mtime_sec = host_to_network_64((uint64_t)statbuf.st_mtim.tv_sec);
      hello.source_mtime_nsec = host_to_network_64((uint64_t)statbuf.st_mtim.tv_nsec);
      hello.manifest_hash = 0;
      hello.source_offset = host_to_network_64((uint64_t)state.source_base);
      hello.destination_offset = host_to_network_64((uint64_t)AD->dst_offset);
      if (sendall(sockfd[0], &hello, sizeof(hello)) != 0 ||
          sendall(sockfd[0], source_path, path_length) != 0 ||
          recvall_exact(sockfd[0], &reply, sizeof(reply)) != 0)
      {
         fprintf(stderr, "MSC resume negotiation failed before data transfer\n");
         exit(MSC_EXIT_NETWORK);
      }
      if (ntohl(reply.magic) != MSC_RESUME_REPLY_MAGIC ||
          ntohl(reply.version) != MSC_CHECKPOINT_VERSION || ntohl(reply.status) != 0)
      {
         fprintf(stderr, "MSC peer rejected checkpoint before destination modification\n");
         exit(MSC_EXIT_INCOMPATIBLE);
      }
      resumed.range_count = ntohl(reply.range_count);
      if (resumed.range_count > MSC_CHECKPOINT_MAX_RANGES)
      {
         fprintf(stderr, "MSC peer advertised too many checkpoint ranges\n");
         exit(MSC_EXIT_INCOMPATIBLE);
      }
      resumed.range_capacity = resumed.range_count;
      resumed.ranges = resumed.range_count
         ? checkmalloc((size_t)resumed.range_count * sizeof(*resumed.ranges), "resume ranges") : NULL;
      for (i = 0; i < resumed.range_count; i++)
      {
         struct msc_wire_range wire;
         if (recvall_exact(sockfd[0], &wire, sizeof(wire)) != 0)
         {
            fprintf(stderr, "MSC peer sent a truncated range list\n");
            exit(MSC_EXIT_NETWORK);
         }
         resumed.ranges[i].start = network_to_host_64(wire.start);
         resumed.ranges[i].end = network_to_host_64(wire.end);
         if (resumed.ranges[i].start >= resumed.ranges[i].end ||
             resumed.ranges[i].end > (uint64_t)state.transfer_length ||
             (i && resumed.ranges[i - 1].end >= resumed.ranges[i].start))
         {
            fprintf(stderr, "MSC peer sent invalid resume ranges\n");
            exit(MSC_EXIT_INCOMPATIBLE);
         }
      }
      {
         struct msc_file_digest_context digest_context;
         int verify_status;
         digest_context.fd = state.inputfd;
         digest_context.base = (uint64_t)state.source_base;
         if (msc_resume_send_digests(sockfd[0], resumed.ranges,
                                     resumed.range_count,
                                     msc_resume_file_digest,
                                     &digest_context) != 0)
         {
            fprintf(stderr, "MSC could not verify resumable source ranges\n");
            exit(MSC_EXIT_NETWORK);
         }
         verify_status = msc_resume_receive_verify_reply(sockfd[0]);
         if (verify_status != 0)
         {
            fprintf(stderr,
                    "MSC durable-range verification failed before data transfer\n");
            exit(verify_status);
         }
      }
      state.checkpoint = &resumed;
      have_checkpoint = 1;
      AD->childinfo->reused = (long long)msc_checkpoint_completed_bytes(&resumed);
   }
   msc_progress_start(&progress, AD, &state.current_bytes,
                      (uint64_t)AD->childinfo->reused,
                      (uint64_t)state.transfer_length);

   workers = checkmalloc(sizeof(*workers) * AD->numstreams, "sender workers");
   threads = checkmalloc(sizeof(*threads) * AD->numstreams, "sender threads");
   bzero(workers, sizeof(*workers) * AD->numstreams);

   for (lc = 0; lc < AD->numstreams; lc++)
   {
      workers[lc].state = &state;
      workers[lc].socketfd = sockfd[lc];
      if (pthread_create(&threads[lc], NULL, segment_sender_thread, &workers[lc]) != 0)
      {
         fprintf(stderr, "Could not create sender worker %u\n", lc);
         set_sender_error(&state);
         break;
      }
      created++;
   }
   for (lc = 0; lc < created; lc++)
      pthread_join(threads[lc], NULL);

   AD->childinfo->xfercount = 0;
   for (lc = 0; lc < created; lc++)
      AD->childinfo->xfercount += workers[lc].bytes_sent;

   if (!state.error)
   {
      struct segment_header ack;
      if (recvall_exact(sockfd[0], &ack, sizeof(ack)) != 0 ||
          ntohl(ack.magic) != MSC_SEGMENT_MAGIC ||
          (ntohl(ack.reserved) & 0xffU) != MSC_SEGMENT_ACK ||
          network_to_host_64(ack.length) != (uint64_t)state.transfer_length)
      {
         fprintf(stderr, "MSC segmented receiver did not acknowledge transfer\n");
         state.error = 1;
      }
      else
      {
         state.destination_write_ns = network_to_host_64(ack.offset);
         state.destination_flush_ns = (uint64_t)(ntohl(ack.reserved) >> 8) * 1000000U;
      }
   }

   pthread_mutex_destroy(&state.error_lock);
   free(workers);
   free(threads);
   if (have_checkpoint)
      msc_checkpoint_destroy(&resumed);
   {
      uint64_t read_ns = __atomic_load_n(&state.read_ns, __ATOMIC_RELAXED);
      uint64_t send_ns = __atomic_load_n(&state.send_ns, __ATOMIC_RELAXED);
      uint64_t write_ns = state.destination_write_ns + state.destination_flush_ns;
      const char *bottleneck = "unable to determine (timing categories are within 25%)";
      if (read_ns > send_ns + send_ns / 4 && read_ns > write_ns + write_ns / 4)
         bottleneck = "source read/storage limited (accumulated read time dominated)";
      else if (write_ns > read_ns + read_ns / 4 && write_ns > send_ns + send_ns / 4)
         bottleneck = state.destination_flush_ns > state.destination_write_ns
            ? "destination flush limited (durability flush time dominated)"
            : "destination write limited (accumulated write time dominated)";
      else if (send_ns > read_ns + read_ns / 4 && send_ns > write_ns + write_ns / 4)
         bottleneck = "network/transport limited (accumulated socket-send time dominated)";
      msc_progress_finish(&progress, !state.error, bottleneck);
   }
   if (state.error)
   {
      fprintf(stderr, "MSC segmented sender failed\n");
      if (msc_cancelled()) return;
      exit(MSC_EXIT_NETWORK);
   }
}
