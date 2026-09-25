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
static long long int inputcounter[NUMBUFFERS];
static char *inputbuffer[NUMBUFFERS];
static int inputfull[NUMBUFFERS];
static int inputlast[NUMBUFFERS];
static pthread_mutex_t input_ring_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t input_ring_cond = PTHREAD_COND_INITIALIZER;

static uint64_t socket_now_ns(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

/* Legacy pipe mode passes buffers through a ring: xferdata fills a buffer
 * only while it is empty and hands it over by marking it full; sendout_thread
 * sends full buffers in order and marks them empty again.  Each side waits on
 * input_ring_cond for its next buffer, so neither can overtake the other, and
 * the buffer handed over with inputlast set ends the stream.  xferdata joins
 * sendout_thread before it returns. */

/* Legacy stdin/command-pipe sender.  File-to-file transfers use the labeled
 * segment workers below, where short I/O and socket errors are handled. */
void *sendout_thread(void *arg)
{
   struct argdata *AD = arg;
   int current = 0;
   int last = 0;
   unsigned int snum = 0;

   DEBUG0("start sendout_thread\n");
   DEBUGSYNC;
   while (!last)
   {
      long long count, offset = 0;

      pthread_mutex_lock(&input_ring_lock);
      while (!inputfull[current])
         pthread_cond_wait(&input_ring_cond, &input_ring_lock);
      count = inputcounter[current];
      last = inputlast[current];
      pthread_mutex_unlock(&input_ring_lock);
      DEBUG1("sendout_thread: process buffer %d\n", current);

      /* Deal the buffer out one packetsize block per socket in turn.  Each
       * block goes to its socket whole: the receiver reads exactly one block
       * from each socket in the same order, so letting the rest of a short
       * write spill onto the next socket would reorder the stream. */
      while (count > 0)
      {
         size_t block = count < (long long)AD->packetsize
                      ? (size_t)count : (size_t)AD->packetsize;
         size_t done = 0;
         while (done < block)
         {
            ssize_t written = write(sockfd[snum],
                                    inputbuffer[current] + offset + done,
                                    block - done);
            if (written < 0 && errno == EINTR)
               continue;
            if (written <= 0)
            {
               /* The pipe path has no resume state, so a dead stream is a
                * fatal transport error. */
               fprintf(stderr, "MSC pipe-mode socket write failed: %s\n",
                       strerror(errno));
               exit(MSC_EXIT_NETWORK);
            }
            done += (size_t)written;
         }
         count -= (long long)block;
         offset += (long long)block;
         snum++;
         if (snum >= AD->numstreams)
            snum = 0;
      }

      /* hand the emptied buffer back to xferdata */
      pthread_mutex_lock(&input_ring_lock);
      inputcounter[current] = 0;
      inputfull[current] = 0;
      pthread_cond_broadcast(&input_ring_cond);
      pthread_mutex_unlock(&input_ring_lock);
      current++;
      if (current >= NUMBUFFERS)
         current = 0;
   }
   DEBUG0("sendout_thread: done\n");
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
   int lc;
   pthread_t threadid;
   int currentbuffer = 0;
   /* A whole number of packetsize blocks per buffer (see sendout_thread),
    * about 64 MiB and at least one block. */
   long long blocks = (64LL * 1024 * 1024) / AD->packetsize;
   long long buffersize;
   long long int xfcount = 0;

   if (blocks < 1)
      blocks = 1;
   if (blocks > 1024)
      blocks = 1024;
   buffersize = blocks * AD->packetsize;
   DEBUG0("in xferdata\n");
   DEBUGSYNC;
   for (lc = 0; lc < NUMBUFFERS; lc++)
   {
      inputbuffer[lc] = checkmalloc((size_t)buffersize, "inputbuffer");
      inputcounter[lc] = 0;
      inputfull[lc] = 0;
      inputlast[lc] = 0;
   }
   if (pthread_create(&threadid, NULL, &sendout_thread, AD) != 0)
   {
      fprintf(stderr, "MSC could not start the pipe-mode sender thread\n");
      exit(MSC_EXIT_INTERNAL);
   }

   DEBUG0("xferdata start xfer\n");
   DEBUGSYNC;
   for (;;)
   {
      long long toread;
      size_t n;

      /* wait until sendout_thread has emptied this buffer */
      pthread_mutex_lock(&input_ring_lock);
      while (inputfull[currentbuffer])
         pthread_cond_wait(&input_ring_cond, &input_ring_lock);
      pthread_mutex_unlock(&input_ring_lock);

      if (AD->xferlen != 0 && AD->xferlen - xfcount < buffersize)
         toread = AD->xferlen - xfcount;   /* limit on data transferred */
      else
         toread = buffersize;
      /* fread on a blocking stream returns short only at EOF or error */
      n = toread > 0 ? fread(inputbuffer[currentbuffer], 1, (size_t)toread, in) : 0;
      xfcount += (long long)n;
      if (n == 0 && ferror(in) != 0)
      {
         fprintf(stderr, "MSC pipe-mode input read failed\n");
         exit(MSC_EXIT_SOURCE);
      }
      DEBUG1("xferdata read from file to buffer %d\n", currentbuffer);

      /* hand it over; an empty read ends the stream */
      pthread_mutex_lock(&input_ring_lock);
      inputcounter[currentbuffer] = (long long)n;
      inputlast[currentbuffer] = n == 0;
      inputfull[currentbuffer] = 1;
      pthread_cond_broadcast(&input_ring_cond);
      pthread_mutex_unlock(&input_ring_lock);
      if (n == 0)
         break;
      currentbuffer++;
      if (currentbuffer >= NUMBUFFERS)
         currentbuffer = 0;
   }
   pthread_join(threadid, NULL);
   for (lc = 0; lc < NUMBUFFERS; lc++)
   {
      free(inputbuffer[lc]);
      inputbuffer[lc] = NULL;
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
