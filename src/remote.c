#define _GNU_SOURCE
#include "msc.h"
#include "udp_session.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <linux/sockios.h>
#include <poll.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <stdint.h>
#include <time.h>

extern void findzero_process(struct argdata *AD);


/* interface between msc remote and output process */
static int writepipe[2];
#define CHILD_READ_REMOTE   writepipe[0]
#define PARENT_WRITE_REMOTE writepipe[1]

/* Pipe-mode receive ring.  The socket loop fills a buffer only while it is
 * empty and hands it over by marking it full; write_thread drains full buffers
 * in order and marks them empty again.  Each side waits on ring_cond for its
 * next buffer, so neither can overtake the other however the threads are
 * scheduled, and the buffer handed over with datalast set ends the stream.
 * readsocket() joins the writer before it returns. */
#define NUMBUFFERS 3
static char *databuffer[NUMBUFFERS];
static long long int datacounter[NUMBUFFERS];
static int datafull[NUMBUFFERS];
static int datalast[NUMBUFFERS];
static pthread_mutex_t ring_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t ring_cond = PTHREAD_COND_INITIALIZER;

/* socket loop -> writer: this buffer is ready (last: and ends the stream) */
static void ring_hand_off(int buffer, int last)
{
   pthread_mutex_lock(&ring_lock);
   datalast[buffer] = last;
   datafull[buffer] = 1;
   pthread_cond_broadcast(&ring_cond);
   pthread_mutex_unlock(&ring_lock);
}

/* socket loop: wait until the writer has emptied this buffer */
static void ring_wait_empty(int buffer)
{
   pthread_mutex_lock(&ring_lock);
   while (datafull[buffer])
      pthread_cond_wait(&ring_cond, &ring_lock);
   pthread_mutex_unlock(&ring_lock);
}

struct receiver_state
{
   int outputfd;
   off_t destination_base;
   size_t segment_size;
   uint64_t bytes_received;
   pthread_mutex_t error_lock;
   int error;
   int *sockets;
   int numstreams;
   struct msc_checkpoint *checkpoint;
   const char *checkpoint_path;
   const char *checkpoint_data_path;
   uint64_t bytes_since_checkpoint;
   uint64_t segments_received;
   uint64_t interrupt_after;
   uint64_t signal_after;
   int signal_number;
   volatile uint64_t write_ns;
   uint64_t flush_ns;
   uint64_t stall_timeout_ms;  /* the sender's no-progress limit; 0 = none */
   int peer_flush_aware;       /* the sender accepts FLUSHING and FAILED records */
   /* The first destination failure, for the sender's FAILED record; under
    * error_lock while workers run. */
   uint32_t failed_phase;      /* MSC_FAILED_*; 0 = none */
   uint64_t failed_offset;     /* UINT64_MAX when no byte applies */
   int failed_errno;
   int streams_closed;         /* abandon_streams() has run */
   /* A fresh regular single-file destination is received straight into
    * shared mappings of the file, so the workers do not take turns on the
    * inode's write lock as they do with pwrite. Region i maps the file from
    * i * region_bytes for region_span bytes; the span runs one segment past
    * the region, so a segment always lies inside the region holding its first
    * byte. map_lock guards the regions and mapped_length. */
   int mapped;
   pthread_mutex_t map_lock;
   char **regions;
   size_t region_count;
   uint64_t region_bytes;
   size_t region_span;
   uint64_t mapped_length;     /* file length set so far */
};

#define MSC_DEST_REGION_MB 1024   /* mapping region; MSC_TEST_TCP_MAP_REGION_MB shrinks it */

static uint64_t receiver_now_ns(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

static uint64_t receiver_thread_cpu_ns(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
   return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

struct receiver_worker
{
   struct receiver_state *state;
   int socketfd;
};

/* separate thread to write output from buffer */
void *write_thread(void *arg)
{
   int current = 0;
   int last = 0;
   FILE *outfile = arg;

   DEBUG0("start write_thread\n");
   DEBUGSYNC;
   while (!last)
   {
      long long count, woff = 0;

      pthread_mutex_lock(&ring_lock);
      while (!datafull[current])
         pthread_cond_wait(&ring_cond, &ring_lock);
      count = datacounter[current];
      last = datalast[current];
      pthread_mutex_unlock(&ring_lock);
      DEBUG2("write_thread: buffer %d size %lld\n", current, count);

      while (count > 0)
      {
         size_t b = fwrite(databuffer[current] + woff, 1, (size_t)count, outfile);
         if (b == 0)
         {
            /* A destination that stops accepting bytes (full disk, dead
             * command pipe) used to spin here forever.  Nothing downstream
             * can recover the pipe path, so fail with the right class. */
            fprintf(stderr, "MSC pipe-mode destination write failed\n");
            _exit(MSC_EXIT_DESTINATION);
         }
         /* a short write resumes after the written bytes, never from the
          * buffer head (which would duplicate data) */
         count -= (long long)b;
         woff += (long long)b;
      }

      /* hand the emptied buffer back to the socket loop */
      pthread_mutex_lock(&ring_lock);
      datacounter[current] = 0;
      datafull[current] = 0;
      pthread_cond_broadcast(&ring_cond);
      pthread_mutex_unlock(&ring_lock);
      current++;
      if (current >= NUMBUFFERS)
         current = 0;
   }
   DEBUG0("write_thread: done\n");
   return NULL;
}

/* Read until size bytes, EOF, or a socket error.  Returns bytes read (0 at
 * EOF before any data) or -1 on error.  A negative recv() result used to be
 * added to the buffer pointer and counters, corrupting the read state. */
int recvall(int fd, char *buffer, int size)
{
   int numread = 0;
   char *where = buffer;

   while (numread < size)
   {
      int nr = recv(fd, where, size - numread, 0);
      if (nr < 0 && errno == EINTR)
         continue;
      if (nr < 0)
      {
         fprintf(stderr, "MSC pipe-mode socket receive failed: %s\n",
                 strerror(errno));
         return -1;
      }
      if (nr == 0)
         break;
      where += nr;
      numread += nr;
   }
   return numread;
}

/* Stop every worker. Shutting down only the receiving side wakes a worker
 * blocked in recv without telling the sender anything: a stream closed for
 * sending would be reset as more data arrived, and the sender would fail
 * before a FAILED record could reach it. */
static void set_receiver_error(struct receiver_state *state)
{
   int i;
   pthread_mutex_lock(&state->error_lock);
   state->error = 1;
   for (i = 0; i < state->numstreams; i++)
      shutdown(state->sockets[i], SHUT_RD);
   pthread_mutex_unlock(&state->error_lock);
}

/* Record the first destination failure; the caller holds error_lock or is
 * the only thread left. */
static void note_failure(struct receiver_state *state, uint32_t phase,
                         uint64_t offset, int error)
{
   state->error = 1;
   if (state->failed_phase != 0)
      return;
   state->failed_phase = phase;
   state->failed_offset = offset;
   state->failed_errno = error;
}

static int receiver_has_error(struct receiver_state *state)
{
   int error;

   pthread_mutex_lock(&state->error_lock);
   error = state->error;
   pthread_mutex_unlock(&state->error_lock);
   return error;
}

static int persist_receiver_checkpoint(struct receiver_state *state)
{
   uint64_t started;
   if (state->checkpoint == NULL) return 0;
   /* Ordering invariant: all destination writes become durable before their
    * ranges can appear in the atomically replaced checkpoint. */
   started = receiver_now_ns();
   if (fsync(state->outputfd) != 0 ||
       msc_checkpoint_capture_destination(state->checkpoint,
                                          state->checkpoint_data_path) != 0 ||
       msc_checkpoint_write_atomic(state->checkpoint_path, state->checkpoint) != 0)
   {
      int error = errno;
      fprintf(stderr, "MSC could not durably update checkpoint %s: %s\n",
              state->checkpoint_path, strerror(error));
      note_failure(state, MSC_FAILED_CHECKPOINT, UINT64_MAX, error);
      return -1;
   }
   state->flush_ns += receiver_now_ns() - started;
   state->bytes_since_checkpoint = 0;
   return 0;
}

static void report_destination_failure(struct receiver_state *state,
                                       uint64_t offset, int error)
{
   fprintf(stderr, "MSC could not write the destination at byte %llu: %s\n",
           (unsigned long long)offset,
           error == EFAULT ? "the kernel could not allocate its pages"
                           : strerror(error));
   pthread_mutex_lock(&state->error_lock);
   note_failure(state, MSC_FAILED_WRITE, offset, error);
   pthread_mutex_unlock(&state->error_lock);
   set_receiver_error(state);
}

/* Use mappings only for a destination this transfer created empty and writes
 * from its first byte, on a filesystem where they help. MSC_TCP_NO_MMAP=1
 * keeps pwrite everywhere. */
static void destination_map_setup(struct receiver_state *state, int fresh)
{
   struct stat st;
   const char *off = getenv("MSC_TCP_NO_MMAP");
   const char *test = getenv("MSC_TEST_TCP_MAP_REGION_MB");
   long page = sysconf(_SC_PAGESIZE);
   void *first;

   if (!fresh || state->destination_base != 0 || (off != NULL && atoi(off) != 0) ||
       fstat(state->outputfd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size != 0 ||
       !msc_fd_maps_well(state->outputfd))
      return;
   if (page <= 0)
      page = 4096;
   state->region_bytes = (uint64_t)MSC_DEST_REGION_MB << 20;
   if (test != NULL && atoll(test) > 0)
      state->region_bytes = (uint64_t)atoll(test) << 20;
   state->region_span = (size_t)state->region_bytes +
      (state->segment_size + (size_t)page - 1) / (size_t)page * (size_t)page;
   /* Mapping beyond the end of the file is allowed; each segment extends the
    * file before anything touches its pages. A filesystem that cannot map
    * files at all keeps pwrite for the whole transfer. */
   first = mmap(NULL, state->region_span, PROT_READ | PROT_WRITE, MAP_SHARED,
                state->outputfd, 0);
   if (first == MAP_FAILED)
      return;
   state->regions = checkmalloc(sizeof(*state->regions), "destination regions");
   state->regions[0] = first;
   state->region_count = 1;
   state->mapped_length = 0;
   pthread_mutex_init(&state->map_lock, NULL);
   state->mapped = 1;
}

static void destination_map_teardown(struct receiver_state *state)
{
   size_t i;
   if (!state->mapped)
      return;
   for (i = 0; i < state->region_count; i++)
      if (state->regions[i] != NULL)
         munmap(state->regions[i], state->region_span);
   free(state->regions);
   state->regions = NULL;
   pthread_mutex_destroy(&state->map_lock);
   state->mapped = 0;
}

/* Where the segment at offset lands in the mapped destination, after
 * extending the file over it. The file is extended exactly to the segments
 * being received, never ahead of them. NULL with errno 0 means that no mapping
 * is available for this segment, and the caller writes it with pwrite; NULL
 * with errno set is a destination failure. */
static char *destination_map_segment(struct receiver_state *state,
                                     uint64_t offset, uint64_t length)
{
   uint64_t end = offset + length;
   uint64_t index = offset / state->region_bytes;
   char *where = NULL;
   int failure = 0;

   pthread_mutex_lock(&state->map_lock);
   if (end > state->mapped_length)
   {
      if (ftruncate(state->outputfd, (off_t)end) != 0)
         failure = errno;
      else
         state->mapped_length = end;
   }
   if (failure == 0 && index >= state->region_count &&
       index < SIZE_MAX / sizeof(*state->regions))
   {
      size_t count = (size_t)index + 1;
      char **grown = realloc(state->regions, count * sizeof(*grown));
      if (grown != NULL)
      {
         memset(grown + state->region_count, 0,
                (count - state->region_count) * sizeof(*grown));
         state->regions = grown;
         state->region_count = count;
      }
   }
   if (failure == 0 && index < state->region_count)
   {
      if (state->regions[index] == NULL)
      {
         void *m = mmap(NULL, state->region_span, PROT_READ | PROT_WRITE,
                        MAP_SHARED, state->outputfd,
                        (off_t)(index * state->region_bytes));
         if (m != MAP_FAILED)
            state->regions[index] = m;
      }
      if (state->regions[index] != NULL)
         where = state->regions[index] + (offset - index * state->region_bytes);
   }
   pthread_mutex_unlock(&state->map_lock);
   errno = failure;
   return where;
}

/* recv reports a destination page it could not allocate only as EFAULT.
 * Reserving the same bytes recovers the filesystem's reason, such as ENOSPC
 * or EDQUOT. */
static int destination_fault_cause(int fd, uint64_t offset, uint64_t length)
{
   if (fallocate(fd, 0, (off_t)offset, (off_t)length) != 0 &&
       errno != EOPNOTSUPP && errno != ENOSYS)
      return errno;
   return EFAULT;
}

/* Receive a segment straight into the mapped destination. Where a store into
 * the mapping would raise SIGBUS on a full filesystem, the kernel's copy fails
 * the receive with EFAULT instead, so no space needs reserving up front. The
 * thread CPU time spent here, copying into and allocating destination pages
 * but not waiting for the network, stands in for the pwrite time that the
 * other path reports as destination write time. */
static int receive_into_destination(struct receiver_state *state, int socketfd,
                                    char *where, uint64_t offset, uint64_t length)
{
   uint64_t total = 0;
   uint64_t started = receiver_thread_cpu_ns();
   int rc = 0;

   while (total < length)
   {
      ssize_t got = recv(socketfd, where + total, (size_t)(length - total), 0);
      if (got < 0 && errno == EINTR)
         continue;
      if (got < 0 && errno == EFAULT)
      {
         report_destination_failure(state, offset + total,
                                    destination_fault_cause(state->outputfd,
                                                            offset + total,
                                                            length - total));
         rc = -1;
         break;
      }
      if (got <= 0)
      {
         if (got < 0)
            fprintf(stderr, "MSC socket receive failed: %s\n", strerror(errno));
         else
            fprintf(stderr, "MSC socket closed during record\n");
         set_receiver_error(state);
         rc = -1;
         break;
      }
      total += (uint64_t)got;
   }
   __atomic_fetch_add(&state->write_ns, receiver_thread_cpu_ns() - started,
                      __ATOMIC_RELAXED);
   return rc;
}

/* One FLUSHING record on stream 0: the receiver is alive and still finishing
 * the transfer. The beat thread sends it until the ACK is ready. */
static int send_flushing(void *arg)
{
   struct segment_header beat;
   memset(&beat, 0, sizeof(beat));
   beat.magic = htonl(MSC_SEGMENT_MAGIC);
   beat.reserved = htonl(MSC_SEGMENT_FLUSHING);
   return sendall(*(int *)arg, &beat, sizeof(beat));
}

/* Tell a sender that accepts it why the destination failed, in place of the
 * ACK. Closing a stream with data still unread resets it and drops whatever
 * this end has not yet sent, so wait (up to 1 s) until the sender's end has
 * acknowledged the record: once there, it stays readable ahead of the reset.
 * The receiver's own message may never reach the user (its launcher can be
 * gone with the sender), so the record carries the cause itself. */
static void send_failed(struct receiver_state *state)
{
   struct segment_header record;
   uint64_t deadline;
   int unsent;

   if (!state->peer_flush_aware || state->failed_phase == 0 || state->streams_closed)
      return;
   memset(&record, 0, sizeof(record));
   record.magic = htonl(MSC_SEGMENT_MAGIC);
   record.reserved = htonl(MSC_SEGMENT_FAILED | state->failed_phase << 16);
   record.offset = host_to_network_64(state->failed_offset);
   record.length = host_to_network_64((uint64_t)state->failed_errno);
   if (sendall(state->sockets[0], &record, sizeof(record)) != 0)
      return;
   deadline = receiver_now_ns() + UINT64_C(1000000000);
   while (ioctl(state->sockets[0], SIOCOUTQ, &unsent) == 0 && unsent > 0 &&
          receiver_now_ns() < deadline)
   {
      struct timespec pause = { 0, 5 * 1000 * 1000 };
      nanosleep(&pause, NULL);
   }
}

/* After a failure during the data, tell the sender at once (FAILED, if the
 * destination failed) and close the streams, before any slow work such as a
 * checkpoint update: a sender blocked on a full window would otherwise reach
 * its stall timeout first. No FLUSHING record may go out ahead of FAILED, so
 * the finish beat never starts after such a failure. */
static void abandon_streams(struct receiver_state *state)
{
   int i;
   send_failed(state);
   for (i = 0; i < state->numstreams; i++)
      close(state->sockets[i]);
   state->streams_closed = 1;
}

/* From the end of the data until the ACK -- unmapping, flushing and, for a
 * checkpointed transfer, publishing -- tell a sender that accepts it that this
 * receiver is alive. The sender bounds the whole finish itself
 * (--flush-timeout), so a flush to slow storage, which can show no progress at
 * all while it runs, does not fail a transfer whose data has arrived. */
static int finish_beat_start(struct receiver_state *state, struct msc_beat *beat)
{
   if (!state->peer_flush_aware)   /* the workers have been joined */
      return 0;
   msc_beat_start(beat, msc_beat_interval_ns(state->stall_timeout_ms), send_flushing,
                  &state->sockets[0]);
   return 1;
}

static void *segment_receiver_thread(void *arg)
{
   struct receiver_worker *worker = arg;
   struct receiver_state *state = worker->state;
   char *buffer = checkmalloc(state->segment_size, "receiver worker buffer");
   int flush_aware = 0;   /* this stream's sender accepts FLUSHING records */

   while (!receiver_has_error(state) && !msc_cancelled())
   {
      struct segment_header header;
      uint64_t offset;
      uint64_t length;

      if (recvall_exact(worker->socketfd, &header, sizeof(header)) != 0)
      {
         set_receiver_error(state);
         break;
      }
      if (ntohl(header.magic) != MSC_SEGMENT_MAGIC)
      {
         fprintf(stderr, "MSC received invalid segment header\n");
         set_receiver_error(state);
         break;
      }
      if (ntohl(header.reserved) & MSC_SEGMENT_FLUSH_AWARE)
         flush_aware = 1;
      offset = network_to_host_64(header.offset);
      length = network_to_host_64(header.length);
      if (length == 0)
         break;
      if (length > state->segment_size)
      {
         fprintf(stderr, "MSC received oversized segment\n");
         set_receiver_error(state);
         break;
      }
      if (offset > (uint64_t)INT64_MAX - (uint64_t)state->destination_base - length)
      {
         fprintf(stderr, "MSC received a segment beyond the largest file offset\n");
         set_receiver_error(state);
         break;
      }
      {
         char *where = NULL;
         if (state->mapped)
         {
            where = destination_map_segment(state, offset, length);
            if (where == NULL && errno != 0)
            {
               report_destination_failure(state, offset, errno);
               break;
            }
         }
         if (where != NULL)
         {
            if (receive_into_destination(state, worker->socketfd, where,
                                         offset, length) != 0)
               break;
         }
         else
         {
            uint64_t started;
            if (recvall_exact(worker->socketfd, buffer, length) != 0)
            {
               set_receiver_error(state);
               break;
            }
            started = receiver_now_ns();
            if (pwriteall(state->outputfd, buffer, length,
                          state->destination_base + offset) != 0)
            {
               report_destination_failure(state, state->destination_base + offset,
                                          errno);
               break;
            }
            __atomic_fetch_add(&state->write_ns, receiver_now_ns() - started,
                               __ATOMIC_RELAXED);
         }
      }
      pthread_mutex_lock(&state->error_lock);
      state->bytes_received += length;
      if (state->checkpoint != NULL)
      {
         state->segments_received++;
         state->bytes_since_checkpoint += length;
         if (msc_checkpoint_add_range(state->checkpoint, offset, offset + length) != 0 ||
             ((state->bytes_since_checkpoint >= 64U * 1024U * 1024U ||
               (state->interrupt_after != 0 &&
                state->segments_received >= state->interrupt_after) ||
               (state->signal_after != 0 &&
                state->segments_received >= state->signal_after)) &&
              persist_receiver_checkpoint(state) != 0))
            state->error = 1;
         if (state->interrupt_after != 0 &&
             state->segments_received >= state->interrupt_after)
            state->error = 1;
         if (state->signal_after != 0 && state->segments_received >= state->signal_after)
         {
            raise(state->signal_number);
            state->error = 1;
         }
      }
      pthread_mutex_unlock(&state->error_lock);
      if (state->error)
      {
         set_receiver_error(state);
         break;
      }
   }
   if (flush_aware)
   {
      pthread_mutex_lock(&state->error_lock);
      state->peer_flush_aware = 1;
      pthread_mutex_unlock(&state->error_lock);
   }
   free(buffer);
   return NULL;
}

/* fresh: this transfer created or truncated the destination and owns all of
 * it, so the file may be mapped and extended segment by segment. */
void readsocket_segmented(int numstreams, int newsockfds[], FILE *outfile,
                          int segment_size, off_t destination_base, int fresh,
                          int stats, uint64_t stall_timeout_ms)
{
   struct receiver_state state;
   struct receiver_worker *workers;
   pthread_t *threads;
   struct msc_beat beat;
   int lc;
   int created = 0;
   int beating = 0;   /* the finish beat runs from data end to the ACK */

   bzero(&state, sizeof(state));
   state.outputfd = fileno(outfile);
   state.destination_base = destination_base;
   state.segment_size = segment_size;
   state.sockets = newsockfds;
   state.numstreams = numstreams;
   state.stall_timeout_ms = stall_timeout_ms;
   pthread_mutex_init(&state.error_lock, NULL);
   destination_map_setup(&state, fresh);
   if (stats)
      fprintf(stderr, "MSC TCP receiver write-path: %s\n",
              state.mapped ? "mmap" : "pwrite");
   workers = checkmalloc(sizeof(*workers) * numstreams, "receiver workers");
   threads = checkmalloc(sizeof(*threads) * numstreams, "receiver threads");

   for (lc = 0; lc < numstreams; lc++)
   {
      workers[lc].state = &state;
      workers[lc].socketfd = newsockfds[lc];
      if (pthread_create(&threads[lc], NULL, segment_receiver_thread, &workers[lc]) != 0)
      {
         fprintf(stderr, "Could not create receiver worker %d\n", lc);
         set_receiver_error(&state);
         break;
      }
      created++;
   }
   for (lc = 0; lc < created; lc++)
      pthread_join(threads[lc], NULL);
   if (state.error)
      abandon_streams(&state);
   else
      beating = finish_beat_start(&state, &beat);
   destination_map_teardown(&state);
   /* A success ACK means every worker finished and the bytes reached the
    * backing file, not merely the stdio/kernel page cache. */
   if (!state.error)
   {
      uint64_t started = receiver_now_ns();
      if (fsync(state.outputfd) != 0)
      {
         int error = errno;
         fprintf(stderr, "MSC segmented receiver fsync: %s\n", strerror(error));
         note_failure(&state, MSC_FAILED_FLUSH, UINT64_MAX, error);
      }
      state.flush_ns = receiver_now_ns() - started;
   }
   if (beating)
      msc_beat_stop(&beat);
   if (!state.error)
   {
      struct segment_header ack;
      bzero(&ack, sizeof(ack));
      ack.magic = htonl(MSC_SEGMENT_MAGIC);
      ack.reserved = htonl(MSC_SEGMENT_ACK |
         (uint32_t)(((state.flush_ns / 1000000U) > 0xffffffU
                      ? 0xffffffU : state.flush_ns / 1000000U) << 8));
      ack.offset = host_to_network_64(__atomic_load_n(&state.write_ns, __ATOMIC_RELAXED));
      ack.length = host_to_network_64(state.bytes_received);
      if (sendall(newsockfds[0], &ack, sizeof(ack)) != 0)
         state.error = 1;
   }
   else
      send_failed(&state);
   if (!state.streams_closed)
      for (lc = 0; lc < numstreams; lc++)
         close(newsockfds[lc]);
   pthread_mutex_destroy(&state.error_lock);
   free(workers);
   free(threads);
   if (state.error)
   {
      fprintf(stderr, "MSC segmented receiver failed\n");
      exit(state.failed_phase != 0 ? MSC_EXIT_DESTINATION : MSC_EXIT_NETWORK);
   }
}

/* A publication step failed: say which, and record it for the sender. */
static void publish_failed(struct receiver_state *state, const char *format,
                           const char *path)
{
   int error = errno;
   fprintf(stderr, format, path, strerror(error));
   note_failure(state, MSC_FAILED_PUBLISH, UINT64_MAX, error);
   pthread_mutex_destroy(&state->error_lock);
}

static char *resume_temp_path(const char *destination)
{
   char *path;
   if (asprintf(&path, "%s.msc-part", destination) < 0) return NULL;
   return path;
}

static int resume_source_matches(const struct msc_checkpoint *cp,
                                 const struct msc_resume_hello *hello,
                                 const char *source_path,
                                 const char *destination_path,
                                 char *error, size_t error_size)
{
   if (cp->recursive || cp->segment_size != network_to_host_64(hello->segment_size) ||
       cp->source_dev != network_to_host_64(hello->source_dev) ||
       cp->source_ino != network_to_host_64(hello->source_ino) ||
       cp->source_size != network_to_host_64(hello->source_size) ||
       cp->source_mtime_sec != (int64_t)network_to_host_64(hello->source_mtime_sec) ||
       cp->source_mtime_nsec != (int64_t)network_to_host_64(hello->source_mtime_nsec) ||
       cp->source_offset != network_to_host_64(hello->source_offset) ||
       cp->destination_offset != network_to_host_64(hello->destination_offset) ||
       cp->transfer_size != network_to_host_64(hello->transfer_size) ||
       strcmp(cp->source_path, source_path) != 0 ||
       strcmp(cp->destination_path, destination_path) != 0)
   {
      snprintf(error, error_size,
               "checkpoint identity does not match source or destination; remove the stale checkpoint or restart without --resume");
      return -1;
   }
   return 0;
}

void readsocket_segmented_resume(int numstreams, int newsockfds[], struct argdata *AD)
{
   struct msc_resume_hello hello;
   struct msc_resume_reply reply;
   struct msc_checkpoint checkpoint;
   struct receiver_state state;
   struct receiver_worker *workers = NULL;
   pthread_t *threads = NULL;
   char *source_path = NULL, *temp_path = NULL;
   char error[512] = "checkpoint rejected";
   uint32_t path_length, i;
   uint64_t transfer_size;
   uint64_t source_offset, destination_offset;
   int fd = -1, created = 0, status = 1;
   int exit_code = MSC_EXIT_INCOMPATIBLE;
   struct msc_beat beat;
   int beating = 0;   /* the finish beat runs from data end to the ACK */
   const char *fault;

   msc_checkpoint_init(&checkpoint);
   memset(&state, 0, sizeof(state));
   memset(&reply, 0, sizeof(reply));
   reply.magic = htonl(MSC_RESUME_REPLY_MAGIC);
   reply.version = htonl(MSC_CHECKPOINT_VERSION);
   if (recvall_exact(newsockfds[0], &hello, sizeof(hello)) != 0 ||
       ntohl(hello.magic) != MSC_RESUME_MAGIC ||
       ntohl(hello.version) != MSC_CHECKPOINT_VERSION)
   {
      snprintf(error, sizeof(error), "peer/checkpoint protocol version is incompatible");
      goto reject;
   }
   path_length = ntohl(hello.path_length);
   transfer_size = network_to_host_64(hello.transfer_size);
   source_offset = network_to_host_64(hello.source_offset);
   destination_offset = network_to_host_64(hello.destination_offset);
   if (path_length == 0 || path_length > 1024U * 1024U ||
       (ntohl(hello.flags) & ~1U) != 0 ||
       network_to_host_64(hello.segment_size) != AD->packetsize ||
       transfer_size > INT64_MAX || source_offset > INT64_MAX ||
       destination_offset > INT64_MAX ||
       source_offset > network_to_host_64(hello.source_size) ||
       transfer_size > network_to_host_64(hello.source_size) - source_offset ||
       transfer_size > (uint64_t)INT64_MAX - destination_offset ||
       destination_offset != (uint64_t)AD->dst_offset)
   {
      snprintf(error, sizeof(error), "peer sent invalid resume bounds");
      goto reject;
   }
   source_path = checkmalloc((size_t)path_length + 1, "resume source path");
   if (recvall_exact(newsockfds[0], source_path, path_length) != 0) goto reject;
   source_path[path_length] = '\0';
   temp_path = resume_temp_path(AD->destfile);
   if (temp_path == NULL) goto reject;

   if (ntohl(hello.flags) & 1U)
   {
      int rc = msc_checkpoint_read(AD->checkpoint_path, &checkpoint,
                                   error, sizeof(error));
      if (rc != 0 || resume_source_matches(&checkpoint, &hello, source_path,
                                           AD->destfile, error, sizeof(error)) != 0 ||
          msc_checkpoint_validate_destination(&checkpoint, temp_path,
                                              error, sizeof(error)) != 0)
         goto reject;
      /* Verified, never relaid out: the partial holds the bytes resume exists to
       * keep, and a Lustre layout can only be chosen at creation. A mismatch
       * means this checkpoint began under a different --dest-stripe-count. */
      if (AD->dest_stripe_count != 0)
      {
         uint32_t actual = 0;
         if (msc_lustre_check_stripe_count(temp_path, AD->dest_stripe_count,
                                           &actual) != 0)
         {
            snprintf(error, sizeof(error),
                     "resumable destination %s has stripe count %u, not the "
                     "requested %ld", temp_path, actual, AD->dest_stripe_count);
            goto reject;
         }
      }
      fd = open(temp_path, O_RDWR | O_NOFOLLOW);
      if (fd < 0) { snprintf(error, sizeof(error), "cannot open resumable destination %s: %s", temp_path, strerror(errno)); goto reject; }
   }
   else
   {
      if (access(AD->checkpoint_path, F_OK) == 0 || access(temp_path, F_OK) == 0)
      {
         if (!AD->force)
         {
            snprintf(error, sizeof(error), "stale checkpoint or partial destination exists; use --resume or --force");
            goto reject;
         }
         unlink(AD->checkpoint_path); unlink(temp_path);
      }
      if (!AD->force && access(AD->destfile, F_OK) == 0)
      { snprintf(error, sizeof(error), "destination exists: %s (use --force)", AD->destfile); goto reject; }
      /* The partial IS the destination inode (published by rename), so this is
       * the one moment its layout can be chosen. Same flags and 0600 as the
       * plain path: the request costs the temp none of its exclusivity. */
      if (AD->dest_stripe_count != 0)
      {
         fd = msc_lustre_create_striped(temp_path, O_CREAT | O_EXCL | O_RDWR | O_NOFOLLOW,
                                        0600, 0, AD->dest_stripe_count);
         if (fd < 0)
         {
            snprintf(error, sizeof(error),
                     "cannot prepare partial destination %s with stripe count "
                     "%ld: %s", temp_path, AD->dest_stripe_count, strerror(errno));
            goto reject;
         }
      }
      else
         fd = open(temp_path, O_CREAT | O_EXCL | O_RDWR | O_NOFOLLOW, 0600);
      if (fd < 0 ||
          ftruncate(fd, (off_t)(destination_offset + transfer_size)) != 0)
      { snprintf(error, sizeof(error), "cannot prepare partial destination %s: %s", temp_path, strerror(errno)); goto reject; }
      checkpoint.segment_size = AD->packetsize;
      checkpoint.source_dev = network_to_host_64(hello.source_dev);
      checkpoint.source_ino = network_to_host_64(hello.source_ino);
      checkpoint.source_size = network_to_host_64(hello.source_size);
      checkpoint.source_mtime_sec = (int64_t)network_to_host_64(hello.source_mtime_sec);
      checkpoint.source_mtime_nsec = (int64_t)network_to_host_64(hello.source_mtime_nsec);
      checkpoint.source_offset = source_offset;
      checkpoint.destination_offset = destination_offset;
      checkpoint.transfer_size = transfer_size;
      checkpoint.source_path = strdup(source_path);
      checkpoint.destination_path = strdup(AD->destfile);
      if (checkpoint.source_path == NULL || checkpoint.destination_path == NULL ||
          fsync(fd) != 0 || msc_fsync_parent(temp_path) != 0 ||
          msc_checkpoint_capture_destination(&checkpoint, temp_path) != 0 ||
          msc_checkpoint_write_atomic(AD->checkpoint_path, &checkpoint) != 0)
      { snprintf(error, sizeof(error), "cannot create durable checkpoint %s: %s", AD->checkpoint_path, strerror(errno)); goto reject; }
   }

   /* The identity check is complete.  Any failure from here through digest
    * exchange is a data-plane failure unless content comparison identifies a
    * stronger integrity class. */
   exit_code = MSC_EXIT_NETWORK;
   reply.status = 0;
   reply.range_count = htonl(checkpoint.range_count);
   if (sendall(newsockfds[0], &reply, sizeof(reply)) != 0) goto cleanup;
   for (i = 0; i < checkpoint.range_count; i++)
   {
      struct msc_wire_range wire;
      wire.start = host_to_network_64(checkpoint.ranges[i].start);
      wire.end = host_to_network_64(checkpoint.ranges[i].end);
      if (sendall(newsockfds[0], &wire, sizeof(wire)) != 0) goto cleanup;
   }
   {
      struct msc_file_digest_context digest_context;
      int verify;
      int verify_status;
      digest_context.fd = fd;
      digest_context.base = destination_offset;
      verify = msc_resume_verify_digests(newsockfds[0], checkpoint.ranges,
                                         checkpoint.range_count,
                                         msc_resume_file_digest,
                                         &digest_context);
      verify_status = verify == 0 ? 0 :
                      verify > 0 ? MSC_EXIT_INTEGRITY : MSC_EXIT_DESTINATION;
      if (msc_resume_send_verify_reply(newsockfds[0], verify_status) != 0)
         goto cleanup;
      if (verify_status != 0)
      {
         fprintf(stderr,
                 "MSC checkpoint durable bytes do not match the current source\n");
         exit_code = verify_status;
         goto cleanup;
      }
   }

   state.outputfd = fd; state.segment_size = AD->packetsize;
   state.destination_base = (off_t)destination_offset;
   state.sockets = newsockfds; state.numstreams = numstreams;
   state.checkpoint = &checkpoint; state.checkpoint_path = AD->checkpoint_path;
   state.checkpoint_data_path = temp_path;
   state.stall_timeout_ms = AD->stall_timeout_ms;
   fault = getenv("MSC_TEST_INTERRUPT_AFTER_SEGMENTS");
   if (fault != NULL) state.interrupt_after = strtoull(fault, NULL, 10);
   fault = getenv("MSC_TEST_SIGNAL_AFTER_SEGMENTS");
   if (fault != NULL)
   {
      state.signal_after = strtoull(fault, NULL, 10);
      state.signal_number = getenv("MSC_TEST_SIGNAL_TERM") != NULL ? SIGTERM : SIGINT;
   }
   pthread_mutex_init(&state.error_lock, NULL);
   workers = checkmalloc(sizeof(*workers) * (size_t)numstreams, "resume receiver workers");
   threads = checkmalloc(sizeof(*threads) * (size_t)numstreams, "resume receiver threads");
   for (i = 0; i < (uint32_t)numstreams; i++)
   {
      workers[i].state = &state; workers[i].socketfd = newsockfds[i];
      if (pthread_create(&threads[i], NULL, segment_receiver_thread, &workers[i]) != 0)
      { set_receiver_error(&state); break; }
      created++;
   }
   for (i = 0; i < (uint32_t)created; i++) pthread_join(threads[i], NULL);
   /* The checkpoint update below syncs the data, then publication follows:
    * all of it before the ACK, with the sender told the receiver is alive.
    * After a failure, the sender is told first and the update keeps what
    * arrived. */
   if (state.error)
      abandon_streams(&state);
   else
      beating = finish_beat_start(&state, &beat);
   pthread_mutex_lock(&state.error_lock);
   if (persist_receiver_checkpoint(&state) != 0) state.error = 1;
   pthread_mutex_unlock(&state.error_lock);
   if (state.error ||
       (transfer_size != 0 && !msc_checkpoint_contains(&checkpoint, 0, transfer_size)))
   {
      fprintf(stderr, "MSC transfer interrupted; durable partial data and checkpoint retained at %s\n",
              AD->checkpoint_path);
      pthread_mutex_destroy(&state.error_lock);
      goto cleanup;
   }
   /* A sender that has given up waiting (the destination outlasted its
    * --flush-timeout) has reported a failure. Publishing now would leave a
    * complete file and no checkpoint behind that report, and break the
    * --resume it suggests, so keep both as they are instead. */
   if (msc_sender_gone(newsockfds[0]))
   {
      fprintf(stderr, "MSC sender stopped waiting; keeping %s and its checkpoint "
                      "for --resume\n", temp_path);
      pthread_mutex_destroy(&state.error_lock);
      goto cleanup;
   }
   if ((AD->force ? rename(temp_path, AD->destfile) :
        (link(temp_path, AD->destfile) == 0 && unlink(temp_path) == 0 ? 0 : -1)) != 0)
   { publish_failed(&state, "MSC could not atomically publish %s: %s\n", AD->destfile); goto cleanup; }
   if (msc_fsync_parent(AD->destfile) != 0)
   { publish_failed(&state, "MSC could not make publication of %s durable: %s\n", AD->destfile); goto cleanup; }
   if (!AD->keep_checkpoint &&
       (unlink(AD->checkpoint_path) != 0 ||
        msc_fsync_parent(AD->checkpoint_path) != 0))
   { publish_failed(&state, "MSC published data but could not remove checkpoint %s: %s\n", AD->checkpoint_path); goto cleanup; }
   if (beating)
   {
      msc_beat_stop(&beat);
      beating = 0;
   }
   {
      struct segment_header ack;
      memset(&ack, 0, sizeof(ack)); ack.magic = htonl(MSC_SEGMENT_MAGIC);
      ack.reserved = htonl(MSC_SEGMENT_ACK |
         (uint32_t)(((state.flush_ns / 1000000U) > 0xffffffU
                      ? 0xffffffU : state.flush_ns / 1000000U) << 8));
      ack.offset = host_to_network_64(__atomic_load_n(&state.write_ns, __ATOMIC_RELAXED));
      ack.length = host_to_network_64(transfer_size);
      if (sendall(newsockfds[0], &ack, sizeof(ack)) == 0) status = 0;
   }
   pthread_mutex_destroy(&state.error_lock);
   goto cleanup;

reject:
   fprintf(stderr, "MSC checkpoint negotiation rejected: %s\n", error);
   reply.status = htonl(1);
   sendall(newsockfds[0], &reply, sizeof(reply));
cleanup:
   if (beating)
      msc_beat_stop(&beat);
   if (status != 0 && state.failed_phase != 0)
   {
      send_failed(&state);
      exit_code = MSC_EXIT_DESTINATION;
   }
   if (!state.streams_closed)
      for (i = 0; i < (uint32_t)numstreams; i++) close(newsockfds[i]);
   if (fd >= 0) close(fd);
   free(workers); free(threads); free(source_path); free(temp_path);
   msc_checkpoint_destroy(&checkpoint);
   if (msc_cancelled()) exit(msc_cancel_exit_code());
   if (status != 0) exit(exit_code);
}


/* read from sockets, send to stdin of child until EOF */
void readsocket(int numstreams, int newsockfds[], FILE * outfile, int buffsize)
{
   int n = 1;
   int lc;
   int snum = 0;
   int currentbuffer = 0;
   int receive_failed = 0;
   pthread_t threadid;
   /* Each recvall reads exactly one buffsize block from one socket, so every
    * buffer must hold at least one block; the soft-start fill size never
    * exceeds what was allocated. */
   long long alloc = buffsize > MAX_INTERPROCESS_BUFFER_SIZE
                     ? buffsize : MAX_INTERPROCESS_BUFFER_SIZE;
   long long buffersize = 4LL * buffsize;

   if (buffersize > alloc)
      buffersize = alloc;
   DEBUG1("in readsocket, buffersize %lld\n", buffersize);
   DEBUGSYNC;
   for (lc = 0; lc < NUMBUFFERS; lc++)
   {
      databuffer[lc] = checkmalloc((size_t)alloc, "databuffer");
      datacounter[lc] = 0;
      datafull[lc] = 0;
      datalast[lc] = 0;
   }
   if (pthread_create(&threadid, NULL, &write_thread, outfile) != 0)
   {
      fprintf(stderr, "MSC could not start the pipe-mode writer thread\n");
      exit(MSC_EXIT_INTERNAL);
   }

   while (n > 0)
   {
      n = recvall(newsockfds[snum],
                  databuffer[currentbuffer] + datacounter[currentbuffer],
                  buffsize);
      if (n < 0)   /* socket error: hand over what we have, then fail */
      {
         receive_failed = 1;
         n = 0;
      }
      datacounter[currentbuffer] += n;
      if (n > 0 && buffersize - datacounter[currentbuffer] < buffsize)
      {
         /* no room for another block: hand this buffer to the writer and
          * move on to the next one once the writer has emptied it */
         ring_hand_off(currentbuffer, 0);
         currentbuffer++;
         if (currentbuffer >= NUMBUFFERS)
            currentbuffer = 0;
         ring_wait_empty(currentbuffer);
         if (buffersize < alloc)   /* soft start */
         {
            buffersize *= 4;
            if (buffersize > alloc)
               buffersize = alloc;
            DEBUG1("readsocket: new buffersize %lld\n", buffersize);
         }
      }
         /* next socket to read from */
      snum++;
      if (snum >= numstreams)
         snum = 0;
   }
   /* the final, possibly empty, buffer ends the stream */
   ring_hand_off(currentbuffer, 1);

   for (snum = 0; snum < numstreams; snum++)
      close(newsockfds[snum]);

   /* wait until the writer has written everything */
   pthread_join(threadid, NULL);
   for (lc = 0; lc < NUMBUFFERS; lc++)
   {
      free(databuffer[lc]);
      databuffer[lc] = NULL;
   }
   if (receive_failed)
   {
      /* recvall reported the error; do not let a truncated stream pass as
       * a completed transfer */
      exit(MSC_EXIT_NETWORK);
   }
}

/* Open a socket, bind it to the first free port in the range, and listen.
 *
 * Listening here, before returning, is what makes the port safe to announce:
 * the caller prints it (MSC-CONNECT) straight away, and a sender that dials a
 * bound-but-not-yet-listening port is refused.  It also settles contention
 * between receivers on one host: SO_REUSEADDR lets two sockets bind the same
 * port, but only one can listen, so the other moves on to the next port
 * instead of announcing a port it cannot serve. */
void bindsocket(unsigned int portnum, unsigned int port_tries,
                int *socketfd, int *finalport)
{
   int numtries;
   struct sockaddr_in serv_addr;
   int portno;
   int reuse = 1;

   bzero((char *) &serv_addr, sizeof(struct sockaddr_in));
   serv_addr.sin_family = AF_INET;
   serv_addr.sin_addr.s_addr = INADDR_ANY;
   if (port_tries == 0) port_tries = 100;
   for (numtries = 0; numtries < (int)port_tries; numtries++)
   {
      /* a fresh socket per port: a socket that bound but failed to listen
       * cannot be bound again */
      *socketfd = socket(AF_INET, SOCK_STREAM, 0);
      if (*socketfd < 0)
      {
         perror("ERROR opening socket");
         exit(MSC_EXIT_INTERNAL);
      }
      setsockopt(*socketfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
      portno = portnum + numtries;
      serv_addr.sin_port = htons(portno);
      if (bind(*socketfd, (struct sockaddr *) &serv_addr,
               sizeof(serv_addr)) >= 0 &&
          listen(*socketfd, SOMAXCONN) == 0)
      {
         *finalport = portno;
         return;   /* bound and accepting connections */
      }
      close(*socketfd);
   }
   fprintf(stderr,"Couldn't bind to any address. Tried %d to %d\n",
           portnum, portnum + port_tries - 1);
   exit(MSC_EXIT_NETWORK);
}


void accept_alarm(int sig)
{
   (void)sig;
   fprintf(stderr,"no accepts in %d seconds\n", ACCEPT_TIMEOUT_SECONDS);
   DEBUG1("Fatal: no accepts in %d seconds\n", ACCEPT_TIMEOUT_SECONDS);
   exit(MSC_EXIT_NETWORK);
}

void acceptsocket(int numaccepts, int socketfd, int newsockfds[],
                  uint64_t stall_timeout_ms)
{
   int snum;
   socklen_t clilen;
   struct sockaddr_in cli_addr;
   struct sigaction act;
   int buffer_size = TCP_SOCKET_BUFFER;

   DEBUG1("in acceptsocket, numaccepts %d\n", numaccepts);
   DEBUGSYNC;
   bzero(&act, sizeof(struct sigaction));
   act.sa_flags = 0;
   act.sa_handler = accept_alarm;

           /* set alarm to die if we don't get accepts on socket */
   sigaction(SIGALRM, &act, NULL);
   alarm(ACCEPT_TIMEOUT_SECONDS); /* set timer.  die if it goes off */
   /* socketfd is already listening: bindsocket() listens before the port is
    * announced to the sender */
   
   for (snum = 0; snum < numaccepts; snum++)
   {
      clilen = sizeof(cli_addr);
      DEBUG1("acceptsocket: fd %d\n",socketfd);
      DEBUGSYNC;
      newsockfds[snum] = accept(socketfd, (struct sockaddr *) &cli_addr, 
                               &clilen);
      if (newsockfds[snum] < 0)
      {
         perror("ERROR on accept");
         DEBUG1("ERROR on accept, errno %d\n",errno);
         exit(MSC_EXIT_NETWORK);
      }
      if (msc_configure_tcp_socket(newsockfds[snum], stall_timeout_ms) != 0)
      {
         fprintf(stderr, "MSC could not configure accepted TCP socket timeout: %s\n",
                 strerror(errno));
         exit(MSC_EXIT_INTERNAL);
      }
      setsockopt(newsockfds[snum], SOL_SOCKET, SO_RCVBUF,
                 &buffer_size, sizeof(buffer_size));
      alarm(ACCEPT_TIMEOUT_SECONDS); /* reset timer.  die if it goes off */
   }
   alarm(0);  /* cancel alarm */
}

void findlastgood(struct argdata *AD)
{
   int fd, e;
   struct stat statbuf;

   /* Multiset resume currently restarts each section from its configured offset. */
   if (AD->obeylastgood != 0)
   {
      if (AD->parentsets > 1)
      {
              /* start at dst_offset, look for a hole before length */
         AD->lastgood = 0;
         DEBUG0("findlastgood: set lastgood 0\n");
         DEBUGSYNC;
      }
      else
      {
         fd = fileno(AD->outfileid);
         e = fstat(fd, &statbuf);
         if (e < 0)
         {
            DEBUG2("findlastgood: fstat failed return %d errno %d\n",e,errno);
            DEBUGSYNC;
            AD->lastgood = 0;
         }
         else
         {
            AD->lastgood = statbuf.st_size;
            DEBUG3("findlastgood: lastgood %lld inode %lu, target inode %ld\n", 
                   AD->lastgood, statbuf.st_ino, AD->inode);
            DEBUGSYNC;
         }
      }
   }
   else
   {
      AD->lastgood = 0;
   }
}

/* A normal single-set file transfer truncates its destination when it opens
 * it, and then owns all of it. */
static int destination_truncated(const struct argdata *AD)
{
   return AD->parentsets <= 1 && AD->dst_offset == 0 && !AD->obeylastgood;
}

/* prepare connection.  See if file exists, and size.  get port number
 * send to caller, and open sockets.
 */
void connect_and_process(struct argdata *AD)
{
   int socketfd;
   int portbound;
   unsigned int udp_portbound;
   int newsockfds[MAX_STREAMS];
   int udp_ctl_mode = AD->udp ? msc_udp_ctl_mode() : MSC_UDP_CTLMODE_TCP;

   DEBUG0("in connect_and_process\n");
   DEBUGSYNC;
   /* The reliable UDP control modes need to advertise a UDP listener.  The
    * legacy path always advertised a TCP listener, which made the CLI sender
    * get an immediate ICMP port-unreachable in `many` and `one` modes. */
   if (AD->udp && msc_udp_ctl_is_stdio(udp_ctl_mode))
   {
      /* stdio modes bind nothing at all: control rides the SSH session this
       * process was launched on, and the data plane is UDP.  No TCP port is
       * ever opened on either end, which is the whole point of the mode. */
      socketfd = -1;
      portbound = 0;
   }
   else if (udp_ctl_mode == MSC_UDP_CTLMODE_MANY ||
       udp_ctl_mode == MSC_UDP_CTLMODE_ONE)
   {
      socketfd = msc_udp_control_channel_bind_range(
         AD->portnum, AD->port_tries, &udp_portbound);
      if (socketfd < 0)
         exit(MSC_EXIT_NETWORK);
      portbound = (int)udp_portbound;
   }
   else
      bindsocket(AD->portnum, AD->port_tries, &socketfd, &portbound);

   /* find last good data */
   findlastgood(AD);
   /* print port number to stdout */
   DEBUG1("connect_and_process: port %d\n", portbound);
   DEBUG1("connect_and_process: lastgood %lld\n", AD->lastgood);
   DEBUGSYNC;

   fprintf(stdout,"MSC-CONNECT: %d,%lld\n", portbound,AD->lastgood);
   fflush(stdout);

   if (AD->udp)
   {
      /* TCP mode accepts the legacy stream.  In many/one mode the bound UDP
       * socket itself becomes the reliable control connection after its SYN
       * handshake; it is then passed through the same transport API. */
      int controlfd;
      if (msc_udp_ctl_is_stdio(udp_ctl_mode))
      {
         /* The rendezvous line above was the last thing allowed on stdout:
          * from here the stream carries framed control traffic, so redirect
          * our stdout to stderr before anything else can corrupt it.
          *
          * stdio is the default control mode, so this discipline is now
          * load-bearing for EVERY transfer, not just an opt-in benchmark arm.
          * Any future write to receiver stdout before this point -- a debug
          * line, a warning, a library banner -- corrupts control for all of
          * them.  Write to stderr. */
         controlfd = msc_udp_control_channel_stdio();
         if (controlfd < 0)
            exit(MSC_EXIT_NETWORK);
         if (dup2(STDERR_FILENO, STDOUT_FILENO) < 0)
            exit(MSC_EXIT_INTERNAL);
      }
      else if (udp_ctl_mode == MSC_UDP_CTLMODE_MANY ||
          udp_ctl_mode == MSC_UDP_CTLMODE_ONE)
      {
         controlfd = msc_udp_control_channel_accept(
            socketfd, (int)AD->stall_timeout_ms);
         if (controlfd < 0)
         {
            close(socketfd);
            exit(MSC_EXIT_NETWORK);
         }
      }
      else
         acceptsocket(1, socketfd, &controlfd, AD->stall_timeout_ms);
      udp_receive(AD, controlfd);
      if (controlfd != socketfd)
         close(controlfd);
      if (socketfd >= 0)          /* stdio modes never bound one */
         close(socketfd);
      if (AD->outfileid != NULL)
         fclose(AD->outfileid);
      return;
   }

   /* open desired number of sockets */
   acceptsocket(AD->numstreams, socketfd, newsockfds, AD->stall_timeout_ms);

   /* read data from sockets, write to input of remote command until EOF */
   if (AD->checkpoint_path != NULL && !AD->recursive && AD->destfile != NULL)
      readsocket_segmented_resume(AD->numstreams, newsockfds, AD);
   else if (AD->recursive)
      receivedirectory(AD->numstreams, newsockfds, AD);
   else if (AD->destfile != NULL)
   {
      off_t destination_base = AD->dst_offset != 0 ? AD->dst_offset : AD->lastgood;
      readsocket_segmented(AD->numstreams, newsockfds, AD->outfileid,
                           AD->packetsize, destination_base,
                           destination_truncated(AD), AD->stats,
                           AD->stall_timeout_ms);
   }
   else
      readsocket(AD->numstreams, newsockfds, AD->outfileid, AD->packetsize);

   close(socketfd);

   /* close descriptor to output */
   if (AD->outfileid != NULL)
      fclose(AD->outfileid);
}



/* child process to exec remote command */
void exec_remote(struct argdata *AD, int childnum)
{
   (void)childnum;
   dup2(CHILD_READ_REMOTE, STDIN_FILENO);
   /* Drop the original pipe ends, above all our copy of the write end: while
    * any process holds it open, the command never sees end of input. */
   if (CHILD_READ_REMOTE != STDIN_FILENO)
      close(CHILD_READ_REMOTE);
   close(PARENT_WRITE_REMOTE);
   DEBUGSYNC;
   DEBUG1("AD->remote_program: %s\n", AD->remote_program);
   DEBUGSYNC;
   execl("/bin/sh", "/bin/sh", "-c", AD->remote_program, NULL);
   fprintf(stderr, "Exec failed in remote, errno %d, command:%s\n", errno,
                  AD->remote_program);
   exit(MSC_EXIT_INTERNAL);
}

/* called from parseargs to run remote command msc -r */
void remote_process(struct argdata *AD)
{
   int childproc = 0;
   int ret;
   int fd;

   DEBUG0("in remote_process\n");
   DEBUGSYNC;
   if (AD->remote_program != NULL)
   {
      /*DEBUG1("remote_process remote_program: %s\n", AD->remote_program);*/
      DEBUG1("remote_process remote_program: %s\n", AD->remote_program);
      DEBUGSYNC;
        /* make pipe to use as stdin to command that is executed */
      if (pipe(writepipe) == -1)
      {
         fprintf(stderr, "Error creating pipe\n");
         exit(MSC_EXIT_INTERNAL);
      }
      AD->outfileid = fdopen(PARENT_WRITE_REMOTE,"w");

        /* fork and exec remote command */
      childproc = makechild_inline(exec_remote, AD, 0, "execute process");
      if (childproc < 0)
         exit(MSC_EXIT_INTERNAL);
      /* The command owns the read end now.  Without our copy, a command that
       * exits early makes our writes fail instead of blocking forever. */
      close(CHILD_READ_REMOTE);
   }
   else
   {
      int openflags = (AD->udp ? O_RDWR : O_WRONLY) | O_CREAT;

      DEBUG1("remote_process destfile: %s\n",AD->destfile);
      DEBUGSYNC;
      if (AD->recursive)
      {
         connect_and_process(AD);
         DEBUG0("done with remote recursive process\n");
         exit(0);
      }
      /* Resumable TCP performs protocol/version/checkpoint validation before
       * opening its sibling partial file; do not touch the final destination
       * here. */
      if (!AD->udp && AD->checkpoint_path != NULL && AD->parentsets <= 1)
      {
         connect_and_process(AD);
         exit(0);
      }
      /* MSC UDP owns single-file destination creation and atomic publication. */
      if (AD->udp && AD->parentsets <= 1)
      {
         connect_and_process(AD);
         exit(0);
      }
        /* we specified an output file, so open that instead */
        /* also the possibility of an output offset */

      /*
       * File-to-file transfers use pwrite from multiple receiver workers.
       * Only a normal single-set transfer should truncate here; multi-set
       * workers must preserve sections written by their peers.
       */
      if (destination_truncated(AD))
      {
         /* Read access as well lets the TCP receiver map the destination
          * (readsocket_segmented); a shared writable mapping needs both. */
         openflags = (openflags & ~O_ACCMODE) | O_RDWR | O_TRUNC;
      }
      /* --dest-stripe-count on the TCP path. A Lustre layout is fixed at
       * creation, so it can only be honored where this open would create the
       * file from scratch: a fresh single-set transfer. Multi-set workers share
       * an inode findzero already created and identity-checked (AD->inode), and
       * an offset/lastgood open is continuing an existing file -- recreating
       * either would destroy peers' bytes or break the identity check, so those
       * verify the layout they were given instead. */
      if (AD->dest_stripe_count != 0)
      {
         if ((openflags & O_TRUNC) != 0)
         {
            unlink(AD->destfile);
            fd = msc_lustre_create_striped(AD->destfile,
                                           (openflags & ~O_TRUNC) | O_EXCL | O_NOFOLLOW,
                                           0666, 0, AD->dest_stripe_count);
            if (fd < 0)
            {
               fprintf(stderr, "Couldn't create %s with stripe count %ld: %s\n",
                       AD->destfile, AD->dest_stripe_count, strerror(errno));
               exit(MSC_EXIT_DESTINATION);
            }
         }
         else
         {
            uint32_t actual = 0;
            if (msc_lustre_check_stripe_count(AD->destfile,
                                              AD->dest_stripe_count, &actual) != 0)
            {
               fprintf(stderr, "Destination %s has stripe count %u, not the "
                               "requested %ld; its layout was fixed when it was "
                               "created\n",
                       AD->destfile, actual, AD->dest_stripe_count);
               exit(MSC_EXIT_DESTINATION);
            }
            fd = open(AD->destfile, openflags, 0666);
         }
      }
      else
      {
         fd = open(AD->destfile, openflags, 0666);
         /* A destination the user may write but not read keeps pwrite. */
         if (fd < 0 && errno == EACCES && !AD->udp &&
             (openflags & O_ACCMODE) == O_RDWR)
            fd = open(AD->destfile, (openflags & ~O_ACCMODE) | O_WRONLY, 0666);
      }
      AD->outfileid = fdopen(fd, "w");
      DEBUG0("remote_process: after open\n");
      if (AD->outfileid == NULL)
      {
         DEBUG2("Couldn't open dest file  %s errno %d\n",AD->destfile, errno);
         fprintf(stderr,"Couldn't open file %s, errno %d\n",
                 AD->destfile, errno);
         exit(MSC_EXIT_DESTINATION);
      }
             /* check the inode number */


      DEBUG0("remote_process: pre-fseek\n");
      ret = fseek(AD->outfileid, AD->dst_offset, SEEK_SET);
      DEBUG3("remote_process seek %ld returned %d errno %d\n", 
              AD->dst_offset, ret, errno);
      DEBUGSYNC;
   }

   connect_and_process(AD);

   DEBUG0("done with remote_process\n");

   /* A -c command must finish before this receiver reports success: the
    * sender returns as soon as we exit, and the command's exit status is the
    * destination's verdict on the transfer. */
   if (childproc > 0)
   {
      int status;
      while (waitpid(childproc, &status, 0) < 0)
      {
         if (errno != EINTR)
         {
            perror("MSC waitpid for the -c command");
            exit(MSC_EXIT_INTERNAL);
         }
         if (msc_cancelled())
            kill(childproc, SIGTERM);
      }
      if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
      {
         if (WIFEXITED(status))
            fprintf(stderr, "MSC remote command exited %d: %s\n",
                    WEXITSTATUS(status), AD->remote_program);
         else
            fprintf(stderr, "MSC remote command killed by signal %d: %s\n",
                    WTERMSIG(status), AD->remote_program);
         exit(MSC_EXIT_DESTINATION);
      }
   }
   exit(0);
}
