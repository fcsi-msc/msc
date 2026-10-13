/*
 * netutil.c - shared socket and byte-order helpers.
 *
 * These routines used to be duplicated (as file-local statics) in
 * local_sockets.c, recursive.c, and remote.c. They are consolidated here so
 * the TCP data paths and the UDP control channel share one implementation.
 */
#include "msc.h"

#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <poll.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

int msc_configure_tcp_socket(int fd, uint64_t timeout_ms)
{
   struct timeval tv;
   int keepalive = 1;
   int timeout;

   if (timeout_ms == 0)
      return 0;
   timeout = timeout_ms > INT_MAX ? INT_MAX : (int)timeout_ms;
   tv.tv_sec = timeout / 1000;
   tv.tv_usec = (timeout % 1000) * 1000;
   if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) != 0 ||
       setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) != 0 ||
       setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive)) != 0)
      return -1;
#ifdef TCP_USER_TIMEOUT
   /* Bound the time transmitted data may remain unacknowledged. This is what
    * turns a pulled cable into ETIMEDOUT instead of Linux's multi-minute TCP
    * retransmission tail. SO_SNDTIMEO/SO_RCVTIMEO cover application I/O that
    * is blocked without any outstanding data for TCP_USER_TIMEOUT to watch. */
   if (setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &timeout, sizeof(timeout)) != 0 &&
       errno != ENOPROTOOPT)
      return -1;
#endif
   return 0;
}

/* A beat well inside the stall timeout, within [100 ms, 5 s]. */
uint64_t msc_beat_interval_ns(uint64_t stall_timeout_ms)
{
   uint64_t ns = stall_timeout_ms * UINT64_C(1000000) / 4;
   if (ns == 0 || ns > UINT64_C(5000000000)) ns = UINT64_C(5000000000);
   if (ns < UINT64_C(100000000)) ns = UINT64_C(100000000);
   return ns;
}

static void *beat_main(void *arg)
{
   struct msc_beat *b = arg;
   pthread_mutex_lock(&b->lock);
   while (!b->stop)
   {
      struct timespec at;
      int rc = 0;
      clock_gettime(CLOCK_MONOTONIC, &at);
      at.tv_sec += (time_t)(b->interval_ns / UINT64_C(1000000000));
      at.tv_nsec += (long)(b->interval_ns % UINT64_C(1000000000));
      if (at.tv_nsec >= 1000000000L)
      {
         at.tv_sec++;
         at.tv_nsec -= 1000000000L;
      }
      while (!b->stop && rc != ETIMEDOUT)
         rc = pthread_cond_timedwait(&b->wake, &b->lock, &at);
      if (b->stop || b->send(b->arg) != 0)
         break;   /* sent under the lock, between other writers' records */
   }
   pthread_mutex_unlock(&b->lock);
   return NULL;
}

void msc_beat_start(struct msc_beat *b, uint64_t interval_ns,
                    int (*send)(void *), void *arg)
{
   pthread_condattr_t attr;
   b->stop = 0;
   b->interval_ns = interval_ns;
   b->send = send;
   b->arg = arg;
   pthread_mutex_init(&b->lock, NULL);
   pthread_condattr_init(&attr);
   pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
   pthread_cond_init(&b->wake, &attr);
   pthread_condattr_destroy(&attr);
   b->running = pthread_create(&b->thread, NULL, beat_main, b) == 0;
}

void msc_beat_stop(struct msc_beat *b)
{
   pthread_mutex_lock(&b->lock);
   b->stop = 1;
   pthread_cond_signal(&b->wake);
   pthread_mutex_unlock(&b->lock);
   if (b->running)
      pthread_join(b->thread, NULL);
   b->running = 0;
   pthread_cond_destroy(&b->wake);
   pthread_mutex_destroy(&b->lock);
}

/* The slowest destination flush rate a sender waits for: 1 MiB/s, well below
 * the 8-17 MiB/s measured for a network filesystem committing a whole file in
 * one fsync. */
#define MSC_FLUSH_FLOOR_BYTES_PER_S (UINT64_C(1) << 20)

uint64_t msc_finish_ceiling_ms(uint64_t flush_timeout_ms, uint64_t bytes)
{
   uint64_t by_size = bytes / MSC_FLUSH_FLOOR_BYTES_PER_S * 1000U +
                      bytes % MSC_FLUSH_FLOOR_BYTES_PER_S * 1000U / MSC_FLUSH_FLOOR_BYTES_PER_S;
   if (flush_timeout_ms == 0)
      return 0;
   return by_size > flush_timeout_ms ? by_size : flush_timeout_ms;
}

static uint64_t netutil_now_ms(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * 1000U + (uint64_t)ts.tv_nsec / 1000000U;
}

static void describe_ms(char *text, size_t size, uint64_t ms)
{
   if (ms % 60000U == 0)
      snprintf(text, size, "%llum", (unsigned long long)(ms / 60000U));
   else
      snprintf(text, size, "%.1fs", (double)ms / 1000.0);
}

/* The one place a sender waits for a receiver to finish a transfer whose data
 * it has sent: read records of size bytes from socketfd into record until one
 * that is_beat() does not call a beat. Each record must arrive within the
 * stall timeout, which a receiver that is alive but still flushing meets with
 * beats, and the whole wait ends at msc_finish_ceiling_ms(flush_timeout_ms,
 * bytes). Returns 0 with the final record in record, MSC_EXIT_NETWORK when
 * the receiver went quiet or the stream failed, and MSC_EXIT_DESTINATION when
 * the receiver stayed alive but the destination never confirmed the data. */
int msc_await_final_record(int socketfd, void *record, size_t size,
                           int (*is_beat)(const void *record),
                           uint64_t stall_timeout_ms, uint64_t flush_timeout_ms,
                           uint64_t bytes)
{
   uint64_t ceiling = msc_finish_ceiling_ms(flush_timeout_ms, bytes);
   uint64_t start = netutil_now_ms(), heard = start;
   char text[32];

   for (;;)
   {
      uint64_t now = netutil_now_ms();
      int64_t wait = -1;
      struct pollfd p;
      int r;
      if (stall_timeout_ms != 0)
         wait = (int64_t)(heard + stall_timeout_ms > now ? heard + stall_timeout_ms - now : 0);
      if (ceiling != 0)
      {
         int64_t left = (int64_t)(start + ceiling > now ? start + ceiling - now : 0);
         if (wait < 0 || left < wait)
            wait = left;
      }
      p.fd = socketfd;
      p.events = POLLIN;
      r = poll(&p, 1, wait < 0 ? -1 : wait > INT_MAX ? INT_MAX : (int)wait);
      if (r < 0 && errno == EINTR)
      {
         if (msc_cancelled())
            return MSC_EXIT_NETWORK;
         continue;
      }
      if (r < 0)
      {
         perror("MSC waiting for the receiver to finish");
         return MSC_EXIT_NETWORK;
      }
      now = netutil_now_ms();
      if (r == 0)
      {
         if (ceiling != 0 && now >= start + ceiling)
         {
            describe_ms(text, sizeof(text), ceiling);
            fprintf(stderr, "MSC delivered all data, but the destination did not confirm "
                            "it was durable within %s (--flush-timeout, or 1 s per MiB "
                            "transferred if longer)\n", text);
            return MSC_EXIT_DESTINATION;
         }
         if (stall_timeout_ms != 0 && now >= heard + stall_timeout_ms)
         {
            describe_ms(text, sizeof(text), stall_timeout_ms);
            fprintf(stderr, "MSC receiver sent nothing for %s while finishing the "
                            "transfer (--stall-timeout)\n", text);
            return MSC_EXIT_NETWORK;
         }
         continue;
      }
      if (recvall_exact(socketfd, record, size) != 0)
         return MSC_EXIT_NETWORK;
      if (!is_beat(record))
         return 0;
      heard = netutil_now_ms();
   }
}

void *checkmalloc(size_t size, const char *msg)
{
   void *t = malloc(size);

   if (t == NULL)
   {
      fprintf(stderr, "Couldn't malloc %s\n", msg);
      exit(MSC_EXIT_INTERNAL);
   }
   return t;
}

uint64_t host_to_network_64(uint64_t value)
{
   uint32_t high = htonl((uint32_t)(value >> 32));
   uint32_t low = htonl((uint32_t)(value & 0xffffffffU));

   return ((uint64_t)low << 32) | high;
}

uint64_t network_to_host_64(uint64_t value)
{
   uint32_t high = ntohl((uint32_t)(value & 0xffffffffU));
   uint32_t low = ntohl((uint32_t)(value >> 32));

   return ((uint64_t)high << 32) | low;
}

int sendall(int fd, const void *buffer, size_t size)
{
   const char *where = buffer;
   size_t sent = 0;

   while (sent < size)
   {
      ssize_t written = write(fd, where + sent, size - sent);
      if (written < 0 && errno == EINTR)
         continue;
      if (written <= 0)
      {
         if (written < 0)
            fprintf(stderr, "MSC socket send failed: %s\n", strerror(errno));
         return -1;
      }
      sent += written;
   }
   return 0;
}

int recvall_exact(int fd, void *buffer, size_t size)
{
   char *where = buffer;
   size_t total = 0;

   while (total < size)
   {
      ssize_t numread = recv(fd, where + total, size - total, 0);
      if (numread < 0 && errno == EINTR)
         continue;
      if (numread <= 0)
      {
         if (numread < 0)
            fprintf(stderr, "MSC socket receive failed: %s\n", strerror(errno));
         else
            fprintf(stderr, "MSC socket closed during record\n");
         return -1;
      }
      total += numread;
   }
   return 0;
}

int preadall(int fd, void *buffer, size_t size, off_t offset)
{
   char *where = buffer;
   size_t total = 0;

   while (total < size)
   {
      ssize_t numread = pread(fd, where + total, size - total, offset + total);
      if (numread < 0 && errno == EINTR)
         continue;
      if (numread <= 0)
         return -1;
      total += numread;
   }
   return 0;
}

int pwriteall(int fd, const void *buffer, size_t size, off_t offset)
{
   const char *where = buffer;
   size_t total = 0;

   while (total < size)
   {
      ssize_t written = pwrite(fd, where + total, size - total, offset + total);
      if (written < 0 && errno == EINTR)
         continue;
      if (written <= 0)
         return -1;
      total += written;
   }
   return 0;
}

/* Write iovcnt buffers to consecutive bytes starting at offset, retrying short
 * writes and EINTR. The iov array is consumed in place (its entries are
 * advanced past the bytes already written), so the caller must not reuse it. */
int pwritevall(int fd, struct iovec *iov, int iovcnt, off_t offset)
{
   off_t pos = offset;

   while (iovcnt > 0)
   {
      ssize_t written = pwritev(fd, iov, iovcnt, pos);
      if (written < 0 && errno == EINTR)
         continue;
      if (written <= 0)
         return -1;
      pos += written;
      /* drop fully-written iovecs, then trim a partially-written leading one */
      while (iovcnt > 0 && (size_t)written >= iov->iov_len)
      {
         written -= (ssize_t)iov->iov_len;
         iov++;
         iovcnt--;
      }
      if (iovcnt > 0 && written > 0)
      {
         iov->iov_base = (char *)iov->iov_base + written;
         iov->iov_len -= (size_t)written;
      }
   }
   return 0;
}

/* The SSH client MSC launches the remote end with.  Overridable so a test can
 * substitute a stand-in (there is no sshd on a build host) and so sites that
 * keep ssh outside /usr/bin, or need a wrapper, do not have to patch the
 * binary.  Resolved through PATH when it contains no slash. */
const char *msc_ssh_program(void)
{
   const char *program = getenv("MSC_SSH");
   return program != NULL && program[0] != '\0' ? program : "/usr/bin/ssh";
}

/* SSH liveness settings for the receiver launch and the reconnect probe.
 *
 * The historic probe values (ConnectTimeout=5, ServerAliveInterval=3,
 * ServerAliveCountMax=1 -- see probe_remote() in local_single.c) were picked on
 * a LAN, where 5 s is hundreds of round trips.  They do not hold on a long
 * path: an SSH connect costs roughly 6-8 round trips before the remote command
 * runs, so a ~600 ms satellite hop can spend most of a 5 s budget just
 * establishing, and a 3 s / 1-count keepalive calls the peer dead after ~6 s of
 * quiet.  Both failures convert a RETRYABLE network fault (exit 6) into a hard
 * one, which is the opposite of what the reconnect loop exists to do.
 *
 * Values follow the declared path class, and each is independently
 * overridable.  MSC_UDP_PROFILE=lan and an unset/auto profile keep the historic
 * numbers exactly, so nothing changes for existing users; a user on a long path
 * declares it (today MSC_UDP_PROFILE=wan) and gets budgets that fit the path.
 *
 * NOTE: `auto` cannot scale itself here.  The engine classifies the path from
 * the handshake RTT it measures inside the transfer (path_profile_select(),
 * udp_session.c), while these settings are needed by a separate process before
 * and after that transfer.  Plumbing the measured class back out is a
 * possible follow-up. */
void msc_ssh_liveness(struct msc_ssh_liveness *out, enum msc_ssh_purpose purpose)
{
   const char *profile = getenv("MSC_UDP_PROFILE");
   const char *value;
   /* Every preset name that means "not a LAN". This has to list the field-
    * facing aliases too, not just the table names: the profile arrives here as
    * the raw MSC_UDP_PROFILE string that --env set, so a user who asked for
    * `fiber-shared` or `wan-long` and got LAN-sized SSH budgets on a 250 ms
    * path would be told nothing about it. Erring long is cheap -- these bound
    * how patient the SSH session is, and msc's own 60 s data-plane stall
    * timeout is the real backstop. */
   static const char *const long_names[] = {
      "wan", "fiber", "fiber-shared", "wan-long", "fiber-long", "longhaul",
      "geo", "satellite"
   };
   int is_long_path = 0;
   if (profile != NULL)
   {
      size_t i;
      for (i = 0; i < sizeof(long_names) / sizeof(long_names[0]); i++)
         if (strcmp(profile, long_names[i]) == 0) { is_long_path = 1; break; }
   }

   if (purpose == MSC_SSH_LAUNCH)
   {
      /* The launch session must not tear down a transfer that is working. Its
       * natural bound is msc's own data-plane stall timeout (60 s), not a
       * liveness poll's. A tight budget here is actively harmful: the control
       * channel shares the wire with the data path, so a transfer that
       * saturates the link starves its own SSH session -- observed when an
       * MSC_UDP_NO_CC=1 transfer filled a gigabit wire and a
       * 3 s / 1-count keepalive killed the session AFTER the transfer had
       * delivered every byte (exit 255 -> exit 5). */
      out->connect_timeout_s = is_long_path ? 30 : 10;
      out->alive_interval_s  = is_long_path ? 30 : 15;
      out->alive_count       = 4;
   }
   else
   {
      /* The reconnect probe has the opposite job: decide quickly whether a peer
       * that already went away has come back, in a loop that retries anyway.
       * Failing fast costs one wasted probe; failing slow stalls recovery. */
      out->connect_timeout_s = is_long_path ? 30 : 5;
      out->alive_interval_s  = is_long_path ? 20 : 3;
      out->alive_count       = is_long_path ?  3 : 1;
   }

   if ((value = getenv("MSC_SSH_CONNECT_TIMEOUT")) != NULL && atoi(value) > 0)
      out->connect_timeout_s = atoi(value);
   if ((value = getenv("MSC_SSH_ALIVE_INTERVAL")) != NULL && atoi(value) > 0)
      out->alive_interval_s = atoi(value);
   if ((value = getenv("MSC_SSH_ALIVE_COUNT")) != NULL && atoi(value) > 0)
      out->alive_count = atoi(value);
}
