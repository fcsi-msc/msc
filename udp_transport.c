/*
 * udp_transport.c - MSC orchestration for the reliable-UDP session engine.
 */
#define _GNU_SOURCE
#include "msc.h"
#include "udp_session.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define MSC_UDP_CHECKPOINT_BYTES (64U * 1024U * 1024U)
#define MSC_UDP_CHECK_MAGIC 0x4d535543U

struct udp_chunk_check {
   uint32_t magic;
   uint32_t status;
   uint64_t durable_end;
   uint64_t checksum;
} __attribute__((packed));

_Static_assert(sizeof(struct udp_chunk_check) == 24, "UDP chunk check wire size changed");

struct udp_test_timer {
   pthread_t thread;
   uint64_t delay_ms;
   volatile int stop;
   int started;
};

static void *udp_test_timer_thread(void *argument)
{
   struct udp_test_timer *timer = argument;
   uint64_t elapsed;
   for (elapsed = 0; elapsed < timer->delay_ms && !timer->stop; elapsed++)
   {
      struct timespec pause = { 0, 1000000L };
      nanosleep(&pause, NULL);
   }
   if (!timer->stop)
      msc_cancel_signal = SIGUSR1; /* test-only transport abort, remapped to NETWORK */
   return NULL;
}

static void udp_test_timer_start(struct udp_test_timer *timer, uint64_t delay_ms)
{
   memset(timer, 0, sizeof(*timer));
   timer->delay_ms = delay_ms;
   timer->started = pthread_create(&timer->thread, NULL,
                                   udp_test_timer_thread, timer) == 0;
}

static int udp_test_timer_finish(struct udp_test_timer *timer)
{
   int fired;
   if (!timer->started) return 0;
   timer->stop = 1;
   pthread_join(timer->thread, NULL);
   fired = msc_cancel_signal == SIGUSR1;
   if (fired) msc_cancel_signal = 0;
   return fired;
}

struct msc_udp_file_table {
   struct msc_udp_file_spec *files;
   int *fds;
   const char **paths;
   struct stat *source_stats;
   uint64_t *logical_bases;
   uint64_t count;
   uint64_t capacity;
};

struct msc_udp_slice_table {
   struct msc_udp_file_spec *files;
   uint64_t count;
};

static int udp_same_source(const struct stat *a, const struct stat *b);

static void udp_fail(const char *what)
{
   fprintf(stderr, "MSC UDP: %s\n", what);
   if (msc_cancelled()) exit(msc_cancel_exit_code());
   exit(MSC_EXIT_NETWORK);
}

static int udp_exit_status_valid(uint32_t status)
{
   return status == MSC_EXIT_CLI || status == MSC_EXIT_SOURCE ||
          status == MSC_EXIT_DESTINATION || status == MSC_EXIT_AUTH ||
          status == MSC_EXIT_NETWORK || status == MSC_EXIT_INTEGRITY ||
          status == MSC_EXIT_INCOMPATIBLE || status == MSC_EXIT_INTERNAL ||
          status == MSC_EXIT_SIGINT || status == MSC_EXIT_SIGTERM;
}

static int udp_configure_control(int fd, uint64_t timeout_ms)
{
   struct sockaddr_storage address;
   socklen_t address_length = sizeof(address);
   int type = 0;
   socklen_t type_length = sizeof(type);
   struct timeval tv;

   if (timeout_ms == 0) return 0;
   /* The stdio control modes hand us a pipe end (the ssh session), not a
    * socket: there is no SO_*TIMEO to set.  Liveness there comes from ssh
    * itself plus the engine's own stall timeout on the data plane. */
   if (getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &type_length) != 0 &&
       errno == ENOTSOCK)
      return 0;
   type = 0;
   type_length = sizeof(type);
   if (getsockname(fd, (struct sockaddr *)&address, &address_length) == 0 &&
       getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &type_length) == 0 &&
       type == SOCK_STREAM &&
       (address.ss_family == AF_INET || address.ss_family == AF_INET6))
      return msc_configure_tcp_socket(fd, timeout_ms);
   tv.tv_sec = (time_t)(timeout_ms / 1000U);
   tv.tv_usec = (suseconds_t)(timeout_ms % 1000U) * 1000;
   return setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == 0 &&
          setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0 ? 0 : -1;
}

static void udp_abort_sender_session(void *session)
{
   int fd;
   if (session == NULL) return;
   fd = msc_udp_sender_fd(session);
   if (fd >= 0) shutdown(fd, SHUT_RDWR);
   msc_udp_sender_close(session);
}

static void udp_abort_receiver_session(void *session)
{
   int fd;
   if (session == NULL) return;
   fd = msc_udp_receiver_fd(session);
   if (fd >= 0) shutdown(fd, SHUT_RDWR);
   msc_udp_receiver_close(session);
}

static void udp_exit_with(int code, const char *what)
{
   fprintf(stderr, "MSC UDP: %s\n", what);
   if (msc_cancelled()) exit(msc_cancel_exit_code());
   exit(code);
}

static uint64_t udp_checksum_fd(int fd, off_t offset, uint64_t length, int *ok)
{
   unsigned char buffer[65536];
   uint64_t value = UINT64_C(1469598103934665603);
   uint64_t done = 0;
   *ok = 0;
   while (done < length)
   {
      size_t want = length - done < sizeof(buffer) ? (size_t)(length - done) : sizeof(buffer);
      size_t i;
      if (preadall(fd, buffer, want, offset + (off_t)done) != 0) return 0;
      for (i = 0; i < want; i++) { value ^= buffer[i]; value *= UINT64_C(1099511628211); }
      done += want;
   }
   *ok = 1;
   return value;
}

static int udp_fsync_parent(const char *path)
{
   char *copy = strdup(path), *slash;
   int fd, rc;
   if (copy == NULL) return -1;
   slash = strrchr(copy, '/');
   if (slash == NULL) strcpy(copy, ".");
   else if (slash == copy) slash[1] = '\0';
   else *slash = '\0';
   fd = open(copy, O_RDONLY | O_DIRECTORY);
   free(copy);
   if (fd < 0) return -1;
   rc = fsync(fd);
   close(fd);
   return rc;
}

/* An implicit -s must reach MSC UDP as 0 so pick_payload() can auto-raise it to the
 * PMTUD-probed path ceiling; an explicit -s is passed verbatim (MSC UDP only ever
 * clamps it down to the probed MTU, never up). Both ends of a session must
 * compute the same value here (msc.h: payload "must match both ends"); the
 * receiver learns explicitness from the forwarded packetsize (0 = implicit). */
static size_t udp_payload_size(const struct argdata *AD)
{
   return AD->packetsize_explicit ? AD->packetsize : 0;
}

int msc_udp_env_forwardable(const char *name, size_t length)
{
   static const char *const allowed[] = {
      "MSC_UDP_ACK_EVERY", "MSC_UDP_AUTOTUNE_MAX", "MSC_UDP_CC",
      "MSC_UDP_CC_BETA", "MSC_UDP_CTL", "MSC_UDP_DROP",
      "MSC_UDP_DSACK", "MSC_UDP_DUPACK_THRESH", "MSC_UDP_ECN", "MSC_UDP_FAIR",
      "MSC_UDP_FORCE_MMAP", "MSC_UDP_FSYNC_THREADS", "MSC_UDP_GRO",
      "MSC_UDP_GSO", "MSC_UDP_CONTROL_DROP", "MSC_UDP_CONTROL_TRACE",
      "MSC_UDP_INIT_CWND", "MSC_UDP_MIN_CWND", "MSC_UDP_MIN_RTO_MS",
      "MSC_UDP_NO_AFFINITY", "MSC_UDP_NO_CC", "MSC_UDP_NO_MMAP",
      "MSC_UDP_PACE", "MSC_UDP_PACE_BURST", "MSC_UDP_PACE_GAIN",
      "MSC_UDP_PACE_RATE_MBIT", "MSC_UDP_PAYLOAD", "MSC_UDP_PMTUD",
      "MSC_UDP_PMTUD_CAP", "MSC_UDP_PORTS", "MSC_UDP_PORT_BASE",
      "MSC_UDP_PORT_LIST", "MSC_UDP_PORT_SPAN",
      "MSC_UDP_RECV_BATCH",
      "MSC_UDP_REORDER_SRTT_DIV", "MSC_UDP_REORDER_WAIT_MS",
      "MSC_UDP_RTO_GENTLE", "MSC_UDP_RWND",
      "MSC_UDP_RWND_STALL_MS", "MSC_UDP_RX_NOWRITE",
      "MSC_UDP_SOCK_AUTOTUNE", "MSC_UDP_SOCK_BUFFER",
      "MSC_UDP_STALL_TIMEOUT_MS", "MSC_UDP_STATS",
      "MSC_UDP_STATS_STREAM", "MSC_UDP_STATS_STREAM_OUTPUT",
      "MSC_UDP_STATS_STREAM_RATE_HZ", "MSC_UDP_STRIPE_COUNT",
      "MSC_UDP_STRIPE_SIZE", "MSC_UDP_TICK_US", "MSC_UDP_WAN_GUARD",
      "MSC_UDP_PROFILE", "MSC_UDP_WAN_RTT_MS", "MSC_UDP_WAN_LONG_RTT_MS",
      "MSC_UDP_GEO_RTT_MS", "MSC_UDP_STARTUP_QUEUE_MULT",
      "MSC_UDP_BW_RESTART",
      /* Not an MSC_UDP_* engine knob: --dest-stripe-count governs destination
       * creation on the TCP path and the resume/findzero setup paths too, so it
       * deliberately carries a transport-neutral name. This list is what
       * msc_udp_remote_env() forwards on every ssh launch, TCP included. */
      "MSC_DEST_STRIPE_COUNT"
   };
   size_t i;
   for (i = 0; i < sizeof(allowed) / sizeof(allowed[0]); i++)
      if (strlen(allowed[i]) == length && memcmp(name, allowed[i], length) == 0)
         return 1;
   return 0;
}

static void apply_transport_options(const struct argdata *AD)
{
   char stall[32];
   /* MSC owns these settings: --no-gso/--no-gro must override a forwarded
    * MSC_UDP_GSO=1/MSC_UDP_GRO=1 just as the default/--gso/--gro path must override
    * a forwarded 0. */
   setenv("MSC_UDP_GSO", AD->gso ? "1" : "0", 1);
   setenv("MSC_UDP_GRO", AD->gro ? "1" : "0", 1);
   if (AD->stats) setenv("MSC_UDP_STATS", "1", 1);
   snprintf(stall, sizeof(stall), "%llu",
            (unsigned long long)AD->stall_timeout_ms);
   setenv("MSC_UDP_STALL_TIMEOUT_MS", stall, 1);
   /* Admit jumbo-frame datagrams (8972-byte payload) into MSC UDP's PMTUD probe
    * ladder; the compiled-in payload cap of 2016 never even attempts
    * them. The real per-path probe still decides what gets used, and per-flow
    * buffers scale with the *confirmed* payload, so a non-jumbo path pays
    * nothing. A user's explicit MSC_UDP_PMTUD_CAP (e.g. 65507 for loopback)
    * always wins. */
   if (getenv("MSC_UDP_PMTUD_CAP") == NULL)
      setenv("MSC_UDP_PMTUD_CAP", "9000", 1);
}

/* MSC UDP's table API takes every manifest file's fd up front, so a big tree
 * needs more descriptors than the usual 1024 soft limit (a typical compute
 * node has soft 1024, hard 262144 -- a 12288-file tree would die with
 * EMFILE). Raise the soft limit as far as needed, capped by the
 * hard limit, BEFORE opening: failing up front with an actionable message
 * beats dying mid-manifest. The margin covers MSC UDP flow sockets (<= 256),
 * control/control_channel fds, and stdio. Runs on both sides -- the receiver opens
 * the same table O_RDWR. */
#define MSC_UDP_FD_MARGIN 512
static int ensure_fd_capacity(uint64_t files)
{
   struct rlimit rl;
   rlim_t need = (rlim_t)files + MSC_UDP_FD_MARGIN;
   if (getrlimit(RLIMIT_NOFILE, &rl) != 0)
      return 0;                       /* best effort: let open() decide */
   if (need <= rl.rlim_cur)
      return 0;
   if (rl.rlim_max != RLIM_INFINITY && need > rl.rlim_max)
   {
      fprintf(stderr, "MSC UDP: this tree needs ~%llu open files but the "
              "hard limit is %llu; raise it (ulimit -Hn) or use TCP -R\n",
              (unsigned long long)need, (unsigned long long)rl.rlim_max);
      return -1;
   }
   rl.rlim_cur = need;
   if (setrlimit(RLIMIT_NOFILE, &rl) != 0)
   {
      fprintf(stderr, "MSC UDP: cannot raise open-file soft limit to "
              "%llu: %s\n", (unsigned long long)need, strerror(errno));
      return -1;
   }
   return 0;
}

static void table_init(struct msc_udp_file_table *t, uint64_t capacity)
{
   memset(t, 0, sizeof(*t));
   if (capacity == 0) capacity = 1;
   t->files = checkmalloc(sizeof(*t->files) * capacity, "MSC UDP file table");
   t->fds = checkmalloc(sizeof(*t->fds) * capacity, "MSC UDP fd table");
   t->paths = checkmalloc(sizeof(*t->paths) * capacity, "MSC UDP path table");
   t->source_stats = checkmalloc(sizeof(*t->source_stats) * capacity,
                                 "MSC UDP stat table");
   t->logical_bases = checkmalloc(sizeof(*t->logical_bases) * capacity,
                                  "MSC UDP logical-base table");
   t->capacity = capacity;
}

static void table_close(struct msc_udp_file_table *t)
{
   uint64_t i;
   for (i = 0; i < t->count; i++)
      if (t->fds[i] >= 0) close(t->fds[i]);
   free(t->files);
   free(t->fds);
   free(t->paths);
   free(t->source_stats);
   free(t->logical_bases);
   memset(t, 0, sizeof(*t));
}

static int table_open_manifest(struct directory_manifest *manifest, int flags,
                               struct msc_udp_file_table *t)
{
   uint64_t i, count = manifest_file_count(manifest);
   if (ensure_fd_capacity(count) != 0)
      return -1;
   table_init(t, count);
   for (i = 0; i < count; i++)
   {
      uint64_t size = manifest_file_size(manifest, i);
      const char *path;
      int fd;
      /* The manifest already created empty files; omitting them keeps MSC UDP's
       * transfer table free of zero-unit flows without losing an entry. */
      if (size == 0) continue;
      path = manifest_file_path(manifest, i);
      fd = open(path, flags);
      if (fd < 0)
      {
         fprintf(stderr, "MSC UDP: cannot open %s: %s\n",
                 path, strerror(errno));
         table_close(t);
         return -1;
      }
      t->files[t->count].fd = fd;
      t->files[t->count].base = 0;
      t->files[t->count].size = size;
      t->fds[t->count] = fd;
      t->paths[t->count] = path;
      t->logical_bases[t->count] = manifest_file_logical_base(manifest, i);
      if (fstat(fd, &t->source_stats[t->count]) != 0)
      {
         fprintf(stderr, "MSC UDP: cannot stat open file %s: %s\n",
                 path, strerror(errno));
         close(fd);
         t->fds[t->count] = -1;
         table_close(t);
         return -1;
      }
      t->count++;
   }
   return 0;
}

static int table_source_unchanged(const struct msc_udp_file_table *table)
{
   uint64_t i;
   for (i = 0; i < table->count; i++)
   {
      struct stat opened, path;
      const struct stat *expected = &table->source_stats[i];
      if (fstat(table->fds[i], &opened) != 0 ||
          lstat(table->paths[i], &path) != 0 ||
          !udp_same_source(expected, &opened) ||
          !udp_same_source(expected, &path))
         return 0;
   }
   return 1;
}

static int table_build_slice(const struct msc_udp_file_table *table,
                             uint64_t start, uint64_t end,
                             struct msc_udp_slice_table *slice)
{
   uint64_t i;
   memset(slice, 0, sizeof(*slice));
   if (start >= end || end > (uint64_t)INT64_MAX)
      return -1;
   slice->files = checkmalloc(
      sizeof(*slice->files) * (table->count ? table->count : 1),
      "MSC UDP recursive slice");
   for (i = 0; i < table->count; i++)
   {
      uint64_t file_start = table->logical_bases[i];
      uint64_t file_end;
      uint64_t piece_start, piece_end;
      if (UINT64_MAX - file_start < table->files[i].size)
         goto fail;
      file_end = file_start + table->files[i].size;
      if (file_end <= start || file_start >= end)
         continue;
      piece_start = start > file_start ? start : file_start;
      piece_end = end < file_end ? end : file_end;
      slice->files[slice->count].fd = table->files[i].fd;
      slice->files[slice->count].base = piece_start - file_start;
      slice->files[slice->count].size = piece_end - piece_start;
      slice->count++;
   }
   if (slice->count == 0)
      goto fail;
   return 0;
fail:
   free(slice->files);
   memset(slice, 0, sizeof(*slice));
   return -1;
}

static void table_free_slice(struct msc_udp_slice_table *slice)
{
   free(slice->files);
   memset(slice, 0, sizeof(*slice));
}

static int run_send_tree(void *session, struct argdata *AD, uint64_t *bytes)
{
   struct directory_manifest *manifest = manifest_build_source(AD);
   struct msc_udp_file_table table;
   struct msc_udp_tree_xfer xfer;
   int rc = -1;
   volatile uint64_t current = 0;
   struct msc_progress progress;

   memset(&table, 0, sizeof(table));
   if (manifest_send_all(msc_udp_sender_fd(session), manifest) != 0 ||
       table_open_manifest(manifest, O_RDONLY, &table) != 0)
      goto out;
   /* Transport delivery of TREE_DONE is not application readiness: the peer
    * still has to create/truncate the tree and open its file table.  Waiting
    * for this explicit barrier prevents the initial data windows from being
    * sent before the recursive receiver workers exist (especially visible
    * with UDP control and shared-socket modes). */
   {
      uint32_t ready;
      if (msc_udp_recv_all(msc_udp_sender_fd(session), &ready, sizeof(ready)) != 0 ||
          ntohl(ready) != MSC_UDP_CTL_TREE_READY)
      {
         fprintf(stderr, "MSC UDP: recursive receiver did not become ready\n");
         goto out;
      }
   }
   /* Fault injection (tests only): vanish right after the manifest/TREE_READY
    * barrier and before the table send, with no control-channel FIN, so the
    * receiver's peer goes silent-but-open while it blocks on TABLE_BEGIN. This
    * exercises the recursive-handoff watchdog (bounded recv) in recv_table. */
   if (getenv("MSC_TEST_SILENT_AFTER_MANIFEST") != NULL)
      _exit(MSC_EXIT_NETWORK);
   memset(&xfer, 0, sizeof(xfer));
   xfer.files = table.files;
   xfer.nfiles = table.count;
   xfer.flow_count = AD->numstreams;
   xfer.payload_size = udp_payload_size(AD);
   xfer.transfer_id = msc_udp_sender_next_transfer_id(session);
   xfer.cancel_flag = &msc_cancel_signal;
   msc_progress_start(&progress, AD, &current, 0, manifest_total_size(manifest));
   rc = msc_udp_sender_send_table(session, &xfer);
   if (rc != 0 && udp_exit_status_valid(xfer.error_code))
      rc = xfer.error_code;
   if (rc == 0) { *bytes = xfer.total_bytes; __atomic_store_n(&current, *bytes, __ATOMIC_RELAXED); }
   msc_progress_finish(&progress, rc == 0,
                       manifest_file_count(manifest) > 10000
                          ? "metadata/small-file overhead likely (more than 10,000 files)"
                          : "network/transport or storage limited (timing separation unavailable)");
   table_close(&table);
out:
   manifest_destroy(manifest);
   return rc;
}

static int run_recv_tree(void *session, struct argdata *AD)
{
   struct directory_manifest *manifest;
   struct msc_udp_file_table table;
   struct msc_udp_tree_xfer xfer;
   int rc = -1;

   memset(&table, 0, sizeof(table));
   manifest = manifest_recv_all(msc_udp_receiver_fd(session), AD);
   if (manifest == NULL ||
       table_open_manifest(manifest, O_RDWR, &table) != 0)
      goto out;
   {
      uint32_t ready = htonl(MSC_UDP_CTL_TREE_READY);
      if (msc_udp_send_all(msc_udp_receiver_fd(session), &ready, sizeof(ready)) != 0)
      {
         fprintf(stderr, "MSC UDP: could not signal recursive receiver readiness\n");
         goto out;
      }
   }
   memset(&xfer, 0, sizeof(xfer));
   xfer.files = table.files;
   xfer.nfiles = table.count;
   xfer.flow_count = AD->numstreams;
   xfer.payload_size = udp_payload_size(AD);
   xfer.transfer_id = msc_udp_receiver_next_transfer_id(session);
   xfer.cancel_flag = &msc_cancel_signal;
   rc = msc_udp_receiver_recv_table(session, &xfer);
   if (rc != 0 && udp_exit_status_valid(xfer.error_code))
      rc = xfer.error_code;
   if (rc == 0) rc = manifest_apply_modes(manifest, AD);
   table_close(&table);
out:
   if (manifest != NULL) manifest_destroy(manifest);
   return rc;
}

static char *make_temp_destination(const char *final, int force)
{
   char *temp;
   int fd;
   size_t n;

   if (!force && access(final, F_OK) == 0)
   {
      fprintf(stderr, "MSC UDP: destination exists: %s (use --force)\n",
              final);
      return NULL;
   }
   if (!force && errno != ENOENT)
   {
      fprintf(stderr, "MSC UDP: cannot inspect %s: %s\n",
              final, strerror(errno));
      return NULL;
   }
   /* Single-file UDP is transactional: MSC UDP writes this sibling and publishes
    * it only after transfer completion and, when enabled, checksum approval.
    * Recursive UDP writes the manifest tree in place and does not use this. */
   n = strlen(final) + 64;
   temp = checkmalloc(n, "temporary destination path");
   snprintf(temp, n, "%s.msc-udptmp.%ld", final, (long)getpid());
   fd = open(temp, O_CREAT | O_EXCL | O_RDWR, 0600);
   if (fd < 0)
   {
      fprintf(stderr, "MSC UDP: cannot create %s: %s\n",
              temp, strerror(errno));
      free(temp);
      return NULL;
   }
   close(fd);
   return temp;
}

static uint64_t udp_checkpoint_chunk(void)
{
   const char *value = getenv("MSC_UDP_CHECKPOINT_BYTES");
   char *end = NULL;
   unsigned long long parsed;
   if (value == NULL) return MSC_UDP_CHECKPOINT_BYTES;
   errno = 0;
   parsed = strtoull(value, &end, 10);
   if (errno != 0 || end == value || *end != '\0' || parsed == 0 || parsed > INT64_MAX)
      return MSC_UDP_CHECKPOINT_BYTES;
   return (uint64_t)parsed;
}

static char *udp_resume_temp_path(const char *destination)
{
   char *path;
   if (asprintf(&path, "%s.msc-part", destination) < 0) return NULL;
   return path;
}

static int udp_ranges_valid(const struct msc_checkpoint *cp, uint64_t total,
                            char *error, size_t error_size)
{
   uint32_t i;
   if (cp->range_count > MSC_CHECKPOINT_MAX_RANGES)
   {
      snprintf(error, error_size, "checkpoint has too many durable ranges");
      return -1;
   }
   for (i = 0; i < cp->range_count; i++)
      if (cp->ranges[i].start >= cp->ranges[i].end || cp->ranges[i].end > total ||
          (i && cp->ranges[i - 1].end >= cp->ranges[i].start))
      {
         snprintf(error, error_size, "checkpoint durable ranges are invalid or out of bounds");
         return -1;
      }
   /* The UDP checkpoint layer commits consecutive application chunks. A disjoint
    * range list cannot be produced by this implementation and is therefore
    * stale/incompatible rather than something to guess around. */
   if (cp->range_count > 1 ||
       (cp->range_count == 1 && cp->ranges[0].start != 0))
   {
      snprintf(error, error_size, "checkpoint does not contain one coalesced durable prefix");
      return -1;
   }
   return 0;
}

static int udp_validate_partial(const struct msc_checkpoint *cp,
                                const char *path, uint64_t total,
                                char *error, size_t error_size)
{
   struct stat st;
   if (lstat(path, &st) != 0)
      snprintf(error, error_size, "cannot inspect resumable destination %s: %s",
               path, strerror(errno));
   else if (!S_ISREG(st.st_mode) || (uint64_t)st.st_dev != cp->destination_dev ||
            (uint64_t)st.st_ino != cp->destination_ino ||
            (uint64_t)st.st_size < cp->destination_size ||
            (uint64_t)st.st_size > total)
      snprintf(error, error_size,
               "resumable destination %s was replaced, truncated, or grew out of bounds",
               path);
   else return 0;
   return -1;
}

static int udp_same_source(const struct stat *a, const struct stat *b)
{
   return a->st_dev == b->st_dev && a->st_ino == b->st_ino &&
          a->st_size == b->st_size &&
          a->st_mtim.tv_sec == b->st_mtim.tv_sec &&
          a->st_mtim.tv_nsec == b->st_mtim.tv_nsec;
}

static int udp_resume_sender_hello(int fd, struct argdata *AD, uint64_t total,
                                   int sourcefd, const struct stat *opened,
                                   uint64_t *reused)
{
   struct stat st;
   struct msc_resume_hello hello;
   struct msc_resume_reply reply;
   size_t length = strlen(AD->sourcefile);
   uint32_t count, i;
   uint64_t previous_end = 0;
   struct msc_range durable = { 0, 0 };
   if (lstat(AD->sourcefile, &st) != 0 || !S_ISREG(st.st_mode) ||
       !udp_same_source(&st, opened)) return MSC_EXIT_SOURCE;
   if (length == 0 || length > 1024U * 1024U) return MSC_EXIT_CLI;
   memset(&hello, 0, sizeof(hello)); hello.magic = htonl(MSC_RESUME_MAGIC);
   hello.version = htonl(MSC_CHECKPOINT_VERSION); hello.flags = htonl(AD->resume ? 1U : 0U);
   hello.path_length = htonl((uint32_t)length); hello.segment_size = host_to_network_64(udp_checkpoint_chunk());
   hello.transfer_size = host_to_network_64(total); hello.source_dev = host_to_network_64(st.st_dev);
   hello.source_ino = host_to_network_64(st.st_ino); hello.source_size = host_to_network_64(st.st_size);
   hello.source_mtime_sec = host_to_network_64((uint64_t)st.st_mtim.tv_sec);
   hello.source_mtime_nsec = host_to_network_64((uint64_t)st.st_mtim.tv_nsec);
   hello.source_offset = 0;
   hello.destination_offset = 0;
   if (msc_udp_send_all(fd, &hello, sizeof(hello)) != 0 ||
       msc_udp_send_all(fd, AD->sourcefile, length) != 0 ||
       msc_udp_recv_all(fd, &reply, sizeof(reply)) != 0)
      return MSC_EXIT_NETWORK;
   if (ntohl(reply.magic) != MSC_RESUME_REPLY_MAGIC ||
       ntohl(reply.version) != MSC_CHECKPOINT_VERSION)
      return MSC_EXIT_INCOMPATIBLE;
   if (ntohl(reply.status) != 0)
      return udp_exit_status_valid(ntohl(reply.status))
         ? (int)ntohl(reply.status) : MSC_EXIT_INCOMPATIBLE;
   count = ntohl(reply.range_count);
   if (count > MSC_CHECKPOINT_MAX_RANGES) return MSC_EXIT_INCOMPATIBLE;
   *reused = 0;
   for (i = 0; i < count; i++)
   {
      struct msc_wire_range wire;
      uint64_t start, end;
      if (msc_udp_recv_all(fd, &wire, sizeof(wire)) != 0) return MSC_EXIT_NETWORK;
      start = network_to_host_64(wire.start);
      end = network_to_host_64(wire.end);
      if (start >= end || end > total || (i && previous_end >= start))
         return MSC_EXIT_INCOMPATIBLE;
      if (i != 0 || start != 0) return MSC_EXIT_INCOMPATIBLE;
      previous_end = end;
      *reused = end;
      durable.start = start;
      durable.end = end;
   }
   {
      struct msc_file_digest_context digest_context;
      int verify_status;
      digest_context.fd = sourcefd;
      digest_context.base = 0;
      if (msc_resume_send_digests(fd, &durable, count,
                                  msc_resume_file_digest,
                                  &digest_context) != 0)
         return MSC_EXIT_NETWORK;
      verify_status = msc_resume_receive_verify_reply(fd);
      if (verify_status != 0)
         return verify_status;
   }
   return 0;
}

static int udp_resume_receiver_hello(int fd, struct argdata *AD,
                                     struct msc_checkpoint *cp, char **temp_out,
                                     int *tempfd_out, uint64_t *total_out,
                                     uint64_t *reused_out)
{
   struct msc_resume_hello hello;
   struct msc_resume_reply reply;
   struct msc_wire_range wire;
   char *source = NULL, *temp = NULL;
   char error[512] = "invalid UDP checkpoint";
   uint32_t length;
   uint64_t old_size = 0;
   int tempfd = -1, ok = 0, state_durable = 0, code = MSC_EXIT_INCOMPATIBLE;
   uint32_t flags = 0;
   memset(&reply, 0, sizeof(reply)); reply.magic = htonl(MSC_RESUME_REPLY_MAGIC);
   reply.version = htonl(MSC_CHECKPOINT_VERSION);
   if (msc_udp_recv_all(fd, &hello, sizeof(hello)) != 0)
   { code = MSC_EXIT_NETWORK; snprintf(error, sizeof(error), "resume hello was truncated"); goto out; }
   if (ntohl(hello.magic) != MSC_RESUME_MAGIC ||
       ntohl(hello.version) != MSC_CHECKPOINT_VERSION)
   { snprintf(error, sizeof(error), "resume protocol magic/version is incompatible"); goto out; }
   flags = ntohl(hello.flags);
   length = ntohl(hello.path_length); *total_out = network_to_host_64(hello.transfer_size);
   if ((flags & ~1U) != 0 || length == 0 || length > 1024U * 1024U ||
       *total_out > INT64_MAX ||
       *total_out != network_to_host_64(hello.source_size) ||
       network_to_host_64(hello.source_offset) != 0 ||
       network_to_host_64(hello.destination_offset) != 0 ||
       network_to_host_64(hello.segment_size) != udp_checkpoint_chunk())
   { snprintf(error, sizeof(error), "resume hello has invalid flags, bounds, or chunk geometry"); goto out; }
   source = checkmalloc((size_t)length + 1, "UDP resume source");
   if (msc_udp_recv_all(fd, source, length) != 0)
   { code = MSC_EXIT_NETWORK; snprintf(error, sizeof(error), "source identity was truncated"); goto out; }
   source[length] = '\0';
   temp = udp_resume_temp_path(AD->destfile);
   if (temp == NULL) { code = MSC_EXIT_INTERNAL; goto out; }
   if (flags & 1U)
   {
      if (msc_checkpoint_read(AD->checkpoint_path, cp, error, sizeof(error)) != 0 || cp->recursive ||
          cp->segment_size != udp_checkpoint_chunk() || cp->source_dev != network_to_host_64(hello.source_dev) ||
          cp->source_ino != network_to_host_64(hello.source_ino) || cp->source_size != network_to_host_64(hello.source_size) ||
          cp->source_mtime_sec != (int64_t)network_to_host_64(hello.source_mtime_sec) ||
          cp->source_mtime_nsec != (int64_t)network_to_host_64(hello.source_mtime_nsec) ||
          cp->source_offset != 0 || cp->destination_offset != 0 ||
          cp->transfer_size != *total_out ||
          strcmp(cp->source_path, source) != 0 || strcmp(cp->destination_path, AD->destfile) != 0 ||
          udp_validate_partial(cp, temp, *total_out, error, sizeof(error)) != 0 ||
          udp_ranges_valid(cp, *total_out, error, sizeof(error)) != 0)
         goto out;
      old_size = cp->destination_size;
      /* A Lustre layout is fixed at creation, so a resumed partial cannot be
       * relaid out -- unlinking it would throw away exactly the bytes resume
       * exists to keep. Verify instead: a mismatch means this checkpoint was
       * started under a different --dest-stripe-count and finishing it would
       * publish a file with the wrong layout. */
      if (AD->dest_stripe_count != 0)
      {
         uint32_t actual = 0;
         if (msc_lustre_check_stripe_count(temp, AD->dest_stripe_count, &actual) != 0)
         {
            code = MSC_EXIT_DESTINATION;
            snprintf(error, sizeof(error),
                     "resumable partial destination has stripe count %u, not the "
                     "requested %ld", actual, AD->dest_stripe_count);
            goto out;
         }
      }
      tempfd = open(temp, O_RDWR | O_NOFOLLOW);
      if (tempfd < 0)
      { code = MSC_EXIT_DESTINATION; snprintf(error, sizeof(error), "cannot open stable partial destination: %s", strerror(errno)); goto out; }
   }
   else
   {
      struct stat stale;
      int checkpoint_error, partial_error;
      int checkpoint_exists, partial_exists;
      errno = 0; checkpoint_exists = lstat(AD->checkpoint_path, &stale) == 0;
      checkpoint_error = errno;
      errno = 0; partial_exists = lstat(temp, &stale) == 0;
      partial_error = errno;
      if ((!checkpoint_exists && checkpoint_error != ENOENT) ||
          (!partial_exists && partial_error != ENOENT))
      { code = MSC_EXIT_DESTINATION; snprintf(error, sizeof(error), "cannot inspect checkpoint state: %s", strerror(errno)); goto out; }
      if (checkpoint_exists || partial_exists)
      {
         if (!AD->force)
         { snprintf(error, sizeof(error), "stale checkpoint or partial destination exists; use --resume or --force"); goto out; }
         if ((checkpoint_exists && unlink(AD->checkpoint_path) != 0) ||
             (partial_exists && unlink(temp) != 0))
         { code = MSC_EXIT_DESTINATION; snprintf(error, sizeof(error), "cannot discard stale state: %s", strerror(errno)); goto out; }
      }
      if (!AD->force)
      {
         errno = 0;
         if (lstat(AD->destfile, &stale) == 0)
         { snprintf(error, sizeof(error), "destination exists; use --force"); goto out; }
         if (errno != ENOENT)
         { code = MSC_EXIT_DESTINATION; snprintf(error, sizeof(error), "cannot inspect destination: %s", strerror(errno)); goto out; }
      }
      /* The partial IS the destination inode (it is published by rename), so an
       * explicit layout has to be applied here, at its creation -- the greeting
       * that carries the request arrives later, against a preset fd. Same flags
       * and 0600 as the plain path, so the request costs the temp none of its
       * exclusivity. */
      if (AD->dest_stripe_count != 0)
      {
         tempfd = msc_lustre_create_striped(temp, O_CREAT | O_EXCL | O_RDWR | O_NOFOLLOW,
                                            0600, 0, AD->dest_stripe_count);
         if (tempfd < 0)
         {
            code = MSC_EXIT_DESTINATION;
            snprintf(error, sizeof(error),
                     "cannot create stable partial destination with stripe "
                     "count %ld: %s", AD->dest_stripe_count, strerror(errno));
            goto out;
         }
      }
      else
         tempfd = open(temp, O_CREAT | O_EXCL | O_RDWR | O_NOFOLLOW, 0600);
      if (tempfd < 0)
      { code = MSC_EXIT_DESTINATION; snprintf(error, sizeof(error), "cannot create stable partial destination: %s", strerror(errno)); goto out; }
      cp->segment_size = udp_checkpoint_chunk(); cp->source_dev = network_to_host_64(hello.source_dev);
      cp->source_ino = network_to_host_64(hello.source_ino); cp->source_size = network_to_host_64(hello.source_size);
      cp->source_mtime_sec = (int64_t)network_to_host_64(hello.source_mtime_sec);
      cp->source_mtime_nsec = (int64_t)network_to_host_64(hello.source_mtime_nsec);
      cp->source_offset = 0; cp->destination_offset = 0;
      cp->transfer_size = *total_out;
      cp->source_path = strdup(source); cp->destination_path = strdup(AD->destfile);
      if (cp->source_path == NULL || cp->destination_path == NULL)
      { code = MSC_EXIT_INTERNAL; snprintf(error, sizeof(error), "out of memory creating checkpoint identity"); goto out; }
   }
   /* A killed MSC UDP receive may have extended this same inode beyond the last
    * checkpointed prefix.  It is safe only because those bytes are unclaimed:
    * discard them before accepting payload, then refresh the stable identity.
    * MSC UDP sizes the current chunk itself, so no full-file preallocation is
    * needed here (and the checkpoint continues to describe the durable size). */
   if ((flags & 1U) &&
       ftruncate(tempfd, (off_t)(cp->range_count ? cp->ranges[0].end : 0)) != 0)
   {
      code = MSC_EXIT_DESTINATION;
      snprintf(error, sizeof(error), "cannot discard uncheckpointed UDP suffix: %s", strerror(errno));
      goto out;
   }
   if (fsync(tempfd) != 0 ||
       udp_fsync_parent(temp) != 0 ||
       msc_checkpoint_capture_destination(cp, temp) != 0 ||
       msc_checkpoint_write_atomic(AD->checkpoint_path, cp) != 0)
   {
      code = MSC_EXIT_DESTINATION;
      snprintf(error, sizeof(error), "cannot durably prepare checkpoint state: %s", strerror(errno));
      if (flags & 1U)
      {
         if (ftruncate(tempfd, (off_t)old_size) != 0)
            fprintf(stderr, "MSC UDP checkpoint rollback failed: %s\n", strerror(errno));
         (void)fsync(tempfd);
      }
      goto out;
   }
   state_durable = 1;
   code = MSC_EXIT_NETWORK;
   *reused_out = cp->range_count ? cp->ranges[0].end : 0;
   reply.status = 0; reply.range_count = htonl(cp->range_count);
   if (msc_udp_send_all(fd, &reply, sizeof(reply)) != 0) goto out;
   if (cp->range_count)
   { wire.start = 0; wire.end = host_to_network_64(*reused_out); if (msc_udp_send_all(fd, &wire, sizeof(wire)) != 0) goto out; }
   {
      struct msc_file_digest_context digest_context;
      int verify;
      int verify_status;
      digest_context.fd = tempfd;
      digest_context.base = 0;
      verify = msc_resume_verify_digests(fd, cp->ranges, cp->range_count,
                                         msc_resume_file_digest,
                                         &digest_context);
      verify_status = verify == 0 ? 0 :
                      verify > 0 ? MSC_EXIT_INTEGRITY : MSC_EXIT_DESTINATION;
      if (msc_resume_send_verify_reply(fd, verify_status) != 0)
      { code = MSC_EXIT_NETWORK; goto out; }
      if (verify_status != 0)
      {
         code = verify_status;
         snprintf(error, sizeof(error),
                  "durable UDP bytes do not match the current source");
         goto out;
      }
   }
   *temp_out = temp; *tempfd_out = tempfd; temp = NULL; tempfd = -1; ok = 1;
out:
   if (!ok)
   {
      fprintf(stderr, "MSC UDP checkpoint negotiation rejected: %s\n", error);
      reply.status = htonl((uint32_t)code);
      msc_udp_send_all(fd, &reply, sizeof(reply));
      if (!state_durable && !(flags & 1U) && tempfd >= 0)
      {
         close(tempfd); tempfd = -1;
         unlink(temp);
      }
   }
   if (tempfd >= 0) close(tempfd);
   free(source); free(temp);
   return ok ? 0 : code;
}

static int udp_sender_verify_chunk(int fd, int sourcefd, off_t source_offset,
                                   uint64_t end, uint64_t length,
                                   int initial_status)
{
   struct udp_chunk_check check, decision;
   uint64_t local;
   int ok;
   local = initial_status == 0
      ? udp_checksum_fd(sourcefd, source_offset, length, &ok) : 0;
   if (initial_status == 0 && !ok) initial_status = MSC_EXIT_SOURCE;
   if (msc_udp_recv_all(fd, &check, sizeof(check)) != 0) return MSC_EXIT_NETWORK;
   memset(&decision, 0, sizeof(decision));
   decision.magic = htonl(MSC_UDP_CHECK_MAGIC);
   decision.durable_end = host_to_network_64(end);
   if (initial_status != 0)
      decision.status = htonl((uint32_t)initial_status);
   else if (ntohl(check.magic) != MSC_UDP_CHECK_MAGIC ||
       network_to_host_64(check.durable_end) != end)
      decision.status = htonl(MSC_EXIT_INCOMPATIBLE);
   else if (network_to_host_64(check.checksum) != local)
      decision.status = htonl(MSC_EXIT_INTEGRITY);
   if (msc_udp_send_all(fd, &decision, sizeof(decision)) != 0) return MSC_EXIT_NETWORK;
   return ntohl(decision.status) == 0 ? 0 : (int)ntohl(decision.status);
}

static int udp_receiver_verify_chunk(int fd, int destinationfd, off_t offset,
                                     uint64_t end, uint64_t length)
{
   struct udp_chunk_check check, decision;
   uint64_t local;
   int ok;
   local = udp_checksum_fd(destinationfd, offset, length, &ok);
   if (!ok) return MSC_EXIT_DESTINATION;
   memset(&check, 0, sizeof(check));
   check.magic = htonl(MSC_UDP_CHECK_MAGIC);
   check.durable_end = host_to_network_64(end);
   check.checksum = host_to_network_64(local);
   if (msc_udp_send_all(fd, &check, sizeof(check)) != 0 ||
       msc_udp_recv_all(fd, &decision, sizeof(decision)) != 0)
      return MSC_EXIT_NETWORK;
   if (ntohl(decision.magic) != MSC_UDP_CHECK_MAGIC ||
       network_to_host_64(decision.durable_end) != end)
      return MSC_EXIT_INCOMPATIBLE;
   if (ntohl(decision.status) == 0) return 0;
   return udp_exit_status_valid(ntohl(decision.status))
      ? (int)ntohl(decision.status) : MSC_EXIT_INCOMPATIBLE;
}

static int udp_send_durable_ack(int fd, int status, uint64_t end)
{
   struct msc_udp_durable_ack ack;
   ack.magic = htonl(MSC_UDP_DURABLE_MAGIC);
   ack.status = htonl((uint32_t)status);
   ack.durable_end = host_to_network_64(end);
   return msc_udp_send_all(fd, &ack, sizeof(ack));
}

static int udp_receive_durable_ack(int fd, uint64_t expected)
{
   struct msc_udp_durable_ack ack;
   uint32_t status;
   if (msc_udp_recv_all(fd, &ack, sizeof(ack)) != 0) return MSC_EXIT_NETWORK;
   if (ntohl(ack.magic) != MSC_UDP_DURABLE_MAGIC ||
       network_to_host_64(ack.durable_end) != expected)
      return MSC_EXIT_INCOMPATIBLE;
   status = ntohl(ack.status);
   if (status == 0) return 0;
   return udp_exit_status_valid(status) ? (int)status : MSC_EXIT_INCOMPATIBLE;
}

static int tree_next_missing_chunk(const struct msc_checkpoint *checkpoint,
                                   uint64_t total, uint64_t chunk_size,
                                   uint64_t *position,
                                   uint64_t *start, uint64_t *end)
{
   uint32_t i;
   if (chunk_size == 0 || *position > total)
      return -1;
   while (*position < total)
   {
      for (i = 0; i < checkpoint->range_count; i++)
      {
         const struct msc_range *range = &checkpoint->ranges[i];
         if (range->start >= range->end || range->end > total ||
             (i != 0 && checkpoint->ranges[i - 1].end >= range->start))
            return -1;
         if (range->end <= *position)
            continue;
         if (range->start <= *position)
         {
            *position = range->end;
            break;
         }
         *start = *position;
         *end = range->start;
         if (*end - *start > chunk_size)
            *end = *start + chunk_size;
         return 1;
      }
      if (i < checkpoint->range_count)
         continue;
      *start = *position;
      *end = total;
      if (*end - *start > chunk_size)
         *end = *start + chunk_size;
      return 1;
   }
   return 0;
}

static void tree_xfer_init(struct msc_udp_tree_xfer *xfer,
                           const struct msc_udp_slice_table *slice,
                           const struct argdata *AD, uint64_t transfer_id)
{
   memset(xfer, 0, sizeof(*xfer));
   xfer->files = slice->files;
   xfer->nfiles = slice->count;
   xfer->flow_count = AD->numstreams;
   xfer->payload_size = udp_payload_size(AD);
   xfer->transfer_id = transfer_id;
   xfer->cancel_flag = &msc_cancel_signal;
}

static int run_send_tree_resume(void *session, struct argdata *AD,
                                uint64_t *bytes)
{
   struct directory_manifest *manifest = NULL;
   struct msc_udp_file_table table;
   struct msc_checkpoint checkpoint;
   struct msc_progress progress;
   struct stat root_identity, current_root;
   volatile uint64_t current = 0;
   uint64_t total, reused, position = 0, start, end;
   uint32_t ready;
   int failure = 0, progress_started = 0, next;

   memset(&table, 0, sizeof(table));
   msc_checkpoint_init(&checkpoint);
   *bytes = 0;
   manifest = manifest_build_source(AD);
   total = manifest_total_size(manifest);
   if (lstat(AD->sourcefile, &root_identity) != 0 ||
       !S_ISDIR(root_identity.st_mode) ||
       table_open_manifest(manifest, O_RDONLY, &table) != 0)
   {
      failure = MSC_EXIT_SOURCE;
      goto out;
   }
   failure = msc_recursive_resume_sender_hello(
      msc_udp_sender_fd(session), AD, manifest, &checkpoint,
      udp_checkpoint_chunk());
   if (failure != 0)
      goto out;
   reused = msc_checkpoint_completed_bytes(&checkpoint);
   if (reused > total)
   {
      failure = MSC_EXIT_INCOMPATIBLE;
      goto out;
   }
   AD->childinfo->reused = (long long)reused;
   if (manifest_send_all(msc_udp_sender_fd(session), manifest) != 0)
   {
      failure = MSC_EXIT_NETWORK;
      goto out;
   }
   if (!table_source_unchanged(&table) ||
       lstat(AD->sourcefile, &current_root) != 0 ||
       !udp_same_source(&root_identity, &current_root))
   {
      failure = MSC_EXIT_SOURCE;
      goto out;
   }
   if (manifest_send_checkpoint_digests(
          msc_udp_sender_fd(session), manifest, &checkpoint) != 0)
   {
      failure = table_source_unchanged(&table)
         ? MSC_EXIT_NETWORK : MSC_EXIT_SOURCE;
      goto out;
   }
   failure = msc_resume_receive_verify_reply(msc_udp_sender_fd(session));
   if (failure != 0)
      goto out;
   if (msc_udp_recv_all(msc_udp_sender_fd(session),
                        &ready, sizeof(ready)) != 0)
   {
      failure = MSC_EXIT_NETWORK;
      goto out;
   }
   ready = ntohl(ready);
   if (ready != MSC_UDP_CTL_TREE_READY)
   {
      failure = udp_exit_status_valid(ready)
         ? (int)ready : MSC_EXIT_INCOMPATIBLE;
      goto out;
   }
   msc_progress_start(&progress, AD, &current, reused, total);
   progress_started = 1;
   while ((next = tree_next_missing_chunk(
              &checkpoint, total, udp_checkpoint_chunk(),
              &position, &start, &end)) > 0)
   {
      struct msc_udp_slice_table slice;
      struct msc_udp_tree_xfer xfer;
      if (!table_source_unchanged(&table) ||
          lstat(AD->sourcefile, &current_root) != 0 ||
          !udp_same_source(&root_identity, &current_root))
      {
         failure = MSC_EXIT_SOURCE;
         break;
      }
      if (table_build_slice(&table, start, end, &slice) != 0)
      {
         failure = MSC_EXIT_INCOMPATIBLE;
         break;
      }
      tree_xfer_init(&xfer, &slice, AD,
                     msc_udp_sender_next_transfer_id(session));
      if (msc_udp_sender_send_table(session, &xfer) != 0 ||
          xfer.total_bytes != end - start)
      {
         failure = udp_exit_status_valid(xfer.error_code)
            ? xfer.error_code
            : (table_source_unchanged(&table)
                 ? MSC_EXIT_NETWORK : MSC_EXIT_SOURCE);
         table_free_slice(&slice);
         break;
      }
      table_free_slice(&slice);
      if (!table_source_unchanged(&table) ||
          lstat(AD->sourcefile, &current_root) != 0 ||
          !udp_same_source(&root_identity, &current_root))
      {
         failure = MSC_EXIT_SOURCE;
         break;
      }
      if (!AD->no_internal_checksum)
      {
         if (manifest_send_range_digests(
                msc_udp_sender_fd(session), manifest, start, end) != 0)
         {
            failure = table_source_unchanged(&table)
               ? MSC_EXIT_NETWORK : MSC_EXIT_SOURCE;
            break;
         }
         if (!table_source_unchanged(&table) ||
             lstat(AD->sourcefile, &current_root) != 0 ||
             !udp_same_source(&root_identity, &current_root))
         {
            failure = MSC_EXIT_SOURCE;
            break;
         }
         failure = msc_resume_receive_verify_reply(
            msc_udp_sender_fd(session));
         if (failure != 0)
            break;
      }
      failure = udp_receive_durable_ack(
         msc_udp_sender_fd(session), end);
      if (failure != 0)
         break;
      *bytes += end - start;
      __atomic_store_n(&current, *bytes, __ATOMIC_RELAXED);
      position = end;
   }
   if (next < 0 && failure == 0)
      failure = MSC_EXIT_INCOMPATIBLE;
   if (failure == 0)
      failure = udp_receive_durable_ack(
         msc_udp_sender_fd(session), total);
out:
   if (progress_started)
      msc_progress_finish(
         &progress, failure == 0,
         manifest != NULL && manifest_file_count(manifest) > 10000
            ? "metadata/small-file overhead likely (more than 10,000 files)"
            : "network/transport or storage limited (timing separation unavailable)");
   table_close(&table);
   if (manifest != NULL) manifest_destroy(manifest);
   msc_checkpoint_destroy(&checkpoint);
   return failure;
}

static int checkpoint_restore_ranges(struct msc_checkpoint *checkpoint,
                                     struct msc_range *old_ranges,
                                     uint32_t old_count)
{
   free(checkpoint->ranges);
   checkpoint->ranges = old_ranges;
   checkpoint->range_count = old_count;
   checkpoint->range_capacity = old_count;
   return 0;
}

static int run_recv_tree_resume(void *session, struct argdata *AD)
{
   struct directory_manifest *manifest = NULL;
   struct msc_udp_file_table table;
   struct msc_checkpoint checkpoint;
   uint64_t total = 0, reused, position = 0, start, end;
   uint64_t checkpoint_count = 0;
   const char *fault_phase = getenv("MSC_TEST_INTERRUPT_PHASE");
   const char *fault = getenv("MSC_TEST_INTERRUPT_AFTER_CHECKPOINTS");
   const char *signal_fault = getenv("MSC_TEST_SIGNAL_AFTER_CHECKPOINTS");
   const char *midchunk_fault = getenv("MSC_TEST_FAIL_DURING_CHUNK_MS");
   uint64_t midchunk_after =
      getenv("MSC_TEST_FAIL_DURING_CHUNK_AFTER_CHECKPOINTS") != NULL
      ? strtoull(getenv("MSC_TEST_FAIL_DURING_CHUNK_AFTER_CHECKPOINTS"),
                 NULL, 10) : 0;
   int failure = 0, next, sender_waiting_final = 0;

   memset(&table, 0, sizeof(table));
   msc_checkpoint_init(&checkpoint);
   failure = msc_recursive_resume_receiver_hello(
      msc_udp_receiver_fd(session), AD, &checkpoint, &total,
      udp_checkpoint_chunk());
   if (failure != 0)
      goto out;
   if (fault_phase != NULL && strcmp(fault_phase, "manifest") == 0)
   {
      fprintf(stderr, "MSC UDP test interruption during recursive manifest phase\n");
      failure = MSC_EXIT_NETWORK;
      goto out;
   }
   manifest = manifest_recv_all_status(
      msc_udp_receiver_fd(session), AD, &failure);
   if (manifest == NULL)
   {
      if (failure == 0) failure = MSC_EXIT_NETWORK;
      goto out;
   }
   if (manifest_total_size(manifest) != total)
   {
      failure = MSC_EXIT_INTEGRITY;
      goto out;
   }
   {
      int verify = manifest_verify_checkpoint_digests(
         msc_udp_receiver_fd(session), manifest, &checkpoint);
      failure = verify == 0 ? 0 :
                verify > 0 ? MSC_EXIT_INTEGRITY : MSC_EXIT_DESTINATION;
   }
   if (msc_resume_send_verify_reply(
          msc_udp_receiver_fd(session), failure) != 0)
   {
      failure = MSC_EXIT_NETWORK;
      goto out;
   }
   if (failure != 0)
      goto out;
   if (table_open_manifest(manifest, O_RDWR, &table) != 0)
   {
      failure = MSC_EXIT_DESTINATION;
      goto ready_failure;
   }
   if (manifest_sync_durable(manifest, AD->destfile) != 0)
   {
      failure = MSC_EXIT_DESTINATION;
      goto ready_failure;
   }
   {
      uint32_t ready = htonl(MSC_UDP_CTL_TREE_READY);
      if (msc_udp_send_all(msc_udp_receiver_fd(session),
                           &ready, sizeof(ready)) != 0)
      {
         failure = MSC_EXIT_NETWORK;
         goto out;
      }
   }
   reused = msc_checkpoint_completed_bytes(&checkpoint);
   if (reused > total)
   {
      failure = MSC_EXIT_INCOMPATIBLE;
      goto out;
   }
   while ((next = tree_next_missing_chunk(
              &checkpoint, total, udp_checkpoint_chunk(),
              &position, &start, &end)) > 0)
   {
      struct msc_udp_slice_table slice;
      struct msc_udp_tree_xfer xfer;
      struct udp_test_timer timer;
      struct msc_range *old_ranges = NULL;
      uint32_t old_count = checkpoint.range_count;
      struct stat root;
      if (old_count != 0)
      {
         old_ranges = malloc((size_t)old_count * sizeof(*old_ranges));
         if (old_ranges == NULL)
         {
            failure = MSC_EXIT_INTERNAL;
            break;
         }
         memcpy(old_ranges, checkpoint.ranges,
                (size_t)old_count * sizeof(*old_ranges));
      }
      if (table_build_slice(&table, start, end, &slice) != 0)
      {
         free(old_ranges);
         failure = MSC_EXIT_INCOMPATIBLE;
         break;
      }
      tree_xfer_init(&xfer, &slice, AD,
                     msc_udp_receiver_next_transfer_id(session));
      memset(&timer, 0, sizeof(timer));
      if (midchunk_fault != NULL && checkpoint_count >= midchunk_after)
         udp_test_timer_start(&timer, strtoull(midchunk_fault, NULL, 10));
      if (msc_udp_receiver_recv_table(session, &xfer) != 0 ||
          xfer.total_bytes != end - start)
         failure = udp_exit_status_valid(xfer.error_code)
            ? xfer.error_code : MSC_EXIT_NETWORK;
      if (udp_test_timer_finish(&timer))
      {
         fprintf(stderr,
                 "MSC UDP test interruption during an uncheckpointed recursive chunk\n");
         failure = MSC_EXIT_NETWORK;
      }
      table_free_slice(&slice);
      if (failure != 0)
      {
         free(old_ranges);
         break;
      }
      if (!AD->no_internal_checksum)
      {
         int verify = manifest_verify_range_digests(
            msc_udp_receiver_fd(session), manifest, start, end);
         failure = verify == 0 ? 0 :
                   verify > 0 ? MSC_EXIT_INTEGRITY : MSC_EXIT_DESTINATION;
         if (msc_resume_send_verify_reply(
                msc_udp_receiver_fd(session), failure) != 0)
            failure = MSC_EXIT_NETWORK;
         if (failure != 0)
         {
            free(old_ranges);
            break;
         }
      }
      if (manifest_sync_durable(manifest, AD->destfile) != 0 ||
          msc_checkpoint_add_range(&checkpoint, start, end) != 0 ||
          lstat(AD->destfile, &root) != 0 || !S_ISDIR(root.st_mode) ||
          (checkpoint.destination_dev != 0 &&
           (checkpoint.destination_dev != (uint64_t)root.st_dev ||
            checkpoint.destination_ino != (uint64_t)root.st_ino)) ||
          msc_checkpoint_write_atomic(
             AD->checkpoint_path, &checkpoint) != 0)
      {
         checkpoint_restore_ranges(&checkpoint, old_ranges, old_count);
         old_ranges = NULL;
         failure = MSC_EXIT_DESTINATION;
         (void)udp_send_durable_ack(
            msc_udp_receiver_fd(session), failure, end);
         break;
      }
      free(old_ranges);
      checkpoint_count++;
      if (getenv("MSC_TEST_CHECKPOINT_MARKER") != NULL)
      {
         int markerfd = open(getenv("MSC_TEST_CHECKPOINT_MARKER"),
                             O_WRONLY | O_CREAT | O_TRUNC, 0600);
         if (markerfd >= 0) close(markerfd);
      }
      if ((fault != NULL &&
           checkpoint_count >= strtoull(fault, NULL, 10)) ||
          (fault_phase != NULL && strcmp(fault_phase, "data") == 0))
      {
         fprintf(stderr,
                 "MSC UDP test interruption after durable recursive checkpoint\n");
         failure = MSC_EXIT_NETWORK;
         break;
      }
      if (signal_fault != NULL &&
          checkpoint_count >= strtoull(signal_fault, NULL, 10))
      {
         raise(getenv("MSC_TEST_SIGNAL_TERM") != NULL ? SIGTERM : SIGINT);
         failure = msc_cancel_exit_code();
         break;
      }
      if (udp_send_durable_ack(
             msc_udp_receiver_fd(session), 0, end) != 0)
      {
         failure = MSC_EXIT_NETWORK;
         break;
      }
      position = end;
   }
   if (next < 0 && failure == 0)
      failure = MSC_EXIT_INCOMPATIBLE;
   if (failure != 0)
      goto out;
   if (msc_checkpoint_completed_bytes(&checkpoint) != total)
   {
      failure = MSC_EXIT_INTERNAL;
      goto out;
   }
   sender_waiting_final = 1;
   if (fault_phase != NULL && strcmp(fault_phase, "metadata") == 0)
   {
      fprintf(stderr,
              "MSC UDP test interruption before recursive metadata phase\n");
      failure = MSC_EXIT_NETWORK;
      goto out;
   }
   if (manifest_punch_zero_holes(manifest) != 0 ||
       manifest_apply_modes(manifest, AD) != 0 ||
       manifest_sync_durable(manifest, AD->destfile) != 0)
   {
      failure = MSC_EXIT_DESTINATION;
      goto out;
   }
   if (!AD->keep_checkpoint &&
       (unlink(AD->checkpoint_path) != 0 ||
        msc_fsync_parent(AD->checkpoint_path) != 0))
   {
      failure = MSC_EXIT_DESTINATION;
      goto out;
   }
   if (udp_send_durable_ack(
          msc_udp_receiver_fd(session), 0, total) != 0)
      failure = MSC_EXIT_NETWORK;
   goto out;

ready_failure:
   {
      uint32_t ready = htonl((uint32_t)failure);
      (void)msc_udp_send_all(msc_udp_receiver_fd(session),
                             &ready, sizeof(ready));
   }
out:
   if (failure != 0 && sender_waiting_final)
      (void)udp_send_durable_ack(
         msc_udp_receiver_fd(session), failure, total);
   table_close(&table);
   if (manifest != NULL) manifest_destroy(manifest);
   msc_checkpoint_destroy(&checkpoint);
   if (failure != 0 && AD->checkpoint_path != NULL)
   {
      struct stat checkpoint_stat;
      if (lstat(AD->checkpoint_path, &checkpoint_stat) == 0)
         fprintf(stderr,
                 "MSC UDP recursive transfer interrupted; checkpoint retained at %s\n",
                 AD->checkpoint_path);
   }
   if (msc_cancelled())
      return msc_cancel_exit_code();
   return failure;
}

static int udp_publish_destination(const char *partial, const char *destination,
                                   int force)
{
   int rc;
   if (force)
      rc = rename(partial, destination);
   else
      rc = link(partial, destination) == 0 && unlink(partial) == 0 ? 0 : -1;
   return rc;
}

void udp_send(struct argdata *AD)
{
   struct msc_udp_config cfg;
   void *session;
   uint64_t bytes = 0;

   if (AD->checkpoint_path != NULL &&
       (AD->src_offset != 0 || AD->dst_offset != 0 || AD->xferlen != 0))
      udp_exit_with(MSC_EXIT_CLI,
                    "checkpointed UDP does not support source/destination offsets or a transfer-length slice");

   apply_transport_options(AD);
   memset(&cfg, 0, sizeof(cfg));
   cfg.receiver_host = AD->interface_host != NULL
                    ? AD->interface_host : AD->remote_machines[0];
   cfg.control_port = AD->finalport;
   cfg.flow_count = AD->numstreams;
   cfg.payload_size = udp_payload_size(AD);
   cfg.source_fd = -1;
   cfg.dest_fd = -1;
   cfg.verify_checksum = !AD->no_internal_checksum;
   cfg.multi = AD->parentsets > 1;
   cfg.my_ost_start = AD->my_ost_start;
   cfg.my_ost_count = AD->my_ost_count;
   cfg.dest_stripe_count = AD->dest_stripe_count;
   cfg.cancel_flag = &msc_cancel_signal;

   /* stdio modes: no rendezvous and no listener anywhere -- control rides the
    * ssh session the launcher already opened, so wrap those pipes instead of
    * dialing cfg.control_port. */
   if (msc_udp_ctl_is_stdio(msc_udp_ctl_mode()))
   {
      if (AD->udp_ctl_rfd < 0 || AD->udp_ctl_wfd < 0)
         udp_exit_with(MSC_EXIT_CLI,
                       "MSC_UDP_CTL=stdio|stdio1 needs the SSH launch path "
                       "(control rides its session; there is no port to dial)");
      cfg.ctl_fd = msc_udp_control_channel_pipe(AD->udp_ctl_rfd, AD->udp_ctl_wfd);
      if (cfg.ctl_fd < 0)
         udp_exit_with(MSC_EXIT_NETWORK, "could not wrap the SSH control stream");
   }

   session = msc_udp_sender_open(&cfg);
   if (session == NULL)
   {
      /* cfg.error_code carries the receiver's real reason when it failed
       * before opening its own session and reported why (e.g. destination
       * already exists) -- see msc_udp_send_transfer_error(). Otherwise this
       * is a bare closed/refused connection, and MSC_EXIT_NETWORK (the only
       * auto-retryable class) is the honest fallback. */
      if (udp_exit_status_valid(cfg.error_code))
         udp_exit_with(cfg.error_code, "sender could not open session");
      udp_fail("sender could not open session");
   }
   if (udp_configure_control(msc_udp_sender_fd(session), AD->stall_timeout_ms) != 0)
   {
      udp_abort_sender_session(session);
      udp_exit_with(MSC_EXIT_INTERNAL, "could not configure UDP control-session timeout");
   }

   if (AD->recursive)
   {
      int failure = AD->checkpoint_path != NULL
         ? run_send_tree_resume(session, AD, &bytes)
         : run_send_tree(session, AD, &bytes);
      if (!udp_exit_status_valid(failure) && failure != 0)
         failure = MSC_EXIT_NETWORK;
      if (failure != 0)
      {
         udp_abort_sender_session(session);
         udp_exit_with(failure, "recursive sender failed");
      }
   }
   else
   {
      off_t base = AD->src_offset != 0 ? AD->src_offset : AD->lastgood;
      cfg.source_path = AD->sourcefile;
      if (AD->checkpoint_path != NULL)
      {
         struct stat st;
         uint64_t total, reused = 0, position, chunk_size = udp_checkpoint_chunk();
         volatile uint64_t current = 0;
         struct msc_progress progress;
         int sourcefd = open(AD->sourcefile, O_RDONLY | O_NOFOLLOW);
         int failure = 0;
         if (sourcefd < 0 || fstat(sourcefd, &st) != 0 || !S_ISREG(st.st_mode))
         {
            if (sourcefd >= 0) close(sourcefd);
            udp_abort_sender_session(session);
            udp_exit_with(MSC_EXIT_SOURCE, "cannot open resumable source");
         }
         total = (uint64_t)st.st_size;
         failure = udp_resume_sender_hello(msc_udp_sender_fd(session), AD,
                                           total, sourcefd, &st, &reused);
         if (failure != 0)
         {
            close(sourcefd);
            udp_abort_sender_session(session);
            udp_exit_with(failure, "checkpoint negotiation failed");
         }
         AD->childinfo->reused = (long long)reused;
         msc_progress_start(&progress, AD, &current, reused, total);
         cfg.source_fd = sourcefd;
         /* MSC performs the optional per-chunk content handshake so checksum
          * mismatch has an unambiguous MSC_EXIT_INTEGRITY classification. */
         cfg.verify_checksum = 0;
         for (position = reused; position < total; )
         {
            uint64_t remaining = total - position;
            uint64_t expected;
            struct stat current_stat;
            if (fstat(sourcefd, &current_stat) != 0 ||
                !udp_same_source(&st, &current_stat))
            { failure = MSC_EXIT_SOURCE; break; }
            cfg.src_offset = base + (off_t)position;
            cfg.xferlen = remaining < chunk_size ? remaining : chunk_size;
            expected = position + cfg.xferlen;
            if (msc_udp_sender_send(session, &cfg) != 0 ||
                cfg.bytes_done != cfg.xferlen)
            {
               failure = udp_exit_status_valid(cfg.error_code)
                  ? cfg.error_code : MSC_EXIT_NETWORK;
               break;
            }
            if (fstat(sourcefd, &current_stat) != 0 ||
                !udp_same_source(&st, &current_stat))
               failure = MSC_EXIT_SOURCE;
            if (!AD->no_internal_checksum)
            {
               failure = udp_sender_verify_chunk(
                  msc_udp_sender_fd(session), sourcefd, cfg.src_offset,
                  expected, cfg.xferlen, failure);
               if (failure != 0) break;
            }
            else if (failure != 0) break;
            failure = udp_receive_durable_ack(msc_udp_sender_fd(session), expected);
            if (failure != 0) break;
            bytes += cfg.bytes_done;
            __atomic_store_n(&current, bytes, __ATOMIC_RELAXED);
            position = expected;
         }
         close(sourcefd);
         msc_progress_finish(&progress, failure == 0,
                             "network/transport or storage limited (timing separation unavailable)");
         if (failure != 0)
         {
            udp_abort_sender_session(session);
            udp_exit_with(failure, "resumable sender session failed");
         }
      }
      else
      {
         struct stat st;
         volatile uint64_t current = 0;
         struct msc_progress progress;
         uint64_t total = 0;
         if (stat(AD->sourcefile, &st) == 0 && st.st_size > base)
            total = (uint64_t)(st.st_size - base);
         if (AD->xferlen > 0 && (uint64_t)AD->xferlen < total) total = (uint64_t)AD->xferlen;
         msc_progress_start(&progress, AD, &current, 0, total);
         cfg.src_offset = base;
         cfg.xferlen = AD->xferlen > 0 ? (uint64_t)AD->xferlen : 0;
         if (msc_udp_sender_send(session, &cfg) != 0)
            udp_exit_with(
               udp_exit_status_valid(cfg.error_code)
                  ? cfg.error_code : MSC_EXIT_NETWORK,
               "sender failed");
         bytes = cfg.bytes_done;
         __atomic_store_n(&current, bytes, __ATOMIC_RELAXED);
         msc_progress_finish(&progress, 1,
                             "network/transport or storage limited (timing separation unavailable)");
      }
   }

   msc_udp_sender_close(session);
   AD->childinfo->xfercount = (long long)bytes;
}

void udp_receive(struct argdata *AD, int controlfd)
{
   struct msc_udp_config cfg;
   void *session;
   char *temp = NULL;
   int multi = AD->parentsets > 1;
   int rc;
   int resume_tempfd = -1;
   struct msc_checkpoint checkpoint;
   uint64_t resume_total = 0, resume_reused = 0;

   apply_transport_options(AD);
   msc_checkpoint_init(&checkpoint);
   memset(&cfg, 0, sizeof(cfg));
   cfg.flow_count = AD->numstreams;
   cfg.payload_size = udp_payload_size(AD);
   cfg.source_fd = -1;
   cfg.dest_fd = -1;
   cfg.verify_checksum = !AD->no_internal_checksum;
   cfg.force = AD->force;
   cfg.multi = multi;
   cfg.my_ost_start = AD->my_ost_start;
   cfg.my_ost_count = AD->my_ost_count;
   cfg.dest_stripe_count = AD->dest_stripe_count;
   cfg.cancel_flag = &msc_cancel_signal;

   if (!AD->recursive && multi)
   {
      if (AD->outfileid == NULL) udp_fail("shared destination was not opened");
      cfg.dest_fd = fileno(AD->outfileid);
      cfg.dst_offset = AD->dst_offset;
   }
   else if (!AD->recursive && AD->checkpoint_path == NULL)
   {
      temp = make_temp_destination(AD->destfile, AD->force);
      if (temp == NULL)
      {
         /* The sender is already blocked reading the port list -- the first
          * thing msc_udp_sender_open() does -- since controlfd is connected
          * by now. Tell it why before exiting, or it just sees the socket
          * close and reports a misleading "bad port list from receiver"
          * instead of this destination-exists reason. Best-effort: fall
          * through to exit regardless of whether the send succeeds. */
         msc_udp_send_transfer_error(controlfd, MSC_EXIT_DESTINATION);
         udp_exit_with(MSC_EXIT_DESTINATION, "could not prepare destination");
      }
      cfg.dest_path = temp;
      cfg.final_dest_path = AD->destfile;
      cfg.dst_offset = AD->dst_offset;
   }

   session = msc_udp_receiver_open(&cfg, controlfd);
   if (session == NULL)
   {
      if (temp != NULL) unlink(temp);
      free(temp);
      udp_fail("receiver could not open session");
   }
   if (udp_configure_control(msc_udp_receiver_fd(session), AD->stall_timeout_ms) != 0)
   {
      udp_abort_receiver_session(session);
      if (temp != NULL) unlink(temp);
      free(temp);
      udp_exit_with(MSC_EXIT_INTERNAL, "could not configure UDP control-session timeout");
   }

   if (!AD->recursive && !multi && AD->checkpoint_path != NULL)
   {
      uint64_t position, checkpoint_count = 0;
      const char *fault = getenv("MSC_TEST_INTERRUPT_AFTER_CHECKPOINTS");
      const char *signal_fault = getenv("MSC_TEST_SIGNAL_AFTER_CHECKPOINTS");
      const char *midchunk_fault = getenv("MSC_TEST_FAIL_DURING_CHUNK_MS");
      uint64_t midchunk_after = getenv("MSC_TEST_FAIL_DURING_CHUNK_AFTER_CHECKPOINTS") != NULL
         ? strtoull(getenv("MSC_TEST_FAIL_DURING_CHUNK_AFTER_CHECKPOINTS"), NULL, 10) : 0;
      int failure = udp_resume_receiver_hello(msc_udp_receiver_fd(session), AD,
                                              &checkpoint, &temp, &resume_tempfd,
                                              &resume_total, &resume_reused);
      if (failure != 0)
      {
         udp_abort_receiver_session(session);
         free(temp); msc_checkpoint_destroy(&checkpoint);
         udp_exit_with(failure, "receiver checkpoint negotiation failed");
      }
      cfg.dest_fd = resume_tempfd; cfg.dest_path = NULL; cfg.final_dest_path = NULL;
      cfg.force = 0; cfg.verify_checksum = 0;
      for (position = resume_reused; position < resume_total; )
      {
         uint64_t remaining = resume_total - position;
         uint64_t expected, old_end = checkpoint.range_count ? checkpoint.ranges[0].end : 0;
         uint32_t old_count = checkpoint.range_count;
         struct udp_test_timer timer;
         cfg.dst_offset = AD->dst_offset + (off_t)position;
         cfg.xferlen = remaining < udp_checkpoint_chunk() ? remaining : udp_checkpoint_chunk();
         expected = position + cfg.xferlen;
         memset(&timer, 0, sizeof(timer));
         if (midchunk_fault != NULL && checkpoint_count >= midchunk_after)
            udp_test_timer_start(&timer, strtoull(midchunk_fault, NULL, 10));
         rc = msc_udp_receiver_recv(session, &cfg);
         if (udp_test_timer_finish(&timer))
         {
            fprintf(stderr, "MSC UDP test interruption during an uncheckpointed chunk\n");
            failure = MSC_EXIT_NETWORK;
            goto resumable_failure;
         }
         if (rc != 0 || cfg.bytes_done != cfg.xferlen)
         {
            failure = udp_exit_status_valid(cfg.error_code)
               ? cfg.error_code : MSC_EXIT_NETWORK;
            goto resumable_failure;
         }
         if (!AD->no_internal_checksum)
         {
            failure = udp_receiver_verify_chunk(msc_udp_receiver_fd(session),
                                                 resume_tempfd, cfg.dst_offset,
                                                 expected, cfg.xferlen);
            if (failure != 0) goto resumable_failure;
         }
         /* Receiver-owned durability barrier: data first, then the range and
          * atomic checkpoint publication.  Never leave the in-memory range
          * ahead of the last valid on-disk checkpoint on an error path. */
         if (fsync(resume_tempfd) != 0 ||
             msc_checkpoint_add_range(&checkpoint, 0, expected) != 0 ||
             msc_checkpoint_capture_destination(&checkpoint, temp) != 0 ||
             msc_checkpoint_write_atomic(AD->checkpoint_path, &checkpoint) != 0)
         {
            checkpoint.range_count = old_count;
            if (old_count != 0) checkpoint.ranges[0].end = old_end;
            failure = MSC_EXIT_DESTINATION;
            (void)udp_send_durable_ack(msc_udp_receiver_fd(session), failure, old_end);
            goto resumable_failure;
         }
         resume_reused = expected;
         checkpoint_count++;
         if (getenv("MSC_TEST_CHECKPOINT_MARKER") != NULL)
         {
            int markerfd = open(getenv("MSC_TEST_CHECKPOINT_MARKER"),
                                O_WRONLY | O_CREAT | O_TRUNC, 0600);
            if (markerfd >= 0) close(markerfd);
         }
         if (fault != NULL && checkpoint_count >= strtoull(fault, NULL, 10))
         {
            fprintf(stderr, "MSC UDP test interruption after durable checkpoint\n");
            failure = MSC_EXIT_NETWORK;
            goto resumable_failure;
         }
         if (signal_fault != NULL && checkpoint_count >= strtoull(signal_fault, NULL, 10))
         {
            raise(getenv("MSC_TEST_SIGNAL_TERM") != NULL ? SIGTERM : SIGINT);
            failure = msc_cancel_exit_code();
            goto resumable_failure;
         }
         if (udp_send_durable_ack(msc_udp_receiver_fd(session), 0, expected) != 0)
         { failure = MSC_EXIT_NETWORK; goto resumable_failure; }
         position = expected;
      }
      if (resume_total != 0 &&
          !msc_checkpoint_contains(&checkpoint, 0, resume_total))
      {
         failure = MSC_EXIT_INTERNAL;
         goto resumable_failure;
      }
      msc_udp_receiver_close(session); session = NULL;
      if (udp_publish_destination(temp, AD->destfile, AD->force) != 0 ||
          udp_fsync_parent(AD->destfile) != 0)
      {
         close(resume_tempfd); resume_tempfd = -1;
         free(temp); msc_checkpoint_destroy(&checkpoint);
         udp_exit_with(MSC_EXIT_DESTINATION, "could not atomically publish durable destination");
      }
      if (!AD->keep_checkpoint &&
          (unlink(AD->checkpoint_path) != 0 || udp_fsync_parent(AD->checkpoint_path) != 0))
      {
         close(resume_tempfd); resume_tempfd = -1;
         free(temp); msc_checkpoint_destroy(&checkpoint);
         udp_exit_with(MSC_EXIT_DESTINATION, "published UDP destination but could not remove checkpoint durably");
      }
      close(resume_tempfd); resume_tempfd = -1;
      free(temp); temp = NULL; msc_checkpoint_destroy(&checkpoint);
      return;
resumable_failure:
      if (session != NULL) udp_abort_receiver_session(session);
      if (resume_tempfd >= 0)
      {
         /* MSC UDP may have written beyond the last durable chunk. Roll that
          * uncheckpointed suffix back before refreshing destination identity. */
         if (ftruncate(resume_tempfd, (off_t)resume_reused) != 0 ||
             fsync(resume_tempfd) != 0 ||
             msc_checkpoint_capture_destination(&checkpoint, temp) != 0 ||
             msc_checkpoint_write_atomic(AD->checkpoint_path, &checkpoint) != 0)
            failure = MSC_EXIT_DESTINATION;
         close(resume_tempfd);
      }
      fprintf(stderr, "MSC UDP transfer interrupted; checkpoint retained at %s\n", AD->checkpoint_path);
      free(temp); msc_checkpoint_destroy(&checkpoint);
      if (msc_cancelled()) exit(msc_cancel_exit_code());
      exit(failure);
   }

   rc = AD->recursive
      ? (AD->checkpoint_path != NULL
           ? run_recv_tree_resume(session, AD)
           : run_recv_tree(session, AD))
      : msc_udp_receiver_recv(session, &cfg);
   msc_udp_receiver_close(session);
   if (rc != 0 && temp != NULL) unlink(temp);
   free(temp);
   msc_checkpoint_destroy(&checkpoint);
   if (rc != 0)
   {
      if (AD->recursive)
         udp_exit_with(
            udp_exit_status_valid(rc) ? rc : MSC_EXIT_NETWORK,
            "recursive receiver failed");
      udp_exit_with(
         udp_exit_status_valid(cfg.error_code)
            ? cfg.error_code : MSC_EXIT_NETWORK,
         "receiver failed");
   }
}
