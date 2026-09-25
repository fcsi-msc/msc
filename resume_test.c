#define _GNU_SOURCE
#include "msc.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

int msc_debug;
FILE *msc_debugout;
extern int *sockfd;
extern void xferdata_segmented(FILE *, struct argdata *);
static int last_receiver_exit;

static off_t test_offset(const char *name)
{
   const char *value = getenv(name);
   return value == NULL ? 0 : (off_t)strtoll(value, NULL, 10);
}

static int wait_ok(pid_t pid)
{
   int status;
   return waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

static int files_equal(const char *a, const char *b)
{
   int fa = open(a, O_RDONLY), fb = open(b, O_RDONLY);
   char ba[65536], bb[65536];
   ssize_t na, nb;
   if (fa < 0 || fb < 0) return 0;
   do {
      na = read(fa, ba, sizeof(ba)); nb = read(fb, bb, sizeof(bb));
      if (na != nb || na < 0 || (na && memcmp(ba, bb, (size_t)na) != 0))
      { close(fa); close(fb); return 0; }
   } while (na != 0);
   close(fa); close(fb); return 1;
}

static int transfer_once(const char *source, const char *dest, const char *checkpoint,
                         int resume, int interrupt_after, long long *sent,
                         long long *reused)
{
   enum { streams = 4, segment = 65536 };
   int pairs[streams][2], sendfds[streams], recvfds[streams], i;
   pid_t receiver, sender;
   for (i = 0; i < streams; i++)
   {
      if (socketpair(AF_UNIX, SOCK_STREAM, 0, pairs[i]) != 0) return -1;
      sendfds[i] = pairs[i][0]; recvfds[i] = pairs[i][1];
   }
   receiver = fork();
   if (receiver == 0)
   {
      struct argdata ad;
      memset(&ad, 0, sizeof(ad)); ad.destfile = (char *)dest;
      ad.checkpoint_path = (char *)checkpoint; ad.resume = resume;
      ad.force = 1; ad.numstreams = streams; ad.packetsize = segment;
      ad.dst_offset = test_offset("MSC_TEST_DST_OFFSET");
      ad.xferlen = test_offset("MSC_TEST_XFER_LENGTH");
      ad.keep_checkpoint = getenv("MSC_TEST_KEEP_CHECKPOINT") != NULL;
      for (i = 0; i < streams; i++) close(sendfds[i]);
      if (interrupt_after > 0)
      {
         char value[32]; snprintf(value, sizeof(value), "%d", interrupt_after);
         setenv("MSC_TEST_INTERRUPT_AFTER_SEGMENTS", value, 1);
      }
      else if (interrupt_after == -SIGINT || interrupt_after == -SIGTERM)
      {
         msc_install_signal_handlers();
         setenv("MSC_TEST_SIGNAL_AFTER_SEGMENTS", "5", 1);
         if (interrupt_after == -SIGTERM) setenv("MSC_TEST_SIGNAL_TERM", "1", 1);
      }
      readsocket_segmented_resume(streams, recvfds, &ad);
      _exit(0);
   }
   sender = fork();
   if (sender == 0)
   {
      struct argdata ad; struct jobinfo job; FILE *input;
      memset(&ad, 0, sizeof(ad)); memset(&job, 0, sizeof(job));
      ad.sourcefile = (char *)source; ad.destfile = (char *)dest;
      ad.checkpoint_path = (char *)checkpoint; ad.resume = resume;
      ad.numstreams = streams; ad.packetsize = segment; ad.childinfo = &job;
      ad.src_offset = test_offset("MSC_TEST_SRC_OFFSET");
      ad.dst_offset = test_offset("MSC_TEST_DST_OFFSET");
      ad.xferlen = test_offset("MSC_TEST_XFER_LENGTH");
      if (getenv("MSC_TEST_PROGRESS_ALWAYS") != NULL)
      { ad.progress_mode = MSC_PROGRESS_ALWAYS; ad.progress_interval_ms = 10; }
      for (i = 0; i < streams; i++) close(recvfds[i]);
      sockfd = sendfds; input = fopen(source, "rb"); if (input == NULL) _exit(1);
      xferdata_segmented(input, &ad); fclose(input);
      if (sent != NULL || reused != NULL)
      {
         int out = open("/tmp/msc-resume-counts", O_WRONLY | O_CREAT | O_TRUNC, 0600);
         if (out >= 0) { dprintf(out, "%lld %lld\n", job.xfercount, job.reused); close(out); }
      }
      _exit(0);
   }
   for (i = 0; i < streams; i++) { close(sendfds[i]); close(recvfds[i]); }
   {
      int sender_ok = wait_ok(sender), receiver_status, receiver_ok;
      receiver_ok = waitpid(receiver, &receiver_status, 0) == receiver &&
                    WIFEXITED(receiver_status) && WEXITSTATUS(receiver_status) == 0;
      last_receiver_exit = WIFEXITED(receiver_status) ? WEXITSTATUS(receiver_status) : -1;
      if (sender_ok && receiver_ok && (sent != NULL || reused != NULL))
      {
         FILE *in = fopen("/tmp/msc-resume-counts", "r");
         if (in != NULL)
         {
            if (fscanf(in, "%lld %lld", sent, reused) != 2)
            { *sent = -1; *reused = -1; }
            fclose(in);
         }
      }
      return sender_ok && receiver_ok ? 0 : -1;
   }
}

static int checkpoint_format_test(const char *path, const char *source, const char *dest)
{
   struct msc_checkpoint a, b;
   char error[256];
   int fd;
   static const unsigned char abc_sha256[MSC_SHA256_BYTES] = {
      0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea,
      0x41, 0x41, 0x40, 0xde, 0x5d, 0xae, 0x22, 0x23,
      0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c,
      0xb4, 0x10, 0xff, 0x61, 0xf2, 0x00, 0x15, 0xad
   };
   unsigned char digest[MSC_SHA256_BYTES];
   struct msc_file_digest_context digest_context;
   msc_checkpoint_init(&a); msc_checkpoint_init(&b);
   fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
   digest_context.fd = fd; digest_context.base = 0;
   if (fd < 0 || write(fd, "abc", 3) != 3 ||
       msc_resume_file_digest(&digest_context, 0, 3, digest) != 0 ||
       memcmp(digest, abc_sha256, sizeof(digest)) != 0)
   { if (fd >= 0) close(fd); return -1; }
   close(fd);
   if (msc_checkpoint_capture_source(&a, source, dest, 65536, 0) != 0 ||
       msc_checkpoint_add_range(&a, 0, 10) != 0 ||
       msc_checkpoint_add_range(&a, 20, 30) != 0 ||
       msc_checkpoint_add_range(&a, 10, 20) != 0 ||
       a.range_count != 1 || a.ranges[0].end != 30 ||
       msc_checkpoint_write_atomic(path, &a) != 0 ||
       msc_checkpoint_read(path, &b, error, sizeof(error)) != 0 ||
       b.range_count != 1 || b.ranges[0].end != 30)
      return -1;
   fd = open(path, O_WRONLY); if (fd < 0) return -1;
   if (pwrite(fd, "X", 1, 20) != 1) return -1;
   close(fd);
   msc_checkpoint_destroy(&b);
   if (msc_checkpoint_read(path, &b, error, sizeof(error)) == 0) return -1;
   unlink(path);
   if (msc_checkpoint_read(path, &b, error, sizeof(error)) == 0) return -1;
   if (msc_checkpoint_write_atomic(path, &a) != 0) return -1;
   fd = open(path, O_WRONLY); if (fd < 0) return -1;
   if (pwrite(fd, "\0\0\0\3", 4, 4) != 4) return -1;
   close(fd);
   if (msc_checkpoint_read(path, &b, error, sizeof(error)) != -2) return -1;
   if (truncate(path, 12) != 0 || msc_checkpoint_read(path, &b, error, sizeof(error)) == 0)
      return -1;
   msc_checkpoint_destroy(&a); msc_checkpoint_destroy(&b);
   return 0;
}

static int flip_claimed_byte(const char *checkpoint_path, const char *partial,
                             off_t *position_out, unsigned char *original_out)
{
   struct msc_checkpoint checkpoint_data;
   char error[256];
   int fd, rc = -1;
   off_t position;
   unsigned char changed;
   msc_checkpoint_init(&checkpoint_data);
   if (msc_checkpoint_read(checkpoint_path, &checkpoint_data,
                           error, sizeof(error)) != 0 ||
       checkpoint_data.range_count == 0)
      goto out;
   position = (off_t)(checkpoint_data.destination_offset +
                      checkpoint_data.ranges[0].start);
   fd = open(partial, O_RDWR);
   if (fd < 0) goto out;
   if (pread(fd, original_out, 1, position) == 1)
   {
      changed = (unsigned char)(*original_out ^ 0xffU);
      if (pwrite(fd, &changed, 1, position) == 1 && fsync(fd) == 0)
      { *position_out = position; rc = 0; }
   }
   close(fd);
out:
   msc_checkpoint_destroy(&checkpoint_data);
   return rc;
}

static int compare_regions(const char *left, off_t left_offset,
                           const char *right, off_t right_offset,
                           off_t length)
{
   int a = open(left, O_RDONLY), b = open(right, O_RDONLY);
   unsigned char abuf[65536], bbuf[65536];
   off_t done = 0;
   if (a < 0 || b < 0) { if (a >= 0) close(a); if (b >= 0) close(b); return 0; }
   while (done < length)
   {
      size_t wanted = (size_t)(length - done);
      if (wanted > sizeof(abuf)) wanted = sizeof(abuf);
      if (pread(a, abuf, wanted, left_offset + done) != (ssize_t)wanted ||
          pread(b, bbuf, wanted, right_offset + done) != (ssize_t)wanted ||
          memcmp(abuf, bbuf, wanted) != 0)
      { close(a); close(b); return 0; }
      done += (off_t)wanted;
   }
   close(a); close(b); return 1;
}

static int region_is_zero(const char *path, off_t offset, off_t length)
{
   int fd = open(path, O_RDONLY);
   unsigned char buffer[65536];
   off_t done = 0;
   if (fd < 0) return 0;
   while (done < length)
   {
      size_t wanted = (size_t)(length - done), i;
      if (wanted > sizeof(buffer)) wanted = sizeof(buffer);
      if (pread(fd, buffer, wanted, offset + done) != (ssize_t)wanted)
      { close(fd); return 0; }
      for (i = 0; i < wanted; i++)
         if (buffer[i] != 0) { close(fd); return 0; }
      done += (off_t)wanted;
   }
   close(fd); return 1;
}

static int geometry_test(const char *source, const char *base)
{
   char *slice_dest = NULL, *slice_cp = NULL;
   char *offset_dest = NULL, *offset_cp = NULL;
   struct stat st;
   int rc = -1;
   if (asprintf(&slice_dest, "%s.slice", base) < 0 ||
       asprintf(&slice_cp, "%s.slice.cp", base) < 0 ||
       asprintf(&offset_dest, "%s.offset", base) < 0 ||
       asprintf(&offset_cp, "%s.offset.cp", base) < 0)
      goto out;
   unlink(slice_dest); unlink(slice_cp);
   unlink(offset_dest); unlink(offset_cp);
   setenv("MSC_TEST_XFER_LENGTH", "1048576", 1);
   setenv("MSC_TEST_SRC_OFFSET", "0", 1);
   setenv("MSC_TEST_DST_OFFSET", "0", 1);
   if (transfer_once(source, slice_dest, slice_cp, 0, 5, NULL, NULL) == 0)
      goto out;
   setenv("MSC_TEST_SRC_OFFSET", "1048576", 1);
   if (transfer_once(source, slice_dest, slice_cp, 1, 0, NULL, NULL) == 0 ||
       last_receiver_exit != MSC_EXIT_INCOMPATIBLE)
      goto out;
   setenv("MSC_TEST_SRC_OFFSET", "0", 1);
   if (transfer_once(source, slice_dest, slice_cp, 1, 0, NULL, NULL) != 0 ||
       !compare_regions(source, 0, slice_dest, 0, 1048576))
      goto out;

   setenv("MSC_TEST_SRC_OFFSET", "1048576", 1);
   setenv("MSC_TEST_DST_OFFSET", "65536", 1);
   if (transfer_once(source, offset_dest, offset_cp, 0, 0, NULL, NULL) != 0 ||
       stat(offset_dest, &st) != 0 || st.st_size != 65536 + 1048576 ||
       !region_is_zero(offset_dest, 0, 65536) ||
       !compare_regions(source, 1048576, offset_dest, 65536, 1048576))
      goto out;
   rc = 0;
out:
   unsetenv("MSC_TEST_XFER_LENGTH");
   unsetenv("MSC_TEST_SRC_OFFSET");
   unsetenv("MSC_TEST_DST_OFFSET");
   if (slice_dest != NULL) unlink(slice_dest);
   if (slice_cp != NULL) unlink(slice_cp);
   if (offset_dest != NULL) unlink(offset_dest);
   if (offset_cp != NULL) unlink(offset_cp);
   free(slice_dest); free(slice_cp); free(offset_dest); free(offset_cp);
   return rc;
}

/* File scope, not main() locals: forked transfer children exit deep inside
 * receiver code with these heap blocks live, and valgrind's child leak scan
 * then reports main's frame pointers as definitely lost.  Data-segment
 * references keep every child's leak check clean. */
static char *checkpoint, *formatcp, *partial, *missingdest, *keepdest,
            *keepcp, *sigdest, *sigcp;

int main(int argc, char **argv)
{
   long long sent = -1, reused = -1;
   struct stat source_stat;
   struct timespec changed[2], restored[2];
   off_t corrupted_position = 0;
   unsigned char original_partial_byte = 0;
   int partial_fd;
   if (argc != 3) { fprintf(stderr, "usage: resume_test source destination\n"); return 2; }
   if (asprintf(&checkpoint, "%s.cp", argv[2]) < 0 ||
       asprintf(&formatcp, "%s.format", argv[2]) < 0 ||
       asprintf(&partial, "%s.msc-part", argv[2]) < 0 ||
       asprintf(&missingdest, "%s.missing", argv[2]) < 0 ||
       asprintf(&keepdest, "%s.keep", argv[2]) < 0 ||
       asprintf(&keepcp, "%s.keep.cp", argv[2]) < 0 ||
       asprintf(&sigdest, "%s.signal", argv[2]) < 0 ||
       asprintf(&sigcp, "%s.signal.cp", argv[2]) < 0) return 1;
   unlink(argv[2]); unlink(checkpoint); unlink(partial);
   if (transfer_once(argv[1], argv[2], checkpoint, 0, 5, NULL, NULL) == 0)
   { fprintf(stderr, "fault-injected transfer unexpectedly succeeded\n"); return 1; }
   if (last_receiver_exit != MSC_EXIT_NETWORK)
   { fprintf(stderr, "post-negotiation interruption was misclassified (exit %d)\n", last_receiver_exit); return 1; }
   if (access(checkpoint, F_OK) != 0 || access(argv[2], F_OK) == 0)
   { fprintf(stderr, "interruption did not preserve checkpoint or published output\n"); return 1; }

   /* Source and destination mutations must reject explicit resume without
    * silently restarting or publishing output. */
   if (stat(argv[1], &source_stat) != 0) return 1;
   restored[0] = source_stat.st_atim; restored[1] = source_stat.st_mtim;
   changed[0] = restored[0]; changed[1] = restored[1]; changed[1].tv_nsec ^= 1;
   if (utimensat(AT_FDCWD, argv[1], changed, 0) != 0 ||
       transfer_once(argv[1], argv[2], checkpoint, 1, 0, NULL, NULL) == 0 ||
       access(argv[2], F_OK) == 0 || utimensat(AT_FDCWD, argv[1], restored, 0) != 0)
   { fprintf(stderr, "source mutation was not rejected safely\n"); return 1; }
   /* Same inode and size are not content identity.  Corrupt one byte that the
    * checkpoint claims, require an integrity rejection, then restore it so the
    * remaining recovery cases exercise the original durable state. */
   if (flip_claimed_byte(checkpoint, partial, &corrupted_position,
                         &original_partial_byte) != 0 ||
       transfer_once(argv[1], argv[2], checkpoint, 1, 0, NULL, NULL) == 0 ||
       last_receiver_exit != MSC_EXIT_INTEGRITY ||
       access(argv[2], F_OK) == 0)
   { fprintf(stderr, "same-size destination corruption was not rejected\n"); return 1; }
   partial_fd = open(partial, O_RDWR);
   if (partial_fd < 0 ||
       pwrite(partial_fd, &original_partial_byte, 1, corrupted_position) != 1 ||
       fsync(partial_fd) != 0)
   { if (partial_fd >= 0) close(partial_fd); return 1; }
   close(partial_fd);
   /* Identity/size changes are unsafe and must still be rejected. */
   if (truncate(partial, source_stat.st_size - 1) != 0) return 1;
   if (transfer_once(argv[1], argv[2], checkpoint, 1, 0, NULL, NULL) == 0 ||
       access(argv[2], F_OK) == 0)
   { fprintf(stderr, "destination mutation was not rejected safely\n"); return 1; }
   if (transfer_once(argv[1], argv[2], checkpoint, 0, 5, NULL, NULL) == 0)
   { fprintf(stderr, "second fault-injected transfer unexpectedly succeeded\n"); return 1; }

   /* A checkpoint can lag writes to ranges it does not claim.  Those writes
    * legitimately advance the preallocated partial file's mtime and must not
    * make the durable ranges unusable; missing ranges are overwritten. */
   if (stat(partial, &source_stat) != 0) return 1;
   changed[0] = source_stat.st_atim; changed[1] = source_stat.st_mtim;
   if (++changed[1].tv_nsec >= 1000000000L)
   { changed[1].tv_sec++; changed[1].tv_nsec = 0; }
   if (utimensat(AT_FDCWD, partial, changed, 0) != 0)
   { fprintf(stderr, "could not simulate post-checkpoint partial write\n"); return 1; }
   if (transfer_once(argv[1], argv[2], checkpoint, 1, 0, &sent, &reused) != 0 ||
       !files_equal(argv[1], argv[2]) || access(checkpoint, F_OK) == 0 ||
       reused <= 0 || sent < 0)
   { fprintf(stderr, "resume/integrity/accounting test failed sent=%lld reused=%lld\n", sent, reused); return 1; }
   if (checkpoint_format_test(formatcp, argv[1], argv[2]) != 0)
   { fprintf(stderr, "checkpoint atomic/integrity/version test failed\n"); return 1; }
   if (geometry_test(argv[1], argv[2]) != 0)
   { fprintf(stderr, "checkpoint slice-geometry/offset test failed\n"); return 1; }

   unlink(missingdest); unlink(keepdest); unlink(keepcp);
   if (transfer_once(argv[1], missingdest, "/tmp/msc-definitely-missing-checkpoint",
                     1, 0, NULL, NULL) == 0)
   { fprintf(stderr, "missing checkpoint was not rejected\n"); return 1; }
   setenv("MSC_TEST_KEEP_CHECKPOINT", "1", 1);
   if (transfer_once(argv[1], keepdest, keepcp, 0, 0, NULL, NULL) != 0 ||
       access(keepcp, F_OK) != 0 || !files_equal(argv[1], keepdest))
   { fprintf(stderr, "--keep-checkpoint behavior failed\n"); return 1; }
   unsetenv("MSC_TEST_KEEP_CHECKPOINT");

   unlink(sigdest); unlink(sigcp);
   if (transfer_once(argv[1], sigdest, sigcp, 0, -SIGINT, NULL, NULL) == 0 ||
       last_receiver_exit != MSC_EXIT_SIGINT || access(sigcp, F_OK) != 0)
   { fprintf(stderr, "SIGINT cancellation contract failed (exit %d)\n", last_receiver_exit); return 1; }
   if (transfer_once(argv[1], sigdest, sigcp, 1, 0, NULL, NULL) != 0 ||
       !files_equal(argv[1], sigdest)) return 1;
   unlink(sigdest); unlink(sigcp);
   if (transfer_once(argv[1], sigdest, sigcp, 0, -SIGTERM, NULL, NULL) == 0 ||
       last_receiver_exit != MSC_EXIT_SIGTERM || access(sigcp, F_OK) != 0)
   { fprintf(stderr, "SIGTERM cancellation contract failed (exit %d)\n", last_receiver_exit); return 1; }
   if (transfer_once(argv[1], sigdest, sigcp, 1, 0, NULL, NULL) != 0 ||
       !files_equal(argv[1], sigdest)) return 1;

   unlink(formatcp); unlink(keepcp);
   free(checkpoint); free(formatcp); free(partial); free(missingdest); free(keepdest); free(keepcp);
   free(sigdest); free(sigcp);
   printf("resume tests passed: sent=%lld reused=%lld\n", sent, reused);
   return 0;
}
