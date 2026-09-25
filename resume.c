#define _GNU_SOURCE
#include "msc.h"
#include "udp_session.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define MSC_CP_MAGIC 0x4d534350U /* MSCP */
#define MSC_CP_HEADER_BYTES 160U
#define MSC_CP_MAX_PATH (1024U * 1024U)
#define MSC_SHA256_BLOCK_BYTES 64U

static void put32(unsigned char *p, uint32_t value)
{
   p[0] = (unsigned char)(value >> 24); p[1] = (unsigned char)(value >> 16);
   p[2] = (unsigned char)(value >> 8); p[3] = (unsigned char)value;
}

static void put64(unsigned char *p, uint64_t value)
{
   int i;
   for (i = 7; i >= 0; i--) { p[i] = (unsigned char)value; value >>= 8; }
}

static uint32_t get32(const unsigned char *p)
{
   return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
          ((uint32_t)p[2] << 8) | p[3];
}

static uint64_t get64(const unsigned char *p)
{
   uint64_t value = 0;
   int i;
   for (i = 0; i < 8; i++) value = (value << 8) | p[i];
   return value;
}

/* FNV-1a is an integrity checksum, not an authentication primitive.  Its job
 * here is to reject torn/truncated/accidentally edited checkpoint files. */
static uint64_t checksum64(const unsigned char *data, size_t length)
{
   uint64_t value = UINT64_C(1469598103934665603);
   size_t i;
   for (i = 0; i < length; i++) { value ^= data[i]; value *= UINT64_C(1099511628211); }
   return value;
}

/* Checkpoint metadata deliberately uses a small FNV checksum for torn-file
 * detection, but durable byte validation needs collision resistance: a sender
 * and receiver compare SHA-256 over every range before either side skips it. */
struct msc_sha256_context
{
   uint32_t state[8];
   uint64_t total_bytes;
   unsigned char block[MSC_SHA256_BLOCK_BYTES];
   size_t used;
};

static const uint32_t sha256_k[64] = {
   0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U,
   0x3956c25bU, 0x59f111f1U, 0x923f82a4U, 0xab1c5ed5U,
   0xd807aa98U, 0x12835b01U, 0x243185beU, 0x550c7dc3U,
   0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U, 0xc19bf174U,
   0xe49b69c1U, 0xefbe4786U, 0x0fc19dc6U, 0x240ca1ccU,
   0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU,
   0x983e5152U, 0xa831c66dU, 0xb00327c8U, 0xbf597fc7U,
   0xc6e00bf3U, 0xd5a79147U, 0x06ca6351U, 0x14292967U,
   0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU, 0x53380d13U,
   0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U,
   0xa2bfe8a1U, 0xa81a664bU, 0xc24b8b70U, 0xc76c51a3U,
   0xd192e819U, 0xd6990624U, 0xf40e3585U, 0x106aa070U,
   0x19a4c116U, 0x1e376c08U, 0x2748774cU, 0x34b0bcb5U,
   0x391c0cb3U, 0x4ed8aa4aU, 0x5b9cca4fU, 0x682e6ff3U,
   0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U,
   0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U
};

static uint32_t sha256_rotr(uint32_t value, unsigned int bits)
{
   return (value >> bits) | (value << (32U - bits));
}

static void sha256_transform(struct msc_sha256_context *ctx,
                             const unsigned char block[MSC_SHA256_BLOCK_BYTES])
{
   uint32_t w[64], a, b, c, d, e, f, g, h;
   unsigned int i;
   for (i = 0; i < 16; i++)
      w[i] = ((uint32_t)block[i * 4] << 24) |
             ((uint32_t)block[i * 4 + 1] << 16) |
             ((uint32_t)block[i * 4 + 2] << 8) |
             (uint32_t)block[i * 4 + 3];
   for (i = 16; i < 64; i++)
   {
      uint32_t s0 = sha256_rotr(w[i - 15], 7) ^
                    sha256_rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
      uint32_t s1 = sha256_rotr(w[i - 2], 17) ^
                    sha256_rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
      w[i] = w[i - 16] + s0 + w[i - 7] + s1;
   }
   a = ctx->state[0]; b = ctx->state[1]; c = ctx->state[2]; d = ctx->state[3];
   e = ctx->state[4]; f = ctx->state[5]; g = ctx->state[6]; h = ctx->state[7];
   for (i = 0; i < 64; i++)
   {
      uint32_t s1 = sha256_rotr(e, 6) ^ sha256_rotr(e, 11) ^ sha256_rotr(e, 25);
      uint32_t choose = (e & f) ^ ((~e) & g);
      uint32_t temp1 = h + s1 + choose + sha256_k[i] + w[i];
      uint32_t s0 = sha256_rotr(a, 2) ^ sha256_rotr(a, 13) ^ sha256_rotr(a, 22);
      uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
      uint32_t temp2 = s0 + majority;
      h = g; g = f; f = e; e = d + temp1;
      d = c; c = b; b = a; a = temp1 + temp2;
   }
   ctx->state[0] += a; ctx->state[1] += b;
   ctx->state[2] += c; ctx->state[3] += d;
   ctx->state[4] += e; ctx->state[5] += f;
   ctx->state[6] += g; ctx->state[7] += h;
}

static void sha256_init(struct msc_sha256_context *ctx)
{
   static const uint32_t initial[8] = {
      0x6a09e667U, 0xbb67ae85U, 0x3c6ef372U, 0xa54ff53aU,
      0x510e527fU, 0x9b05688cU, 0x1f83d9abU, 0x5be0cd19U
   };
   memcpy(ctx->state, initial, sizeof(initial));
   ctx->total_bytes = 0;
   ctx->used = 0;
}

static void sha256_update(struct msc_sha256_context *ctx,
                          const void *data, size_t length)
{
   const unsigned char *p = data;
   ctx->total_bytes += length;
   while (length != 0)
   {
      size_t take = MSC_SHA256_BLOCK_BYTES - ctx->used;
      if (take > length) take = length;
      memcpy(ctx->block + ctx->used, p, take);
      ctx->used += take; p += take; length -= take;
      if (ctx->used == MSC_SHA256_BLOCK_BYTES)
      {
         sha256_transform(ctx, ctx->block);
         ctx->used = 0;
      }
   }
}

static void sha256_final(struct msc_sha256_context *ctx,
                         unsigned char digest[MSC_SHA256_BYTES])
{
   uint64_t bits = ctx->total_bytes * UINT64_C(8);
   unsigned int i;
   ctx->block[ctx->used++] = 0x80;
   if (ctx->used > 56)
   {
      memset(ctx->block + ctx->used, 0, MSC_SHA256_BLOCK_BYTES - ctx->used);
      sha256_transform(ctx, ctx->block);
      ctx->used = 0;
   }
   memset(ctx->block + ctx->used, 0, 56 - ctx->used);
   for (i = 0; i < 8; i++)
      ctx->block[63 - i] = (unsigned char)(bits >> (i * 8));
   sha256_transform(ctx, ctx->block);
   for (i = 0; i < 8; i++)
   {
      digest[i * 4] = (unsigned char)(ctx->state[i] >> 24);
      digest[i * 4 + 1] = (unsigned char)(ctx->state[i] >> 16);
      digest[i * 4 + 2] = (unsigned char)(ctx->state[i] >> 8);
      digest[i * 4 + 3] = (unsigned char)ctx->state[i];
   }
}

int msc_resume_file_digest(void *context, uint64_t start, uint64_t end,
                           unsigned char digest[MSC_SHA256_BYTES])
{
   struct msc_file_digest_context *file = context;
   struct msc_sha256_context sha;
   unsigned char buffer[64U * 1024U];
   uint64_t position, remaining;
   if (file == NULL || file->fd < 0 || start >= end ||
       file->base > (uint64_t)INT64_MAX ||
       start > (uint64_t)INT64_MAX - file->base)
      return -1;
   position = file->base + start;
   remaining = end - start;
   if (remaining > (uint64_t)INT64_MAX - position + 1U) return -1;
   sha256_init(&sha);
   while (remaining != 0)
   {
      size_t wanted = remaining < sizeof(buffer) ? (size_t)remaining : sizeof(buffer);
      ssize_t got = pread(file->fd, buffer, wanted, (off_t)position);
      if (got < 0 && errno == EINTR) continue;
      if (got <= 0) return -1;
      sha256_update(&sha, buffer, (size_t)got);
      position += (uint64_t)got;
      remaining -= (uint64_t)got;
   }
   sha256_final(&sha, digest);
   return 0;
}

int msc_resume_send_digests(int fd, const struct msc_range *ranges,
                            uint32_t range_count,
                            msc_resume_digest_fn digest_fn,
                            void *digest_context)
{
   uint32_t i;
   for (i = 0; i < range_count; i++)
   {
      struct msc_wire_digest wire;
      memset(&wire, 0, sizeof(wire));
      wire.magic = htonl(MSC_RESUME_DIGEST_MAGIC);
      wire.version = htonl(MSC_CHECKPOINT_VERSION);
      wire.start = host_to_network_64(ranges[i].start);
      wire.end = host_to_network_64(ranges[i].end);
      if (digest_fn(digest_context, ranges[i].start, ranges[i].end,
                    wire.sha256) != 0 ||
          msc_udp_send_all(fd, &wire, sizeof(wire)) != 0)
         return -1;
   }
   return 0;
}

int msc_resume_verify_digests(int fd, const struct msc_range *ranges,
                              uint32_t range_count,
                              msc_resume_digest_fn digest_fn,
                              void *digest_context)
{
   uint32_t i;
   int mismatch = 0;
   for (i = 0; i < range_count; i++)
   {
      struct msc_wire_digest wire;
      unsigned char expected[MSC_SHA256_BYTES];
      if (msc_udp_recv_all(fd, &wire, sizeof(wire)) != 0)
         return -1;
      if (ntohl(wire.magic) != MSC_RESUME_DIGEST_MAGIC ||
          ntohl(wire.version) != MSC_CHECKPOINT_VERSION ||
          network_to_host_64(wire.start) != ranges[i].start ||
          network_to_host_64(wire.end) != ranges[i].end ||
          digest_fn(digest_context, ranges[i].start, ranges[i].end,
                    expected) != 0)
         return -1;
      if (memcmp(expected, wire.sha256, sizeof(expected)) != 0)
         mismatch = 1;
   }
   return mismatch;
}

int msc_resume_send_verify_reply(int fd, int status)
{
   struct msc_resume_verify_reply reply;
   memset(&reply, 0, sizeof(reply));
   reply.magic = htonl(MSC_RESUME_VERIFY_MAGIC);
   reply.version = htonl(MSC_CHECKPOINT_VERSION);
   reply.status = htonl((uint32_t)status);
   return msc_udp_send_all(fd, &reply, sizeof(reply));
}

int msc_resume_receive_verify_reply(int fd)
{
   struct msc_resume_verify_reply reply;
   uint32_t status;
   if (msc_udp_recv_all(fd, &reply, sizeof(reply)) != 0)
      return MSC_EXIT_NETWORK;
   if (ntohl(reply.magic) != MSC_RESUME_VERIFY_MAGIC ||
       ntohl(reply.version) != MSC_CHECKPOINT_VERSION ||
       ntohl(reply.reserved) != 0)
      return MSC_EXIT_INCOMPATIBLE;
   status = ntohl(reply.status);
   if (status == 0 || status == MSC_EXIT_INTEGRITY ||
       status == MSC_EXIT_SOURCE || status == MSC_EXIT_DESTINATION)
      return (int)status;
   return MSC_EXIT_INCOMPATIBLE;
}

static int write_all(int fd, const void *buffer, size_t size)
{
   const char *p = buffer;
   while (size != 0)
   {
      ssize_t n = write(fd, p, size);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) return -1;
      p += n; size -= (size_t)n;
   }
   return 0;
}

static int read_all(int fd, void *buffer, size_t size)
{
   char *p = buffer;
   while (size != 0)
   {
      ssize_t n = read(fd, p, size);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) return -1;
      p += n; size -= (size_t)n;
   }
   return 0;
}

void msc_checkpoint_init(struct msc_checkpoint *cp)
{
   memset(cp, 0, sizeof(*cp));
   cp->version = MSC_CHECKPOINT_VERSION;
}

void msc_checkpoint_destroy(struct msc_checkpoint *cp)
{
   free(cp->source_path); free(cp->destination_path); free(cp->ranges);
   msc_checkpoint_init(cp);
}

int msc_checkpoint_add_range(struct msc_checkpoint *cp, uint64_t start, uint64_t end)
{
   uint32_t first = 0, last, remove;
   struct msc_range *ranges;

   /* Locate the first interval that touches or overlaps the new range, then
    * replace the whole touching run with its union.  Adjacent intervals are
    * deliberately coalesced: [0, 10) plus [10, 20) is durable [0, 20). */
   if (start >= end) return start == end ? 0 : -1;
   while (first < cp->range_count && cp->ranges[first].end < start) first++;
   last = first;
   while (last < cp->range_count && cp->ranges[last].start <= end)
   {
      if (cp->ranges[last].start < start) start = cp->ranges[last].start;
      if (cp->ranges[last].end > end) end = cp->ranges[last].end;
      last++;
   }
   remove = last - first;
   if (remove == 0)
   {
      if (cp->range_count >= MSC_CHECKPOINT_MAX_RANGES) return -1;
      if (cp->range_count == cp->range_capacity)
      {
         uint32_t capacity = cp->range_capacity ? cp->range_capacity * 2U : 64U;
         if (capacity > MSC_CHECKPOINT_MAX_RANGES) capacity = MSC_CHECKPOINT_MAX_RANGES;
         ranges = realloc(cp->ranges, (size_t)capacity * sizeof(*ranges));
         if (ranges == NULL) return -1;
         cp->ranges = ranges; cp->range_capacity = capacity;
      }
      memmove(&cp->ranges[first + 1], &cp->ranges[first],
              (size_t)(cp->range_count - first) * sizeof(*cp->ranges));
      cp->range_count++;
   }
   else if (remove > 1)
   {
      memmove(&cp->ranges[first + 1], &cp->ranges[last],
              (size_t)(cp->range_count - last) * sizeof(*cp->ranges));
      cp->range_count -= remove - 1;
   }
   cp->ranges[first].start = start; cp->ranges[first].end = end;
   return 0;
}

uint64_t msc_checkpoint_completed_bytes(const struct msc_checkpoint *cp)
{
   uint64_t total = 0;
   uint32_t i;
   for (i = 0; i < cp->range_count; i++)
   {
      uint64_t length = cp->ranges[i].end - cp->ranges[i].start;
      if (UINT64_MAX - total < length) return UINT64_MAX;
      total += length;
   }
   return total;
}

int msc_checkpoint_contains(const struct msc_checkpoint *cp, uint64_t start, uint64_t end)
{
   uint32_t i;
   for (i = 0; i < cp->range_count; i++)
      if (cp->ranges[i].start <= start && cp->ranges[i].end >= end) return 1;
   return 0;
}

int msc_fsync_parent(const char *path)
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
   rc = fsync(fd); close(fd);
   return rc;
}

int msc_checkpoint_write_atomic(const char *path, const struct msc_checkpoint *cp)
{
   size_t slen, dlen, size, offset;
   unsigned char *data;
   char *temp;
   int fd = -1, rc = -1;
   uint32_t i;
   if (path == NULL || cp->source_path == NULL || cp->destination_path == NULL ||
       cp->range_count > MSC_CHECKPOINT_MAX_RANGES) { errno = EINVAL; return -1; }
   slen = strlen(cp->source_path); dlen = strlen(cp->destination_path);
   if (slen > MSC_CP_MAX_PATH || dlen > MSC_CP_MAX_PATH ||
       cp->range_count > (SIZE_MAX - MSC_CP_HEADER_BYTES - slen - dlen) / 16U)
   { errno = EOVERFLOW; return -1; }
   size = MSC_CP_HEADER_BYTES + slen + dlen + (size_t)cp->range_count * 16U;
   data = calloc(1, size); if (data == NULL) return -1;
   put32(data, MSC_CP_MAGIC); put32(data + 4, cp->version);
   put32(data + 8, cp->recursive); put32(data + 12, cp->range_count);
   put64(data + 16, cp->segment_size); put64(data + 24, cp->source_dev);
   put64(data + 32, cp->source_ino); put64(data + 40, cp->source_size);
   put64(data + 48, (uint64_t)cp->source_mtime_sec);
   put64(data + 56, (uint64_t)cp->source_mtime_nsec);
   put64(data + 64, cp->destination_dev); put64(data + 72, cp->destination_ino);
   put64(data + 80, cp->destination_size);
   put64(data + 88, (uint64_t)cp->destination_mtime_sec);
   put64(data + 96, (uint64_t)cp->destination_mtime_nsec);
   put64(data + 104, cp->manifest_hash); put64(data + 112, slen); put64(data + 120, dlen);
   put64(data + 128, cp->source_offset); put64(data + 136, cp->destination_offset);
   put64(data + 144, cp->transfer_size);
   offset = MSC_CP_HEADER_BYTES;
   memcpy(data + offset, cp->source_path, slen); offset += slen;
   memcpy(data + offset, cp->destination_path, dlen); offset += dlen;
   for (i = 0; i < cp->range_count; i++)
   { put64(data + offset, cp->ranges[i].start); put64(data + offset + 8, cp->ranges[i].end); offset += 16; }
   put64(data + 152, checksum64(data, size));
   if (asprintf(&temp, "%s.tmp.%ld", path, (long)getpid()) < 0) { free(data); return -1; }
   /* Publication order is part of the crash-safety contract.  A complete,
    * fsynced temporary replaces the old checkpoint atomically; fsyncing the
    * parent directory then makes that rename durable across a system crash. */
   fd = open(temp, O_WRONLY | O_CREAT | O_EXCL, 0600);
   if (fd >= 0 && write_all(fd, data, size) == 0 && fsync(fd) == 0 && close(fd) == 0)
   {
      fd = -1;
      if (rename(temp, path) == 0 && msc_fsync_parent(path) == 0) rc = 0;
   }
   if (fd >= 0) close(fd);
   if (rc != 0) unlink(temp);
   free(temp); free(data); return rc;
}

int msc_checkpoint_read(const char *path, struct msc_checkpoint *cp,
                        char *error, size_t error_size)
{
   struct stat st;
   unsigned char *data;
   size_t size, slen, dlen, offset;
   uint64_t expected, actual;
   uint32_t count, i;
   int fd = open(path, O_RDONLY);
   if (fd < 0) { snprintf(error, error_size, "cannot open checkpoint %s: %s", path, strerror(errno)); return -1; }
   if (fstat(fd, &st) != 0 || st.st_size < 8 ||
       (uint64_t)st.st_size > MSC_CP_HEADER_BYTES + 2U * MSC_CP_MAX_PATH + 16ULL * MSC_CHECKPOINT_MAX_RANGES)
   { snprintf(error, error_size, "checkpoint %s is truncated or too large", path); close(fd); return -1; }
   size = (size_t)st.st_size; data = malloc(size);
   if (data == NULL || read_all(fd, data, size) != 0)
   { snprintf(error, error_size, "cannot read complete checkpoint %s", path); close(fd); free(data); return -1; }
   close(fd);
   if (get32(data) != MSC_CP_MAGIC) { snprintf(error, error_size, "checkpoint %s has invalid magic", path); free(data); return -1; }
   if (get32(data + 4) != MSC_CHECKPOINT_VERSION)
   { snprintf(error, error_size, "checkpoint %s uses unsupported version %u", path, get32(data + 4)); free(data); return -2; }
   if (size < MSC_CP_HEADER_BYTES)
   { snprintf(error, error_size, "checkpoint %s is truncated", path); free(data); return -1; }
   /* Validate the fixed header, checksum, and all variable-length arithmetic
    * before replacing cp or allocating from file-controlled counts. */
   expected = get64(data + 152); put64(data + 152, 0); actual = checksum64(data, size);
   if (expected != actual) { snprintf(error, error_size, "checkpoint %s failed integrity validation", path); free(data); return -1; }
   count = get32(data + 12); slen = (size_t)get64(data + 112); dlen = (size_t)get64(data + 120);
   if (count > MSC_CHECKPOINT_MAX_RANGES || slen > MSC_CP_MAX_PATH || dlen > MSC_CP_MAX_PATH ||
       slen + dlen > size - MSC_CP_HEADER_BYTES ||
       (size_t)count > (size - MSC_CP_HEADER_BYTES - slen - dlen) / 16U ||
       MSC_CP_HEADER_BYTES + slen + dlen + (size_t)count * 16U != size)
   { snprintf(error, error_size, "checkpoint %s has invalid bounds", path); free(data); return -1; }
   msc_checkpoint_destroy(cp);
   cp->version = get32(data + 4); cp->recursive = get32(data + 8); cp->range_count = count;
   cp->segment_size = get64(data + 16); cp->source_dev = get64(data + 24);
   cp->source_ino = get64(data + 32); cp->source_size = get64(data + 40);
   cp->source_mtime_sec = (int64_t)get64(data + 48); cp->source_mtime_nsec = (int64_t)get64(data + 56);
   cp->destination_dev = get64(data + 64); cp->destination_ino = get64(data + 72);
   cp->destination_size = get64(data + 80);
   cp->destination_mtime_sec = (int64_t)get64(data + 88);
   cp->destination_mtime_nsec = (int64_t)get64(data + 96);
   cp->manifest_hash = get64(data + 104);
   cp->source_offset = get64(data + 128);
   cp->destination_offset = get64(data + 136);
   cp->transfer_size = get64(data + 144);
   cp->source_path = malloc(slen + 1); cp->destination_path = malloc(dlen + 1);
   cp->ranges = count ? malloc((size_t)count * sizeof(*cp->ranges)) : NULL;
   if (cp->source_path == NULL || cp->destination_path == NULL || (count && cp->ranges == NULL))
   { snprintf(error, error_size, "out of memory reading checkpoint %s", path); free(data); msc_checkpoint_destroy(cp); return -1; }
   offset = MSC_CP_HEADER_BYTES;
   memcpy(cp->source_path, data + offset, slen); cp->source_path[slen] = '\0'; offset += slen;
   memcpy(cp->destination_path, data + offset, dlen); cp->destination_path[dlen] = '\0'; offset += dlen;
   cp->range_capacity = count;
   for (i = 0; i < count; i++)
   {
      cp->ranges[i].start = get64(data + offset); cp->ranges[i].end = get64(data + offset + 8); offset += 16;
      if (cp->ranges[i].start >= cp->ranges[i].end ||
          cp->ranges[i].end > cp->transfer_size ||
          (i && cp->ranges[i - 1].end >= cp->ranges[i].start))
      { snprintf(error, error_size, "checkpoint %s has invalid completed ranges", path); free(data); msc_checkpoint_destroy(cp); return -1; }
   }
   free(data); return 0;
}

static char *canonical_or_copy(const char *path)
{
   char *canonical = realpath(path, NULL);
   if (canonical != NULL) return canonical;
   return strdup(path);
}

int msc_checkpoint_capture_source(struct msc_checkpoint *cp, const char *source,
                                  const char *destination, uint64_t segment_size,
                                  int recursive)
{
   struct stat st;
   if (lstat(source, &st) != 0) return -1;
   cp->source_path = canonical_or_copy(source);
   cp->destination_path = canonical_or_copy(destination);
   if (cp->source_path == NULL || cp->destination_path == NULL) return -1;
   cp->segment_size = segment_size; cp->recursive = (uint32_t)recursive;
   cp->source_dev = st.st_dev; cp->source_ino = st.st_ino; cp->source_size = st.st_size;
   cp->source_mtime_sec = st.st_mtim.tv_sec; cp->source_mtime_nsec = st.st_mtim.tv_nsec;
   cp->source_offset = 0; cp->destination_offset = 0;
   cp->transfer_size = (uint64_t)st.st_size;
   return 0;
}

int msc_checkpoint_validate_source(const struct msc_checkpoint *cp,
                                   const char *source, const char *destination,
                                   char *error, size_t error_size)
{
   struct stat st;
   char *src = canonical_or_copy(source), *dst = canonical_or_copy(destination);
   int ok = 0;
   if (src == NULL || dst == NULL || lstat(source, &st) != 0)
      snprintf(error, error_size, "cannot inspect source %s: %s", source, strerror(errno));
   else if (strcmp(src, cp->source_path) != 0 || strcmp(dst, cp->destination_path) != 0)
      snprintf(error, error_size, "checkpoint endpoint identity does not match source/destination");
   else if ((uint64_t)st.st_dev != cp->source_dev || (uint64_t)st.st_ino != cp->source_ino ||
            (uint64_t)st.st_size != cp->source_size || st.st_mtim.tv_sec != cp->source_mtime_sec ||
            st.st_mtim.tv_nsec != cp->source_mtime_nsec)
      snprintf(error, error_size, "source %s changed since checkpoint creation", source);
   else ok = 1;
   free(src); free(dst); return ok ? 0 : -1;
}

char *msc_default_checkpoint_path(const char *destination)
{
   char *path;
   if (asprintf(&path, "%s.msc-checkpoint", destination) < 0) return NULL;
   return path;
}

int msc_checkpoint_capture_destination(struct msc_checkpoint *cp, const char *path)
{
   struct stat st;
   if (lstat(path, &st) != 0 || !S_ISREG(st.st_mode)) return -1;
   cp->destination_dev = st.st_dev; cp->destination_ino = st.st_ino;
   cp->destination_size = st.st_size;
   cp->destination_mtime_sec = st.st_mtim.tv_sec;
   cp->destination_mtime_nsec = st.st_mtim.tv_nsec;
   return 0;
}

int msc_checkpoint_validate_destination(const struct msc_checkpoint *cp,
                                        const char *path,
                                        char *error, size_t error_size)
{
   struct stat st;
   if (lstat(path, &st) != 0)
      snprintf(error, error_size, "cannot inspect resumable destination %s: %s", path, strerror(errno));
   else if (!S_ISREG(st.st_mode) || (uint64_t)st.st_dev != cp->destination_dev ||
            (uint64_t)st.st_ino != cp->destination_ino ||
            (uint64_t)st.st_size != cp->destination_size)
      snprintf(error, error_size, "resumable destination %s changed unexpectedly", path);
   else return 0;
   return -1;
}
