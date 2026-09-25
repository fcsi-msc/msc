#define _GNU_SOURCE
#include "msc.h"
#include "udp_session.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <time.h>
#ifdef __linux__
#include <linux/falloc.h>
#endif

#define INITIAL_ENTRY_CAPACITY 64
#define MAX_RELATIVE_PATH 1048576
#define MAX_MANIFEST_ENTRIES 1048576

struct directory_entry
{
   char *relative_path;
   char *full_path;
   uint64_t file_id;
   uint64_t size;
   uint64_t received;
   mode_t mode;
   uint32_t type;
   uid_t uid;
   gid_t gid;
   int64_t mtime_sec;
   uint32_t mtime_nsec;
   char *link_target;
   dev_t dev;
   ino_t ino;
   uint64_t logical_base;
};

struct directory_manifest
{
   struct directory_entry *entries;
   size_t *file_entry_indices;
   size_t count;
   size_t capacity;
   size_t file_count;
   size_t file_capacity;
   mode_t root_mode;
   uid_t root_uid;
   gid_t root_gid;
   int64_t root_mtime_sec;
   uint32_t root_mtime_nsec;
   uint64_t total_size;
   uint64_t manifest_hash;
};

/* Checkpoints flatten all regular-file bytes into one logical address space.
 * directory_entry.logical_base maps a {file_id, file offset} back into that
 * space, allowing the generic range code to skip durable recursive segments. */

struct directory_sender_state
{
   struct argdata *AD;
   struct directory_manifest *manifest;
   size_t next_entry;
   uint64_t next_offset;
   pthread_mutex_t cursor_lock;
   pthread_mutex_t error_lock;
   int *sockets;
   int numstreams;
   int error;
   struct msc_checkpoint *checkpoint;
   volatile uint64_t current_bytes;
};

struct directory_sender_worker
{
   struct directory_sender_state *state;
   int socketfd;
   long long int bytes_sent;
};

struct directory_receiver_state
{
   struct directory_manifest *manifest;
   size_t segment_size;
   pthread_mutex_t error_lock;
   pthread_mutex_t completion_lock;
   int *sockets;
   int numstreams;
   int error;
   struct msc_checkpoint *checkpoint;
   const char *checkpoint_path;
   const char *destination_root;
   uint64_t bytes_since_checkpoint;
   uint64_t segments_received;
   uint64_t interrupt_after;
};

struct directory_receiver_worker
{
   struct directory_receiver_state *state;
   int socketfd;
};

static void encode_header(struct directory_record_header *header, uint32_t type,
                          uint32_t mode, uint64_t file_id, uint64_t offset,
                          uint64_t length)
{
   header->magic = htonl(MSC_DIRECTORY_MAGIC);
   header->version = htonl(MSC_DIRECTORY_VERSION);
   header->type = htonl(type);
   header->mode = htonl(mode);
   header->file_id = host_to_network_64(file_id);
   header->offset = host_to_network_64(offset);
   header->length = host_to_network_64(length);
   header->mtime_sec = 0;
   header->mtime_nsec = 0;
   header->uid = 0;
   header->gid = 0;
   header->link_length = 0;
}

static int decode_header(struct directory_record_header *header)
{
   if (ntohl(header->magic) != MSC_DIRECTORY_MAGIC ||
       ntohl(header->version) != MSC_DIRECTORY_VERSION)
      return -1;
   header->type = ntohl(header->type);
   header->mode = ntohl(header->mode);
   header->file_id = network_to_host_64(header->file_id);
   header->offset = network_to_host_64(header->offset);
   header->length = network_to_host_64(header->length);
   header->mtime_sec = network_to_host_64(header->mtime_sec);
   header->mtime_nsec = ntohl(header->mtime_nsec);
   header->uid = ntohl(header->uid);
   header->gid = ntohl(header->gid);
   header->link_length = ntohl(header->link_length);
   return 0;
}

static char *join_path(const char *left, const char *right)
{
   size_t length = strlen(left) + strlen(right) + 2;
   char *result = checkmalloc(length, "directory path");
   if (right[0] == '\0')
      snprintf(result, length, "%s", left);
   else
      snprintf(result, length, "%s/%s", left, right);
   return result;
}

static void manifest_add(struct directory_manifest *manifest,
                         const char *relative_path, const char *full_path,
                         uint32_t type, uint64_t size, mode_t mode,
                         uint64_t file_id, const struct stat *metadata,
                         const char *link_target)
{
   struct directory_entry *entry;
   size_t entry_index;

   if (manifest->count >= MAX_MANIFEST_ENTRIES)
   {
      fprintf(stderr, "MSC directory manifest exceeds %u entries\n",
              (unsigned)MAX_MANIFEST_ENTRIES);
      exit(MSC_EXIT_SOURCE);
   }

   if (manifest->count == manifest->capacity)
   {
      size_t capacity = manifest->capacity == 0
         ? INITIAL_ENTRY_CAPACITY : manifest->capacity * 2;
      if (capacity > MAX_MANIFEST_ENTRIES) capacity = MAX_MANIFEST_ENTRIES;
      void *entries = realloc(manifest->entries, capacity * sizeof(*entry));
      if (entries == NULL)
      {
         fprintf(stderr, "MSC could not grow directory manifest\n");
         exit(MSC_EXIT_INTERNAL);
      }
      manifest->entries = entries;
      manifest->capacity = capacity;
   }
   entry_index = manifest->count++;
   entry = &manifest->entries[entry_index];
   memset(entry, 0, sizeof(*entry));
   entry->relative_path = strdup(relative_path);
   entry->full_path = full_path == NULL ? NULL : strdup(full_path);
   if (entry->relative_path == NULL || (full_path != NULL && entry->full_path == NULL))
   {
      fprintf(stderr, "MSC could not allocate directory manifest path\n");
      exit(MSC_EXIT_INTERNAL);
   }
   entry->type = type;
   entry->size = size;
   entry->mode = mode;
   entry->file_id = file_id;
   entry->link_target = link_target == NULL ? NULL : strdup(link_target);
   if (metadata != NULL)
   {
      entry->uid = metadata->st_uid; entry->gid = metadata->st_gid;
      entry->mtime_sec = metadata->st_mtim.tv_sec;
      entry->mtime_nsec = (uint32_t)metadata->st_mtim.tv_nsec;
      entry->dev = metadata->st_dev; entry->ino = metadata->st_ino;
   }
   if (link_target != NULL && entry->link_target == NULL)
   { fprintf(stderr, "MSC could not allocate link target\n"); exit(MSC_EXIT_INTERNAL); }
   if (type == MSC_RECORD_FILE)
   {
      if (file_id != manifest->file_count)
      {
         fprintf(stderr, "MSC directory manifest file ids are not sequential\n");
         exit(MSC_EXIT_INCOMPATIBLE);
      }
      if (manifest->file_count == manifest->file_capacity)
      {
         size_t capacity = manifest->file_capacity == 0
            ? INITIAL_ENTRY_CAPACITY : manifest->file_capacity * 2;
         if (capacity > MAX_MANIFEST_ENTRIES) capacity = MAX_MANIFEST_ENTRIES;
         void *indices = realloc(manifest->file_entry_indices,
                                 capacity * sizeof(*manifest->file_entry_indices));
         if (indices == NULL)
         {
            fprintf(stderr, "MSC could not grow directory file index\n");
            exit(MSC_EXIT_INTERNAL);
         }
         manifest->file_entry_indices = indices;
         manifest->file_capacity = capacity;
      }
      manifest->file_entry_indices[manifest->file_count++] = entry_index;
      entry->logical_base = manifest->total_size;
      if (UINT64_MAX - manifest->total_size < size)
      { fprintf(stderr, "MSC recursive manifest total size overflow\n"); exit(MSC_EXIT_SOURCE); }
      manifest->total_size += size;
   }
}

static uint64_t hash_bytes(uint64_t hash, const void *data, size_t length)
{
   const unsigned char *p = data;
   size_t i;
   for (i = 0; i < length; i++) { hash ^= p[i]; hash *= UINT64_C(1099511628211); }
   return hash;
}

static uint64_t manifest_fingerprint(struct directory_manifest *manifest)
{
   uint64_t hash = UINT64_C(1469598103934665603);
   size_t i;
   for (i = 0; i < manifest->count; i++)
   {
      struct directory_entry *e = &manifest->entries[i];
      hash = hash_bytes(hash, e->relative_path, strlen(e->relative_path) + 1);
      hash = hash_bytes(hash, &e->type, sizeof(e->type));
      hash = hash_bytes(hash, &e->size, sizeof(e->size));
      hash = hash_bytes(hash, &e->mode, sizeof(e->mode));
      hash = hash_bytes(hash, &e->mtime_sec, sizeof(e->mtime_sec));
      hash = hash_bytes(hash, &e->mtime_nsec, sizeof(e->mtime_nsec));
      hash = hash_bytes(hash, &e->dev, sizeof(e->dev));
      hash = hash_bytes(hash, &e->ino, sizeof(e->ino));
      if (e->link_target != NULL)
         hash = hash_bytes(hash, e->link_target, strlen(e->link_target) + 1);
   }
   manifest->manifest_hash = hash;
   return hash;
}

static void manifest_free(struct directory_manifest *manifest)
{
   size_t i;
   for (i = 0; i < manifest->count; i++)
   {
      free(manifest->entries[i].relative_path);
      free(manifest->entries[i].full_path);
      free(manifest->entries[i].link_target);
   }
   free(manifest->entries);
   free(manifest->file_entry_indices);
   memset(manifest, 0, sizeof(*manifest));
}

/* Checkpoint ranges use one logical byte space formed by concatenating regular
 * files.  Split them at file boundaries for resume verification so each digest
 * can be computed with bounded memory from exactly one opened file. */
static int manifest_verification_ranges(
   struct directory_manifest *manifest, const struct msc_checkpoint *checkpoint,
   struct msc_range **ranges_out, uint32_t *count_out)
{
   struct msc_range *ranges = NULL;
   size_t capacity = 0;
   uint32_t count = 0, range_index;
   size_t file_cursor = 0;

   for (range_index = 0; range_index < checkpoint->range_count; range_index++)
   {
      uint64_t position = checkpoint->ranges[range_index].start;
      uint64_t end = checkpoint->ranges[range_index].end;
      while (position < end)
      {
         struct directory_entry *entry;
         uint64_t file_end, piece_end;
         while (file_cursor < manifest->file_count)
         {
            entry = &manifest->entries[
               manifest->file_entry_indices[file_cursor]];
            file_end = entry->logical_base + entry->size;
            if (entry->size != 0 && position < file_end) break;
            file_cursor++;
         }
         if (file_cursor >= manifest->file_count) goto fail;
         entry = &manifest->entries[manifest->file_entry_indices[file_cursor]];
         file_end = entry->logical_base + entry->size;
         if (position < entry->logical_base || position >= file_end) goto fail;
         piece_end = end < file_end ? end : file_end;
         if (count == UINT32_MAX) goto fail;
         if ((size_t)count == capacity)
         {
            size_t next = capacity ? capacity * 2U : 64U;
            void *grown;
            if (next < capacity || next > UINT32_MAX) next = UINT32_MAX;
            grown = realloc(ranges, next * sizeof(*ranges));
            if (grown == NULL) goto fail;
            ranges = grown;
            capacity = next;
         }
         ranges[count].start = position;
         ranges[count].end = piece_end;
         count++;
         position = piece_end;
      }
   }
   *ranges_out = ranges;
   *count_out = count;
   return 0;
fail:
   free(ranges);
   return -1;
}

struct manifest_digest_context
{
   struct directory_manifest *manifest;
   size_t file_cursor;
};

static int manifest_range_digest(void *opaque, uint64_t start, uint64_t end,
                                 unsigned char digest[MSC_SHA256_BYTES])
{
   struct manifest_digest_context *context = opaque;
   struct directory_manifest *manifest = context->manifest;
   while (context->file_cursor < manifest->file_count)
   {
      struct directory_entry *entry =
         &manifest->entries[
            manifest->file_entry_indices[context->file_cursor]];
      uint64_t file_end = entry->logical_base + entry->size;
      if (entry->size != 0 && start >= entry->logical_base && end <= file_end)
      {
         struct msc_file_digest_context file_context;
         int rc;
         file_context.fd = open(entry->full_path, O_RDONLY | O_NOFOLLOW);
         file_context.base = 0;
         if (file_context.fd < 0) return -1;
         rc = msc_resume_file_digest(&file_context,
                                     start - entry->logical_base,
                                     end - entry->logical_base, digest);
         close(file_context.fd);
         return rc;
      }
      if (start < file_end) return -1;
      context->file_cursor++;
   }
   return -1;
}

static int manifest_checkpoint_digests(
   int fd, struct directory_manifest *manifest,
   const struct msc_checkpoint *checkpoint, int verify)
{
   struct msc_range *ranges = NULL;
   struct manifest_digest_context context;
   uint32_t count = 0;
   int rc;
   context.manifest = manifest;
   context.file_cursor = 0;
   if (manifest_verification_ranges(manifest, checkpoint,
                                    &ranges, &count) != 0)
      return -1;
   rc = verify
      ? msc_resume_verify_digests(fd, ranges, count,
                                  manifest_range_digest, &context)
      : msc_resume_send_digests(fd, ranges, count,
                                manifest_range_digest, &context);
   free(ranges);
   return rc;
}

int manifest_send_checkpoint_digests(
   int fd, struct directory_manifest *manifest,
   const struct msc_checkpoint *checkpoint)
{
   return manifest_checkpoint_digests(fd, manifest, checkpoint, 0);
}

int manifest_verify_checkpoint_digests(
   int fd, struct directory_manifest *manifest,
   const struct msc_checkpoint *checkpoint)
{
   return manifest_checkpoint_digests(fd, manifest, checkpoint, 1);
}

static int manifest_range_digests(int fd, struct directory_manifest *manifest,
                                  uint64_t start, uint64_t end, int verify)
{
   struct msc_checkpoint checkpoint;
   struct msc_range range;
   if (start > end || end > manifest->total_size)
      return -1;
   if (start == end)
      return 0;
   memset(&checkpoint, 0, sizeof(checkpoint));
   range.start = start;
   range.end = end;
   checkpoint.ranges = &range;
   checkpoint.range_count = 1;
   return manifest_checkpoint_digests(fd, manifest, &checkpoint, verify);
}

int manifest_send_range_digests(int fd, struct directory_manifest *manifest,
                                uint64_t start, uint64_t end)
{
   return manifest_range_digests(fd, manifest, start, end, 0);
}

int manifest_verify_range_digests(int fd, struct directory_manifest *manifest,
                                  uint64_t start, uint64_t end)
{
   return manifest_range_digests(fd, manifest, start, end, 1);
}

static void scan_directory(struct directory_manifest *manifest,
                           const char *root, const char *relative)
{
   char *directory_path = join_path(root, relative);
   DIR *directory = opendir(directory_path);
   struct dirent *item;

   if (directory == NULL)
   {
      fprintf(stderr, "MSC could not open directory %s: %s\n",
              directory_path, strerror(errno));
      exit(MSC_EXIT_SOURCE);
   }
   while ((item = readdir(directory)) != NULL)
   {
      char *child_relative;
      char *child_full;
      struct stat statbuf;

      if (strcmp(item->d_name, ".") == 0 || strcmp(item->d_name, "..") == 0)
         continue;
      child_relative = relative[0] == '\0'
         ? strdup(item->d_name) : join_path(relative, item->d_name);
      if (child_relative == NULL)
      {
         fprintf(stderr, "MSC out of memory scanning %s\n", directory_path);
         exit(MSC_EXIT_INTERNAL);
      }
      child_full = join_path(root, child_relative);
      if (lstat(child_full, &statbuf) != 0)
      {
         fprintf(stderr, "MSC could not inspect %s: %s\n",
                 child_full, strerror(errno));
         exit(MSC_EXIT_SOURCE);
      }
      if (S_ISDIR(statbuf.st_mode))
      {
         manifest_add(manifest, child_relative, child_full, MSC_RECORD_DIRECTORY, 0,
                      statbuf.st_mode & 07777, 0, &statbuf, NULL);
         scan_directory(manifest, root, child_relative);
      }
      else if (S_ISREG(statbuf.st_mode))
      {
         size_t prior;
         /* Traversal order is also manifest order.  A repeated device/inode
          * therefore becomes a hard link to an entry the receiver has already
          * created, and only the first path receives file data. */
         for (prior = 0; prior < manifest->count; prior++)
            if (manifest->entries[prior].type == MSC_RECORD_FILE &&
                manifest->entries[prior].dev == statbuf.st_dev &&
                manifest->entries[prior].ino == statbuf.st_ino)
               break;
         if (prior < manifest->count)
            manifest_add(manifest, child_relative, child_full, MSC_RECORD_HARDLINK, 0,
                         statbuf.st_mode & 07777, manifest->entries[prior].file_id,
                         &statbuf, manifest->entries[prior].relative_path);
         else
            manifest_add(manifest, child_relative, child_full, MSC_RECORD_FILE,
                         statbuf.st_size, statbuf.st_mode & 07777,
                         manifest->file_count, &statbuf, NULL);
      }
      else if (S_ISLNK(statbuf.st_mode))
      {
         size_t capacity = statbuf.st_size > 0 ? (size_t)statbuf.st_size + 2 : 4096;
         char *target;
         ssize_t got;
         if (capacity > MAX_RELATIVE_PATH + 1) capacity = MAX_RELATIVE_PATH + 1;
         target = checkmalloc(capacity, "symlink target");
         got = readlink(child_full, target, capacity - 1);
         if (got < 0 || (size_t)got >= capacity - 1)
         { fprintf(stderr, "MSC could not safely read symlink %s: %s\n", child_full, strerror(errno)); exit(MSC_EXIT_SOURCE); }
         target[got] = '\0';
         manifest_add(manifest, child_relative, child_full, MSC_RECORD_SYMLINK, 0,
                      statbuf.st_mode & 07777, 0, &statbuf, target);
         free(target);
      }
      else
      {
         fprintf(stderr, "MSC recursive mode rejects devices, sockets, and FIFOs: %s\n",
                 child_full);
         exit(MSC_EXIT_SOURCE);
      }
      free(child_relative);
      free(child_full);
   }
   closedir(directory);
   free(directory_path);
}

static int valid_relative_path(const char *path)
{
   const char *part = path;

   if (path[0] == '\0' || path[0] == '/')
      return 0;
   while (*part != '\0')
   {
      const char *end = strchr(part, '/');
      size_t length = end == NULL ? strlen(part) : (size_t)(end - part);
      if (length == 0 || (length == 1 && part[0] == '.') ||
          (length == 2 && part[0] == '.' && part[1] == '.'))
         return 0;
      if (end == NULL)
         break;
      part = end + 1;
   }
   return 1;
}

static int mkdir_existing(const char *path, mode_t mode)
{
   if (mkdir(path, mode) == 0)
      return 0;
   if (errno == EEXIST)
   {
      struct stat statbuf;
      return lstat(path, &statbuf) == 0 && S_ISDIR(statbuf.st_mode) ? 0 : -1;
   }
   return -1;
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

static int send_manifest(int socketfd, struct directory_manifest *manifest)
{
   size_t i;
   struct directory_record_header header;

   for (i = 0; i < manifest->count; i++)
   {
      struct directory_entry *entry = &manifest->entries[i];
      size_t path_length = strlen(entry->relative_path);
      size_t link_length = entry->link_target == NULL ? 0 : strlen(entry->link_target);
      /* Manifest records reuse offset for file size and length for path bytes;
       * DATA records later restore their ordinary offset/length meanings. */
      encode_header(&header,
                    entry->type,
                    entry->mode, entry->file_id, entry->size, path_length);
      header.mtime_sec = host_to_network_64((uint64_t)entry->mtime_sec);
      header.mtime_nsec = htonl(entry->mtime_nsec);
      header.uid = htonl((uint32_t)entry->uid);
      header.gid = htonl((uint32_t)entry->gid);
      header.link_length = htonl((uint32_t)link_length);
      /* This manifest also travels over MSC UDP's reliable control shim.
       * send_iov_all()/sendall() write the descriptor directly and therefore
       * bypass that shim when its descriptor is a SOCK_DGRAM.  Route every
       * manifest byte through the transport-aware stream wrapper; on the
       * legacy TCP recursive path the wrapper falls back to the same writes. */
      if (msc_udp_send_all(socketfd, &header, sizeof(header)) != 0 ||
          msc_udp_send_all(socketfd, entry->relative_path, path_length) != 0 ||
          (link_length != 0 &&
           msc_udp_send_all(socketfd, entry->link_target, link_length) != 0))
         return -1;
   }
   encode_header(&header, MSC_RECORD_MANIFEST_DONE, manifest->root_mode, 0, 0, 0);
   header.mtime_sec = host_to_network_64((uint64_t)manifest->root_mtime_sec);
   header.mtime_nsec = htonl(manifest->root_mtime_nsec);
   header.uid = htonl((uint32_t)manifest->root_uid);
   header.gid = htonl((uint32_t)manifest->root_gid);
   return msc_udp_send_all(socketfd, &header, sizeof(header));
}

static int send_data_record(int socketfd, struct directory_record_header *header,
                            const void *buffer, size_t length)
{
   struct iovec iov[2];

   iov[0].iov_base = header;
   iov[0].iov_len = sizeof(*header);
   iov[1].iov_base = (void *)buffer;
   iov[1].iov_len = length;
   return send_iov_all(socketfd, iov, 2);
}

static int buffer_is_zero(const unsigned char *buffer, size_t length)
{
   size_t i;
   for (i = 0; i < length; i++) if (buffer[i] != 0) return 0;
   return 1;
}

static int send_done_record(int socketfd)
{
   struct directory_record_header done;

   encode_header(&done, MSC_RECORD_WORKER_DONE, 0, 0, 0, 0);
   return sendall(socketfd, &done, sizeof(done));
}

static int send_ack_record(int socketfd)
{
   struct directory_record_header ack;

   encode_header(&ack, MSC_RECORD_ACK, 0, 0, 0, 0);
   return sendall(socketfd, &ack, sizeof(ack));
}

int msc_recursive_resume_sender_hello(
   int socketfd, struct argdata *AD, struct directory_manifest *manifest,
   struct msc_checkpoint *checkpoint, uint64_t checkpoint_granularity)
{
   struct stat st;
   struct msc_resume_hello hello;
   struct msc_resume_reply reply;
   size_t path_length = strlen(AD->sourcefile);
   uint32_t i;
   uint32_t status;
   if (lstat(AD->sourcefile, &st) != 0 || !S_ISDIR(st.st_mode))
      return MSC_EXIT_SOURCE;
   if (path_length == 0 || path_length > MAX_RELATIVE_PATH ||
       checkpoint_granularity == 0)
      return MSC_EXIT_CLI;
   memset(&hello, 0, sizeof(hello));
   hello.magic = htonl(MSC_RESUME_MAGIC); hello.version = htonl(MSC_CHECKPOINT_VERSION);
   hello.flags = htonl(AD->resume ? 3U : 2U); hello.path_length = htonl((uint32_t)path_length);
   hello.segment_size = host_to_network_64(checkpoint_granularity);
   hello.transfer_size = host_to_network_64(manifest->total_size);
   hello.source_dev = host_to_network_64((uint64_t)st.st_dev);
   hello.source_ino = host_to_network_64((uint64_t)st.st_ino);
   hello.source_size = host_to_network_64((uint64_t)st.st_size);
   hello.source_mtime_sec = host_to_network_64((uint64_t)st.st_mtim.tv_sec);
   hello.source_mtime_nsec = host_to_network_64((uint64_t)st.st_mtim.tv_nsec);
   hello.manifest_hash = host_to_network_64(manifest->manifest_hash);
   hello.source_offset = 0;
   hello.destination_offset = 0;
   if (msc_udp_send_all(socketfd, &hello, sizeof(hello)) != 0 ||
       msc_udp_send_all(socketfd, AD->sourcefile, path_length) != 0 ||
       msc_udp_recv_all(socketfd, &reply, sizeof(reply)) != 0)
      return MSC_EXIT_NETWORK;
   if (ntohl(reply.magic) != MSC_RESUME_REPLY_MAGIC ||
       ntohl(reply.version) != MSC_CHECKPOINT_VERSION)
      return MSC_EXIT_INCOMPATIBLE;
   status = ntohl(reply.status);
   if (status != 0)
      return status == MSC_EXIT_CLI || status == MSC_EXIT_SOURCE ||
             status == MSC_EXIT_DESTINATION || status == MSC_EXIT_NETWORK ||
             status == MSC_EXIT_INTEGRITY ||
             status == MSC_EXIT_INCOMPATIBLE || status == MSC_EXIT_INTERNAL
             ? (int)status : MSC_EXIT_INCOMPATIBLE;
   checkpoint->range_count = ntohl(reply.range_count);
   if (checkpoint->range_count > MSC_CHECKPOINT_MAX_RANGES)
      return MSC_EXIT_INCOMPATIBLE;
   checkpoint->range_capacity = checkpoint->range_count;
   checkpoint->ranges = checkpoint->range_count
      ? checkmalloc((size_t)checkpoint->range_count * sizeof(*checkpoint->ranges), "tree resume ranges") : NULL;
   for (i = 0; i < checkpoint->range_count; i++)
   {
      struct msc_wire_range wire;
      if (msc_udp_recv_all(socketfd, &wire, sizeof(wire)) != 0)
         return MSC_EXIT_NETWORK;
      checkpoint->ranges[i].start = network_to_host_64(wire.start);
      checkpoint->ranges[i].end = network_to_host_64(wire.end);
      if (checkpoint->ranges[i].start >= checkpoint->ranges[i].end ||
          checkpoint->ranges[i].end > manifest->total_size ||
          (i && checkpoint->ranges[i - 1].end >= checkpoint->ranges[i].start))
         return MSC_EXIT_INCOMPATIBLE;
   }
   return 0;
}

int msc_recursive_resume_receiver_hello(
   int socketfd, struct argdata *AD, struct msc_checkpoint *checkpoint,
   uint64_t *transfer_size, uint64_t checkpoint_granularity)
{
   struct msc_resume_hello hello;
   struct msc_resume_reply reply;
   struct stat dst;
   char *source_path = NULL;
   char error[512] = "invalid recursive checkpoint";
   uint32_t length, i;
   int ok = 0, code = MSC_EXIT_INCOMPATIBLE;
   memset(&reply, 0, sizeof(reply));
   reply.magic = htonl(MSC_RESUME_REPLY_MAGIC); reply.version = htonl(MSC_CHECKPOINT_VERSION);
   if (msc_udp_recv_all(socketfd, &hello, sizeof(hello)) != 0)
   { code = MSC_EXIT_NETWORK; snprintf(error, sizeof(error), "recursive resume hello was truncated"); goto out; }
   if (ntohl(hello.magic) != MSC_RESUME_MAGIC || ntohl(hello.version) != MSC_CHECKPOINT_VERSION ||
       (ntohl(hello.flags) != 2U && ntohl(hello.flags) != 3U))
   { snprintf(error, sizeof(error), "recursive peer protocol is incompatible"); goto out; }
   length = ntohl(hello.path_length);
   *transfer_size = network_to_host_64(hello.transfer_size);
   if (length == 0 || length > MAX_RELATIVE_PATH ||
       checkpoint_granularity == 0 ||
       network_to_host_64(hello.segment_size) != checkpoint_granularity ||
       *transfer_size > INT64_MAX ||
       network_to_host_64(hello.source_offset) != 0 ||
       network_to_host_64(hello.destination_offset) != 0)
   { snprintf(error, sizeof(error), "recursive resume bounds are invalid"); goto out; }
   source_path = checkmalloc((size_t)length + 1, "recursive resume source");
   if (msc_udp_recv_all(socketfd, source_path, length) != 0)
   { code = MSC_EXIT_NETWORK; goto out; }
   source_path[length] = '\0';
   if (ntohl(hello.flags) & 1U)
   {
      if (msc_checkpoint_read(AD->checkpoint_path, checkpoint,
                              error, sizeof(error)) != 0)
         goto out;
      if (!checkpoint->recursive ||
          checkpoint->segment_size != checkpoint_granularity ||
          checkpoint->source_dev != network_to_host_64(hello.source_dev) ||
          checkpoint->source_ino != network_to_host_64(hello.source_ino) ||
          checkpoint->source_size != network_to_host_64(hello.source_size) ||
          checkpoint->source_mtime_sec != (int64_t)network_to_host_64(hello.source_mtime_sec) ||
          checkpoint->source_mtime_nsec != (int64_t)network_to_host_64(hello.source_mtime_nsec) ||
          checkpoint->manifest_hash != network_to_host_64(hello.manifest_hash) ||
          checkpoint->source_offset != 0 || checkpoint->destination_offset != 0 ||
          checkpoint->transfer_size != *transfer_size ||
          strcmp(checkpoint->source_path, source_path) != 0 ||
          strcmp(checkpoint->destination_path, AD->destfile) != 0)
      {
         snprintf(error, sizeof(error),
                  "recursive checkpoint/source identity changed");
         goto out;
      }
      if (lstat(AD->destfile, &dst) != 0 || !S_ISDIR(dst.st_mode))
      {
         snprintf(error, sizeof(error),
                  "recursive checkpoint destination is missing or invalid");
         goto out;
      }
      if (checkpoint->destination_dev != (uint64_t)dst.st_dev ||
          checkpoint->destination_ino != (uint64_t)dst.st_ino)
      {
         snprintf(error, sizeof(error),
                  "recursive checkpoint destination identity changed");
         goto out;
      }
   }
   else
   {
      if (access(AD->checkpoint_path, F_OK) == 0)
      {
         if (!AD->force) { snprintf(error, sizeof(error), "stale recursive checkpoint exists; use --resume or --force"); goto out; }
         if (unlink(AD->checkpoint_path) != 0 ||
             msc_fsync_parent(AD->checkpoint_path) != 0)
         { code = MSC_EXIT_DESTINATION; snprintf(error, sizeof(error), "cannot discard stale recursive checkpoint: %s", strerror(errno)); goto out; }
      }
      if (mkdir_existing(AD->destfile, 0777) != 0 || lstat(AD->destfile, &dst) != 0)
      { code = MSC_EXIT_DESTINATION; snprintf(error, sizeof(error), "cannot prepare recursive destination %s: %s", AD->destfile, strerror(errno)); goto out; }
      checkpoint->recursive = 1;
      checkpoint->segment_size = checkpoint_granularity;
      checkpoint->source_dev = network_to_host_64(hello.source_dev);
      checkpoint->source_ino = network_to_host_64(hello.source_ino);
      checkpoint->source_size = network_to_host_64(hello.source_size);
      checkpoint->source_mtime_sec = (int64_t)network_to_host_64(hello.source_mtime_sec);
      checkpoint->source_mtime_nsec = (int64_t)network_to_host_64(hello.source_mtime_nsec);
      checkpoint->manifest_hash = network_to_host_64(hello.manifest_hash);
      checkpoint->source_offset = 0; checkpoint->destination_offset = 0;
      checkpoint->transfer_size = *transfer_size;
      checkpoint->source_path = strdup(source_path); checkpoint->destination_path = strdup(AD->destfile);
      checkpoint->destination_dev = dst.st_dev; checkpoint->destination_ino = dst.st_ino;
      if (checkpoint->source_path == NULL || checkpoint->destination_path == NULL ||
          msc_fsync_parent(AD->destfile) != 0 ||
          msc_checkpoint_write_atomic(AD->checkpoint_path, checkpoint) != 0)
      { code = MSC_EXIT_DESTINATION; snprintf(error, sizeof(error), "cannot create recursive checkpoint %s: %s", AD->checkpoint_path, strerror(errno)); goto out; }
   }
   reply.status = 0; reply.range_count = htonl(checkpoint->range_count);
   code = MSC_EXIT_NETWORK;
   if (msc_udp_send_all(socketfd, &reply, sizeof(reply)) != 0) goto out;
   for (i = 0; i < checkpoint->range_count; i++)
   {
      struct msc_wire_range wire;
      wire.start = host_to_network_64(checkpoint->ranges[i].start);
      wire.end = host_to_network_64(checkpoint->ranges[i].end);
      if (msc_udp_send_all(socketfd, &wire, sizeof(wire)) != 0) goto out;
   }
   ok = 1;
out:
   if (!ok)
   {
      fprintf(stderr, "MSC recursive checkpoint negotiation rejected: %s\n", error);
      reply.status = htonl((uint32_t)code);
      (void)msc_udp_send_all(socketfd, &reply, sizeof(reply));
   }
   free(source_path);
   return ok ? 0 : code;
}

static int receive_manifest(int socketfd, const char *destination_root,
                            struct directory_manifest *manifest, int resume)
{
   struct directory_record_header header;
   int root_ready = 0;

   for (;;)
   {
      char *relative_path;
      char *full_path;
      int fd;

      if (msc_udp_recv_all(socketfd, &header, sizeof(header)) != 0)
         return MSC_EXIT_NETWORK;
      if (decode_header(&header) != 0)
      {
         fprintf(stderr, "MSC received invalid directory record header\n");
         return MSC_EXIT_INCOMPATIBLE;
      }
      if (!root_ready)
      {
         if (mkdir_existing(destination_root, 0777) != 0)
         {
            fprintf(stderr, "MSC could not create destination directory %s: %s\n",
                    destination_root, strerror(errno));
            return MSC_EXIT_DESTINATION;
         }
         root_ready = 1;
      }
      if (header.type == MSC_RECORD_MANIFEST_DONE)
      {
         manifest->root_mode = header.mode;
         manifest->root_uid = (uid_t)header.uid; manifest->root_gid = (gid_t)header.gid;
         manifest->root_mtime_sec = (int64_t)header.mtime_sec;
         manifest->root_mtime_nsec = header.mtime_nsec;
         return 0;
      }
      if (header.type != MSC_RECORD_DIRECTORY && header.type != MSC_RECORD_FILE &&
          header.type != MSC_RECORD_SYMLINK && header.type != MSC_RECORD_HARDLINK)
         return MSC_EXIT_INCOMPATIBLE;
      if (header.length == 0 || header.length > MAX_RELATIVE_PATH)
         return MSC_EXIT_INCOMPATIBLE;
      if (header.link_length > MAX_RELATIVE_PATH ||
          ((header.type == MSC_RECORD_SYMLINK || header.type == MSC_RECORD_HARDLINK) &&
           header.link_length == 0) ||
          ((header.type == MSC_RECORD_FILE || header.type == MSC_RECORD_DIRECTORY) &&
           header.link_length != 0))
         return MSC_EXIT_INCOMPATIBLE;
      relative_path = checkmalloc(header.length + 1, "manifest relative path");
      if (msc_udp_recv_all(socketfd, relative_path, header.length) != 0)
      {
         free(relative_path);
         return MSC_EXIT_NETWORK;
      }
      relative_path[header.length] = '\0';
      if (!valid_relative_path(relative_path))
      {
         fprintf(stderr, "MSC rejected unsafe destination path %s\n", relative_path);
         free(relative_path);
         return MSC_EXIT_INCOMPATIBLE;
      }
      full_path = join_path(destination_root, relative_path);
      {
         char *link_target = NULL;
         struct directory_entry *added;
         if (header.link_length != 0)
         {
            link_target = checkmalloc((size_t)header.link_length + 1, "manifest link target");
            if (msc_udp_recv_all(socketfd, link_target, header.link_length) != 0)
            { free(link_target); free(relative_path); free(full_path); return MSC_EXIT_NETWORK; }
            link_target[header.link_length] = '\0';
         }
      if (header.type == MSC_RECORD_DIRECTORY)
      {
         if (mkdir_existing(full_path, 0777) != 0)
         {
            fprintf(stderr, "MSC could not create directory %s: %s\n",
                    full_path, strerror(errno));
            free(relative_path);
            free(full_path);
            return MSC_EXIT_DESTINATION;
         }
         manifest_add(manifest, relative_path, full_path, MSC_RECORD_DIRECTORY, 0,
                      header.mode, 0, NULL, NULL);
      }
      else if (header.type == MSC_RECORD_FILE)
      {
         if (header.file_id != manifest->file_count)
         {
            fprintf(stderr, "MSC received invalid file id\n");
            free(relative_path);
            free(full_path);
            return MSC_EXIT_INCOMPATIBLE;
         }
         fd = open(full_path, O_WRONLY | O_CREAT | O_NOFOLLOW |
                              (resume ? 0 : O_TRUNC), 0666);
         if (fd < 0 || ftruncate(fd, header.offset) != 0)
         {
            fprintf(stderr, "MSC could not create file %s: %s\n",
                    full_path, strerror(errno));
            if (fd >= 0)
               close(fd);
            free(relative_path);
            free(full_path);
            return MSC_EXIT_DESTINATION;
         }
         close(fd);
         manifest_add(manifest, relative_path, full_path, MSC_RECORD_FILE, header.offset,
                      header.mode, header.file_id, NULL, NULL);
      }
      else if (header.type == MSC_RECORD_SYMLINK)
      {
         struct stat existing;
         /* A symlink target is payload, not a destination pathname: absolute
          * targets and ".." are valid link contents and are never followed
          * while the manifest is being applied. */
         if (symlink(link_target, full_path) != 0)
         {
            char current[MAX_RELATIVE_PATH + 1];
            ssize_t n;
            if (errno != EEXIST || lstat(full_path, &existing) != 0 ||
                !S_ISLNK(existing.st_mode) ||
                (n = readlink(full_path, current, MAX_RELATIVE_PATH)) < 0)
            { fprintf(stderr, "MSC could not create symlink %s: %s\n", full_path, strerror(errno)); free(link_target); free(relative_path); free(full_path); return MSC_EXIT_DESTINATION; }
            current[n] = '\0';
            if (strcmp(current, link_target) != 0)
            { fprintf(stderr, "MSC destination symlink changed unexpectedly: %s\n", full_path); free(link_target); free(relative_path); free(full_path); return MSC_EXIT_DESTINATION; }
         }
         manifest_add(manifest, relative_path, full_path, MSC_RECORD_SYMLINK, 0,
                      header.mode, 0, NULL, link_target);
      }
      else
      {
         char *target_full;
         struct stat a, b;
         if (!valid_relative_path(link_target))
         { fprintf(stderr, "MSC rejected unsafe hard-link target %s\n", link_target); free(link_target); free(relative_path); free(full_path); return MSC_EXIT_INCOMPATIBLE; }
         target_full = join_path(destination_root, link_target);
         if (link(target_full, full_path) != 0 &&
             (errno != EEXIST || lstat(target_full, &a) != 0 ||
              lstat(full_path, &b) != 0 || a.st_dev != b.st_dev || a.st_ino != b.st_ino))
         { fprintf(stderr, "MSC could not create hard link %s -> %s: %s\n", full_path, target_full, strerror(errno)); free(target_full); free(link_target); free(relative_path); free(full_path); return MSC_EXIT_DESTINATION; }
         free(target_full);
         manifest_add(manifest, relative_path, full_path, MSC_RECORD_HARDLINK, 0,
                      header.mode, header.file_id, NULL, link_target);
      }
         added = &manifest->entries[manifest->count - 1];
         added->uid = (uid_t)header.uid; added->gid = (gid_t)header.gid;
         added->mtime_sec = (int64_t)header.mtime_sec; added->mtime_nsec = header.mtime_nsec;
         free(link_target);
      }
      free(relative_path);
      free(full_path);
   }
}

static struct directory_entry *file_entry(struct directory_manifest *manifest,
                                          uint64_t file_id)
{
   if (file_id >= manifest->file_count)
      return NULL;
   return &manifest->entries[manifest->file_entry_indices[file_id]];
}

static void shutdown_sockets(int *sockets, int numstreams)
{
   int i;
   for (i = 0; i < numstreams; i++)
      shutdown(sockets[i], SHUT_RDWR);
}

static void set_sender_error(struct directory_sender_state *state)
{
   pthread_mutex_lock(&state->error_lock);
   if (!state->error)
   {
      state->error = 1;
      shutdown_sockets(state->sockets, state->numstreams);
   }
   pthread_mutex_unlock(&state->error_lock);
}

static int sender_has_error(struct directory_sender_state *state)
{
   int error;
   pthread_mutex_lock(&state->error_lock);
   error = state->error;
   pthread_mutex_unlock(&state->error_lock);
   return error;
}

static void *directory_sender_thread(void *arg)
{
   struct directory_sender_worker *worker = arg;
   struct directory_sender_state *state = worker->state;
   size_t segment_size = state->AD->packetsize;
   char *buffer = checkmalloc(segment_size, "directory sender buffer");
   uint64_t open_file_id = UINT64_MAX;
   int inputfd = -1;

   while (!sender_has_error(state) && !msc_cancelled())
   {
      struct directory_entry *entry = NULL;
      uint64_t offset = 0;
      size_t length = 0;
      struct directory_record_header header;

      /* Claim work from one cursor spanning the whole manifest.  A worker may
       * switch files between claims; file_id keeps out-of-order records
       * unambiguous at the receiver. */
      pthread_mutex_lock(&state->cursor_lock);
      while (state->next_entry < state->manifest->count)
      {
         entry = &state->manifest->entries[state->next_entry];
         if (entry->type != MSC_RECORD_FILE || entry->size == 0)
         {
            state->next_entry++;
            state->next_offset = 0;
            continue;
         }
         offset = state->next_offset;
         length = entry->size - offset;
         if (length > segment_size)
            length = segment_size;
         state->next_offset += length;
         if (state->next_offset >= entry->size)
         {
            state->next_entry++;
            state->next_offset = 0;
         }
         break;
      }
      pthread_mutex_unlock(&state->cursor_lock);
      if (entry == NULL || length == 0)
         break;

      if (state->checkpoint != NULL &&
          msc_checkpoint_contains(state->checkpoint,
                                  entry->logical_base + offset,
                                  entry->logical_base + offset + length))
         continue;

      if (open_file_id != entry->file_id)
      {
         if (inputfd >= 0)
            close(inputfd);
         inputfd = open(entry->full_path, O_RDONLY);
         open_file_id = entry->file_id;
      }
      if (inputfd < 0 || preadall(inputfd, buffer, length, offset) != 0)
      {
         set_sender_error(state);
         break;
      }
      encode_header(&header, buffer_is_zero((unsigned char *)buffer, length)
                             ? MSC_RECORD_HOLE : MSC_RECORD_DATA,
                    0, entry->file_id, offset, length);
      if ((ntohl(header.type) == MSC_RECORD_HOLE
             ? sendall(worker->socketfd, &header, sizeof(header))
             : send_data_record(worker->socketfd, &header, buffer, length)) != 0)
      {
         set_sender_error(state);
         break;
      }
      worker->bytes_sent += length;
      __atomic_fetch_add(&state->current_bytes, length, __ATOMIC_RELAXED);
   }
   if (msc_cancelled()) set_sender_error(state);
   if (!sender_has_error(state))
   {
      if (send_done_record(worker->socketfd) != 0)
         set_sender_error(state);
   }
   if (inputfd >= 0)
      close(inputfd);
   free(buffer);
   return NULL;
}

static void set_receiver_error(struct directory_receiver_state *state)
{
   pthread_mutex_lock(&state->error_lock);
   if (!state->error)
   {
      state->error = 1;
      shutdown_sockets(state->sockets, state->numstreams);
   }
   pthread_mutex_unlock(&state->error_lock);
}

static int receiver_has_error(struct directory_receiver_state *state)
{
   int error;
   pthread_mutex_lock(&state->error_lock);
   error = state->error;
   pthread_mutex_unlock(&state->error_lock);
   return error;
}

int manifest_sync_durable(struct directory_manifest *manifest,
                          const char *destination_root)
{
   size_t i;
   int rootfd;
   /* Flush regular-file data before publishing a checkpoint range. */
   for (i = 0; i < manifest->count; i++)
   {
      struct directory_entry *entry = &manifest->entries[i];
      int fd;
      if (entry->type != MSC_RECORD_FILE) continue;
      fd = open(entry->full_path, O_RDONLY | O_NOFOLLOW);
      if (fd < 0 || fsync(fd) != 0)
      { if (fd >= 0) close(fd); return -1; }
      close(fd);
   }
   /* File fsync does not make its directory entry durable.  Flush every
    * manifest directory as well as the root before the checkpoint can claim
    * bytes nested beneath it. */
   for (i = 0; i < manifest->count; i++)
   {
      struct directory_entry *entry = &manifest->entries[i];
      int fd;
      if (entry->type != MSC_RECORD_DIRECTORY) continue;
      fd = open(entry->full_path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
      if (fd < 0 || fsync(fd) != 0)
      { if (fd >= 0) close(fd); return -1; }
      close(fd);
   }
   rootfd = open(destination_root, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
   if (rootfd < 0 || fsync(rootfd) != 0)
   { if (rootfd >= 0) close(rootfd); return -1; }
   close(rootfd);
   return 0;
}

int manifest_punch_zero_holes(struct directory_manifest *manifest)
{
#if defined(__linux__) && defined(FALLOC_FL_PUNCH_HOLE)
   const size_t scan_bytes = 64U * 1024U;
   unsigned char *buffer = checkmalloc(scan_bytes,
                                       "recursive sparse scan buffer");
   size_t i;
   for (i = 0; i < manifest->file_count; i++)
   {
      struct directory_entry *entry =
         &manifest->entries[manifest->file_entry_indices[i]];
      uint64_t position = 0, hole_start = 0;
      int in_hole = 0;
      int fd = open(entry->full_path, O_RDWR | O_NOFOLLOW);
      if (fd < 0)
      {
         free(buffer);
         return -1;
      }
      while (position < entry->size)
      {
         size_t wanted = entry->size - position < scan_bytes
            ? (size_t)(entry->size - position) : scan_bytes;
         ssize_t got = pread(fd, buffer, wanted, (off_t)position);
         if (got < 0 && errno == EINTR)
            continue;
         if (got != (ssize_t)wanted)
         {
            close(fd);
            free(buffer);
            return -1;
         }
         if (buffer_is_zero(buffer, wanted))
         {
            if (!in_hole)
            {
               hole_start = position;
               in_hole = 1;
            }
         }
         else if (in_hole)
         {
            if (fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                          (off_t)hole_start,
                          (off_t)(position - hole_start)) != 0 &&
                errno != EOPNOTSUPP && errno != ENOSYS && errno != EINVAL)
            {
               close(fd);
               free(buffer);
               return -1;
            }
            in_hole = 0;
         }
         position += wanted;
      }
      if (in_hole &&
          fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                    (off_t)hole_start,
                    (off_t)(entry->size - hole_start)) != 0 &&
          errno != EOPNOTSUPP && errno != ENOSYS && errno != EINVAL)
      {
         close(fd);
         free(buffer);
         return -1;
      }
      close(fd);
   }
   free(buffer);
#else
   (void)manifest;
#endif
   return 0;
}

static int persist_directory_checkpoint(struct directory_receiver_state *state)
{
   struct stat st;
   if (state->checkpoint == NULL) return 0;
   /* Workers may have completed ranges in many files. Flush file content and
    * every manifest directory before publishing their shared range set. */
   if (manifest_sync_durable(state->manifest, state->destination_root) != 0)
      return -1;
   if (lstat(state->destination_root, &st) != 0) return -1;
   state->checkpoint->destination_dev = st.st_dev;
   state->checkpoint->destination_ino = st.st_ino;
   if (msc_checkpoint_write_atomic(state->checkpoint_path, state->checkpoint) != 0)
      return -1;
   state->bytes_since_checkpoint = 0;
   return 0;
}

static void *directory_receiver_thread(void *arg)
{
   struct directory_receiver_worker *worker = arg;
   struct directory_receiver_state *state = worker->state;
   char *buffer = checkmalloc(state->segment_size, "directory receiver buffer");
   uint64_t open_file_id = UINT64_MAX;
   int outputfd = -1;

   while (!receiver_has_error(state) && !msc_cancelled())
   {
      struct directory_record_header header;
      struct directory_entry *entry;

      if (recvall_exact(worker->socketfd, &header, sizeof(header)) != 0 ||
          decode_header(&header) != 0)
      {
         set_receiver_error(state);
         break;
      }
      if (header.type == MSC_RECORD_WORKER_DONE)
         break;
      if ((header.type != MSC_RECORD_DATA && header.type != MSC_RECORD_HOLE) ||
          header.length > state->segment_size)
      {
         set_receiver_error(state);
         break;
      }
      entry = file_entry(state->manifest, header.file_id);
      if (entry == NULL || header.offset > entry->size ||
          header.length > entry->size - header.offset)
      {
         set_receiver_error(state);
         break;
      }
      if (open_file_id != entry->file_id)
      {
         if (outputfd >= 0)
            close(outputfd);
         outputfd = open(entry->full_path, O_WRONLY);
         open_file_id = entry->file_id;
      }
      if (outputfd < 0 ||
          (header.type == MSC_RECORD_DATA &&
           (recvall_exact(worker->socketfd, buffer, header.length) != 0 ||
            pwriteall(outputfd, buffer, header.length, header.offset) != 0)))
      {
         set_receiver_error(state);
         break;
      }
      pthread_mutex_lock(&state->completion_lock);
      /* Serialize range coalescing with the durability barrier.  No worker may
       * add newly written bytes while persist_directory_checkpoint() is
       * fsyncing the files represented by the current range set. */
      entry->received += header.length;
      if (state->checkpoint != NULL)
      {
         state->segments_received++;
         state->bytes_since_checkpoint += header.length;
         if (msc_checkpoint_add_range(state->checkpoint,
                                      entry->logical_base + header.offset,
                                      entry->logical_base + header.offset + header.length) != 0 ||
             ((state->bytes_since_checkpoint >= 64U * 1024U * 1024U ||
               (state->interrupt_after && state->segments_received >= state->interrupt_after)) &&
              persist_directory_checkpoint(state) != 0))
            state->error = 1;
         if (state->interrupt_after && state->segments_received >= state->interrupt_after)
            state->error = 1;
      }
      pthread_mutex_unlock(&state->completion_lock);
      if (state->error) { set_receiver_error(state); break; }
   }
   if (outputfd >= 0)
   {
      fsync(outputfd);
      close(outputfd);
   }
   free(buffer);
   return NULL;
}

void xferdirectory(int sockets[], struct argdata *AD)
{
   struct directory_manifest manifest;
   struct directory_sender_state state;
   struct directory_sender_worker *workers;
   pthread_t *threads;
   struct directory_record_header ack;
   unsigned int created = 0;
   unsigned int i;
   struct msc_checkpoint checkpoint;
   struct msc_progress progress;
   int many_small_files;

   memset(&manifest, 0, sizeof(manifest));
   memset(&state, 0, sizeof(state));
   msc_checkpoint_init(&checkpoint);
   {
      struct stat statbuf;
      if (lstat(AD->sourcefile, &statbuf) != 0 || !S_ISDIR(statbuf.st_mode))
      {
         fprintf(stderr, "MSC recursive source must be a directory: %s\n",
                 AD->sourcefile);
         exit(MSC_EXIT_SOURCE);
      }
      manifest.root_mode = statbuf.st_mode & 07777;
      manifest.root_uid = statbuf.st_uid; manifest.root_gid = statbuf.st_gid;
      manifest.root_mtime_sec = statbuf.st_mtim.tv_sec;
      manifest.root_mtime_nsec = (uint32_t)statbuf.st_mtim.tv_nsec;
   }
   scan_directory(&manifest, AD->sourcefile, "");
   manifest_fingerprint(&manifest);
   many_small_files = manifest.file_count > 10000;
   if (AD->checkpoint_path != NULL)
   {
      int hello_status = msc_recursive_resume_sender_hello(
         sockets[0], AD, &manifest, &checkpoint, AD->packetsize);
      if (hello_status != 0)
      { fprintf(stderr, "MSC recursive resume negotiation failed before manifest transfer\n"); exit(hello_status); }
      state.checkpoint = &checkpoint;
      AD->childinfo->reused = (long long)msc_checkpoint_completed_bytes(&checkpoint);
   }
   msc_progress_start(&progress, AD, &state.current_bytes,
                      (uint64_t)AD->childinfo->reused, manifest.total_size);
   if (send_manifest(sockets[0], &manifest) != 0)
   {
      fprintf(stderr, "MSC failed sending directory manifest\n");
      exit(MSC_EXIT_NETWORK);
   }
   if (AD->checkpoint_path != NULL)
   {
      struct msc_range *verify_ranges = NULL;
      struct manifest_digest_context digest_context;
      uint32_t verify_count = 0;
      int verify_status;
      digest_context.manifest = &manifest;
      digest_context.file_cursor = 0;
      if (manifest_verification_ranges(&manifest, &checkpoint,
                                       &verify_ranges, &verify_count) != 0 ||
          msc_resume_send_digests(sockets[0], verify_ranges, verify_count,
                                  manifest_range_digest,
                                  &digest_context) != 0)
      {
         free(verify_ranges);
         fprintf(stderr, "MSC could not send recursive durable-range digests\n");
         exit(MSC_EXIT_NETWORK);
      }
      free(verify_ranges);
      verify_status = msc_resume_receive_verify_reply(sockets[0]);
      if (verify_status != 0)
      {
         fprintf(stderr,
                 "MSC recursive durable-range verification failed before data transfer\n");
         exit(verify_status);
      }
   }
   state.AD = AD;
   state.manifest = &manifest;
   state.sockets = sockets;
   state.numstreams = AD->numstreams;
   pthread_mutex_init(&state.cursor_lock, NULL);
   pthread_mutex_init(&state.error_lock, NULL);
   workers = checkmalloc(sizeof(*workers) * AD->numstreams, "directory sender workers");
   threads = checkmalloc(sizeof(*threads) * AD->numstreams, "directory sender threads");
   memset(workers, 0, sizeof(*workers) * AD->numstreams);
   for (i = 0; i < AD->numstreams; i++)
   {
      workers[i].state = &state;
      workers[i].socketfd = sockets[i];
      if (pthread_create(&threads[i], NULL, directory_sender_thread, &workers[i]) != 0)
      {
         set_sender_error(&state);
         break;
      }
      created++;
   }
   for (i = 0; i < created; i++)
      pthread_join(threads[i], NULL);
   AD->childinfo->xfercount = 0;
   for (i = 0; i < created; i++)
      AD->childinfo->xfercount += workers[i].bytes_sent;
   if (!state.error &&
       (recvall_exact(sockets[0], &ack, sizeof(ack)) != 0 ||
        decode_header(&ack) != 0 || ack.type != MSC_RECORD_ACK))
      state.error = 1;
   pthread_mutex_destroy(&state.cursor_lock);
   pthread_mutex_destroy(&state.error_lock);
   free(workers);
   free(threads);
   manifest_free(&manifest);
   msc_checkpoint_destroy(&checkpoint);
   msc_progress_finish(&progress, !state.error,
                       many_small_files
                          ? "metadata/small-file overhead likely (more than 10,000 files)"
                          : "unable to determine (source/network/destination timing overlaps)");
   if (state.error)
   {
      fprintf(stderr, "MSC recursive sender failed\n");
      if (msc_cancelled()) return;
      exit(AD->checkpoint_path != NULL ? MSC_EXIT_NETWORK : MSC_EXIT_INTERNAL);
   }
}

void receivedirectory(int numstreams, int sockets[], struct argdata *AD)
{
   struct directory_manifest manifest;
   struct directory_receiver_state state;
   struct directory_receiver_worker *workers;
   pthread_t *threads;
   int created = 0;
   int i;
   struct msc_checkpoint checkpoint;
   uint64_t expected_total = 0;
   const char *fault_phase;

   memset(&manifest, 0, sizeof(manifest));
   memset(&state, 0, sizeof(state));
   msc_checkpoint_init(&checkpoint);
   if (AD->checkpoint_path != NULL)
   {
      int hello_status = msc_recursive_resume_receiver_hello(
         sockets[0], AD, &checkpoint, &expected_total, AD->packetsize);
      if (hello_status != 0)
         exit(hello_status);
      fault_phase = getenv("MSC_TEST_INTERRUPT_PHASE");
      if (fault_phase != NULL && strcmp(fault_phase, "manifest") == 0)
      { fprintf(stderr, "MSC test interruption during manifest phase\n"); exit(MSC_EXIT_NETWORK); }
   }
   {
      int manifest_status = receive_manifest(
         sockets[0], AD->destfile, &manifest, AD->resume);
      if (manifest_status != 0)
      {
         fprintf(stderr, "MSC failed receiving directory manifest\n");
         exit(manifest_status);
      }
   }
   state.manifest = &manifest;
   state.segment_size = AD->packetsize;
   state.sockets = sockets;
   state.numstreams = numstreams;
   if (AD->checkpoint_path != NULL)
   {
      const char *fault = getenv("MSC_TEST_INTERRUPT_AFTER_SEGMENTS");
      state.checkpoint = &checkpoint; state.checkpoint_path = AD->checkpoint_path;
      state.destination_root = AD->destfile;
      if (fault != NULL) state.interrupt_after = strtoull(fault, NULL, 10);
      if (manifest.total_size != expected_total)
      { fprintf(stderr, "MSC recursive manifest byte total changed during negotiation\n"); exit(MSC_EXIT_INTEGRITY); }
      {
         struct msc_range *verify_ranges = NULL;
         struct manifest_digest_context digest_context;
         uint32_t verify_count = 0;
         int verify, verify_status;
         digest_context.manifest = &manifest;
         digest_context.file_cursor = 0;
         if (manifest_verification_ranges(&manifest, &checkpoint,
                                          &verify_ranges, &verify_count) != 0)
         {
            fprintf(stderr, "MSC recursive checkpoint ranges do not map to the manifest\n");
            exit(MSC_EXIT_INCOMPATIBLE);
         }
         verify = msc_resume_verify_digests(sockets[0], verify_ranges,
                                            verify_count,
                                            manifest_range_digest,
                                            &digest_context);
         free(verify_ranges);
         verify_status = verify == 0 ? 0 :
                         verify > 0 ? MSC_EXIT_INTEGRITY : MSC_EXIT_DESTINATION;
         if (msc_resume_send_verify_reply(sockets[0], verify_status) != 0)
            exit(MSC_EXIT_NETWORK);
         if (verify_status != 0)
         {
            fprintf(stderr,
                    "MSC recursive durable bytes do not match the current source\n");
            exit(verify_status);
         }
      }
   }
   pthread_mutex_init(&state.error_lock, NULL);
   pthread_mutex_init(&state.completion_lock, NULL);
   workers = checkmalloc(sizeof(*workers) * numstreams, "directory receiver workers");
   threads = checkmalloc(sizeof(*threads) * numstreams, "directory receiver threads");
   for (i = 0; i < numstreams; i++)
   {
      workers[i].state = &state;
      workers[i].socketfd = sockets[i];
      if (pthread_create(&threads[i], NULL, directory_receiver_thread, &workers[i]) != 0)
      {
         set_receiver_error(&state);
         break;
      }
      created++;
   }
   for (i = 0; i < created; i++)
      pthread_join(threads[i], NULL);
   if (!state.error)
   {
      size_t entry_index;
      for (entry_index = 0; entry_index < manifest.count; entry_index++)
      {
         struct directory_entry *entry = &manifest.entries[entry_index];
         if (entry->type == MSC_RECORD_FILE &&
             (AD->checkpoint_path == NULL
                ? entry->received != entry->size
                : !msc_checkpoint_contains(&checkpoint, entry->logical_base,
                                            entry->logical_base + entry->size)))
         {
            fprintf(stderr, "MSC received incomplete file %s\n",
                    entry->relative_path);
            state.error = 1;
            break;
         }
      }
   }
   if (AD->checkpoint_path != NULL)
   {
      pthread_mutex_lock(&state.completion_lock);
      if (persist_directory_checkpoint(&state) != 0) state.error = 1;
      pthread_mutex_unlock(&state.completion_lock);
   }
   fault_phase = getenv("MSC_TEST_INTERRUPT_PHASE");
   if (!state.error && fault_phase != NULL && strcmp(fault_phase, "metadata") == 0)
   {
      fprintf(stderr, "MSC test interruption before final metadata phase\n");
      state.error = 1;
   }
   if (!state.error && manifest_apply_modes(&manifest, AD) != 0)
      state.error = 1;
   if (!state.error)
   {
      if (AD->checkpoint_path != NULL && !AD->keep_checkpoint &&
          (unlink(AD->checkpoint_path) != 0 ||
           msc_fsync_parent(AD->checkpoint_path) != 0))
      { fprintf(stderr, "MSC could not remove completed recursive checkpoint %s: %s\n", AD->checkpoint_path, strerror(errno)); state.error = 1; }
   }
   if (!state.error)
   {
      if (send_ack_record(sockets[0]) != 0)
         state.error = 1;
   }
   for (i = 0; i < numstreams; i++)
      close(sockets[i]);
   pthread_mutex_destroy(&state.error_lock);
   pthread_mutex_destroy(&state.completion_lock);
   free(workers);
   free(threads);
   manifest_free(&manifest);
   msc_checkpoint_destroy(&checkpoint);
   if (state.error)
   {
      fprintf(stderr, "MSC recursive receiver failed\n");
      if (msc_cancelled()) exit(msc_cancel_exit_code());
      exit(MSC_EXIT_NETWORK);
   }
}

/* ---- shared manifest API (used by the UDP recursive path) --------------- */

int manifest_apply_modes(struct directory_manifest *manifest, struct argdata *AD)
{
   size_t entry_index;
   struct timespec times[2];

   /* Files first, then directories in reverse preorder (children before
    * parents), then the root.  This avoids removing directory write/search
    * permission before all descendants have been finalized. */
   for (entry_index = 0; entry_index < manifest->count; entry_index++)
   {
      struct directory_entry *entry = &manifest->entries[entry_index];
      if (entry->type == MSC_RECORD_FILE && chmod(entry->full_path, entry->mode) != 0)
      {
         fprintf(stderr, "MSC could not apply file mode to %s: %s\n",
                 entry->relative_path, strerror(errno));
         return -1;
      }
   }
   /* Ownership is privileged. Preserve it when authorized; an unprivileged
    * receiver deliberately leaves ownership as the receiving user. */
   if (geteuid() == 0)
      for (entry_index = 0; entry_index < manifest->count; entry_index++)
      {
         struct directory_entry *entry = &manifest->entries[entry_index];
         if (lchown(entry->full_path, entry->uid, entry->gid) != 0)
         { fprintf(stderr, "MSC could not apply ownership to %s: %s\n", entry->relative_path, strerror(errno)); return -1; }
      }
   /* File/link timestamps before directories. Symlinks are never followed. */
   for (entry_index = 0; entry_index < manifest->count; entry_index++)
   {
      struct directory_entry *entry = &manifest->entries[entry_index];
      if (entry->type == MSC_RECORD_DIRECTORY) continue;
      times[0].tv_sec = entry->mtime_sec; times[0].tv_nsec = entry->mtime_nsec;
      times[1] = times[0];
      if (utimensat(AT_FDCWD, entry->full_path, times, AT_SYMLINK_NOFOLLOW) != 0)
      { fprintf(stderr, "MSC could not apply timestamp to %s: %s\n", entry->relative_path, strerror(errno)); return -1; }
   }
   for (entry_index = manifest->count; entry_index > 0; entry_index--)
   {
      struct directory_entry *entry = &manifest->entries[entry_index - 1];
      if (entry->type == MSC_RECORD_DIRECTORY && chmod(entry->full_path, entry->mode) != 0)
      {
         fprintf(stderr, "MSC could not apply directory mode to %s: %s\n",
                 entry->relative_path, strerror(errno));
         return -1;
      }
      if (entry->type == MSC_RECORD_DIRECTORY)
      {
         times[0].tv_sec = entry->mtime_sec; times[0].tv_nsec = entry->mtime_nsec;
         times[1] = times[0];
         if (utimensat(AT_FDCWD, entry->full_path, times, AT_SYMLINK_NOFOLLOW) != 0)
         { fprintf(stderr, "MSC could not apply directory timestamp to %s: %s\n", entry->relative_path, strerror(errno)); return -1; }
      }
   }
   times[0].tv_sec = manifest->root_mtime_sec;
   times[0].tv_nsec = manifest->root_mtime_nsec;
   times[1] = times[0];
   if (utimensat(AT_FDCWD, AD->destfile, times, AT_SYMLINK_NOFOLLOW) != 0)
   { fprintf(stderr, "MSC could not apply destination root timestamp: %s\n", strerror(errno)); return -1; }
   if (geteuid() == 0 && lchown(AD->destfile, manifest->root_uid, manifest->root_gid) != 0)
   { fprintf(stderr, "MSC could not apply destination root ownership: %s\n", strerror(errno)); return -1; }
   if (chmod(AD->destfile, manifest->root_mode) != 0)
   {
      fprintf(stderr, "MSC could not apply destination root mode: %s\n",
              strerror(errno));
      return -1;
   }
   return 0;
}

struct directory_manifest *manifest_build_source(struct argdata *AD)
{
   struct directory_manifest *manifest = checkmalloc(sizeof(*manifest),
                                                     "directory manifest");
   struct stat statbuf;

   memset(manifest, 0, sizeof(*manifest));
   if (lstat(AD->sourcefile, &statbuf) != 0 || !S_ISDIR(statbuf.st_mode))
   {
      fprintf(stderr, "MSC recursive source must be a directory: %s\n",
              AD->sourcefile);
      exit(MSC_EXIT_SOURCE);
   }
   manifest->root_mode = statbuf.st_mode & 07777;
   manifest->root_uid = statbuf.st_uid; manifest->root_gid = statbuf.st_gid;
   manifest->root_mtime_sec = statbuf.st_mtim.tv_sec;
   manifest->root_mtime_nsec = (uint32_t)statbuf.st_mtim.tv_nsec;
   scan_directory(manifest, AD->sourcefile, "");
   manifest_fingerprint(manifest);
   return manifest;
}

int manifest_send_all(int fd, struct directory_manifest *manifest)
{
   return send_manifest(fd, manifest);
}

struct directory_manifest *manifest_recv_all_status(
   int fd, struct argdata *AD, int *status)
{
   struct directory_manifest *manifest = checkmalloc(sizeof(*manifest),
                                                     "directory manifest");
   int receive_status;
   memset(manifest, 0, sizeof(*manifest));
   receive_status = receive_manifest(
      fd, AD->destfile, manifest, AD->resume);
   if (receive_status != 0)
   {
      fprintf(stderr, "MSC failed receiving directory manifest\n");
      manifest_free(manifest);
      free(manifest);
      if (status != NULL) *status = receive_status;
      return NULL;
   }
   if (status != NULL) *status = 0;
   return manifest;
}

struct directory_manifest *manifest_recv_all(int fd, struct argdata *AD)
{
   return manifest_recv_all_status(fd, AD, NULL);
}

uint64_t manifest_file_count(struct directory_manifest *manifest)
{
   return manifest->file_count;
}

const char *manifest_file_path(struct directory_manifest *manifest, uint64_t id)
{
   struct directory_entry *entry = file_entry(manifest, id);
   return entry == NULL ? NULL : entry->full_path;
}

uint64_t manifest_file_size(struct directory_manifest *manifest, uint64_t id)
{
   struct directory_entry *entry = file_entry(manifest, id);
   return entry == NULL ? 0 : entry->size;
}

uint64_t manifest_file_logical_base(struct directory_manifest *manifest,
                                    uint64_t id)
{
   struct directory_entry *entry = file_entry(manifest, id);
   return entry == NULL ? UINT64_MAX : entry->logical_base;
}

uint64_t manifest_total_size(struct directory_manifest *manifest)
{
   return manifest->total_size;
}

uint64_t manifest_hash(struct directory_manifest *manifest)
{
   return manifest->manifest_hash;
}

void manifest_destroy(struct directory_manifest *manifest)
{
   manifest_free(manifest);
   free(manifest);
}
