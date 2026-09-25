#define _FILE_OFFSET_BITS 64
/* release version, reported by `msc --version` and the usage text */
#define MSC_VERSION "1.0.0"
/* program defaults */
#define DEFAULT_STREAMS 30
#define MAX_STREAMS 2048
#define DEFAULT_PORT    16400
#define DEFAULT_PACKETSIZE (1024*1024)
/* Lustre's own LOV_MAX_STRIPE_COUNT; the CLI bound for --dest-stripe-count, so a
 * typo fails at parse time rather than as a Lustre error after the ssh launch. */
#define MAX_DEST_STRIPE_COUNT 2000
#define DEFAULT_STALL_TIMEOUT_MS 30000U
#define DEFAULT_RECONNECT_INTERVAL_MS 10000U
#define ACCEPT_TIMEOUT_SECONDS 120
#define MAX_INTERPROCESS_BUFFER_SIZE 1024*1024*2  /* 2MB, legacy pipe path */
#define TCP_SOCKET_BUFFER (16*1024*1024) /* best-effort SO_SNDBUF/SO_RCVBUF */
#define PIPE_PARENT_READ  0
#define PIPE_CHILD_WRITE  1
#define PIPE_CHILD_READ   2
#define PIPE_PARENT_WRITE 3


#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <signal.h>
#include <pthread.h>

#define MSC_SEGMENT_MAGIC 0x4d534331U
#define MSC_SEGMENT_ACK 1U
#define MSC_RESUME_MAGIC 0x4d535252U
#define MSC_RESUME_REPLY_MAGIC 0x4d535250U
#define MSC_UDP_DURABLE_MAGIC 0x4d535544U
#define MSC_DIRECTORY_MAGIC 0x4d534432U
#define MSC_DIRECTORY_VERSION 2

#define MSC_RECORD_DIRECTORY 1
#define MSC_RECORD_FILE 2
#define MSC_RECORD_MANIFEST_DONE 3
#define MSC_RECORD_DATA 4
#define MSC_RECORD_WORKER_DONE 5
#define MSC_RECORD_ACK 6
#define MSC_RECORD_SYMLINK 7
#define MSC_RECORD_HARDLINK 8
#define MSC_RECORD_HOLE 9

/* Public process exit status contract.  Keep these values stable: scripts and
 * the SSH parent use them to distinguish permanent from retryable failures. */
enum msc_exit_code
{
   MSC_EXIT_OK = 0,
   MSC_EXIT_CLI = 2,
   MSC_EXIT_SOURCE = 3,
   MSC_EXIT_DESTINATION = 4,
   MSC_EXIT_AUTH = 5,
   MSC_EXIT_NETWORK = 6,
   MSC_EXIT_INTEGRITY = 7,
   MSC_EXIT_INCOMPATIBLE = 8,
   MSC_EXIT_INTERNAL = 70,
   MSC_EXIT_SIGINT = 130,
   MSC_EXIT_SIGTERM = 143
};

#define MSC_CHECKPOINT_VERSION 2U
#define MSC_CHECKPOINT_MAX_RANGES 1048576U
#define MSC_SHA256_BYTES 32U
#define MSC_RESUME_DIGEST_MAGIC 0x4d534448U
#define MSC_RESUME_VERIFY_MAGIC 0x4d535648U

enum msc_progress_mode
{
   MSC_PROGRESS_AUTO = 0,
   MSC_PROGRESS_ALWAYS = 1,
   MSC_PROGRESS_NEVER = 2
};

struct msc_range
{
   /* Durable coverage is represented as a half-open interval [start, end).
    * Keeping ranges sorted, disjoint, and coalesced makes both containment
    * checks and the on-disk/wire bounds deterministic. */
   uint64_t start;
   uint64_t end;
};

struct msc_checkpoint
{
   uint32_t version;
   uint32_t recursive;
   uint64_t segment_size;
   uint64_t source_dev;
   uint64_t source_ino;
   uint64_t source_size;
   int64_t source_mtime_sec;
   int64_t source_mtime_nsec;
   uint64_t destination_dev;
   uint64_t destination_ino;
   uint64_t destination_size;
   int64_t destination_mtime_sec;
   int64_t destination_mtime_nsec;
   uint64_t manifest_hash;
   /* A range is relative to this exact transfer geometry.  Binding all three
    * values prevents a valid checkpoint for one slice from being reused for a
    * different source or destination slice of the same inode. */
   uint64_t source_offset;
   uint64_t destination_offset;
   uint64_t transfer_size;
   char *source_path;
   char *destination_path;
   struct msc_range *ranges;
   uint32_t range_count;
   uint32_t range_capacity;
};

struct msc_progress
{
   volatile uint64_t *current_bytes;
   uint64_t reused_bytes;
   uint64_t total_bytes;
   uint64_t interval_ms;
   uint64_t start_ns;
   unsigned int retry_count;
   volatile int stop;
   int enabled;
   pthread_t thread;
};

/* --- UDP transport (-U) --------------------------------------------------- */
#define DEFAULT_UDP_PAYLOAD 1400      /* default file-data bytes per datagram */
#define DEFAULT_UDP_FLOWS 8           /* MSC UDP transport default */
/* Workload-shaped implicit -n defaults, chosen from a stream-count sweep on a
 * Lustre scratch file system (3 reps per setting). Recursive transfers scale to 64 with no
 * measured penalty on a 12288-small-file tree and +27% (TCP) / +129% (UDP)
 * on an 8x1GB tree; single-file TCP peaks at 8 streams (+17..23% vs the
 * flat 30); single-file UDP is best at 30 flows (the only setting beating the
 * old 8-flow default at every size, +21..29%). Pipe mode (no -i) was not
 * measured and keeps DEFAULT_STREAMS. Explicit -n always wins. */
#define DEFAULT_TREE_STREAMS 64       /* -R, TCP and UDP */
#define DEFAULT_SINGLE_STREAMS 8      /* single-file TCP */
#define DEFAULT_UDP_SINGLE_FLOWS 30   /* single-file UDP */
#define MAX_UDP_FLOWS 256             /* MSC UDP protocol limit */

struct segment_header
{
   /* A zero length is the per-stream end marker.  For data records, offset is
    * relative to this transfer (destination offsets are applied separately).
    * Every field is sent in network byte order, including the 64-bit fields. */
   uint32_t magic;
   uint32_t reserved;
   uint64_t offset;
   uint64_t length;
};

struct msc_resume_hello
{
   /* Resume negotiation runs on stream zero before any payload is accepted.
    * flags bit 0 means "resume existing state"; bit 1 selects the recursive
    * variant.  The remaining streams wait for negotiation to succeed. */
   uint32_t magic;
   uint32_t version;
   uint32_t flags;
   uint32_t path_length;
   uint64_t segment_size;
   uint64_t transfer_size;
   uint64_t source_dev;
   uint64_t source_ino;
   uint64_t source_size;
   uint64_t source_mtime_sec;
   uint64_t source_mtime_nsec;
   uint64_t manifest_hash;
   uint64_t source_offset;
   uint64_t destination_offset;
} __attribute__((packed));

struct msc_resume_reply
{
   uint32_t magic;
   uint32_t version;
   uint32_t status;
   uint32_t range_count;
} __attribute__((packed));

struct msc_wire_range
{
   uint64_t start;
   uint64_t end;
} __attribute__((packed));

struct msc_wire_digest
{
   uint32_t magic;
   uint32_t version;
   uint64_t start;
   uint64_t end;
   unsigned char sha256[MSC_SHA256_BYTES];
} __attribute__((packed));

struct msc_resume_verify_reply
{
   uint32_t magic;
   uint32_t version;
   uint32_t status;
   uint32_t reserved;
} __attribute__((packed));

struct msc_udp_durable_ack
{
   /* Receiver-to-sender barrier after each application-level UDP chunk.
    * A success means the bytes through durable_end were fsynced and the
    * atomically replaced checkpoint is durable in its parent directory. */
   uint32_t magic;
   uint32_t status;
   uint64_t durable_end;
} __attribute__((packed));

struct directory_record_header
{
   /* The manifest is sent on stream zero, then DATA/HOLE records may arrive
    * independently on every stream.  file_id and offset let receivers use
    * pwrite without depending on cross-stream arrival order. */
   uint32_t magic;
   uint32_t version;
   uint32_t type;
   uint32_t mode;
   uint64_t file_id;
   uint64_t offset;
   uint64_t length;
   uint64_t mtime_sec;
   uint32_t mtime_nsec;
   uint32_t uid;
   uint32_t gid;
   uint32_t link_length;
} __attribute__((packed));

_Static_assert(sizeof(struct segment_header) == 24, "segment wire size changed");
_Static_assert(sizeof(struct directory_record_header) == 64, "directory wire size changed");
_Static_assert(sizeof(struct msc_resume_hello) == 96, "resume hello wire size changed");
_Static_assert(sizeof(struct msc_wire_digest) == 56, "resume digest wire size changed");
_Static_assert(sizeof(struct msc_resume_verify_reply) == 16, "resume verify wire size changed");
_Static_assert(sizeof(struct msc_udp_durable_ack) == 16, "UDP durable ACK wire size changed");

/* directory_record_header is shared by manifest and payload records.  For a
 * manifest entry, offset carries the declared file size and length carries the
 * relative-path byte count.  For MSC_RECORD_DATA they have their usual byte
 * offset/payload-length meanings.  The record type is therefore part of the
 * field interpretation, not merely a tag. */


/* control information, parsed from arguments */
struct argdata
{
   /* This structure is both CLI state and the in-process contract between the
    * launcher, sender, and receiver paths.  It is not itself serialized; the
    * private -y command in local_single.c forwards an explicit positional
    * subset to the SSH-launched peer.  Keep that encoder and parse_remote()
    * synchronized when adding a remotely relevant option. */
   unsigned int portnum;       /*-p port number*/
   unsigned int finalport;     /*port actually allocated */
   /* MSC_UDP_CTL=stdio|stdio1 only: the local ends of the pipes this process
    * spawned ssh with.  They ARE the control channel in those modes, so no
    * port is dialed and none is listened on.  -1 when unused. */
   int udp_ctl_rfd;
   int udp_ctl_wfd;
   unsigned int numstreams;    /*-n streams/workers for file transfers */
   unsigned int packetsize;    /*-s segment size for file transfers */
   char **remote_machines;     /*from -r */
   char **local_machines;      /*from -l */
   char *remote_program;       /*program to run on remote */
   char *remote_binary;        /*-B: msc binary to launch on the remote side;
                                 defaults to "msc" (found on PATH). Set this to an
                                 absolute path (as the benchmark does, per the
                                 destination's configured path) so msc need not be
                                 on the remote's non-interactive PATH. */
   char *remote_user;          /*user on remote sysetem */
   char *sourcefile;           /*-i source file.  if null, use stdin */
   char *destfile;             /*-o destination file output */
   int recursive;              /*-R recursively transfer a directory */
   int udp;                    /* use the reliable-UDP data path: the default,
                                * -T selects TCP, -U spells the default out */
   int udp_explicit;           /* -U was typed, so an unsupported transfer shape
                                * is a CLI error rather than a TCP fallback */
   int numstreams_explicit;    /* -n was supplied; selects UDP vs TCP defaults */
   int packetsize_explicit;    /* -s was supplied; selects UDP vs TCP defaults */
   /* Workload-shape facts for adaptive default tuning; probed on the
    * INITIATING side only (parse.c probe_workload_shape), -1 = not probed/
    * unknown. Meaningless (zero) in -y remote mode. */
   long long shape_single_bytes;   /* single-file source size from stat() */
   long shape_toplevel_entries;    /* -R source's shallow top-level entry count */
   long shape_stripe_count;        /* Lustre source stripe count; -1 off Lustre,
                                    * no explicit layout, or non-LUSTRE build */
   char *interface_host;       /* -I: MSC UDP data/control connect host */
   unsigned int port_tries;    /* --port-tries receiver control-port range */
   /* -U data-port footprint. All
    * four are plumbed to the UDP engine (and to the ssh-launched receiver) as
    * MSC_UDP_PORTS / MSC_UDP_PORT_BASE / MSC_UDP_PORT_SPAN / MSC_UDP_PORT_LIST;
    * unset means the engine's default: min(-n, 8) ports scanned from 17400. */
   unsigned int udp_data_ports; /* --udp-data-ports: K data sockets for -n flows */
   unsigned int udp_port_base;  /* --udp-port-base: first port scanned */
   unsigned int udp_port_span;  /* --udp-port-span: window width; 0 = exact block */
   int udp_port_span_explicit;  /* --udp-port-span was supplied (0 is meaningful) */
   char *udp_port_list;         /* --udp-ports: exact ports, e.g. "20000,20004-20007" */
   /* --dest-stripe-count: Lustre OST width for the DESTINATION file, independent
    * of the source's layout. 0 = not requested (the destination keeps matching
    * the source, as before), >0 = exact count, -1 = every OST. parse.c resolves
    * it once and re-exports it as MSC_DEST_STRIPE_COUNT so the ssh-launched
    * receiver, the -x findzero probe and per-pair children cannot disagree with
    * the UDP greeting. Never changes the transport's source geometry. */
   long dest_stripe_count;
   int force;                  /* --force atomically replaces one UDP file */
   int gso;                    /* MSC UDP_SEGMENT; on by default, --no-gso disables */
   int gro;                    /* --gro enable MSC UDP_GRO */
   int stats;                  /* --stats enable MSC UDP telemetry */
   int no_internal_checksum;   /* default 1; --checksum=true clears it */
   int resume;                 /* --resume: require and validate checkpoint */
   char *checkpoint_path;      /* --checkpoint: receiver-side checkpoint */
   int keep_checkpoint;        /* retain a completed checkpoint */
   unsigned int retries;       /* whole-session retries */
   unsigned int retry_current; /* current whole-session attempt */
   uint64_t retry_delay_ms;    /* initial bounded-backoff delay */
   int progress_mode;          /* enum msc_progress_mode */
   uint64_t progress_interval_ms;
   uint64_t stall_timeout_ms;  /* maximum interval without network progress */
   uint64_t reconnect_interval_ms; /* SSH probe cadence after a resumable stall */
   int my_ost_start;           /* multi-machine Lustre slice metadata */
   int my_ost_count;
   int  multicopy;             /* flag: 0 if this is single machine copy */
   off_t src_offset;   /*offset*/
   off_t dst_offset;   /*offset*/
   long long int sourcesize;   /*size of source file */
   long long int xferlen;      /*amount of data to transfer*/
   long long int destskip;     /*where to start in remote file. -1 if end*/
   long long int lastgood;     /*last good data in file chunk */
   int obeylastgood;           /*if not zero, then obey lastgood for offset*/
   ino_t inode;                /* inode check for destination file */
   unsigned int numsets;       /* number of local and remote sets */
   unsigned int parentsets;    /* number of sets from parent */
   FILE * outfileid;           /* output file id */
   char * version;             /* program version */
   struct jobinfo *childinfo;  /* array of child info structures */
        /* globals for debug */
};

struct jobinfo
{
   int fd_child_write;        /* file descriptor */
   int fd_child_read;         /* file descriptor */
   int fd_parent_write;       /* file descriptor */
   int fd_parent_read;        /* file descriptor */
   char *localmachine;        /* name of local machine */
   char *remotemachine;       /* name of remote machine */
   FILE *instr;               /* stream for file descriptor */
   long long int xfercount;   /* bytes transferred */
   int doneread;              /* 0 if still waiting for read */
   long long int reused;      /* durable bytes reused from a checkpoint */
   int pid;                   /* job process id */
   int reaped;                /* nonzero once waitpid() collected this child */
   int exit_status;           /* raw waitpid status, valid when reaped */
   off_t startloc;            /* starting location of this section */
   off_t xferlen;             /* bytes to transfer for this section */
   off_t chunksize;           /* actually the same for all jobs, convenience*/
};

extern int msc_debug;
extern FILE *msc_debugout;

/* Fork helpers call exactly one of these signatures in the child.  Indexed
 * callbacks receive a child/job slot, not an untyped auxiliary pointer. */
typedef void (*msc_child_fn)(struct argdata *);
typedef void (*msc_indexed_child_fn)(struct argdata *, int);

/* global routines */
extern void parseargs(int, char**, struct argdata *);
extern void print_manual(void);  /* man.c: `msc man` long-form manual */
extern void makechild(msc_child_fn, msc_child_fn, struct argdata *, char *);
extern void local_msc(struct argdata *);
extern void *checkmalloc(size_t, const char *);
extern pid_t makechild_inline(msc_indexed_child_fn, struct argdata *, int,
                              char *);
extern void get_source_filesize(struct argdata *);
extern void multimachine_setup_boundaries(struct argdata *);
extern void xferdirectory(int *, struct argdata *);
extern void receivedirectory(int, int *, struct argdata *);
extern void readsocket_segmented_resume(int, int *, struct argdata *);

/* shared socket + byte-order helpers (netutil.c) */
extern uint64_t host_to_network_64(uint64_t value);
extern uint64_t network_to_host_64(uint64_t value);
extern int sendall(int fd, const void *buffer, size_t size);
extern int recvall_exact(int fd, void *buffer, size_t size);
extern int preadall(int fd, void *buffer, size_t size, off_t offset);
extern int pwriteall(int fd, const void *buffer, size_t size, off_t offset);
extern int pwritevall(int fd, struct iovec *iov, int iovcnt, off_t offset);
extern int msc_configure_tcp_socket(int fd, uint64_t timeout_ms);
extern const char *msc_ssh_program(void);

/* SSH connect/keepalive budgets, scaled by the declared path class (see
 * msc_ssh_liveness() in netutil.c).  Every launch of the SSH client -- the
 * receiver launch and the reconnect probe -- takes its -o values from here so a
 * long path is configured in exactly one place. */
struct msc_ssh_liveness
{
   int connect_timeout_s;   /* -o ConnectTimeout */
   int alive_interval_s;    /* -o ServerAliveInterval */
   int alive_count;         /* -o ServerAliveCountMax */
};
/* The two callers want opposite things and must not share a budget: the launch
 * session carries a live transfer and has to tolerate the data path starving
 * it, while the reconnect probe is a liveness poll that should give up fast. */
enum msc_ssh_purpose
{
   MSC_SSH_LAUNCH = 0,      /* the session the transfer runs over */
   MSC_SSH_PROBE  = 1       /* the reconnect loop's is-it-back-yet check */
};
extern void msc_ssh_liveness(struct msc_ssh_liveness *out, enum msc_ssh_purpose purpose);

/* reliable-UDP transport (udp_transport.c) */
extern void udp_send(struct argdata *AD);
extern void udp_receive(struct argdata *AD, int controlfd);
extern int msc_udp_env_forwardable(const char *name, size_t length);

/* directory manifest API (recursive.c), shared by the TCP and UDP paths.
 * The manifest struct stays private to recursive.c; callers hold an opaque
 * pointer and use these accessors. */
struct directory_manifest;
extern struct directory_manifest *manifest_build_source(struct argdata *AD);
extern int manifest_send_all(int fd, struct directory_manifest *m);
extern struct directory_manifest *manifest_recv_all(int fd, struct argdata *AD);
extern struct directory_manifest *manifest_recv_all_status(
   int fd, struct argdata *AD, int *status);
extern uint64_t manifest_file_count(struct directory_manifest *m);
extern const char *manifest_file_path(struct directory_manifest *m, uint64_t id);
extern uint64_t manifest_file_size(struct directory_manifest *m, uint64_t id);
extern uint64_t manifest_file_logical_base(struct directory_manifest *m,
                                           uint64_t id);
extern uint64_t manifest_total_size(struct directory_manifest *m);
extern uint64_t manifest_hash(struct directory_manifest *m);
extern int manifest_apply_modes(struct directory_manifest *m, struct argdata *AD);
extern int manifest_sync_durable(struct directory_manifest *m,
                                 const char *destination_root);
extern int manifest_punch_zero_holes(struct directory_manifest *m);
extern int manifest_send_checkpoint_digests(
   int fd, struct directory_manifest *m, const struct msc_checkpoint *cp);
extern int manifest_verify_checkpoint_digests(
   int fd, struct directory_manifest *m, const struct msc_checkpoint *cp);
extern int manifest_send_range_digests(int fd, struct directory_manifest *m,
                                       uint64_t start, uint64_t end);
extern int manifest_verify_range_digests(int fd, struct directory_manifest *m,
                                         uint64_t start, uint64_t end);
extern int msc_recursive_resume_sender_hello(
   int fd, struct argdata *AD, struct directory_manifest *m,
   struct msc_checkpoint *cp, uint64_t checkpoint_granularity);
extern int msc_recursive_resume_receiver_hello(
   int fd, struct argdata *AD, struct msc_checkpoint *cp,
   uint64_t *transfer_size, uint64_t checkpoint_granularity);
extern void manifest_destroy(struct directory_manifest *m);

/* checkpoint/resume.c */
extern void msc_checkpoint_init(struct msc_checkpoint *cp);
extern void msc_checkpoint_destroy(struct msc_checkpoint *cp);
extern int msc_checkpoint_add_range(struct msc_checkpoint *cp,
                                    uint64_t start, uint64_t end);
extern uint64_t msc_checkpoint_completed_bytes(const struct msc_checkpoint *cp);
extern int msc_checkpoint_contains(const struct msc_checkpoint *cp,
                                   uint64_t start, uint64_t end);
extern int msc_checkpoint_write_atomic(const char *path,
                                       const struct msc_checkpoint *cp);
extern int msc_checkpoint_read(const char *path, struct msc_checkpoint *cp,
                               char *error, size_t error_size);
extern int msc_checkpoint_capture_source(struct msc_checkpoint *cp,
                                         const char *source,
                                         const char *destination,
                                         uint64_t segment_size,
                                         int recursive);
extern int msc_checkpoint_validate_source(const struct msc_checkpoint *cp,
                                          const char *source,
                                          const char *destination,
                                          char *error, size_t error_size);
extern char *msc_default_checkpoint_path(const char *destination);
extern int msc_checkpoint_capture_destination(struct msc_checkpoint *cp,
                                              const char *path);
extern int msc_checkpoint_validate_destination(const struct msc_checkpoint *cp,
                                               const char *path,
                                               char *error, size_t error_size);
extern int msc_fsync_parent(const char *path);
typedef int (*msc_resume_digest_fn)(void *context, uint64_t start,
                                    uint64_t end,
                                    unsigned char digest[MSC_SHA256_BYTES]);
struct msc_file_digest_context
{
   int fd;
   uint64_t base;
};
extern int msc_resume_file_digest(void *context, uint64_t start, uint64_t end,
                                  unsigned char digest[MSC_SHA256_BYTES]);
extern int msc_resume_send_digests(int fd, const struct msc_range *ranges,
                                   uint32_t range_count,
                                   msc_resume_digest_fn digest_fn,
                                   void *digest_context);
/* Return 0 for an exact byte match, 1 for a content mismatch, and -1 for a
 * transport/read failure.  All records are consumed before mismatch returns,
 * so a peer sending many ranges cannot deadlock on a full control socket. */
extern int msc_resume_verify_digests(int fd, const struct msc_range *ranges,
                                     uint32_t range_count,
                                     msc_resume_digest_fn digest_fn,
                                     void *digest_context);
extern int msc_resume_send_verify_reply(int fd, int status);
extern int msc_resume_receive_verify_reply(int fd);

/* cancellation.c */
extern volatile sig_atomic_t msc_cancel_signal;
extern void msc_install_signal_handlers(void);
extern int msc_cancelled(void);
extern int msc_cancel_exit_code(void);
extern int msc_interruptible_delay(uint64_t milliseconds);
extern int msc_progress_start(struct msc_progress *, struct argdata *,
                              volatile uint64_t *, uint64_t, uint64_t);
extern void msc_progress_finish(struct msc_progress *, int, const char *);


#define DEBUG0(A) if(msc_debug){fprintf(msc_debugout,A);fflush(msc_debugout);}
#define DEBUG1(A,B) if(msc_debug){fprintf(msc_debugout,A,B);fflush(msc_debugout);}
#define DEBUG2(A,B,C) if(msc_debug){fprintf(msc_debugout,A,B,C);fflush(msc_debugout);}
#define DEBUG3(A,B,C,D) if(msc_debug){fprintf(msc_debugout,A,B,C,D);fflush(msc_debugout);}
#define DEBUGSYNC if(msc_debug)fflush(msc_debugout)
