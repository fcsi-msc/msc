#ifndef MSC_UDP_H
#define MSC_UDP_H
/*
 * udp_session.h - MSC's multithreaded reliable-UDP session API.
 *
 * Data is partitioned across independent flows. Each flow has a worker thread,
 * sliding send window, cumulative ACK and SACK recovery, congestion window,
 * receiver window, adaptive retransmission timer, and pacing state. A reliable
 * control channel carries session negotiation and final byte accounting.
 */
#define _FILE_OFFSET_BITS 64

#include <stdint.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <signal.h>

/* ----- identity / defaults ------------------------------------------------ */
#define MSC_UDP_VERSION_STRING "MSC reliable UDP 1"
#define MSC_UDP_PROTO_VERSION  6              /* v4: repeated table transfers plus
                                               * typed receiver failure records
                                               * v5: greeting carries the requested
                                               * destination stripe count
                                               * v6: sender HELLO carries a random
                                               * first transfer id */
#define MSC_UDP_MAGIC          0x4d534331U   /* "MSC1": protocol signature on every datagram */

#define MSC_UDP_DEFAULT_FLOWS 8           /* the measured sweet spot on a fast LAN */
#define MSC_UDP_MAX_FLOWS     256
/* Default first data port scanned (avoid msc's 16400 TCP control range). It
 * must stay BELOW net.ipv4.ip_local_port_range (32768 by default) or the scan
 * competes with the kernel's ephemeral allocator for the same numbers and the
 * window stops being predictable. */
#define MSC_UDP_DEFAULT_PORT     17400

/* default file-data bytes per datagram (one payload). Tunable with -s; an explicit
 * value must fit one IPv4 datagram after the packet header. */
#define MSC_UDP_DEFAULT_PAYLOAD_SIZE    1400
#define MSC_UDP_MAX_DATAGRAM     65507        /* 65535 - 20 (IP) - 8 (UDP) */

/* ===== data-port selection (port_spec.c) =================================
 * The receiver owns the choice of UDP data ports; the sender learns the
 * outcome from the MSC_UDP_CTL_PORTS record and verifies it against its own
 * copy of the request (both ends see the same env). Four ways to choose,
 * one of them the default:
 *
 *   LIST   --udp-ports 20000,20004,20100-20103  exactly these, in this order
 *   BLOCK  --udp-port-span 0                    exactly base..base+K-1
 *   SCAN   --udp-port-base B                    K free ports drawn from [B, B+span)
 *   default                                     SCAN from MSC_UDP_DEFAULT_PORT
 *
 * The two exact modes refuse to start when a requested port is busy; SCAN
 * skips busy ports and only fails once the window is exhausted. That is what
 * lets several users share a machine without coordinating: the data sockets
 * bind WITHOUT SO_REUSEADDR, so EADDRINUSE is a real exclusion and two
 * receivers scanning the same window simply interleave onto disjoint ports.
 * Never set SO_REUSEADDR/SO_REUSEPORT on them; it would silently break this.
 *
 * Env (forwarded to the ssh-launched receiver by msc_udp_env_forwardable):
 *   MSC_UDP_PORTS=K          socket count (default: min(flows, 8))
 *   MSC_UDP_PORT_BASE=B      first port scanned (default MSC_UDP_DEFAULT_PORT)
 *   MSC_UDP_PORT_SPAN=S      window width; 0 selects BLOCK (default 4*K)
 *   MSC_UDP_PORT_LIST=spec   exact ports; selects LIST */
#define MSC_UDP_DEFAULT_DATA_PORTS 8          /* default K ceiling: min(flows, this) */
#define MSC_UDP_PORT_SPAN_MULT     4          /* default span = MULT * K, i.e. how many
                                           * concurrent transfers of this shape fit
                                           * in the window before one has to fail */
#define MSC_UDP_PORT_SPAN_MAX      1024
#define MSC_UDP_PORT_ERRLEN        256        /* caller-supplied error buffer size */
#define MSC_UDP_PORT_TEXTLEN       4096       /* caller-supplied port-list text size */

enum msc_port_mode
{
   MSC_PORT_MODE_SCAN  = 0,   /* [base, base+span), first K that bind */
   MSC_PORT_MODE_BLOCK = 1,   /* exactly base..base+K-1 */
   MSC_PORT_MODE_LIST  = 2    /* exactly `list`, in order */
};

struct msc_port_spec
{
   int mode;
   unsigned int base;          /* SCAN/BLOCK: first port */
   unsigned int span;          /* SCAN: window width (resolved) */
   unsigned int *list;         /* LIST: owned by the spec */
   int nlist;
   int count;                  /* resolved socket count K */
   int count_requested;        /* 0 = not asked for */
   int base_explicit;          /* base came from the user, not the default */
   int span_explicit;
   unsigned int span_requested;
   int count_clamped;          /* resolve() reduced an explicit K to the flow count */
};

/* All of these are pure: no sockets, no env, no printing. Errors are written
 * into the caller's buffer so the CLI and the engine report identically. */
extern void msc_port_spec_init(struct msc_port_spec *spec);
extern void msc_port_spec_clear(struct msc_port_spec *spec);
extern int msc_port_spec_set_list(struct msc_port_spec *spec, const char *text,
                                  char *err, size_t errlen);
extern int msc_port_spec_set_base(struct msc_port_spec *spec, const char *text,
                                  char *err, size_t errlen);
extern int msc_port_spec_set_span(struct msc_port_spec *spec, const char *text,
                                  char *err, size_t errlen);
extern int msc_port_spec_set_count(struct msc_port_spec *spec, const char *text,
                                   char *err, size_t errlen);
/* Read MSC_UDP_PORTS/PORT_BASE/PORT_SPAN/PORT_LIST into a fresh spec. */
extern int msc_port_spec_from_env(struct msc_port_spec *spec, char *err, size_t errlen);
/* Fix `count` and `span` against the transfer's flow count and range-check the
 * resulting window. Idempotent. */
extern int msc_port_spec_resolve(struct msc_port_spec *spec, int flow_count,
                                 char *err, size_t errlen);
extern int msc_port_spec_default_count(int flow_count);
/* Human-readable, range-collapsed: "17400-17403,17405,17407-17409". */
extern void msc_port_spec_format(const unsigned int *ports, int n, char *buf, size_t buflen);
/* The window a port must fall in for `spec` to consider it honored. */
extern int msc_port_spec_contains(const struct msc_port_spec *spec, unsigned int port);

/* ----- datagram types (the `type` field) ---------------------------------- */
#define MSC_UDP_DATA   1                     /* sender -> receiver: DATA (a packet) */
#define MSC_UDP_ACK 2                     /* receiver -> sender: ACK/SACK */
#define MSC_UDP_FIN     3                     /* sender -> receiver: this flow is fully
                                           * acked; stop lingering now. A header-
                                           * only datagram (no payload). Best-effort:
                                           * if every FIN is lost the receiver falls
                                           * back to its MSC_UDP_FLOW_LINGER_MS timer. */
#define MSC_UDP_PROBE   4                     /* sender -> receiver: PMTUD probe. A padded
                                           * header-only datagram sent at session
                                           * open (xfer_id 0, seq = payload bytes);
                                           * the receiver reports the largest one
                                           * that survived the path (MSC_UDP_CTL_MTU). */
#define MSC_UDP_RTT_PROBE 5                   /* sender -> receiver: pre-data RTT probe */
#define MSC_UDP_RTT_ACK   6                   /* receiver -> sender: echoed RTT probe */

/* ----- control_channel (TCP control) record tags, sent as a uint32_t -------------- */
#define MSC_UDP_CTL_GREETING 1                /* sender -> receiver: session params */
#define MSC_UDP_CTL_PORTS    2                /* receiver -> sender: UDP port list */
#define MSC_UDP_CTL_DONE     3                /* receiver -> sender: final byte count */
#define MSC_UDP_CTL_PUBLISH  4                /* sender -> receiver: checksum ok */
#define MSC_UDP_CTL_ABORT    5                /* sender -> receiver: checksum/error fail */
#define MSC_UDP_CTL_TREE_FILE 6               /* sender -> receiver: recursive file path */
#define MSC_UDP_CTL_TREE_DIR  7               /* sender -> receiver: recursive dir path */
#define MSC_UDP_CTL_TREE_DONE 8               /* sender -> receiver: recursive complete */
#define MSC_UDP_CTL_HELLO    9                /* both ways at session open: proto
                                           * version + feature bits; sender also
                                           * sends a u64 initial transfer ID. A version
                                           * mismatch fails loudly here instead of
                                           * corrupting a longer wire struct. */
#define MSC_UDP_CTL_MTU      10               /* reserved for the PMTUD probe exchange
                                           * (sender announces probes done; receiver
                                           * replies with the largest that arrived) */
#define MSC_UDP_CTL_TREE_READY 11              /* receiver -> sender: recursive manifest
                                           * is materialized and data workers may start */
#define MSC_UDP_CTL_TABLE_BEGIN 12             /* sender -> receiver: synchronized table
                                           * transfer id, entry count, and byte total */
#define MSC_UDP_CTL_ERROR 13                   /* receiver -> sender: stable MSC exit
                                               * status for a local failure */

/* ----- HELLO feature bits (uint32_t, intersected by both ends) ------------
 * Reserved now so a feature can ship on one end at a time; each bit lights up
 * when its feature lands (see the transport design notes). */
#define MSC_UDP_FEAT_RWND  (1u << 0)          /* receiver computes a real rwnd */
#define MSC_UDP_FEAT_PMTUD (1u << 1)          /* run the probe exchange at session open */
#define MSC_UDP_FEAT_ECN   (1u << 2)          /* ECT(0) marking + CE echo in acknowledgments */
#define MSC_UDP_FEAT_FAIR  (1u << 3)          /* sender-local aggregate pacer (telemetry
                                           * only -- needs no receiver cooperation) */
#define MSC_UDP_FEAT_DSACK (1u << 4)          /* receiver reports duplicate arrivals in
                                           * the acknowledgment's first SACK block
                                           * (RFC 2883 convention); the sender uses
                                           * them to detect spurious retransmits and
                                           * adapt its reordering window */
#define MSC_UDP_FEAT_DATA_RTT (1u << 5)       /* pre-data UDP RTT probe/response */

/* ----- transport tunables ------------------------------------------------- */
#define MSC_UDP_RECV_BATCH   64               /* packets per recvmmsg syscall */
#define MSC_UDP_SEND_BATCH   64               /* packets per sendmmsg syscall */
#define MSC_UDP_SOCK_BUFFER  (16*1024*1024)   /* best-effort SO_RCVBUF/SO_SNDBUF */
#define MSC_UDP_WIN_RING     16384            /* maximum per-flow ring slots;
                                           * actual rings may shrink to budget */
#define MSC_UDP_INIT_CWND    16               /* initial sender capacity (units) */
#define MSC_UDP_MAX_SACK     16               /* SACK blocks carried per acknowledgment */
#define MSC_UDP_DUPACK_THRESH 3               /* duplicate cum-acks that trip fast rtx */

#define MSC_UDP_TICK_MS         2             /* sender loop clock when window-blocked */
#define MSC_UDP_FEEDBACK_MS     1             /* receiver: max gap between acknowledgments */
#define MSC_UDP_IDLE_ACK_MS     20            /* receiver: max gap between UNSOLICITED
                                           * acknowledgments while idle with no hole
                                           * open -- those are only window updates.
                                           * FEEDBACK_MS is the cadence once a hole
                                           * exists, so repair is never delayed. */
#define MSC_UDP_ACK_EVERY       16            /* in-order units per stretch acknowledgment:
                                           * a tight ACK clock keeps the sender's
                                           * window filling (measured ~1.8x vs 256
                                           * on a fast LAN). Tunable via MSC_UDP_ACK_EVERY. */
#define MSC_UDP_FLOW_LINGER_MS  300           /* receiver: quiet time before a done flow exits */

#define MSC_UDP_INIT_RTO_NS  (200ULL*1000*1000)   /* 200 ms before the first RTT sample */
/* RTO floor. Linux TCP uses 200 ms (TCP_RTO_MIN) and RFC 6298 recommends 1 s;
 * MSC used 2 ms on the reasoning that loopback and IB are fast. That reasoning
 * confuses the *typical* RTT with the *tail*. A timeout only has to be longer
 * than the worst delay the path can impose, and any path with a queue in it can
 * stall far longer than its median RTT -- so the floor has to cover the tail no
 * matter how quick the fabric looks when idle.
 *
 * Measured on a plain gigabit LAN, SRTT 1.5 ms, 256 MiB, defaults:
 *
 *   floor    rtx        Gbit/s
 *   2 ms     4155/8347  0.02
 *   200 ms   0/8347     0.97   (line rate)
 *
 * At 2 ms the RTO fired during ordinary fq_codel queueing, retransmitting half
 * the transfer with no packet loss at all (UdpInErrors +0) and pinning cwnd at
 * 3. Fast retransmit, not the RTO, is what recovers real loss promptly here, so
 * a conservative floor costs nothing on a healthy path -- exactly the tradeoff
 * TCP already makes. */
#define MSC_UDP_MIN_RTO_NS   (200ULL*1000*1000)
/* Adaptive reordering window, used when neither MSC_UDP_REORDER_WAIT_MS nor
 * MSC_UDP_WAN_GUARD pins one. A SACK above a hole proves a later unit arrived,
 * not that the hole was lost, so wait this long before remaking it. Expressed
 * as a divisor of the flow's SRTT. RFC 8985's RACK uses SRTT/4; MSC needs
 * SRTT/2, measured on a netem WAN path (256 MiB, -n8):
 *
 *   window   rto_rtx  sack_rtx  Gbit/s
 *   10 ms      1142      216     0.02
 *   15 ms        31        8     0.09
 *   25 ms         0        8     2.19   <- SRTT/2, the knee
 *   35 ms         0       14     2.01
 *   50 ms         0        9     1.91
 *
 * Two thresholds sit on top of each other. The path's real reordering spread is
 * ~10-15 ms, an order of magnitude beyond netem's nominal 2 ms jitter, so a
 * window under that reads reordering as loss. And because the RTO floor is
 * SRTT + this window, a window under ~SRTT/2 also leaves RTO too tight to
 * survive the path's own variance -- which is why 10 ms produced 1142 spurious
 * RTO retransmissions with zero real loss. Above the knee throughput decays
 * slowly as genuine loss recovery is deferred, so SRTT/2 is the corner, not a
 * plateau to sit past.
 *
 * On IB/loopback SRTT is ~1 ms, so the window is ~0.5 ms -- close to the old
 * immediate behaviour, and the fast-fabric suites are unaffected. */
#define MSC_UDP_REORDER_SRTT_DIV   2
#define MSC_UDP_MAX_REORDER_WAIT_NS (100ULL*1000*1000)   /* sanity cap */
#define MSC_UDP_MAX_RTO_NS   (2ULL*1000*1000*1000)/* 2 s ceiling */
#define MSC_UDP_DATA_PROBE_TIMEOUT_MS 1000         /* pre-data UDP probe budget: worst
                                                    * case both ends wait this out, so
                                                    * keep it small -- the sender
                                                    * retransmits the probe within it,
                                                    * so a lost probe/ack recovers in
                                                    * ~1 RTT, not at the ceiling. If the
                                                    * probe never answers (e.g. the
                                                    * stdio receiver batches its acks at
                                                    * its own deadline) the sample is
                                                    * discarded and the first data ACK's
                                                    * path_profile_reconsider sets the
                                                    * profile instead -- so this bounds
                                                    * the dead time, it does not gate
                                                    * correctness. */
#define MSC_UDP_DATA_PROBE_RETX_MS    150          /* min gap between probe retransmits;
                                                    * the effective gap is max(this,
                                                    * handshake RTT) so a WAN path retries
                                                    * about once per round trip. */
#define MSC_UDP_DATA_PROBE_WAIT_MS    250          /* how long the sender will keep
                                                    * draining for the probe ack AFTER the
                                                    * PMTUD ladder and BEFORE the control
                                                    * MTU exchange -- the last moment the
                                                    * receiver is still answering probes.
                                                    * Absolute on purpose: a multiple of
                                                    * the handshake RTT cannot be used,
                                                    * because that value is deliberately
                                                    * untrustworthy (stdio measures a local
                                                    * pipe, `many` over-reads ~7x). Paths
                                                    * slower than this keep the control
                                                    * seed rather than adopt a sample
                                                    * contaminated by the exchange. */
#define MSC_UDP_PMTUD_IDLE_MS         25           /* receiver: stop draining the ladder
                                                    * once no probe has arrived for this
                                                    * long. Must exceed the sender's 10 ms
                                                    * inter-round gap so it cannot fire
                                                    * between rounds. Replaces waiting out
                                                    * the full 60 ms grace for a ceiling
                                                    * candidate that a below-cap path can
                                                    * never deliver. */

/* socket-buffer autotuning: hard ceiling on a grown SO_RCVBUF/SO_SNDBUF (the
 * kernel's net.core.{r,w}mem_max still clamps below this). */
#define MSC_UDP_AUTOTUNE_MAX (256*1024*1024)

/* PMTUD: engine ceiling on an auto-raised file payload (bytes). The probe may
 * confirm a 64 KiB loopback datagram, but per-flow I/O buffers still scale with
 * payload even though retransmission rings now share a memory budget. Keep the
 * conservative engine default; the CLI selects 9000. Raise it with
 * MSC_UDP_PMTUD_CAP on jumbo/connected-mode paths. An explicit -s is never raised,
 * only clamped down to what the path can actually carry. */
#define MSC_UDP_PMTUD_PAYLOAD_CAP 2016

/* CUBIC scaling constant C (RFC 8312), in units/sec^3 */
#define MSC_UDP_CUBIC_C 0.4

/* the default controller (MSC_UDP_CC=rate): delivery-rate estimation +
 * delay-aware window, modeled
 * on BBR's two-estimator design. The BDP bound is btl_bw * min_rtt; the gains
 * and window lengths follow the BBR paper / Linux tcp_bbr defaults. */
#define MSC_UDP_RATE_HIGH_GAIN     2.885   /* startup pacing/cwnd gain (2/ln 2) */
#define MSC_UDP_RATE_CWND_GAIN     2.0     /* steady-state cwnd = gain * BDP */
#define MSC_UDP_RATE_MIN_CWND      4.0     /* floor; also the PROBE_RTT window */
#define MSC_UDP_RATE_BW_RTTS       10      /* bw max-filter window, packet-timed rounds */
#define MSC_UDP_RATE_MINRTT_WIN_NS (10ULL*1000*1000*1000) /* min-RTT staleness bound */
#define MSC_UDP_RATE_PROBE_RTT_NS  (200ULL*1000*1000)     /* PROBE_RTT dwell */

/* ===== wire formats (all integers network byte order) ===================== */

/* one packet: this header, then `length` payload bytes. `offset` is the absolute
 * destination-file offset and is always flow.lo_unit*payload + seq*payload, so the
 * receiver writes every packet in place and out-of-order arrival needs no buffer.
 *
 * Packed (alignment 1, same 48-byte layout -- there is no padding): GSO/GRO
 * batches place headers at a stride of sizeof(header) + payload, and a payload
 * that is not a multiple of 8 (an explicit -s, or a PMTUD-clamped size) leaves
 * every other header misaligned for its uint64_t fields. */
struct __attribute__((packed)) msc_udp_packet_header
{
   uint32_t magic;     /* MSC_UDP_MAGIC */
   uint32_t type;      /* MSC_UDP_DATA */
   uint32_t flow_id;   /* which flow / packet flow_worker */
   uint32_t length;    /* payload bytes following this header */
   uint64_t xfer_id;   /* transfer id within a reused MSC UDP session */
   uint64_t seq;       /* unit index within this flow (the order number) */
   uint64_t offset;    /* absolute offset within the destination file */
   uint64_t send_tsc;  /* sender timestamp (ns); echoed in the acknowledgment for RTT */
};
_Static_assert(sizeof(struct msc_udp_packet_header) == 48,
               "MSC UDP data header wire size changed");

/* one SACK block: units [start, end) received above the cumulative ack */
struct msc_udp_sack_block
{
   uint64_t start;
   uint64_t end;
};

/* one acknowledgment: this header, then `sack_count` msc_udp_sack_block. A acknowledgment is a pure
 * status snapshot, so losing one is harmless -- the next one supersedes it. */
struct msc_udp_acknowledgment_header
{
   uint32_t magic;       /* MSC_UDP_MAGIC */
   uint32_t type;        /* MSC_UDP_ACK */
   uint32_t flow_id;
   uint32_t sack_count;  /* SACK blocks following this header */
   uint64_t xfer_id;     /* transfer id within a reused MSC UDP session */
   uint64_t cum_ack;     /* next in-order unit expected (RCV.NXT) */
   uint64_t rwnd_units;  /* receiver space advertised, in units */
   uint64_t echo_tsc;    /* send_tsc of the packet that last moved cum_ack */
   uint64_t delivered;   /* total units written by this flow (telemetry) */
   uint64_t ce_units;    /* cumulative units that arrived CE-marked (ECN); the
                          * sender treats a rise as congestion without loss */
};

/* ===== run configuration (one transfer) =================================== */
struct msc_udp_config
{
   /* connection */
   const char *receiver_host;   /* sender: host to connect the control_channel to */
   unsigned int control_port;  /* sender: control port to connect to */
   int ctl_fd;                 /* sender, MSC_UDP_CTL=stdio|stdio1 only: the already-
                                * connected control byte stream (the local end of
                                * the SSH session's socketpair). Ignored in every
                                * other mode, so the usual memset(0) init is fine;
                                * a stdio-mode session open with ctl_fd <= 0 fails
                                * loudly (those modes need the host:dest CLI). */
   unsigned int base_control_port; /* receiver: first control port to try */
   unsigned int actual_control_port; /* receiver: selected control port */
   unsigned int port_tries;     /* receiver: control-port range length */
   int force;                   /* receiver: allow replacing final_dest_path */
   int verify_checksum;         /* include checksum + publish handshake */
   int multi;                   /* multi-machine worker: this transfer carries only
                                 * a byte range of a larger file. The sender skips the
                                 * Lustre layout query so each worker partitions its
                                 * own slice contiguously (cluster-level OST striping
                                 * is the orchestrator's job), and the receiver writes
                                 * into a shared, pre-created destination in place --
                                 * no temp file, no O_TRUNC, no publish rename. */
   int flow_count;            /* worker count / UDP flows (default 8) */
   size_t payload_size;          /* UDP payload size; 0 -> MSC_UDP_DEFAULT_PAYLOAD_SIZE */

   /* single-file endpoints (recursive transfers are a later phase) */
   const char *source_path;    /* sender source path, or NULL to use source_fd */
   int source_fd;              /* sender: preset readable fd, else -1 */
   const char *dest_path;      /* receiver dest path, or NULL to use dest_fd */
   const char *final_dest_path; /* receiver: publish dest_path here after checksum */
   int dest_fd;                /* receiver: preset writable fd, else -1 */
   off_t src_offset;           /* sender: first source byte to read */
   off_t dst_offset;           /* receiver: first destination byte to write */
   uint64_t xferlen;           /* bytes to move; 0 -> whole file from src_offset */

   /* multi-machine OST partitioning: each worker owns a disjoint subset of the
    * file's Lustre OSTs so no two machines contend for the same OST's LDLM lock.
    * my_ost_count == 0 means legacy byte-range mode (no OST filtering). */
   int my_ost_start;           /* first OST index this worker owns */
   int my_ost_count;           /* number of OSTs this worker owns; 0 = all */

   /* Lustre layout: the sender queries the source's stripe geometry and carries
    * it in the greeting so the receiver can create the destination with a matching
    * layout (and the transport can OST-affinity partition). Both 0 when the source
    * is not on Lustre or its layout is unknown. */
   uint64_t lustre_stripe_size;   /* source stripe size in bytes */
   uint32_t lustre_stripe_count;  /* source stripe count (OST width) */

   /* --dest-stripe-count: the OST width requested for the DESTINATION file only.
    * 0 = not requested (the destination keeps matching the source layout), >0 =
    * exact count, -1 = every OST. Deliberately separate from the two fields
    * above: those stay the protocol's source geometry, which both ends map units
    * with, so the destination layout can differ without desynchronizing
    * xfer_index(). The sender puts it in the greeting; the receiver also accepts
    * it from MSC_DEST_STRIPE_COUNT for the setup paths that run before a greeting
    * arrives. */
   long dest_stripe_count;

   /* result */
   uint64_t bytes_done;        /* set on success */
   uint64_t checksum;          /* 64-bit FNV-1a checksum of transferred bytes */
   int error_code;             /* 0 success; stable MSC exit code when known,
                                * otherwise -1 */
   volatile sig_atomic_t *cancel_flag; /* optional orchestrator cancellation */
};

/* ===== public entry points ================================================ */
/* sender (sender): connects the control_channel to receiver_host:control_port, then runs
 * the transfer. Returns 0 on success. */
extern int msc_udp_sender_run(struct msc_udp_config *cfg);
/* receiver (receiver): given an already-accepted control_channel fd, runs the
 * transfer. Returns 0 on success. */
extern int msc_udp_receiver_run(struct msc_udp_config *cfg, int controlfd);

/* Reusable session API for recursive transfers: one control_channel and one UDP flow
 * set can carry many files. The void * handles are owned by udp_session.c. */
extern void *msc_udp_sender_open(struct msc_udp_config *cfg);
extern int msc_udp_sender_fd(void *session);
extern int msc_udp_sender_send(void *session, struct msc_udp_config *cfg);
extern void msc_udp_sender_close(void *session);
extern void *msc_udp_receiver_open(struct msc_udp_config *cfg, int controlfd);
extern int msc_udp_receiver_fd(void *session);
extern int msc_udp_receiver_recv(void *session, struct msc_udp_config *cfg);
extern void msc_udp_receiver_close(void *session);

/* Report a receiver-local failure (a stable MSC exit code) to the sender over
 * an already-connected control fd. Safe to call even before a session/channel
 * exists (e.g. a pre-open destination-prep failure): TCP and stdio fds need no
 * special handling, and the UDP-shim modes drain internally. Best-effort --
 * the caller should still fail locally regardless of this call's result. */
extern int msc_udp_send_transfer_error(int fd, int status);

/* ----- recursive / multi-file table transfer -----------------------------
 * A whole directory tree moves as ONE logical transfer: every regular file with
 * size > 0 is concatenated into a single global unit stream that the flows carry
 * without stopping between files (the congestion window stays warm, small files
 * just fill the pipe). Directories and empty files are created from the manifest
 * by the caller; only non-empty files appear in this table. The caller (msc.c)
 * opens every fd and exchanges the manifest over the control_channel so both ends build
 * an identical table before the transfer runs. Integrity is byte-count
 * accounting over the reliable transport -- no per-file content checksum. */
struct msc_udp_file_spec
{
   int fd;          /* open source (sender) or destination (receiver) fd */
   uint64_t base;   /* first byte to read/write within the backing file */
   uint64_t size;   /* bytes to move from this file (> 0) */
};

struct msc_udp_tree_xfer
{
   struct msc_udp_file_spec *files; /* non-empty regular files, in manifest order */
   uint64_t nfiles;
   int flow_count;
   size_t payload_size;                /* 0 -> MSC_UDP_DEFAULT_PAYLOAD_SIZE (must match both ends) */
   uint64_t transfer_id;        /* carried in every packet; agreed by both ends.
                                 * Must be the session's next id (see below). */
   uint64_t total_bytes;        /* out: sum of file sizes actually moved */
   int error_code;              /* 0 success; stable MSC exit when known, else -1 */
   volatile sig_atomic_t *cancel_flag;
};

extern int msc_udp_sender_send_table(void *session, struct msc_udp_tree_xfer *t);
extern int msc_udp_receiver_recv_table(void *session, struct msc_udp_tree_xfer *t);
/* The id the session expects for its next table transfer. Sessions start from a
 * random id the sender announces in HELLO, so a data packet cannot be forged by
 * a host that has not seen the session's traffic. */
extern uint64_t msc_udp_sender_next_transfer_id(void *session);
extern uint64_t msc_udp_receiver_next_transfer_id(void *session);

/* ===== UDP control channel (udp_control.c) ================================
 * MSC_UDP_CTL selects where the control channel lives. Default is stdio: it
 * rides the SSH session that already launches the receiver, so an ordinary
 * transfer opens no listening TCP port anywhere. The other four arms stay
 * selectable for benchmarks and constrained callers; nothing falls back to
 * them automatically. */
enum msc_udp_ctl_mode
{
   MSC_UDP_CTLMODE_TCP    = 0, /* TCP control_channel, N ephemeral UDP data sockets */
   MSC_UDP_CTLMODE_MANY   = 1, /* UDP control_channel (reliable shim); data path unchanged */
   MSC_UDP_CTLMODE_ONE    = 2, /* UDP control_channel AND all data flows on ONE shared socket */
   MSC_UDP_CTLMODE_STDIO  = 3, /* DEFAULT. Control on the launching SSH session's
                            * stdin/stdout; N ephemeral UDP data sockets. Needs a
                            * connected byte stream from the caller (the ssh pipes
                            * under host:dest, a socketpair in udp_test), not a port */
   MSC_UDP_CTLMODE_STDIO1 = 4  /* SSH-stdio control; all data flows on ONE shared UDP
                            * socket (the funnel demux, minus its control queue) */
};
extern int msc_udp_ctl_mode(void);              /* MSC_UDP_CTL env, parsed once, cached */
extern const char *msc_udp_ctl_mode_name(int mode);

/* the two orthogonal questions the five arms answer */
static inline int msc_udp_ctl_is_stdio(int m)
{
   return m == MSC_UDP_CTLMODE_STDIO || m == MSC_UDP_CTLMODE_STDIO1;
}
static inline int msc_udp_ctl_one_socket(int m)
{
   return m == MSC_UDP_CTLMODE_ONE || m == MSC_UDP_CTLMODE_STDIO1;
}

/* The control_channel shim is a reliable, ordered byte-stream over UDP (a mini-TCP):
 * go-back-N with cumulative ACKs behind the msc_udp_send_all/msc_udp_recv_all contract.
 * Wire header is a fixed 24-byte big-endian frame, packed/unpacked explicitly
 * (never a C struct on the wire):
 *
 *    0  u32 magic    MSC_UDP_CONTROL_MAGIC -- distinct from MSC_UDP_MAGIC so a stray data
 *                    packet on the fixed port is dropped, and so the one-mode
 *                    demux can split control from data
 *    4  u32 conn_id  random nonce from the initiator; echoed; dedups stale
 *                    connections / rejects wrong peers
 *    8  u16 flags    SYN|ACK|FIN|RST|DATA
 *   10  u16 length   payload bytes (0..MSC_UDP_CONTROL_MSS)
 *   12  u32 seq      per-direction stream byte offset of first payload byte
 *   16  u32 ack      cumulative next-in-order byte expected from the peer
 *   20  u32 win      free recv-buffer bytes (manifest flow control)
 *
 * The SYN / SYN|ACK payload is 2 bytes -- [proto_version, ctl_mode] -- so a
 * many/one mismatch is refused with RST before any control record flows. */
#define MSC_UDP_CONTROL_MAGIC       0x4d634801U
#define MSC_UDP_CONTROL_SYN         0x0001
#define MSC_UDP_CONTROL_ACK         0x0002
#define MSC_UDP_CONTROL_FIN         0x0004
#define MSC_UDP_CONTROL_RST         0x0008
#define MSC_UDP_CONTROL_DATA        0x0010
#define MSC_UDP_CONTROL_HDR_BYTES   24
#define MSC_UDP_CONTROL_MSS         1376           /* header + payload = 1400, safely under
                                           * the IPoIB datagram-mode 2044 cap */
#define MSC_UDP_CONTROL_WIN_BYTES   (64 * 1024)    /* send/recv stream buffer per direction */
#define MSC_UDP_CONTROL_VERSION     1              /* shim handshake version (SYN payload) */
#define MSC_UDP_CONTROL_INIT_RTO_MS 100
#define MSC_UDP_CONTROL_MAX_RTO_MS  2000
#define MSC_UDP_CONTROL_MAX_RETRIES 20             /* silence after this many -> peer dead */
#define MSC_UDP_CONTROL_CONNECT_TIMEOUT_MS 15000   /* bounded SYN window (UDP cannot refuse) */

struct msc_udp_control_channel;   /* opaque; owned by udp_control.c */

/* Bind a SOCK_DGRAM control socket somewhere in [base, base+tries), exclusive
 * bind (NO SO_REUSEADDR: UDP has no listen() to reject a second binder, so a
 * plain EADDRINUSE drives the scan and keeps today's one-receiver-per-port
 * guarantee). base == 0 binds one ephemeral port. Returns the fd, or -1. */
extern int msc_udp_control_channel_bind_range(unsigned int base, unsigned int tries,
                                  unsigned int *actual);
/* Wait for a SYN on the bound fd, run the handshake + mode negotiation, and
 * register the shim. Returns the SAME fd on success (the UDP listen socket IS
 * the connection), -1 on timeout ("no sender connected") or mode mismatch
 * (replies RST). In many mode the socket is connect()ed to the learned peer;
 * in one mode it stays unconnected and replies go out via sendto. */
extern int msc_udp_control_channel_accept(int fd, int timeout_ms);
/* SOCK_DGRAM + connect + SYN handshake with mode negotiation; registers the
 * shim and returns the fd. Mirrors connect_tcp's loud failure: exits with a
 * clear message on RST/timeout (a tcp/udp cross-mode pair can only surface
 * here as the bounded SYN timeout). */
extern int msc_udp_control_channel_connect(const char *host, unsigned int port);
/* stdio control (MSC_UDP_CTL=stdio|stdio1), receiver side: register a control_channel over
 * inherited stdin/stdout -- the byte stream of the SSH session that launched
 * us. dup()s both fds and returns the read side as the control fd (the shim's
 * send writes the dup'ed stdout; close() closes the dup, never fd 0/1). No
 * handshake: the pipe existing IS the rendezvous, and the sender constructs
 * our env, so both ends agree on the mode by construction. Only meaningful
 * when a peer launched us over a pipe/ssh -- from a terminal it just blocks. */
extern int msc_udp_control_channel_stdio(void);
extern int msc_udp_control_channel_pipe(int rfd, int wfd);
/* fd -> shim registry. NULL means "not a control channel fd" (i.e. plain TCP). */
extern struct msc_udp_control_channel *msc_udp_control_channel_lookup(int fd);
/* msc_udp_send_all/msc_udp_recv_all semantics: deliver exactly n in-order bytes or -1. */
extern int msc_udp_control_channel_send(struct msc_udp_control_channel *channel, const void *buf, size_t n);
extern int msc_udp_control_channel_recv(struct msc_udp_control_channel *channel, void *buf, size_t n);
/* Block until every sent byte is acked (not just transmitted). Call before
 * handing off to code that will stop servicing this control_channel for a long
 * stretch (the raw-UDP data-flow threads never touch it), so a lost tail
 * byte from the last control record still gets retransmitted instead of
 * stranding the peer's blocking read. NULL channel (TCP mode: no shim registered)
 * is a no-op returning 0 -- the kernel already guarantees this for TCP. */
extern int msc_udp_control_channel_drain(struct msc_udp_control_channel *channel);
/* 1 if in-order bytes are (or become) available within timeout_ms, 0 if not,
 * -1 if the peer is gone. Backs the PMTUD control poll. */
extern int msc_udp_control_channel_wait_readable(struct msc_udp_control_channel *channel, int timeout_ms);
/* Tests only: drop the next `frames` outgoing shim frames (deterministic
 * one-shot loss), to exercise a specific record's retransmit path. */
extern void msc_udp_control_channel_test_drop_next(struct msc_udp_control_channel *channel, uint32_t frames);
/* FIN exchange (bounded retransmit + mini TIME_WAIT), unregister, free. NEVER
 * closes the fd -- the fd's creator closes it exactly once, which is what
 * makes the one-mode aliasing (controlfd == every data fd) safe. */
extern void msc_udp_control_channel_close(struct msc_udp_control_channel *channel);

/* ----- shared-socket RX demux ---------------------------------------------
 * Whenever several flows share one data socket (MSC_UDP_CTLMODE_ONE/STDIO1, or a
 * K-port topology via MSC_UDP_PORTS), a demux thread owns EVERY read on that
 * socket from the moment it starts until teardown -- control, data, acknowledgments,
 * and PMTUD probes all arrive through its bounded per-flow queues; nothing
 * else may recv()/poll() the fd. The demux routes by leading magic
 * (MSC_UDP_CONTROL_MAGIC -> control queue, MSC_UDP_MAGIC -> flow queue by flow_id), splits
 * UDP_GRO super-datagrams into their constituent packets before routing, and
 * preserves the per-datagram ECN TOS byte and source address. Enqueues never
 * block: a full flow queue drops the datagram (the data reliability layer
 * retransmissions it) and counts the drop. */
struct msc_udp_receive_demux;   /* opaque; owned by udp_control.c */

/* Spawn the demux on a shared fd and (when channel != NULL) switch channel's inbound
 * path to the control queue. Call immediately after the handshake, before any
 * other traffic. nflows is the transfer's TOTAL flow count (the global
 * flow_id space); this demux owns exactly the flows where
 * f % qstride == qbase, i.e. the flows mapped to this socket by the
 * flow f -> socket f % K assignment. (qbase 0, qstride 1) owns every flow --
 * the classic one-socket funnel. */
extern struct msc_udp_receive_demux *msc_udp_receive_demux_start(int fd, int nflows, int qbase, int qstride,
                                     struct msc_udp_control_channel *channel);
/* Pop one datagram for `flow` (or the control queue when flow < 0) into buf.
 * Waits up to timeout_us for the first datagram (0 = nonblocking). Returns
 * copied bytes > 0, 0 on timeout, -1 once the demux has shut down. tos_out
 * (optional) receives the datagram's IP TOS byte for ECN CE accounting. */
extern int msc_udp_receive_demux_pop(struct msc_udp_receive_demux *d, int flow, void *buf, size_t cap,
                       long timeout_us, unsigned char *tos_out);
/* Peer address learned at handshake/first datagram (the receiver's acknowledgments
 * need it for sendto on the unconnected shared socket). 0 on success. */
extern int msc_udp_receive_demux_peer(struct msc_udp_receive_demux *d, struct sockaddr_storage *ss,
                        socklen_t *len);
/* Signal + join the demux thread, free the queues, print MSC_UDP_STATS metrics.
 * Two-phase shutdown: msc_udp_control_channel_close(channel) MUST run first (the FIN/ACK
 * exchange rides the still-live demux), then this, then close the fd once. */
/* Request stop on every socket before joining any of them. Control FIN/ACK
 * completion must precede the first request. NULL is allowed. */
extern void msc_udp_receive_demux_request_stop(struct msc_udp_receive_demux *d);
extern void msc_udp_receive_demux_stop(struct msc_udp_receive_demux *d);

/* ===== shared helpers (udp_io.c) ======================================== */
extern void *msc_udp_alloc(size_t size, const char *what);
extern int msc_udp_fsync_parent(const char *path);
extern uint64_t msc_udp_hton64(uint64_t v);
extern uint64_t msc_udp_ntoh64(uint64_t v);
extern int msc_udp_send_all(int fd, const void *buf, size_t n);
extern int msc_udp_recv_all(int fd, void *buf, size_t n);
extern int msc_udp_pread_all(int fd, void *buf, size_t n, off_t off);
extern int msc_udp_pwrite_all(int fd, const void *buf, size_t n, off_t off);
extern int msc_udp_pwritev_all(int fd, struct iovec *iov, int iovcnt, off_t off);
extern uint64_t msc_udp_now_ns(void);        /* CLOCK_MONOTONIC nanoseconds */
extern int msc_udp_checksum_fd(int fd, off_t off, uint64_t len, uint64_t *sum);

/* ===== Lustre layout helpers (udp_io.c) ================================= */
/* All degrade gracefully when built without HAVE_LUSTRE: detection returns 0,
 * the query returns -1, and msc_udp_open_dest falls back to a plain open(). */
extern int msc_udp_is_lustre(const char *path);
extern int msc_udp_fd_is_lustre(int fd);
extern int msc_udp_query_stripe(const char *path, uint64_t *stripe_size,
                            uint32_t *stripe_count);
/* Create `path` with an explicitly requested Lustre layout, keeping the caller's
 * flags and mode (so an O_EXCL 0600 temp stays one). stripe_count is > 0 for an
 * exact OST count or -1 for every OST; 0 is rejected. Returns the fd, or -1 with
 * errno set -- unlike the source-layout heuristic this never degrades to a
 * default-striped file, and it removes any layoutless file a failed request left
 * behind. Always fails with ENOTSUP in a build without HAVE_LUSTRE. */
extern int msc_lustre_create_striped(const char *path, int flags, mode_t mode,
                                 uint64_t stripe_size, long stripe_count);
/* Confirm an already-created file carries the requested layout, for the paths
 * that must not unlink and recreate it (a shared multimachine destination, a
 * resumable partial). want == 0 always passes; want == -1 can only confirm that
 * a striped layout exists. *actual receives the observed count when the layout
 * could be read. Returns 0 on a match, -1 on a mismatch or unreadable layout. */
extern int msc_lustre_check_stripe_count(const char *path, long want,
                                     uint32_t *actual);
/* Open/create the destination. A nonzero dest_stripe_count is an explicit
 * request: it wins over the source layout and a Lustre failure returns -1 rather
 * than falling back. Otherwise, when stripe_count > 1 and built with HAVE_LUSTRE,
 * create it with the source's Lustre layout, falling back to a plain open();
 * force adds O_TRUNC. Returns an fd, or -1 on error. */
extern int msc_udp_open_dest(const char *path, uint64_t stripe_size,
                         uint32_t stripe_count, long dest_stripe_count,
                         int force);

#endif /* MSC_UDP_H */
