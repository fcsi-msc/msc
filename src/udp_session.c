#define _GNU_SOURCE   /* recvmmsg/sendmmsg, struct mmsghdr */
/*
 * udp_session.c - MSC UDP's TCP-like reliable-UDP transport.
 *
 * The file's units (payload-sized pieces) are striped across N flows by either the
 * portable contiguous partition or, on Lustre, an OST-affine stripe partition.
 * Each flow is one packet flow_worker thread running a self-contained, TCP-like
 * reliable transfer over its slice:
 *
 *   sender (sender)   slides a window of units bounded by min(cwnd, rwnd), reads
 *                    acknowledgments (cumulative ACK + SACK), grows/shrinks cwnd with
 *                    Reno-style slow start / congestion avoidance / fast
 *                    retransmit, and retransmissions on an adaptive RTO.
 *   receiver    writes every packet in place (pwritev runs or a shared
 *   (receiver)       mmap), tracks an receive bitmap, and emits
 *                    cumulative-ACK + SACK acknowledgments.
 *
 * One reliable TCP "control channel" carries setup (ports, greeting) and the final
 * byte-count acknowledgement, so correctness never depends on a UDP acknowledgment
 * surviving. See the transport design notes.
 *
 * Current scope: single-file and recursive table transfers, sendmmsg batching,
 * optional UDP GSO/GRO, Lustre layout/OST affinity, and multi-machine worker
 * slices. Retransmissions are served directly from the retransmission-cache RAM cache. The window
 * ring bounds in-flight to ring_slots-1 units, so per-unit metadata is
 * O(ring), not O(file).
 */
#include "udp_session.h"
#include "udp_stats.h"
#include "msc.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/* UDP GSO (UDP_SEGMENT): one sendmsg of many contiguous packets that the kernel
 * segments into MTU-sized datagrams. Defined here so the source compiles on
 * toolchains without the kernel headers; an old kernel just fails the sendmsg
 * and MSC UDP falls back to sendmmsg at runtime. */
#ifndef SOL_UDP
#define SOL_UDP 17
#endif
#ifndef UDP_SEGMENT
#define UDP_SEGMENT 103
#endif
/* UDP GRO (UDP_GRO): the receive-side mirror of UDP_SEGMENT. The kernel coalesces
 * equal-sized, back-to-back datagrams from one flow into a single recvmsg buffer
 * and reports the per-segment stride via a SOL_UDP/UDP_GRO cmsg. Best-effort:
 * an old kernel ignores the sockopt and we just read one packet per slot. */
#ifndef UDP_GRO
#define UDP_GRO 104
#endif

/* SO_MEMINFO: per-socket meminfo (rmem occupancy, rcvbuf, drops) for receive-
 * buffer autotuning. Linux 4.14+; an older kernel fails the getsockopt and the
 * receiver just skips autotune growth. Indexes follow linux/sock_diag.h. */
#ifndef SO_MEMINFO
#define SO_MEMINFO 55
#endif
#define MSC_UDP_SKMEM_RMEM_ALLOC 0
#define MSC_UDP_SKMEM_RCVBUF     1
#define MSC_UDP_SKMEM_DROPS      8
#define MSC_UDP_SKMEM_VARS       9

/* give up rather than spin forever if the peer disappears mid-flow */
#define MSC_UDP_RECV_IDLE_LIMIT_MS 60000

static int g_drop_pct = 0;   /* MSC_UDP_DROP: sender drops this % of packets (test) */
static int g_stats = 0;      /* MSC_UDP_STATS: per-flow control-loop telemetry */
static uint64_t g_stall_timeout_ns =
   (uint64_t)MSC_UDP_RECV_IDLE_LIMIT_MS * 1000ULL * 1000ULL;

/* tunables (env-overridable) for diagnosing the throughput bottleneck:
 *   MSC_UDP_NO_CC=1     pin the window wide open (no slow start / no cwnd cut) -- a
 *                   reliable "blast", to test whether congestion control is the
 *                   throttle vs. per-packet/receiver cost.
 *   MSC_UDP_ACK_EVERY=N receiver feedback cadence in in-order units (default 16).
 *   MSC_UDP_INIT_CWND=N initial sender capacity in units (default MSC_UDP_INIT_CWND).
 *   MSC_UDP_TICK_US=N   sender window-blocked poll timeout, microseconds (default 200).
 *   MSC_UDP_GSO=1       sender batches each ~64-packet send into one sendmsg with
 *                   UDP_SEGMENT (kernel segments to MTU) -- fewer trips through
 *                   the egress stack. Falls back to sendmmsg if the kernel lacks
 *                   it; not compatible with MSC_UDP_DROP loss injection.
 *   MSC_UDP_GRO=1       receiver sets UDP_GRO so the kernel coalesces back-to-back
 *                   same-sized packets into one recvmsg buffer -- the receive-side
 *                   mirror of MSC_UDP_GSO, fewer trips up the ingress stack. Old
 *                   kernels ignore the sockopt and fall back to one packet/slot.
 *   MSC_UDP_FSYNC_THREADS=N  receiver: parallel workers for the recursive tail fsync
 *                   (default 64). Decoupled from the flow count because flushing
 *                   a many-file tree is I/O-wait bound and journal commits
 *                   coalesce across concurrent fsyncs; the serial per-file fsync
 *                   barrier otherwise dominates many-small-files transfers.
 *   Dest write path (receiver, base==0 whole-file writers): the default now auto-
 *   picks by filesystem -- mmap on non-Lustre (shm/local: lock-free memcpy wins),
 *   pwritev on Lustre (mmap writeback there is ~7x slower: measured 1.6 vs 11
 *   Gbit/s single-node). Overrides:
 *   MSC_UDP_NO_MMAP=1    force the pwritev coalescer everywhere (never mmap).
 *   MSC_UDP_FORCE_MMAP=1 force mmap even on Lustre (reproduces the old default; for A/B).
 *   MSC_UDP_RECV_BATCH=N receiver: recvmmsg slots per call AND the ceiling on one
 *                   coalesced pwritev run (default 64, cap 1024). ~54 KiB runs
 *                   already hit ~11 Gbit/s; bigger batches gave no further gain.
 *   MSC_UDP_STATS=1 also prints per-flow `writes` and `avgwrite` (bytes/write) so you
 *                   can confirm the write granularity.
 *   MSC_UDP_PACE=1      token-bucket pace each flow's fresh sends at gain*cwnd/srtt
 *                   instead of blasting a freed window all at once. Spreads the
 *                   window over the SRTT so N flows don't microburst the receiver
 *                   into rcvbuf overflow + retransmit storms (pacing). The window
 *                   still bounds inflight; pacing only smooths *when* it goes out.
 *                   MSC_UDP_PACE=0 PINS pacing off, including under the WAN
 *                   profile (which otherwise turns it on unconditionally) --
 *                   that is what makes a pacing A/B possible on a WAN path.
 *   MSC_UDP_PACE_GAIN=F pacing rate headroom over cwnd/srtt (default 1.25, like
 *                   Linux TCP's pacing_ca_ratio) so pacing smooths bursts without
 *                   throttling steady-state below the window's natural rate.
 *   MSC_UDP_PACE_BURST=N token-bucket depth in units (default MSC_UDP_SEND_BATCH): the
 *                   most fresh units a single paced round may release.
 *   MSC_UDP_PACE_RATE_MBIT=F cap fresh payload across all flow_workers to F Mbit/s.
 *                   Unlike MSC_UDP_FAIR's coordination-only bucket, this gives a
 *                   shaped path a known aggregate ceiling.
 *   MSC_UDP_REORDER_WAIT_MS=N hold a SACK/duplicate-ACK hole for N ms before
 *                   remaking it, and defer that hole's RTO by the same amount.
 *                   Unset, the window is derived per flow from that flow's SRTT
 *                   (SRTT/MSC_UDP_REORDER_SRTT_DIV, RFC 8985's RACK ratio): ~12 us
 *                   on IB, ~12 ms on a 50 ms WAN. Setting this pins it instead.
 *   MSC_UDP_DUPACK_THRESH=N duplicate cumulative acknowledgments required before the
 *                   no-SACK fast-retransmit path retransmissions SND.UNA (default 3).
 *   MSC_UDP_WAN_GUARD=1 opt-in delayed-path profile: paced fresh sends, 4-unit
 *                   initial cwnd, 8-unit burst, 10 ms pacing bootstrap and
 *                   SACK/RTO reordering hold, and a 16-acknowledgment dupack threshold.
 *                   Explicit MSC_UDP_PACE_*, MSC_UDP_INIT_CWND, MSC_UDP_REORDER_WAIT_MS,
 *                   and MSC_UDP_DUPACK_THRESH values override those defaults.
 */
static int g_no_cc = 0;
static uint64_t g_ack_every = MSC_UDP_ACK_EVERY;
static _Atomic double g_init_cwnd = MSC_UDP_INIT_CWND;
static long g_tick_us = 200;   /* window-blocked poll; tight clock measured best */
static int g_gso = 0;          /* MSC_UDP_GSO=1: batch sends with UDP_SEGMENT */
static int g_gro = 0;          /* MSC_UDP_GRO=1: receiver coalesces recvs with UDP_GRO */
static int g_rx_nowrite = 0;   /* MSC_UDP_RX_NOWRITE=1: receiver skips pwritev (discards
                                * data) -- diagnostic to isolate the dest write path
                                * cost from the network/recv path. Corrupts output. */
static int g_no_affinity = 0;  /* MSC_UDP_NO_AFFINITY=1: force the contiguous partition
                                * (for the OST-affinity vs contiguous A/B) */
/* MSC_UDP_STRIPE_SIZE / MSC_UDP_STRIPE_COUNT: sender-side override of the source's queried
 * Lustre layout, so the OST-affinity partition can be exercised (and benchmarked)
 * on a non-Lustre mount. 0 means "use the real query result". */
static uint64_t g_stripe_size = 0;
static uint32_t g_stripe_count = 0;
static uint64_t g_retransmit_budget = 512ULL * 1024 * 1024;
static int g_fsync_threads = 64; /* parallel fsync workers for the tree tail flush */
static int g_no_mmap = 0;      /* MSC_UDP_NO_MMAP=1: never mmap the dest; use the pwritev
                                * coalescer even for base==0 (whole-file) writers. */
static int g_force_mmap = 0;   /* MSC_UDP_FORCE_MMAP=1: mmap even on Lustre (to reproduce
                                * the old default for the A/B). Default now auto-picks:
                                * mmap on non-Lustre, pwritev on Lustre (measured 7x
                                * faster there: 1.6 -> 11 Gbit/s single-node). */
static int g_recv_batch = MSC_UDP_RECV_BATCH; /* MSC_UDP_RECV_BATCH=N: recvmmsg slots per call.
                                * Also the ceiling on one coalesced pwritev run, so
                                * bigger batches -> bigger sequential writes (the whole
                                * point: ~1 MiB writes hit Lustre's large-write rate).
                                * Capped at 1024 (IOV_MAX). */
static _Atomic int g_pace = 0;           /* MSC_UDP_PACE=1: token-bucket pace fresh sends */
static int g_pace_set = 0;       /* user pinned MSC_UDP_PACE (a profile cannot undo it) */
static _Atomic double g_pace_gain = 1.25;   /* rate headroom over cwnd/srtt */
static _Atomic uint64_t g_pace_burst = MSC_UDP_SEND_BATCH; /* token-bucket depth (units) */
static double g_pace_rate_mbit = 0.0; /* explicit aggregate fresh-payload cap */
static _Atomic uint64_t g_pace_bootstrap_ns = 1000; /* no RTT sample yet: legacy 1 us floor */
static _Atomic uint64_t g_reorder_wait_ns = 0; /* SACK/RTO reordering hold; 0 = immediate */
static _Atomic int g_reorder_wait_set = 0;     /* an explicit knob (env or WAN_GUARD) pinned the
                                        * value above; otherwise it is derived per flow
                                        * from SRTT -- see flow_reorder_wait_ns() */
static _Atomic int g_dupack_thresh = MSC_UDP_DUPACK_THRESH;
/* MSC_UDP_STARTUP_QUEUE_MULT / the active profile's startup_queue_mult: STARTUP
 * gives up on the pipe once SRTT reaches this multiple of the flow's min RTT.
 * 0 = off, which is the LAN default and the historic behaviour. */
static _Atomic double g_startup_queue_mult = 0.0;
/* MSC_UDP_BW_RESTART / the active profile's bw_restart. 0 = a flow never
 * re-enters STARTUP, which is the LAN default and the historic behaviour. */
static _Atomic int g_bw_restart = 0;
static _Atomic int g_wan_guard = 0;      /* WAN profile engaged (MSC_UDP_WAN_GUARD=1, or auto) */
/* Path profile (W1). The engine grew up on IB/loopback, so every default is a
 * LAN default: pacing off, dupack-3, a 2 ms RTO floor. On a 50 ms path those
 * interact into a collapse loop -- spurious RTOs cut cwnd to 1, the reopened
 * window microbursts, drops, and cuts again. MSC_UDP_WAN_GUARD fixes it, but
 * only if the operator recognizes the path. `auto` makes that call from the
 * measured handshake RTT instead. */
enum msc_udp_profile
{
   MSC_UDP_PROFILE_AUTO = 0,
   MSC_UDP_PROFILE_LAN,
   MSC_UDP_PROFILE_WAN,     /* long path, any speed: the historic `wan` */
   MSC_UDP_PROFILE_WAN_LONG,/* long-haul/intercontinental: long, clean, fat */
   MSC_UDP_PROFILE_GEO      /* satellite hop: WAN plus RTO headroom */
};
static enum msc_udp_profile g_profile = MSC_UDP_PROFILE_AUTO;
static uint64_t g_wan_rtt_threshold_ns = 5ULL * 1000 * 1000; /* MSC_UDP_WAN_RTT_MS */
/* Above this, `auto` selects wan-long rather than wan: past ~120 ms the path is
 * intercontinental rather than cross-country, the BDP is large enough that the
 * startup ramp dominates a normal transfer, and the settings diverge (see the
 * wan-long table below). MSC_UDP_WAN_LONG_RTT_MS. */
static uint64_t g_wan_long_rtt_threshold_ns = 120ULL * 1000 * 1000;
/* Above this, `auto` selects geo. A one-hop GEO round trip is ~500-600 ms.
 * The threshold sits well clear of long terrestrial paths (~250 ms): any
 * closer and the measured RTT straddles it run to run, putting those paths on
 * the satellite preset by coin flip. MSC_UDP_GEO_RTT_MS. */
static uint64_t g_geo_rtt_threshold_ns = 450ULL * 1000 * 1000;

/* Environment presets.
 *
 * Only two tables, not one per environment class, because the measurements did
 * not support more. The sweep set out to give `fiber-private` (long, clean,
 * fast) and `fiber-shared` (long, commodity, ~1 Gbit) different values and
 * found they want the SAME ones: the whole 3.4x gap on the fast fabric was
 * pace_burst, and raising it is neutral-to-better on the slow path too
 * (0.81 vs 0.76 Gbit/s), so it is a wrong default rather than a class-specific
 * value. That also resolves a problem `auto` could not otherwise solve --
 * those two classes differ by BANDWIDTH, not latency, so no RTT threshold
 * could ever separate them.
 *
 * pace_burst is the token-bucket depth: the most units one paced round may
 * release. At 8 units of ~1984 B against the 200 us send tick that is roughly
 * 5 Gbit/s aggregate over 8 flows, which is why it was invisible on a gigabit
 * wire and cost 3.4x on an 18 Gbit fabric.
 *
 * min_rto_rtt_mult scales the RTO floor by the measured RTT. 1.0 (floor at the
 * RTT itself) leaves no variance headroom, which at 604 ms with 8 ms of jitter
 * fired constantly: 247 of 975 retransmits were PROVEN spurious by DSACK.
 * Raising it to 2.5 took that to zero, on every arm it was tested in. */
struct msc_udp_profile_def
{
   const char *name;
   double init_cwnd;
   uint64_t pace_burst;
   double pace_gain;
   int dupack_thresh;
   double min_rto_rtt_mult;
   /* STARTUP's delay exit: declare the pipe full once SRTT reaches this
    * multiple of the flow's min RTT for two consecutive rounds. 0 disables it
    * and leaves the bandwidth-plateau test as the only exit (the historic
    * behaviour, and what LAN keeps -- a sub-millisecond fabric's RTT is mostly
    * scheduling noise, so a delay ratio there is not a queue signal).
    *
    * Sized on the long paths it exists for: with the plateau test alone, eight
    * flows drove 92 MB of inflight against a 31 MB BDP on a 250 ms path,
    * inflating SRTT 257 -> 693 ms. The resulting RTO storms landed unevenly
    * across the flows, and since the work partition is static and equal, that
    * imbalance became an 11.8-second tail running at one eighth of the path. */
   double startup_queue_mult;
   /* Allow a flow to re-enter STARTUP when its bandwidth estimate is provably
    * stale (rate_pipe_not_full, or a sibling retiring). OFF for LAN, and that
    * is not caution -- it is the mechanism being pointless there.
    *
    * What it buys scales with RTT: it exists because PROBE_BW can only grow the
    * estimate 25% per 8 x min_rtt, which is ~2 s on a 250 ms path and so slower
    * than the transfer. On a 0.15 ms InfiniBand (IPoIB) fabric that same
    * recovery takes about a millisecond, so re-probing wins nothing -- while
    * the 2.885 pacing gain it re-engages is a real burst. Measured there over
    * 8 GiB: ~25 restarts per transfer, medians level (27.59 vs 27.67 Gbit/s)
    * but enabling it produced a 19.13 Gbit/s run that was never seen without
    * it. A wider low tail on fast fabrics is precisely what the delivery-rate
    * controller exists to remove, so this stays off where it cannot pay for
    * itself. */
   int bw_restart;
};

static const struct msc_udp_profile_def g_profile_wan = {
   "wan", 4.0, 64, 1.0, 16, 1.0, 0.0, 1
};
/* Long-haul terrestrial: long like geo, clean and fat like fiber-private.
 * Tuned on an emulated long-haul path (250 ms RTT, 0.01% loss, gigabit wire).
 *
 * init_cwnd 32 rather than the WAN presets' 4: on the highest-BDP paths msc
 * runs, starting BELOW the 16-unit LAN default is backwards. It is also the
 * single biggest knob here -- MSC_UDP_INIT_CWND=64 alone was +20-40% on the
 * data phase -- because at 250 ms every doubling the ramp does not have to do
 * is a quarter second saved, and a transfer only lasts tens of seconds.
 *
 * min_rto_rtt_mult 2.0, not `wan`'s 1.0: flooring the RTO at exactly the
 * measured RTT leaves nothing for the path's own variance, and forcing the
 * `wan` preset onto this path measured 0.11-0.34 Gbit/s against 0.44-0.58 for
 * geo's 2.5. geo's own value is the more conservative one because a satellite
 * hop's jitter is larger; 2.0 is enough headroom for a terrestrial path. */
static const struct msc_udp_profile_def g_profile_wan_long = {
   "wan-long", 32.0, 256, 1.0, 16, 2.0, 1.5, 1
};
/* PROVISIONAL. burst 256 + a 2.5x RTO floor measured 0.04 Gbit/s against the
 * shipped profile's 0.01 on a 604 ms / 0.7%-loss / 200 Mbit path -- 4x, but
 * still only 20% of what an uncontrolled blast achieves there (0.13), so ~3x
 * remains unexplained on this class. The two changes MUST ship together:
 * burst 256 without the RTO headroom was actively harmful (3 of 4 runs slow),
 * because bigger bursts with a too-tight floor just manufacture more false
 * timeouts. */
/* init_cwnd stays at 4 here rather than following wan-long to 32: geo's own
 * numbers were taken with 4 and nothing has re-measured it on a 604 ms / 0.7%
 * path. The same argument that raised it for wan-long applies, so this is a
 * standing question, not a settled value. */
static const struct msc_udp_profile_def g_profile_geo = {
   "geo", 4.0, 256, 1.0, 16, 2.5, 1.5, 1
};
/* Which table is in force, for telemetry and for the reconsider path. */
static const struct msc_udp_profile_def * _Atomic g_profile_active;
static const struct msc_udp_profile_def *profile_for_rtt(uint64_t rtt_ns);
static uint64_t g_handshake_rtt_ns = 0;  /* control-channel HELLO round trip (0 = none) */
static uint64_t g_data_rtt_ns = 0;       /* pre-data UDP probe RTT (0 = unavailable) */
static int g_sock_buffer = MSC_UDP_SOCK_BUFFER; /* SO_RCVBUF/SO_SNDBUF target (bytes) */
static int g_sock_buffer_set = 0;   /* explicit MSC_UDP_SOCK_BUFFER: no rcvbuf seeding */
/* congestion-response shaping. The delivery-rate controller is the default; MSC_UDP_CC=cubic|reno falls back to the
 * loss-based ladder, whose knobs below let a loss stop collapsing the window to
 * a crawl while pacing keeps it from bufferbloating. g_cc_beta starts at the
 * classic Reno value so an explicit MSC_UDP_CC=reno retains it, then read_env
 * selects CUBIC's 0.7 unless the user pinned MSC_UDP_CC_BETA. */
static double g_cc_beta = 0.5;   /* MSC_UDP_CC_BETA: multiplicative decrease factor */
static int g_cc_beta_set = 0;    /* user pinned MSC_UDP_CC_BETA (cubic keeps it) */

/* Congestion control is an event interface rather than a set of
 * algorithm-specific conditionals. A new controller should provide an ops
 * table below and add one selector case; ACK/SACK/retransmission plumbing
 * remains in the transport. */
enum msc_udp_cc_algorithm
{
   MSC_UDP_CC_ALG_RATE = 0,
   MSC_UDP_CC_ALG_RENO,
   MSC_UDP_CC_ALG_CUBIC
};

struct sender_flow;
struct msc_udp_cc_ops
{
   const char *name;
   int uses_rate_samples;
   void (*init)(struct sender_flow *g);
   void (*destroy)(struct sender_flow *g);
   void (*on_sent)(struct sender_flow *g, uint64_t seq, uint64_t now);
   void (*on_delivered)(struct sender_flow *g, uint64_t delivered, uint64_t now);
   void (*on_rtt)(struct sender_flow *g, uint64_t sample, uint64_t now);
   void (*on_ack)(struct sender_flow *g, uint64_t newest_seq,
                  uint64_t acked, uint64_t now);
   void (*on_sack)(struct sender_flow *g, uint64_t hi_sack, uint64_t now);
   void (*on_loss)(struct sender_flow *g);
   void (*on_ecn)(struct sender_flow *g);
   void (*on_rto)(struct sender_flow *g);
};

static enum msc_udp_cc_algorithm g_cc_algorithm = MSC_UDP_CC_ALG_RATE;
static const struct msc_udp_cc_ops *g_cc_ops;
static const struct msc_udp_cc_ops *cc_ops_for(enum msc_udp_cc_algorithm algorithm);
static const struct msc_udp_cc_ops *cc_current(void);
static int cc_uses_rate_samples(void);
static void cc_on_sent(struct sender_flow *g, uint64_t seq, uint64_t now);
static void cc_on_delivered(struct sender_flow *g, uint64_t delivered, uint64_t now);
static void cc_on_rtt(struct sender_flow *g, uint64_t sample, uint64_t now);
static void cc_on_ack(struct sender_flow *g, uint64_t newest_seq,
                      uint64_t acked, uint64_t now);
static void cc_on_sack(struct sender_flow *g, uint64_t hi_sack, uint64_t now);
static void cc_on_loss(struct sender_flow *g);
static void cc_on_ecn(struct sender_flow *g);
static void cc_on_rto(struct sender_flow *g);
static double g_min_cwnd = 1.0;  /* MSC_UDP_MIN_CWND: cwnd floor after any reduction */
static int g_rto_gentle = 0;     /* MSC_UDP_RTO_GENTLE=1: RTO does cwnd*=beta, not cwnd=1 */
static _Atomic uint64_t g_min_rto_ns = MSC_UDP_MIN_RTO_NS; /* MSC_UDP_MIN_RTO_MS: RTO floor override */
/* real receive window:
 *   MSC_UDP_RWND=0      revert to the constant advertised window. Default on: the
 *                   receiver advertises its real window (ring space minus the
 *                   out-of-order units it is holding, halved under write stalls)
 *                   so a slow receiver throttles the sender WITHOUT loss.
 *   MSC_UDP_RWND_STALL_MS=N  flush-latency EWMA above this halves the advertised
 *                   window (default 5 ms). */
static int g_rwnd = 1;
/* DSACK + adaptive reordering window:
 *   MSC_UDP_DSACK=0     drop the feature bit: the receiver stops reporting
 *                   duplicate arrivals and the sender stops adapting. Default
 *                   on: each spurious retransmit widens the flow's reordering
 *                   window (srtt/4 per RTT, capped) and an episode whose every
 *                   retransmit proves spurious gets its cwnd cut reverted, so
 *                   a path whose reordering spread exceeds SRTT/2 self-corrects
 *                   instead of relapsing into the 100x collapse. */
static int g_dsack = 1;
static int g_reorder_div = MSC_UDP_REORDER_SRTT_DIV; /* MSC_UDP_REORDER_SRTT_DIV
                                  * env override; the DSACK acceptance test sets
                                  * it back to RACK's 4 to prove adaptation
                                  * recovers what the static ratio cannot */
static uint64_t g_rwnd_stall_ns = 5ULL * 1000 * 1000;
/* socket-buffer autotuning:
 *   MSC_UDP_SOCK_AUTOTUNE=0  fixed socket buffers. Default on: grow SO_SNDBUF toward
 *                   2*cwnd*payload (sender) and double SO_RCVBUF on per-socket drops
 *                   or >3/4 occupancy (receiver), up to MSC_UDP_AUTOTUNE_MAX bytes
 *                   (env-overridable), stopping at the net.core.{r,w}mem_max clamp. */
static int g_autotune = 1;
static int64_t g_autotune_max = MSC_UDP_AUTOTUNE_MAX;
/* path-MTU discovery:
 *   MSC_UDP_PMTUD=0     skip path-MTU discovery. Default on: at session open the
 *                   sender probes a ladder of datagram sizes (seeded by the
 *                   kernel's IP_MTU) and the receiver confirms the largest that
 *                   arrived; a default payload is raised to it (capped at
 *                   MSC_UDP_PMTUD_CAP, default 2016) and an explicit -s is clamped
 *                   down so a small-MTU path degrades instead of black-holing. */
static int g_pmtud = 1;
static size_t g_pmtud_cap = MSC_UDP_PMTUD_PAYLOAD_CAP;
static size_t g_payload_probe = 0;   /* discovered path payload ceiling; 0 = unknown */
/* ECN:
 *   MSC_UDP_ECN=0       no ECT marking / CE reaction. Default on: the sender marks
 *                   ECT(0), the receiver counts CE-marked units into the acknowledgment,
 *                   and a rise cuts the window once per RTT without a retransmit.
 *                   A fabric that never marks CE makes this a no-op. */
static int g_ecn = 1;
/* aggregate cross-flow pacer:
 *   MSC_UDP_FAIR=1      one shared token bucket bounds the SUM of all flows' fresh
 *                   sends so N per-flow buckets cannot align into one
 *                   microburst. Off by default (opt-in for the flow-sweep A/B). */
static int g_fair = 0;
/* control-channel arm (MSC_UDP_CTL, parsed by udp_control.c): tcp keeps the
 * existing TCP control_channel + N ephemeral data sockets; many swaps only the
 * control_channel to the reliable UDP shim; one funnels the control_channel AND every data
 * flow through a single shared UDP socket behind an RX demux thread. */
static int g_ctl = MSC_UDP_CTLMODE_TCP;
/* data-socket topology. K sockets carry the
 * N flows, flow f on socket f % K; every socket carrying >1 flow gets its own
 * RX demux thread (K=1 is the classic funnel). K defaults to min(N, 8) and the
 * ports themselves are scanned out of a predictable window -- see the mode
 * table in udp_session.h. The receiver resolves the spec and advertises the
 * resulting port list; the sender adapts to whatever arrives but VERIFIES it
 * against its own copy of the env (msc and the harnesses set it on both ends),
 * so a receiver that ignored the request fails loudly instead of silently
 * opening ports nobody opened a firewall for. */
static struct msc_port_spec g_ports;
/* msc.h enum msc_exit_code, mirrored: this engine is standalone (no msc.h) but
 * its failures must land in the same stable contract. */
#define MSC_UDP_EXIT_CLI     2   /* a malformed request (env or flag) */
#define MSC_UDP_EXIT_NETWORK 6   /* a well-formed request the network refused */

/* NOTE: each knob is applied only when its env var is *present*, so the module
 * globals keep their declared defaults otherwise -- they are NOT reset to the
 * defaults at entry. msc runs process-per-transfer, so read_env fires once and
 * this is fine. If a single process ever calls read_env more than once (e.g. an
 * in-process A/B loop in msc_udp_node_test), unsetting a var will NOT revert it;
 * reset the globals to their defaults at the top of this function first. */
/* Engage the conservative delayed-path profile: paced fresh sends, a small
 * initial window, a reordering hold before fast retransmit, and a wide dupack
 * threshold. It trades a little loss-recovery latency for not mistaking a
 * shaper's reordering for congestion. Any knob an explicit MSC_UDP_* set stays
 * authoritative, so the A/B harnesses keep working.
 *
 * rtt_ns is the measured path RTT, or 0 when the profile was forced before any
 * measurement (MSC_UDP_WAN_GUARD=1 at read_env time). A real sample scales the
 * RTO floor and the pacing bootstrap to the path instead of fabric constants. */
static void apply_profile_def(const struct msc_udp_profile_def *p, uint64_t rtt_ns)
{
   g_wan_guard = 1;
   g_profile_active = p;
   if (!g_pace_set)              /* an explicit MSC_UDP_PACE=0 stays authoritative */
      g_pace = 1;
   if (getenv("MSC_UDP_INIT_CWND") == NULL)
      g_init_cwnd = p->init_cwnd;
   if (getenv("MSC_UDP_PACE_BURST") == NULL)
      g_pace_burst = p->pace_burst;
   if (getenv("MSC_UDP_PACE_GAIN") == NULL)
      g_pace_gain = p->pace_gain;
   if (getenv("MSC_UDP_REORDER_WAIT_MS") == NULL)
   {
      /* Leave this UNPINNED when we have a real measurement, so the window
       * tracks SRTT via flow_reorder_wait_ns() instead of a fabric constant.
       * The historic fixed 10 ms is what `auto` was actually handing every WAN
       * transfer, and on a 50 ms path 10 ms sits below BOTH thresholds the
       * window has to clear (see MSC_UDP_REORDER_SRTT_DIV): it read the path's
       * ~10-15 ms reordering spread as loss, and it left the RTO floor
       * (SRTT + window) too tight to survive the path's own variance. Measured
       * cost: 1142 spurious RTO retransmissions with zero packet loss, and
       * 0.02 Gbit/s where SRTT/2 gives 2.19.
       *
       * With no measurement (MSC_UDP_WAN_GUARD=1 forced before the handshake)
       * there is nothing to scale to yet, so keep the old constant until the
       * first RTT sample replaces it. */
      if (rtt_ns == 0)
      {
         g_reorder_wait_ns = 10ULL * 1000 * 1000;
         g_reorder_wait_set = 1;
      }
   }
   if (getenv("MSC_UDP_DUPACK_THRESH") == NULL)
      g_dupack_thresh = p->dupack_thresh;
   if (getenv("MSC_UDP_STARTUP_QUEUE_MULT") == NULL)
      g_startup_queue_mult = p->startup_queue_mult;
   if (getenv("MSC_UDP_BW_RESTART") == NULL)
      g_bw_restart = p->bw_restart;
   g_pace_bootstrap_ns = 10ULL * 1000 * 1000;
   if (rtt_ns > 0)
   {
      /* A 2 ms RTO floor on a 50 ms path fires spurious timeouts, and each one
       * collapses cwnd to 1. Floor at the measured RTT instead -- times the
       * profile's multiplier, because the RTT alone leaves no headroom for the
       * path's own variance (see the geo row of the profile table). */
      uint64_t floor_ns = (uint64_t)((double)rtt_ns * p->min_rto_rtt_mult);
      if (getenv("MSC_UDP_MIN_RTO_MS") == NULL && floor_ns > g_min_rto_ns)
         g_min_rto_ns = floor_ns;
      /* The bootstrap only matters until the first data-ACK RTT sample; the
       * handshake RTT is a better stand-in than a fixed 10 ms. */
      if (rtt_ns > g_pace_bootstrap_ns)
         g_pace_bootstrap_ns = rtt_ns;
   }
}

/* Called once by each end after the HELLO round trip, with that round trip as
 * the first RTT sample. Under `auto` this is what removes the need for the
 * operator to recognize a WAN and hand-set MSC_UDP_WAN_GUARD. */
/* Has the profile decision already been revisited against a real data-path RTT?
 * Compare-and-swapped, because the first genuine sample arrives on whichever
 * flow_worker thread happens to get its first ACK first. */
static int g_profile_reconsidered = 0;

/* Re-decide the path profile from the first REAL data-path RTT sample.
 *
 * The handshake round trip is measured on the control channel, and only the
 * TCP control channel actually rides the path being profiled. Measured on a
 * 50 ms netem pair (48 runs):
 *
 *   MSC_UDP_CTL=tcp     ~52 ms   -> wan, correct
 *   MSC_UDP_CTL=stdio    ~0.24 ms -> LAN on a 50 ms WAN. stdio's control channel
 *                                 is a pipe to the local ssh process, so the
 *                                 sample is that pipe's latency, not the path's.
 *   MSC_UDP_CTL=many     ~350 ms  -> wan, but ~7x the true RTT, and
 *                                 apply_wan_profile() floors the RTO at it.
 *
 * Rather than teach each control channel to measure the path, distrust all of
 * them: the data path measures itself as soon as the first ACK lands, so treat
 * the handshake value as provisional and correct it here. That fixes every
 * control mode at once, including any added later, and needs no wire change.
 *
 * Only `auto` is revisited -- an operator who pinned lan or wan meant it. */
static void path_profile_reconsider(uint64_t rtt_ns)
{
   if (g_profile != MSC_UDP_PROFILE_AUTO || rtt_ns == 0)
      return;
   if (!__sync_bool_compare_and_swap(&g_profile_reconsidered, 0, 1))
      return;

   if (!g_wan_guard && rtt_ns >= g_wan_rtt_threshold_ns)
   {
      /* The control channel read short and we started on LAN defaults. This is
       * the stdio case, and it is the one that silently changes the transport's
       * behaviour when a user selects SSH control. */
      const struct msc_udp_profile_def *want = profile_for_rtt(rtt_ns);
      apply_profile_def(want, rtt_ns);
      if (g_stats)
         fprintf(stderr, "MSC-UDP-STATS profile=%s rtt_ms=%.3f reason=data-rtt "
                 "(handshake said %.3f ms)\n", want->name, (double)rtt_ns / 1e6,
                 (double)g_handshake_rtt_ns / 1e6);
   }
   else if (g_wan_guard && getenv("MSC_UDP_MIN_RTO_MS") == NULL &&
            g_min_rto_ns > rtt_ns && g_handshake_rtt_ns > rtt_ns * 2)
   {
      /* The control channel read long and left the RTO floored far above the
       * path (the UDP-control case). A floor several times the real RTT delays
       * every timeout recovery by that much. */
      uint64_t was = g_min_rto_ns;
      g_min_rto_ns = rtt_ns > MSC_UDP_MIN_RTO_NS ? rtt_ns : MSC_UDP_MIN_RTO_NS;
      if (g_stats)
         fprintf(stderr, "MSC-UDP-STATS profile=wan rtt_ms=%.3f reason=data-rtt "
                 "rto_floor_ms %.1f -> %.1f (handshake said %.3f ms)\n",
                 (double)rtt_ns / 1e6, (double)was / 1e6,
                 (double)g_min_rto_ns / 1e6, (double)g_handshake_rtt_ns / 1e6);
   }
}

/* Which preset an RTT implies under `auto`. NULL means the LAN defaults.
 *
 * Note what this deliberately does NOT try to do: pick between a fast private
 * fiber path and a commodity one. They differ by bandwidth, not latency, so no
 * RTT band could separate them -- and the sweep found they want identical
 * settings anyway, which is why there is one wan table rather than two. */
static const struct msc_udp_profile_def *profile_for_rtt(uint64_t rtt_ns)
{
   if (rtt_ns >= g_geo_rtt_threshold_ns) return &g_profile_geo;
   if (rtt_ns >= g_wan_long_rtt_threshold_ns) return &g_profile_wan_long;
   if (rtt_ns >= g_wan_rtt_threshold_ns) return &g_profile_wan;
   return NULL;
}

static void path_profile_select(uint64_t rtt_ns)
{
   const struct msc_udp_profile_def *want = NULL;

   g_handshake_rtt_ns = rtt_ns;
   switch (g_profile)
   {
   case MSC_UDP_PROFILE_LAN:
      return;                  /* forced LAN: keep the historic fabric defaults */
   case MSC_UDP_PROFILE_WAN: want = &g_profile_wan; break;
   case MSC_UDP_PROFILE_WAN_LONG: want = &g_profile_wan_long; break;
   case MSC_UDP_PROFILE_GEO: want = &g_profile_geo; break;
   case MSC_UDP_PROFILE_AUTO: want = profile_for_rtt(rtt_ns); break;
   }
   if (want != NULL)
   {
      int was_on = g_wan_guard;
      apply_profile_def(want, rtt_ns);
      if (g_stats && !was_on)
         fprintf(stderr, "MSC-UDP-STATS profile=%s rtt_ms=%.3f reason=%s\n",
                 want->name, (double)rtt_ns / 1e6,
                 g_profile == MSC_UDP_PROFILE_AUTO ? "measured-rtt" : "forced");
   }
   else if (g_stats)
      fprintf(stderr, "MSC-UDP-STATS profile=lan rtt_ms=%.3f\n", (double)rtt_ns / 1e6);
}

static void read_env(void)
{
   const char *drop = getenv("MSC_UDP_DROP");
   const char *stats = getenv("MSC_UDP_STATS");
   const char *nocc = getenv("MSC_UDP_NO_CC");
   const char *acke = getenv("MSC_UDP_ACK_EVERY");
   const char *icwnd = getenv("MSC_UDP_INIT_CWND");
   const char *tick = getenv("MSC_UDP_TICK_US");
   const char *dupth = getenv("MSC_UDP_DUPACK_THRESH");
   const char *rwait = getenv("MSC_UDP_REORDER_WAIT_MS");
   const char *wguard = getenv("MSC_UDP_WAN_GUARD");
   const char *memory = getenv("MSC_UDP_RETRANSMIT_MB");
   if (memory != NULL)
   {
      char *end;
      unsigned long long mb;
      errno = 0;
      mb = strtoull(memory, &end, 10);
      if (errno || end == memory || *end || *memory == '-' || mb < 1 || mb > 1048576)
      {
         fprintf(stderr, "MSC_UDP_RETRANSMIT_MB must be an integer from 1 to 1048576\n");
         exit(MSC_EXIT_CLI);
      }
      g_retransmit_budget = (uint64_t)mb * 1024 * 1024;
   }
   if (drop != NULL)
   {
      g_drop_pct = atoi(drop);
      if (g_drop_pct < 0) g_drop_pct = 0;
      if (g_drop_pct > 100) g_drop_pct = 100;
   }
   if (stats != NULL && atoi(stats) != 0)
      g_stats = 1;
   if (nocc != NULL && atoi(nocc) != 0)
      g_no_cc = 1;
   if (acke != NULL && atoll(acke) > 0)
      g_ack_every = (uint64_t)atoll(acke);
   if (icwnd != NULL && atof(icwnd) >= 1.0)
      g_init_cwnd = atof(icwnd);
   if (tick != NULL && atol(tick) >= 0)
      g_tick_us = atol(tick);
   if (dupth != NULL && atoll(dupth) > 0)
   {
      long long n = atoll(dupth);
      g_dupack_thresh = n > INT_MAX ? INT_MAX : (int)n;
   }
   if (rwait != NULL && atoll(rwait) >= 0)
   {
      uint64_t ms = (uint64_t)atoll(rwait);
      /* A longer hold could never be honored by the 2 s RTO ceiling anyway;
       * clamp it here so the subsequent RTO-floor arithmetic stays bounded. */
      if (ms > MSC_UDP_MAX_RTO_NS / 1000000ULL)
         ms = MSC_UDP_MAX_RTO_NS / 1000000ULL;
      g_reorder_wait_ns = ms * 1000000ULL;
      g_reorder_wait_set = 1;
   }
   if (getenv("MSC_UDP_GSO") != NULL && atoi(getenv("MSC_UDP_GSO")) != 0)
      g_gso = 1;
   /* Loss injection is defined per packet.  The GSO path emits an entire
    * segmented batch with one sendmsg, so it cannot selectively suppress
    * packets; keep tests (including 100% loss/stall detection) on sendmmsg. */
   if (g_drop_pct != 0)
      g_gso = 0;
   if (getenv("MSC_UDP_GRO") != NULL && atoi(getenv("MSC_UDP_GRO")) != 0)
      g_gro = 1;
   if (getenv("MSC_UDP_RX_NOWRITE") != NULL && atoi(getenv("MSC_UDP_RX_NOWRITE")) != 0)
      g_rx_nowrite = 1;
   if (getenv("MSC_UDP_NO_MMAP") != NULL && atoi(getenv("MSC_UDP_NO_MMAP")) != 0)
      g_no_mmap = 1;
   if (getenv("MSC_UDP_FORCE_MMAP") != NULL && atoi(getenv("MSC_UDP_FORCE_MMAP")) != 0)
      g_force_mmap = 1;
   if (getenv("MSC_UDP_RECV_BATCH") != NULL && atoi(getenv("MSC_UDP_RECV_BATCH")) > 0)
   {
      g_recv_batch = atoi(getenv("MSC_UDP_RECV_BATCH"));
      if (g_recv_batch < 1) g_recv_batch = 1;
      if (g_recv_batch > 1024) g_recv_batch = 1024;   /* IOV_MAX: one run == one pwritev */
   }
   if (getenv("MSC_UDP_NO_AFFINITY") != NULL && atoi(getenv("MSC_UDP_NO_AFFINITY")) != 0)
      g_no_affinity = 1;
   if (getenv("MSC_UDP_STRIPE_SIZE") != NULL && atoll(getenv("MSC_UDP_STRIPE_SIZE")) > 0)
      g_stripe_size = (uint64_t)atoll(getenv("MSC_UDP_STRIPE_SIZE"));
   if (getenv("MSC_UDP_STRIPE_COUNT") != NULL && atoi(getenv("MSC_UDP_STRIPE_COUNT")) > 0)
      g_stripe_count = (uint32_t)atoi(getenv("MSC_UDP_STRIPE_COUNT"));
   {
      const char *fst = getenv("MSC_UDP_FSYNC_THREADS");
      if (fst != NULL && atoi(fst) > 0) g_fsync_threads = atoi(fst);
   }
   /* MSC_UDP_PACE=0 must PIN pacing off, not merely fail to turn it on: the WAN
    * profile enables pacing unconditionally, so without the pin there is no way
    * to run the pacing-off arm of an A/B on any path the profile engages -- and
    * every other profile default here is overridable. */
   if (getenv("MSC_UDP_PACE") != NULL)
   {
      g_pace = atoi(getenv("MSC_UDP_PACE")) != 0;
      g_pace_set = 1;
   }
   if (getenv("MSC_UDP_PACE_GAIN") != NULL && atof(getenv("MSC_UDP_PACE_GAIN")) > 0.0)
      g_pace_gain = atof(getenv("MSC_UDP_PACE_GAIN"));
   if (getenv("MSC_UDP_PACE_BURST") != NULL && atoll(getenv("MSC_UDP_PACE_BURST")) > 0)
      g_pace_burst = (uint64_t)atoll(getenv("MSC_UDP_PACE_BURST"));
   /* >= 0 so an explicit 0 can pin STARTUP's delay exit OFF on a profile that
    * ships it on, the same way MSC_UDP_PACE=0 pins pacing off. */
   if (getenv("MSC_UDP_STARTUP_QUEUE_MULT") != NULL &&
       atof(getenv("MSC_UDP_STARTUP_QUEUE_MULT")) >= 0.0)
      g_startup_queue_mult = atof(getenv("MSC_UDP_STARTUP_QUEUE_MULT"));
   if (getenv("MSC_UDP_BW_RESTART") != NULL)
      g_bw_restart = atoi(getenv("MSC_UDP_BW_RESTART")) != 0;
   if (getenv("MSC_UDP_PACE_RATE_MBIT") != NULL &&
       atof(getenv("MSC_UDP_PACE_RATE_MBIT")) > 0.0)
   {
      g_pace_rate_mbit = atof(getenv("MSC_UDP_PACE_RATE_MBIT"));
      g_pace = 1;
   }
   if (getenv("MSC_UDP_SOCK_BUFFER") != NULL && atoll(getenv("MSC_UDP_SOCK_BUFFER")) > 0)
   {
      g_sock_buffer = (int)atoll(getenv("MSC_UDP_SOCK_BUFFER"));
      g_sock_buffer_set = 1;
   }
   if (getenv("MSC_UDP_CC_BETA") != NULL && atof(getenv("MSC_UDP_CC_BETA")) > 0.0 &&
       atof(getenv("MSC_UDP_CC_BETA")) < 1.0)
   {
      g_cc_beta = atof(getenv("MSC_UDP_CC_BETA"));
      g_cc_beta_set = 1;
   }
   {
      const char *cc = getenv("MSC_UDP_CC");
      if (cc != NULL && strcmp(cc, "cubic") == 0)
         g_cc_algorithm = MSC_UDP_CC_ALG_CUBIC;
      else if (cc != NULL && strcmp(cc, "reno") == 0)
         g_cc_algorithm = MSC_UDP_CC_ALG_RENO;
      else if (cc != NULL && strcmp(cc, "rate") == 0)
         g_cc_algorithm = MSC_UDP_CC_ALG_RATE;
      else if (cc != NULL)
         fprintf(stderr, "MSC UDP: unknown MSC_UDP_CC=%s (want reno|cubic|rate), using rate\n", cc);
   }
   /* rate mode is pointless without pacing (the whole point is that the send
    * rate follows the bandwidth estimate, not a freed window's edge), so it
    * implies MSC_UDP_PACE=1. A NO_CC blast must stay a blast: rate pacing
    * would throttle it to the estimate, so NO_CC disables rate mode outright. */
   if (g_no_cc && g_cc_algorithm == MSC_UDP_CC_ALG_RATE)
      g_cc_algorithm = MSC_UDP_CC_ALG_RENO;
   g_cc_ops = cc_ops_for(g_cc_algorithm);
   if (cc_uses_rate_samples())
   {
      /* Say so rather than silently overriding the user. An A/B that sets
       * MSC_UDP_PACE=0 against the default controller is measuring the SAME
       * config as its control arm, and nothing said otherwise -- an easy way
       * to conclude "pacing is not the bottleneck" when it was never off. To actually
       * run unpaced, pick a controller that does not pace: MSC_UDP_CC=cubic. */
      if (g_pace_set && !g_pace)
         fprintf(stderr, "MSC UDP: MSC_UDP_PACE=0 ignored -- MSC_UDP_CC=%s paces by "
                 "definition (the send rate IS its output). Use MSC_UDP_CC=cubic "
                 "for an unpaced arm.\n", cc_current()->name);
      g_pace = 1;
   }
   /* CUBIC's standard multiplicative decrease is 0.7 (vs Reno's 0.5); an
    * explicit MSC_UDP_CC_BETA still wins so the shaping A/Bs keep working. */
   if (g_cc_algorithm == MSC_UDP_CC_ALG_CUBIC && !g_cc_beta_set)
      g_cc_beta = 0.7;
   if (getenv("MSC_UDP_MIN_CWND") != NULL && atof(getenv("MSC_UDP_MIN_CWND")) >= 1.0)
      g_min_cwnd = atof(getenv("MSC_UDP_MIN_CWND"));
   if (getenv("MSC_UDP_RTO_GENTLE") != NULL && atoi(getenv("MSC_UDP_RTO_GENTLE")) != 0)
      g_rto_gentle = 1;
   if (getenv("MSC_UDP_MIN_RTO_MS") != NULL && atoll(getenv("MSC_UDP_MIN_RTO_MS")) > 0)
      g_min_rto_ns = (uint64_t)atoll(getenv("MSC_UDP_MIN_RTO_MS")) * 1000ULL * 1000ULL;
   if (getenv("MSC_UDP_STALL_TIMEOUT_MS") != NULL)
   {
      unsigned long long ms = strtoull(getenv("MSC_UDP_STALL_TIMEOUT_MS"), NULL, 10);
      g_stall_timeout_ns = ms > UINT64_MAX / 1000000ULL
                         ? UINT64_MAX : (uint64_t)ms * 1000000ULL;
   }
   if (getenv("MSC_UDP_RWND") != NULL && atoi(getenv("MSC_UDP_RWND")) == 0)
      g_rwnd = 0;
   if (getenv("MSC_UDP_DSACK") != NULL && atoi(getenv("MSC_UDP_DSACK")) == 0)
      g_dsack = 0;
   if (getenv("MSC_UDP_REORDER_SRTT_DIV") != NULL &&
       atoi(getenv("MSC_UDP_REORDER_SRTT_DIV")) > 0)
      g_reorder_div = atoi(getenv("MSC_UDP_REORDER_SRTT_DIV"));
   if (getenv("MSC_UDP_RWND_STALL_MS") != NULL && atoll(getenv("MSC_UDP_RWND_STALL_MS")) > 0)
      g_rwnd_stall_ns = (uint64_t)atoll(getenv("MSC_UDP_RWND_STALL_MS")) * 1000ULL * 1000ULL;
   if (getenv("MSC_UDP_SOCK_AUTOTUNE") != NULL && atoi(getenv("MSC_UDP_SOCK_AUTOTUNE")) == 0)
      g_autotune = 0;
   if (getenv("MSC_UDP_AUTOTUNE_MAX") != NULL && atoll(getenv("MSC_UDP_AUTOTUNE_MAX")) > 0)
      g_autotune_max = atoll(getenv("MSC_UDP_AUTOTUNE_MAX"));
   if (g_autotune_max > INT_MAX)
      g_autotune_max = INT_MAX;   /* targets pass through (int) setsockopt */
   if (getenv("MSC_UDP_PMTUD") != NULL && atoi(getenv("MSC_UDP_PMTUD")) == 0)
      g_pmtud = 0;
   if (getenv("MSC_UDP_PMTUD_CAP") != NULL && atoll(getenv("MSC_UDP_PMTUD_CAP")) > 0)
      g_pmtud_cap = (size_t)atoll(getenv("MSC_UDP_PMTUD_CAP"));
   if (getenv("MSC_UDP_ECN") != NULL && atoi(getenv("MSC_UDP_ECN")) == 0)
      g_ecn = 0;
   if (getenv("MSC_UDP_FAIR") != NULL && atoi(getenv("MSC_UDP_FAIR")) != 0)
      g_fair = 1;
   if (wguard != NULL && atoi(wguard) != 0)
      apply_profile_def(&g_profile_wan, 0);   /* forced before any measurement */
   {
      /* Field-facing names. `fiber` and `fiber-shared` are deliberately the
       * same table as `wan`: the sweep that set out to give them different
       * values measured identical ones (FINDINGS.md section 7). They exist as
       * names because a user picks the environment they are crossing, not a
       * transport profile -- and if a later measurement does separate them,
       * the names are already in the contract. `wan` stays for existing
       * scripts that use it. */
      const char *prof = getenv("MSC_UDP_PROFILE");
      if (prof == NULL || strcmp(prof, "auto") == 0)
         g_profile = MSC_UDP_PROFILE_AUTO;
      else if (strcmp(prof, "lan") == 0)
         g_profile = MSC_UDP_PROFILE_LAN;
      else if (strcmp(prof, "wan") == 0 || strcmp(prof, "fiber") == 0 ||
               strcmp(prof, "fiber-shared") == 0)
         g_profile = MSC_UDP_PROFILE_WAN;
      else if (strcmp(prof, "wan-long") == 0 || strcmp(prof, "fiber-long") == 0 ||
               strcmp(prof, "longhaul") == 0)
         g_profile = MSC_UDP_PROFILE_WAN_LONG;
      else if (strcmp(prof, "geo") == 0 || strcmp(prof, "satellite") == 0)
         g_profile = MSC_UDP_PROFILE_GEO;
      else
      {
         fprintf(stderr, "MSC UDP: unknown MSC_UDP_PROFILE=%s (want "
                 "auto|lan|wan|fiber|fiber-shared|wan-long|fiber-long|geo), "
                 "using auto\n", prof);
         g_profile = MSC_UDP_PROFILE_AUTO;
      }
   }
   if (getenv("MSC_UDP_GEO_RTT_MS") != NULL && atoll(getenv("MSC_UDP_GEO_RTT_MS")) > 0)
      g_geo_rtt_threshold_ns =
         (uint64_t)atoll(getenv("MSC_UDP_GEO_RTT_MS")) * 1000ULL * 1000ULL;
   if (getenv("MSC_UDP_WAN_LONG_RTT_MS") != NULL &&
       atoll(getenv("MSC_UDP_WAN_LONG_RTT_MS")) > 0)
      g_wan_long_rtt_threshold_ns =
         (uint64_t)atoll(getenv("MSC_UDP_WAN_LONG_RTT_MS")) * 1000ULL * 1000ULL;
   if (getenv("MSC_UDP_WAN_RTT_MS") != NULL && atoll(getenv("MSC_UDP_WAN_RTT_MS")) > 0)
      g_wan_rtt_threshold_ns =
         (uint64_t)atoll(getenv("MSC_UDP_WAN_RTT_MS")) * 1000ULL * 1000ULL;
   {
      char err[MSC_UDP_PORT_ERRLEN];
      if (msc_port_spec_from_env(&g_ports, err, sizeof(err)) != 0)
      {
         fprintf(stderr, "MSC UDP: %s\n", err);
         exit(MSC_UDP_EXIT_CLI);
      }
   }
   g_ctl = msc_udp_ctl_mode();
   /* Report the mode for EVERY arm, not only the non-tcp ones. The old
    * condition meant "say something only when it is unusual", which was written
    * when tcp was the default; now tcp is the unusual one, and a suppressed
    * line made a tcp run indistinguishable from a run whose mode line was never
    * printed at all -- which is precisely what the acceptance harnesses read to
    * tell the arms apart. */
   if (g_stats)
      fprintf(stderr, "MSC UDP: control mode %s\n", msc_udp_ctl_mode_name(g_ctl));
}

/* Resolve the data-socket count for `flow_count` flows from MSC_UDP_PORTS and the
 * control mode, warning about combinations the mode cannot honor. Both ends
 * call this with the same env (msc's msc_udp_remote_env and both harnesses forward
 * MSC_UDP_* to the receiver), which is what lets the sender verify the receiver's
 * advertised list. */
static int resolve_nports(int flow_count)
{
   char err[MSC_UDP_PORT_ERRLEN];
   if (msc_udp_ctl_one_socket(g_ctl))
   {
      /* `one` IS the control socket and stdio1 is defined as the single-socket
       * funnel: the count is the mode, not a knob. The port SELECTION still
       * applies (stdio1 binds a real data socket), so resolve as a one-socket
       * transfer -- skipping that would leave the window unsized. A port list
       * keeps its first entry: the user named ports for a reason, and landing
       * somewhere else entirely would be worse than using fewer of them. */
      int asked = g_ports.mode == MSC_PORT_MODE_LIST ? g_ports.nlist
                                                     : g_ports.count_requested;
      if (asked > 1)
         fprintf(stderr, "MSC UDP: MSC_UDP_CTL=%s is a single-socket funnel; "
                 "using 1 data port, not %d\n", msc_udp_ctl_mode_name(g_ctl), asked);
      g_ports.count_requested = 0;
      if (g_ports.mode == MSC_PORT_MODE_LIST)
         g_ports.nlist = 1;
      flow_count = 1;
   }
   if (msc_port_spec_resolve(&g_ports, flow_count, err, sizeof(err)) != 0)
   {
      fprintf(stderr, "MSC UDP: %s\n", err);
      exit(MSC_UDP_EXIT_CLI);
   }
   if (msc_udp_ctl_one_socket(g_ctl))
      return 1;
   if (g_ports.count_clamped)
      fprintf(stderr, "MSC UDP: %d data ports were requested but there are only "
              "%d flows; using %d\n", g_ports.count_requested, flow_count,
              g_ports.count);
   return g_ports.count;
}

/* ===== shared transfer context ===========================================
 * A transfer is an ordered list of files concatenated into one global unit
 * space: global units [files[i].unit_base, files[i].unit_base + files[i].nunits)
 * belong to file i. A single-file transfer is just a one-entry table. Units are
 * file-aligned (a unit never straddles two files), so the partial final unit of
 * each file lands at the file's tail. */
struct msc_udp_file
{
   int fd;             /* shared by all flow_workers; pread/pwrite are positional */
   off_t base;         /* first byte to read (sender) / write (receiver) */
   uint64_t size;      /* bytes to move from this file */
   uint64_t unit_base; /* global unit index of this file's first unit */
   uint64_t nunits;    /* ceil(size / payload); always > 0 for a table entry */
   void *map;          /* receiver: MAP_SHARED mmap of the dest, or NULL. Flows
                        * memcpy into it instead of pwritev'ing -- avoids the
                        * single-inode write-lock serialization that caps a big
                        * file at ~1 writer's rate. Only when base == 0. */
};

struct msc_udp_xfer
{
   int flow_count;
   size_t payload_size;
   uint64_t transfer_id;
   uint64_t total_units;   /* sum of files[].nunits */
   uint64_t total_bytes;   /* sum of files[].size */
   struct msc_udp_file *files; /* in manifest order; every entry has nunits > 0 */
   uint64_t nfiles;
   /* source Lustre layout, used only on the single-file path (the recursive table
    * paths leave these 0, which forces the contiguous partition for a tree). */
   uint64_t stripe_size;   /* source Lustre stripe size, bytes; 0 if unknown */
   uint32_t stripe_count;  /* source Lustre stripe count; 0 if unknown */
   int allow_mmap;         /* receiver may size/map a complete single file */
   /* multi-machine OST partitioning: this worker only owns stripes whose OST
    * index falls in [my_ost_start, my_ost_start + my_ost_count). When
    * my_ost_count == 0 the worker owns all stripes (single-machine mode). */
   int my_ost_start;
   int my_ost_count;
   /* Sender side, rate mode: bumped once by each flow that runs out of work.
    * The work is split into flow_count equal partitions up front, so flows
    * retire at different times and every survivor's share of the path grows as
    * a STEP -- which is exactly the shape a windowed-max bandwidth filter
    * cannot follow. The survivors can infer it from the queue draining (see
    * rate_pipe_not_full), but only a round trip or three later; the sender
    * already knows the instant it happens, so say so.
    * Atomic because flow workers are threads. */
   _Atomic uint32_t flows_retired;
   volatile sig_atomic_t *cancel_flag;
};

/* Data-socket topology, shared by both sides. `nports` (K) sockets carry
 * `flow_count` (N) flows under the mapping flow f -> socket f % K; the two
 * per-flow arrays are aliases into the K owned sockets, so the flow threads
 * keep indexing by flow id and never learn about K.
 *
 *   sock_fds[k]  the K distinct descriptors -- the ONLY close list (in `one`
 *                mode sock_fds[0] aliases controlfd, which the fd's creator
 *                closes; see the session_close comments)
 *   receive_demux[k]       socket k's demux, or NULL when it carries a single flow and
 *                that flow reads it directly (no demux thread at all)
 *   udp_fds[f]   == sock_fds[f % K]
 *   receive_demux_of[f]    == receive_demux[f % K]
 *
 * K == N with every receive_demux NULL is the historic default; K == 1 with one demux
 * is the historic funnel. */
struct msc_udp_sockset
{
   int nports;              /* K */
   int flow_count;           /* N */
   int *sock_fds;           /* K entries */
   struct msc_udp_receive_demux **receive_demux;    /* K entries; NULL where the socket has one flow */
   int *udp_fds;            /* N entries, aliasing sock_fds */
   struct msc_udp_receive_demux **receive_demux_of; /* N entries, aliasing receive_demux */
};

struct msc_udp_sender
{
   int controlfd;
   struct msc_udp_sockset ss;
   int flow_count;
   uint64_t next_transfer_id;
   int one_mode;          /* data rides the control socket (MSC_UDP_CTLMODE_ONE) */
};

struct msc_udp_receiver
{
   int controlfd;
   struct msc_udp_sockset ss;
   int flow_count;
   uint64_t next_transfer_id;
   int one_mode;          /* data rides the control socket (MSC_UDP_CTLMODE_ONE) */
};

/* flows on socket k under f -> f % K: k, k+K, k+2K, ... */
static int sock_share(int flow_count, int nports, int k)
{
   return (flow_count - k + nports - 1) / nports;
}

/* How a flow's local units map onto global file units. Two modes:
 *
 *   contiguous  - flow f owns one byte-range [lo_unit, lo_unit+units); local seq
 *                 maps to global lo_unit+seq. The portable default.
 *   affine      - OST-affinity: stripes round-robin across flows (stripe j -> flow
 *                 j mod nflows), so when nflows == stripe_count each flow owns
 *                 exactly one OST's stripes and flow_workers stop contending for the
 *                 same OST's LDLM lock. A stripe holds ustripe = ceil(stripe_size/
 *                 payload) units; the last unit in every stripe is a partial-payload
 *                 tail (stripe_size - (ustripe-1)*payload bytes) so no unit ever
 *                 straddles a stripe boundary, whatever the payload size.
 *
 * In affine mode a flow's local seq is laid out stripe-by-stripe: the s-th owned
 * stripe is global stripe f + s*nflows, and seq = s*ustripe + within. */
struct msc_udp_part
{
   uint64_t lo_unit;     /* contiguous: first global unit */
   uint64_t units;       /* units this flow owns */
   int affine;           /* 1 = OST-affine stripe mapping, 0 = contiguous */
   int flow_id;
   int nflows;
   uint64_t ustripe;     /* units per stripe (affine only) */
   int my_ost_start;    /* multi-machine: first OST this flow's worker owns; -1 = all */
   int my_ost_count;    /* multi-machine: number of OSTs owned; 0 = all */
   uint32_t stripe_count; /* total Lustre stripe count (for OST filtering) */
};

/* whether OST-affinity geometry is usable: we know the layout and have at least
 * one flow per OST. No longer requires the stripe to divide evenly into patties --
 * a partial-payload tail closes each stripe. xfer_index and make_part both consult
 * this so the unit count and the partition always agree. */
static int affine_geom_ok(int nflows, uint64_t stripe_size, uint32_t stripe_count)
{
   return !g_no_affinity && stripe_count >= 1 && nflows >= (int)stripe_count &&
          stripe_size > 0;
}

static void make_part(struct msc_udp_part *p, uint64_t total_units, int nflows, int f,
                      size_t payload, uint64_t stripe_size, uint32_t stripe_count,
                      int x_ost_start, int x_ost_count)
{
   p->flow_id = f;
   p->nflows = nflows;
   p->lo_unit = 0;
   p->ustripe = 0;
   p->affine = 0;
   p->my_ost_start = x_ost_start;
   p->my_ost_count = x_ost_count;
   p->stripe_count = stripe_count;

   /* OST-affinity needs known stripe geometry, enough flows to cover every OST,
    * and a file with data; stripes that don't divide evenly into patties just get
    * a shorter final unit (ustripe = ceil), so we no longer fall back on them. */
   if (affine_geom_ok(nflows, stripe_size, stripe_count) && total_units > 0)
   {
      uint64_t U = (stripe_size + (uint64_t)payload - 1) / (uint64_t)payload;
      uint64_t total_stripes = (total_units + U - 1) / U;
      uint64_t last_units = total_units - (total_stripes - 1) * U;  /* 1..U */
      uint64_t owned = 0;

      if (x_ost_count > 0 && stripe_count > 0)
      {
         /* OST-partitioned multi-machine mode: this worker only owns stripes
          * whose OST index falls in [x_ost_start, x_ost_start + x_ost_count).
          * Stripes round-robin across OSTs: stripe s -> OST (s % stripe_count).
          * Within the owned stripes, distribute across nflows flow_workers. */
         uint64_t s, my_idx = 0;
         for (s = 0; s < total_stripes; s++)
         {
            int ost = (int)(s % (uint64_t)stripe_count);
            if (ost >= x_ost_start && ost < x_ost_start + x_ost_count)
            {
               if ((int)(my_idx % (uint64_t)nflows) == f)
                  owned++;
               my_idx++;
            }
         }
      }
      else
      {
         /* standard single-machine affine: stripe j -> flow j % nflows */
         if ((uint64_t)f < total_stripes)
            owned = (total_stripes - 1 - (uint64_t)f) / (uint64_t)nflows + 1;
      }
      p->affine = 1;
      p->ustripe = U;
      p->units = owned * U;
      /* if this flow owns the file's final (possibly partial) stripe, trim it */
      if (owned > 0)
      {
         int last_owned = 0;
         if (x_ost_count > 0 && stripe_count > 0)
         {
            int last_ost = (int)((total_stripes - 1) % (uint64_t)stripe_count);
            if (last_ost >= x_ost_start && last_ost < x_ost_start + x_ost_count)
            {
               uint64_t s2, my_idx2 = 0;
               for (s2 = 0; s2 < total_stripes; s2++)
               {
                  int o = (int)(s2 % (uint64_t)stripe_count);
                  if (o >= x_ost_start && o < x_ost_start + x_ost_count)
                  {
                     if (s2 == total_stripes - 1 &&
                         (int)(my_idx2 % (uint64_t)nflows) == f)
                        last_owned = 1;
                     my_idx2++;
                  }
               }
            }
         }
         else
            last_owned = ((total_stripes - 1) % (uint64_t)nflows == (uint64_t)f);
         if (last_owned)
            p->units -= (U - last_units);
      }
      return;
   }

   /* contiguous fallback */
   {
      uint64_t per = total_units / (uint64_t)nflows;
      uint64_t rem = total_units % (uint64_t)nflows;
      uint64_t fu = (uint64_t)f;
      p->lo_unit = fu * per + (fu < rem ? fu : rem);
      p->units = per + (fu < rem ? 1 : 0);
   }
}

/* global file unit for this flow's local unit `seq` */
static uint64_t part_global(const struct msc_udp_part *p, uint64_t seq)
{
   uint64_t s, within, stripe;
   if (!p->affine)
      return p->lo_unit + seq;

   if (p->my_ost_count > 0 && p->stripe_count > 0)
   {
      /* OST-partitioned multi-machine mode: local seq numbers only cover
       * stripes belonging to this worker's OSTs. We must find the n-th
       * owned stripe (where n = seq / ustripe) and the position within it
       * (within = seq % ustripe). The owned stripes are distributed across
       * flow_workers round-robin, so we need the n-th stripe assigned to flow_id
       * among all owned stripes. */
      uint64_t target_flow_stripe = seq / p->ustripe;  /* which of MY stripes */
      uint64_t flow_hit = 0;
      uint64_t global_stripe;
      uint64_t my_idx = 0;
      within = seq % p->ustripe;

      for (global_stripe = 0; ; global_stripe++)
      {
         int ost = (int)(global_stripe % (uint64_t)p->stripe_count);
         if (ost >= p->my_ost_start && ost < p->my_ost_start + p->my_ost_count)
         {
            if ((int)(my_idx % (uint64_t)p->nflows) == p->flow_id)
            {
               if (flow_hit == target_flow_stripe)
                  return global_stripe * p->ustripe + within;
               flow_hit++;
            }
            my_idx++;
         }
      }
      /* unreachable if make_part counted owned correctly */
   }

   /* standard single-machine affine: stripe j -> flow j % nflows */
   s = seq / p->ustripe;
   within = seq % p->ustripe;
   stripe = (uint64_t)p->flow_id + s * (uint64_t)p->nflows;
   return stripe * p->ustripe + within;
}

/* fill unit_base/nunits and the running totals from each file's size + payload.
 * In affine mode units are counted stripe-by-stripe (ceil(stripe_size/payload) per
 * stripe, the last stripe partial) rather than ceil(size/payload), because the
 * partial-payload stripe tails make a multi-stripe file need slightly more units
 * than flat payload tiling -- undercount it and the file's tail bytes get no unit.
 * Affine geometry only exists on the single-file path; trees pass 0 -> ceil tiling.
 * x->stripe_size/stripe_count must be set before this runs. */
static void xfer_index(struct msc_udp_xfer *x)
{
   uint64_t base = 0, bytes = 0, i;
   int affine = affine_geom_ok(x->flow_count, x->stripe_size, x->stripe_count);
   uint64_t U = affine
      ? (x->stripe_size + (uint64_t)x->payload_size - 1) / (uint64_t)x->payload_size : 0;
   for (i = 0; i < x->nfiles; i++)
   {
      uint64_t sz = x->files[i].size, nu;
      if (affine)
      {
         uint64_t full = sz / x->stripe_size;          /* whole stripes */
         uint64_t rem = sz - full * x->stripe_size;     /* bytes in the last stripe */
         nu = full * U + (rem ? (rem + (uint64_t)x->payload_size - 1) / (uint64_t)x->payload_size
                              : 0);
      }
      else
         nu = (sz + (uint64_t)x->payload_size - 1) / (uint64_t)x->payload_size;
      x->files[i].unit_base = base;
      x->files[i].nunits = nu;
      base += nu;
      bytes += sz;
   }
   x->total_units = base;
   x->total_bytes = bytes;
}

/* file index owning global unit g (g < total_units). *hint caches the last hit
 * for O(1) amortized lookup on the in-order access a flow mostly does; a binary
 * search covers retransmits and out-of-order arrival. Every table entry has
 * nunits > 0, so the owning file is unique. */
static uint64_t xfer_file_of(const struct msc_udp_xfer *x, uint64_t g, uint64_t *hint)
{
   uint64_t i = (hint != NULL && *hint < x->nfiles) ? *hint : 0;
   if (g < x->files[i].unit_base ||
       g >= x->files[i].unit_base + x->files[i].nunits)
   {
      uint64_t lo = 0, hi = x->nfiles;   /* largest unit_base <= g */
      while (hi - lo > 1)
      {
         uint64_t mid = lo + (hi - lo) / 2;
         if (x->files[mid].unit_base <= g) lo = mid; else hi = mid;
      }
      i = lo;
   }
   if (hint != NULL) *hint = i;
   return i;
}

/* byte offset of global unit g within file fl's transferred region (excludes base),
 * under this flow's partition. Contiguous: units tile the file at payload stride.
 * Affine: each stripe is one stripe_size block of ustripe units that are payload-
 * spaced within the stripe, so unit g lands at (g/ustripe)*stripe_size +
 * (g%ustripe)*payload -- the partial tails never shift later units off their payload
 * grid, they only shorten the last unit of each stripe. */
static uint64_t unit_off(const struct msc_udp_xfer *x, const struct msc_udp_part *p,
                         const struct msc_udp_file *fl, uint64_t g)
{
   uint64_t local = g - fl->unit_base;
   if (p->affine)
   {
      uint64_t s = local / p->ustripe;
      uint64_t i = local % p->ustripe;
      return s * x->stripe_size + i * (uint64_t)x->payload_size;
   }
   return local * (uint64_t)x->payload_size;
}

/* payload length of global unit g within file fl. A unit is payload-sized unless it
 * is the partial-payload tail of a stripe (affine) or runs into the end of the file;
 * either way it is capped so it never reads past fl->size. */
static uint64_t unit_len(const struct msc_udp_xfer *x, const struct msc_udp_part *p,
                         const struct msc_udp_file *fl, uint64_t g)
{
   uint64_t off = unit_off(x, p, fl, g);
   uint64_t len = (uint64_t)x->payload_size;
   if (p->affine)
   {
      uint64_t i = (g - fl->unit_base) % p->ustripe;
      if (i == p->ustripe - 1)                          /* last unit of the stripe */
         len = x->stripe_size - i * (uint64_t)x->payload_size;
   }
   if (off + len > fl->size) len = fl->size - off;      /* last unit of the file */
   return len;
}

/* ===== control_channel (TCP control) helpers ===================================== */
static int send_u32(int fd, uint32_t v) { uint32_t n = htonl(v); return msc_udp_send_all(fd, &n, 4); }
static int send_u64(int fd, uint64_t v) { uint64_t n = msc_udp_hton64(v); return msc_udp_send_all(fd, &n, 8); }
static int recv_u32(int fd, uint32_t *v)
{
   uint32_t n;
   if (msc_udp_recv_all(fd, &n, 4) != 0) return -1;
   *v = ntohl(n);
   return 0;
}
static int recv_u64(int fd, uint64_t *v)
{
   uint64_t n;
   if (msc_udp_recv_all(fd, &n, 8) != 0) return -1;
   *v = msc_udp_ntoh64(n);
   return 0;
}

static int transfer_status_valid(uint32_t status)
{
   return status == MSC_EXIT_CLI || status == MSC_EXIT_SOURCE ||
          status == MSC_EXIT_DESTINATION || status == MSC_EXIT_AUTH ||
          status == MSC_EXIT_NETWORK || status == MSC_EXIT_INTEGRITY ||
          status == MSC_EXIT_INCOMPATIBLE || status == MSC_EXIT_INTERNAL ||
          status == MSC_EXIT_SIGINT || status == MSC_EXIT_SIGTERM;
}

/* Report a receiver-local failure over the reliable control stream. In the
 * UDP-control modes drain waits for acknowledgment before teardown; TCP needs
 * no explicit drain. Exported (not static): udp_transport.c also needs this
 * for receiver-side failures that happen before msc_udp_receiver_open(), e.g.
 * make_temp_destination() rejecting a pre-existing destination -- without it
 * the sender (already blocked reading the port list, the first thing
 * msc_udp_sender_open() does) just sees the socket close and reports a
 * misleading "bad port list from receiver" instead of the real reason. */
int msc_udp_send_transfer_error(int fd, int status)
{
   if (!transfer_status_valid((uint32_t)status) ||
       send_u32(fd, MSC_UDP_CTL_ERROR) != 0 ||
       send_u32(fd, (uint32_t)status) != 0)
      return -1;
   return msc_udp_control_channel_drain(
      msc_udp_control_channel_lookup(fd));
}

/* A data-flow failure may be local to the sender, so never block indefinitely
 * looking for a peer status. Receiver errors are written before its flows are
 * torn down and are already pending by the time the sender's stall bound fires. */
static int receive_transfer_error_if_ready(int fd)
{
   struct msc_udp_control_channel *channel =
      msc_udp_control_channel_lookup(fd);
   uint32_t tag, status;
   int ready;
   if (channel != NULL)
      ready = msc_udp_control_channel_wait_readable(channel, 50);
   else
   {
      struct pollfd p;
      p.fd = fd;
      p.events = POLLIN;
      do
         ready = poll(&p, 1, 50);
      while (ready < 0 && errno == EINTR);
      if (ready > 0 && !(p.revents & (POLLIN | POLLHUP)))
         ready = 0;
   }
   if (ready <= 0 ||
       recv_u32(fd, &tag) != 0 || tag != MSC_UDP_CTL_ERROR ||
       recv_u32(fd, &status) != 0 || !transfer_status_valid(status))
      return -1;
   return (int)status;
}

/* recv_all with an idle bound, for the recursive data handoff where the peer
 * has just gone silent on the shim (it is entering the raw-UDP data flows) and
 * a plain msc_udp_recv_all would orphan us past every watchdog: the stall/idle
 * timeout lives inside the flow loops this wedged handoff never reaches, and the
 * shim's own retransmit-limit death needs unacked SENT data we do not have while
 * waiting to receive. Bounds the total idle wait at g_stall_timeout_ns (reset on
 * each byte received; 0 disables it, blocking like before). TCP/stdio fds are
 * not shims -- peer death is already an EOF there -- so fall back to the plain
 * blocking read. */
static int recv_all_stall_bounded(int fd, void *buf, size_t n)
{
   struct msc_udp_control_channel *channel = msc_udp_control_channel_lookup(fd);
   uint8_t *p = buf;
   size_t got = 0;
   uint64_t deadline;
   if (channel == NULL || g_stall_timeout_ns == 0)
      return msc_udp_recv_all(fd, buf, n);
   deadline = msc_udp_now_ns() + g_stall_timeout_ns;
   while (got < n)
   {
      int r = msc_udp_control_channel_wait_readable(channel, 100);
      if (r < 0)
         return -1;
      if (r == 0)
      {
         if (msc_udp_now_ns() >= deadline)
         {
            fprintf(stderr, "MSC UDP: no control progress for %.1f s at the "
                    "recursive data handoff, giving up\n",
                    (double)g_stall_timeout_ns / 1e9);
            return -1;
         }
         continue;
      }
      /* readable (or peer_fin): pull the byte that is buffered without blocking,
       * then keep going -- recv of 1 returns immediately here, or errors on a
       * peer FIN, which is exactly the teardown we want to surface. */
      if (msc_udp_control_channel_recv(channel, p + got, 1) != 0)
         return -1;
      got++;
      deadline = msc_udp_now_ns() + g_stall_timeout_ns;
   }
   return 0;
}
static int recv_u32_stall_bounded(int fd, uint32_t *v)
{
   uint32_t n;
   if (recv_all_stall_bounded(fd, &n, 4) != 0) return -1;
   *v = ntohl(n);
   return 0;
}
static int recv_u64_stall_bounded(int fd, uint64_t *v)
{
   uint64_t n;
   if (recv_all_stall_bounded(fd, &n, 8) != 0) return -1;
   *v = msc_udp_ntoh64(n);
   return 0;
}

/* ===== session-open HELLO (proto version + feature negotiation) ==========
 * Runs inside the session-open calls on both ends, right after the port list
 * and transparently to the callers: each end sends MSC_UDP_CTL_HELLO + its proto
 * version + the features it is willing to run, and keeps the intersection. A
 * version mismatch fails here with a clear message instead of mis-parsing a
 * changed wire struct somewhere deep in the transfer. */

static uint32_t g_feat_agreed = 0;   /* intersection of both ends' feature bits */

/* the feature set this end is willing to run; bits light up as the kernel-
 * parity features land (the transport design notes) */
static uint32_t my_feature_bits(void)
{
   uint32_t bits = 0;
   if (g_rwnd) bits |= MSC_UDP_FEAT_RWND;
   if (g_pmtud) bits |= MSC_UDP_FEAT_PMTUD;
   if (g_ecn) bits |= MSC_UDP_FEAT_ECN;
   if (g_fair) bits |= MSC_UDP_FEAT_FAIR;
   if (g_dsack) bits |= MSC_UDP_FEAT_DSACK;
   bits |= MSC_UDP_FEAT_DATA_RTT;
   return bits;
}

/* A random first transfer id, nonzero and with the top bit clear so the
 * per-transfer increment cannot wrap to 0 (0 marks "no transfer"). Every data
 * packet must carry the current id, so starting from 63 random bits instead of
 * 1 stops an off-path host from injecting packets it could otherwise predict. */
static uint64_t random_transfer_id(void)
{
   uint64_t v = 0;
   ssize_t got = getrandom(&v, sizeof(v), 0);
   if (got != (ssize_t)sizeof(v))
   {
      /* getrandom is Linux 3.17+; fall back to a weaker but unpredictable-
       * enough mix rather than refusing to transfer. */
      struct timespec ts;
      clock_gettime(CLOCK_REALTIME, &ts);
      v = ((uint64_t)ts.tv_nsec << 32) ^ (uint64_t)ts.tv_sec ^
          ((uint64_t)getpid() << 16) ^ msc_udp_now_ns();
   }
   v &= ~(UINT64_C(1) << 63);
   return v != 0 ? v : 1;
}

static int hello_sender(int controlfd, uint64_t first_transfer_id)
{
   uint32_t tag, ver, peer;
   /* The HELLO round trip is the first -- and until data ACKs arrive, the only
    * -- RTT sample available. W0/W1 use it to seed the flows and pick the
    * profile, so a WAN is recognized before the first byte of data moves. */
   uint64_t t0 = msc_udp_now_ns();
   if (send_u32(controlfd, MSC_UDP_CTL_HELLO) != 0 ||
       send_u32(controlfd, MSC_UDP_PROTO_VERSION) != 0 ||
       send_u32(controlfd, my_feature_bits()) != 0 ||
       send_u64(controlfd, first_transfer_id) != 0 ||
       recv_u32(controlfd, &tag) != 0 || tag != MSC_UDP_CTL_HELLO ||
       recv_u32(controlfd, &ver) != 0 ||
       recv_u32(controlfd, &peer) != 0)
   {
      fprintf(stderr, "MSC UDP sender: HELLO failed (peer running an older MSC UDP?)\n");
      return -1;
   }
   if (ver != MSC_UDP_PROTO_VERSION)
   {
      fprintf(stderr, "MSC UDP sender: protocol mismatch (receiver v%u, mine v%u) -- "
              "rebuild both ends\n", ver, (unsigned int)MSC_UDP_PROTO_VERSION);
      return -1;
   }
   g_feat_agreed = my_feature_bits() & peer;
   {
      uint64_t now = msc_udp_now_ns();
      path_profile_select(now > t0 ? now - t0 : 0);
   }
   return 0;
}

static int hello_receiver(int controlfd, uint64_t *first_transfer_id)
{
   uint32_t tag, ver, peer;
   if (recv_u32(controlfd, &tag) != 0 || tag != MSC_UDP_CTL_HELLO ||
       recv_u32(controlfd, &ver) != 0 ||
       recv_u32(controlfd, &peer) != 0)
   {
      fprintf(stderr, "MSC UDP receiver: HELLO failed (peer running an older MSC UDP?)\n");
      return -1;
   }
   /* Check the version before reading further: a different version may not
    * send the fields that follow. */
   if (ver != MSC_UDP_PROTO_VERSION)
   {
      fprintf(stderr, "MSC UDP receiver: protocol mismatch (sender v%u, mine v%u) -- "
              "rebuild both ends\n", ver, (unsigned int)MSC_UDP_PROTO_VERSION);
      return -1;
   }
   if (recv_u64(controlfd, first_transfer_id) != 0 || *first_transfer_id == 0 ||
       (*first_transfer_id >> 63) != 0)
   {
      fprintf(stderr, "MSC UDP receiver: HELLO carried no valid transfer id\n");
      return -1;
   }
   if (send_u32(controlfd, MSC_UDP_CTL_HELLO) != 0 ||
       send_u32(controlfd, MSC_UDP_PROTO_VERSION) != 0 ||
       send_u32(controlfd, my_feature_bits()) != 0)
   {
      fprintf(stderr, "MSC UDP receiver: HELLO reply failed\n");
      return -1;
   }
   g_feat_agreed = my_feature_bits() & peer;
   return 0;
}

/* ===== socket setup ====================================================== */
/* apply one buffer size and report what the kernel actually granted (it stores
 * double the applied size for bookkeeping), or -1. The autotune growth path
 * uses the return to detect the net.core.{r,w}mem_max clamp. */
static int set_buf_size(int fd, int opt, int bytes)
{
   int sz = bytes;
   socklen_t len = sizeof(sz);
   int got = 0;
   setsockopt(fd, SOL_SOCKET, opt, &sz, sizeof(sz)); /* best-effort */
   if (getsockopt(fd, SOL_SOCKET, opt, &got, &len) != 0)
      return -1;
   return got / 2;
}

/* current kernel-granted buffer for fd (kernel books double the applied size),
 * or -1. Used where one mode must OBSERVE the shared socket's buffer instead
 * of re-applying a per-flow size over the aggregate one. */
static int get_buf_size(int fd, int opt)
{
   int got = 0;
   socklen_t len = sizeof(got);
   if (getsockopt(fd, SOL_SOCKET, opt, &got, &len) != 0)
      return -1;
   return got / 2;
}

static void set_buf(int fd, int opt)
{
   int applied = set_buf_size(fd, opt, g_sock_buffer);
   /* The kernel silently clamps to net.core.{r,w}mem_max. If we got noticeably
    * less than asked, the sysctl ceiling is throttling us -- warn under
    * MSC_UDP_STATS so a too-small rmem_max is visible rather than
    * silently capping the receive window. Raise it with e.g.
    * sysctl -w net.core.rmem_max=. */
   static int warned_rcv = 0, warned_snd = 0;   /* one warning per opt per process */
   int *warned = (opt == SO_RCVBUF) ? &warned_rcv : &warned_snd;
   if (g_stats && !*warned && applied >= 0 && applied < g_sock_buffer)
   {
      *warned = 1;
      fprintf(stderr, "MSC UDP: %s clamped to %d bytes (asked %d) -- raise "
              "net.core.%smem_max\n",
              opt == SO_RCVBUF ? "SO_RCVBUF" : "SO_SNDBUF",
              applied, g_sock_buffer, opt == SO_RCVBUF ? "r" : "w");
   }
}

/* Initial SO_RCVBUF request for receiver data sockets. With autotune on and no
 * explicit MSC_UDP_SOCK_BUFFER, ask for the autotune ceiling up front and let
 * the kernel clamp to rmem_max: SO_RCVBUF is a limit, not an allocation, and
 * reactive growth only arrives after the burst that needed it. Measured over
 * 2 GiB on a 36 Gbit/s IPoIB link: UdpInErrors 522-986 per run with 16 MiB +
 * reactive growth, 0 with the ceiling seeded, at equal or better throughput. */
static int rcvbuf_seed_bytes(void)
{
   int64_t want;
   if (!g_autotune || g_sock_buffer_set)
      return g_sock_buffer;
   want = g_autotune_max;
   if (want > INT_MAX) want = INT_MAX;
   if (want < g_sock_buffer) want = g_sock_buffer;
   return (int)want;
}

/* Normalize aggregate socket buffering when several flows share one socket.
 * The default topology gives each of N flows its own g_sock_buffer-sized
 * SO_RCVBUF/SO_SNDBUF; a socket carrying `share` flows gets ~share x that
 * (clamped to MSC_UDP_AUTOTUNE_MAX/INT_MAX; the kernel's net.core.*mem_max still
 * clamps below) so kernel buffering is comparable and a K-port A/B measures
 * the funnel, not a starved socket. share == 1 therefore reproduces the
 * per-flow default exactly. The applied sizes are reported under MSC_UDP_STATS as
 * an explicit experiment variable. */
static void set_shared_bufs(int fd, int share, const char *side)
{
   int64_t agg = (int64_t)g_sock_buffer * (share > 0 ? share : 1);
   int rcv_target, rcv, snd;
   if (agg > g_autotune_max) agg = g_autotune_max;
   if (agg > INT_MAX) agg = INT_MAX;
   rcv_target = (int)agg;
   if (strcmp(side, "receiver") == 0 && rcvbuf_seed_bytes() > rcv_target)
      rcv_target = rcvbuf_seed_bytes();
   rcv = set_buf_size(fd, SO_RCVBUF, rcv_target);
   snd = set_buf_size(fd, SO_SNDBUF, (int)agg);
   if (g_stats)
      fprintf(stderr, "MSC UDP %s: shared socket SO_RCVBUF %dKiB "
              "SO_SNDBUF %dKiB (aggregate target %lldKiB for %d flows)\n",
              side, rcv / 1024, snd / 1024, (long long)(agg / 1024), share);
}

/* disable Nagle on the control_channel: greeting, port list, the manifest stream, and
 * the final DONE/PUBLISH are all small and latency-sensitive, and Nagle's
 * interaction with delayed ACKs can stall each one by tens of ms -- pure idle
 * time on a short transfer. Best-effort. */
static void set_nodelay(int fd)
{
   int one = 1;
   setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

static int connect_tcp(const char *host, unsigned int port)
{
   struct sockaddr_in addr;
   struct hostent *server = gethostbyname(host);
   int fd;
   if (server == NULL)
   {
      fprintf(stderr, "MSC UDP: no such host %s\n", host);
      exit(MSC_EXIT_NETWORK);
   }
   fd = socket(AF_INET, SOCK_STREAM, 0);
   if (fd < 0) { perror("MSC UDP control channel socket"); exit(MSC_EXIT_INTERNAL); }
   memset(&addr, 0, sizeof(addr));
   addr.sin_family = AF_INET;
   memcpy(&addr.sin_addr.s_addr, server->h_addr, (size_t)server->h_length);
   addr.sin_port = htons((uint16_t)port);
   if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
   {
      fprintf(stderr, "MSC UDP: control channel connect to %s:%u failed: %s\n",
              host, port, strerror(errno));
      exit(MSC_EXIT_NETWORK);
   }
   set_nodelay(fd);
   return fd;
}

/* One exclusive UDP bind. Returns the fd, or -1 with errno preserved so the
 * caller can tell "busy" (EADDRINUSE, expected on a shared machine) from a
 * real fault (EACCES on a privileged port, ENOBUFS, ...). NO SO_REUSEADDR and
 * NO SO_REUSEPORT: EADDRINUSE is the exclusion that makes concurrent receivers
 * land on disjoint ports without coordinating, exactly as
 * msc_udp_control_channel_bind_range depends on it. */
static int bind_one_port(unsigned int port)
{
   struct sockaddr_in addr;
   int fd = socket(AF_INET, SOCK_DGRAM, 0);
   if (fd < 0)
      return -1;
   memset(&addr, 0, sizeof(addr));
   addr.sin_family = AF_INET;
   addr.sin_addr.s_addr = INADDR_ANY;
   addr.sin_port = htons((uint16_t)port);
   if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
   {
      int saved = errno;
      close(fd);
      errno = saved;
      return -1;
   }
   return fd;
}

static void setup_data_socket(int fd)
{
   struct timeval tv;
   tv.tv_sec = 0;
   tv.tv_usec = MSC_UDP_FEEDBACK_MS * 1000;
   set_buf(fd, SO_RCVBUF);
   set_buf(fd, SO_SNDBUF);
   setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
   if (g_gro)
   {
      int one = 1;
      setsockopt(fd, SOL_UDP, UDP_GRO, &one, sizeof(one)); /* best-effort */
   }
   if (g_ecn)
   {
      int one = 1;   /* deliver each datagram's TOS byte so CE marks count */
      setsockopt(fd, IPPROTO_IP, IP_RECVTOS, &one, sizeof(one));
   }
}

/* receiver: bind n udp data sockets per `spec` and record their ports.
 *
 * LIST/BLOCK ask for exact ports, so a busy one is fatal -- silently shifting
 * would break the firewall contract the user asked for. Every requested port is
 * tried before giving up, so one run reports the whole conflict instead of
 * making the user rediscover it a port at a time.
 *
 * SCAN walks [base, base+span) and keeps the first n that bind, skipping busy
 * ports. Nothing is racy: a won port stays bound in this process for the life
 * of the transfer, so a second receiver scanning the same window at the same
 * instant simply lands elsewhere. */
static int *bind_udp_receiver(const struct msc_port_spec *spec, int n,
                              unsigned int *ports)
{
   int *fds = msc_udp_alloc(sizeof(int) * (size_t)n, "udp fds");
   unsigned int busy[MSC_UDP_MAX_FLOWS];
   int nbusy = 0, nbound = 0, i;

   for (i = 0; i < n; i++)
      fds[i] = -1;

   if (spec->mode == MSC_PORT_MODE_LIST || spec->mode == MSC_PORT_MODE_BLOCK)
   {
      for (i = 0; i < n; i++)
      {
         unsigned int p = spec->mode == MSC_PORT_MODE_LIST
                             ? spec->list[i] : spec->base + (unsigned)i;
         fds[i] = bind_one_port(p);
         if (fds[i] < 0)
         {
            if (errno != EADDRINUSE)
            {
               fprintf(stderr, "MSC UDP receiver: cannot bind data port %u: %s\n",
                       p, strerror(errno));
               goto fail;
            }
            busy[nbusy++] = p;
            continue;
         }
         ports[i] = p;
         nbound++;
      }
      if (nbusy > 0)
      {
         char list[MSC_UDP_PORT_TEXTLEN];
         msc_port_spec_format(busy, nbusy, list, sizeof(list));
         fprintf(stderr, "MSC UDP receiver: requested data port%s %s %s in use by "
                 "another transfer\n", nbusy == 1 ? "" : "s", list,
                 nbusy == 1 ? "is" : "are");
         goto fail;
      }
   }
   else
   {
      unsigned int p;
      for (p = spec->base; p < spec->base + spec->span && nbound < n; p++)
      {
         int fd = bind_one_port(p);
         if (fd < 0)
         {
            if (errno == EADDRINUSE)
               continue;             /* someone else's transfer; try the next */
            fprintf(stderr, "MSC UDP receiver: cannot bind data port %u: %s\n",
                    p, strerror(errno));
            goto fail;
         }
         fds[nbound] = fd;
         ports[nbound] = p;
         nbound++;
      }
      if (nbound < n)
      {
         fprintf(stderr, "MSC UDP receiver: could not find %d free data port%s in "
                 "%u-%u (%d free); retry, or widen the window with "
                 "--udp-port-span\n", n, n == 1 ? "" : "s", spec->base,
                 spec->base + spec->span - 1, nbound);
         goto fail;
      }
   }

   for (i = 0; i < n; i++)
      setup_data_socket(fds[i]);
   return fds;

fail:
   for (i = 0; i < n; i++)
      if (fds[i] >= 0)
         close(fds[i]);
   free(fds);
   exit(MSC_UDP_EXIT_NETWORK);
}

/* The one line every transfer prints about its ports, on both ends, before any
 * data moves. Always stderr: stdout may be the transferred file itself (-o -),
 * and under MSC_UDP_CTL=stdio* the receiver's stdout IS the control channel. */
static void report_data_ports(const char *side, const char *host,
                              const unsigned int *ports, int n, int flow_count)
{
   char list[MSC_UDP_PORT_TEXTLEN];
   msc_port_spec_format(ports, n, list, sizeof(list));
   fprintf(stderr, "MSC UDP %s: %d data port%s%s%s: %s (%d flow%s", side, n,
           n == 1 ? "" : "s", host != NULL ? " on " : "",
           host != NULL ? host : "", list, flow_count, flow_count == 1 ? "" : "s");
   if (n < flow_count)
      fprintf(stderr, ", flow f -> socket f mod %d", n);
   fprintf(stderr, ")\n");
}

/* sender: create N udp sockets, connect each to the receiver's matching port */
static int *connect_udp_sender(const char *host, int n, const unsigned int *ports)
{
   int *fds = msc_udp_alloc(sizeof(int) * (size_t)n, "udp fds");
   struct hostent *server = gethostbyname(host);
   int i;
   if (server == NULL) { fprintf(stderr, "MSC UDP: no such host %s\n", host); exit(MSC_EXIT_NETWORK); }
   for (i = 0; i < n; i++)
   {
      struct sockaddr_in addr;
      fds[i] = socket(AF_INET, SOCK_DGRAM, 0);
      if (fds[i] < 0) { perror("MSC UDP sender udp socket"); exit(MSC_EXIT_INTERNAL); }
      memset(&addr, 0, sizeof(addr));
      addr.sin_family = AF_INET;
      memcpy(&addr.sin_addr.s_addr, server->h_addr, (size_t)server->h_length);
      addr.sin_port = htons((uint16_t)ports[i]);
      if (connect(fds[i], (struct sockaddr *)&addr, sizeof(addr)) < 0)
      { fprintf(stderr, "MSC UDP: data connect failed: %s\n", strerror(errno)); exit(MSC_EXIT_NETWORK); }
      set_buf(fds[i], SO_SNDBUF);
      set_buf(fds[i], SO_RCVBUF);
      if (g_ecn)
      {
         int tos = 0x02;   /* ECT(0): let the fabric mark CE instead of dropping */
         setsockopt(fds[i], IPPROTO_IP, IP_TOS, &tos, sizeof(tos));
      }
   }
   return fds;
}

/* Wire up a sockset's alias arrays and spawn a demux for every socket carrying
 * more than one flow. A single-flow socket gets no demux: its flow reads it
 * directly, which is both the fast path and exactly what the default topology
 * has always done. `hs` (non-NULL only in `one` mode, where the data socket IS
 * the control socket) attaches the shim's inbound path to socket 0's demux.
 * Takes ownership of sock_fds. */
static void sockset_build(struct msc_udp_sockset *ss, int flow_count, int nports,
                          int *sock_fds, struct msc_udp_control_channel *hs,
                          const char *side)
{
   int k, f;
   ss->nports = nports;
   ss->flow_count = flow_count;
   ss->sock_fds = sock_fds;
   ss->receive_demux = msc_udp_alloc(sizeof(*ss->receive_demux) * (size_t)nports, "receive_demux table");
   ss->udp_fds = msc_udp_alloc(sizeof(int) * (size_t)flow_count, "udp fds");
   ss->receive_demux_of = msc_udp_alloc(sizeof(*ss->receive_demux_of) * (size_t)flow_count, "receive_demux aliases");
   for (k = 0; k < nports; k++)
   {
      int share = sock_share(flow_count, nports, k);
      ss->receive_demux[k] = NULL;
      if (share > 1)
      {
         /* several flows on one socket: exactly one reader (this demux) fans
          * packets out to their flow queues, and the buffers scale to the
          * flows sharing it */
         set_shared_bufs(sock_fds[k], share, side);
         ss->receive_demux[k] = msc_udp_receive_demux_start(sock_fds[k], flow_count, k, nports,
                                    k == 0 ? hs : NULL);
      }
   }
   for (f = 0; f < flow_count; f++)
   {
      ss->udp_fds[f] = sock_fds[f % nports];
      ss->receive_demux_of[f] = ss->receive_demux[f % nports];
   }
   if (g_stats && nports != flow_count)
      fprintf(stderr, "MSC UDP %s: %d flows over %d data socket%s "
              "(flow f -> socket f %% %d)\n", side, flow_count, nports,
              nports == 1 ? "" : "s", nports);
}

/* Stop every demux and free the alias tables. Never closes a descriptor: the
 * session owns sock_fds and closes each exactly once (see session_close). */
static void sockset_teardown(struct msc_udp_sockset *ss)
{
   int k;
   if (ss->receive_demux != NULL)
      for (k = 0; k < ss->nports; k++)
         msc_udp_receive_demux_request_stop(ss->receive_demux[k]);
   if (ss->receive_demux != NULL)
      for (k = 0; k < ss->nports; k++)
         msc_udp_receive_demux_stop(ss->receive_demux[k]);   /* NULL-safe */
   free(ss->receive_demux);
   free(ss->receive_demux_of);
   free(ss->udp_fds);
   ss->receive_demux = NULL;
   ss->receive_demux_of = NULL;
   ss->udp_fds = NULL;
}

/* Payload policy, PMTUD-aware. Both ends run this with the SAME probe result (the
 * receiver replies what it saw; the sender adopts the reply), so the tree path --
 * where each end computes its own payload from the CLI -- still agrees. A default
 * payload rises to the discovered ceiling (capped at MSC_UDP_PMTUD_CAP); an explicit
 * -s is clamped DOWN to the path's real capacity but never raised, so a too-big
 * request degrades with a warning instead of black-holing. */
static size_t pick_payload(size_t requested)
{
   size_t maxpay = MSC_UDP_MAX_DATAGRAM - sizeof(struct msc_udp_packet_header);
   if (requested == 0)
   {
      size_t p = MSC_UDP_DEFAULT_PAYLOAD_SIZE;
      if (g_pmtud && g_payload_probe > 0)
      {
         p = g_payload_probe;
         if (p > g_pmtud_cap) p = g_pmtud_cap;
      }
      return p;
   }
   if (requested > maxpay)
   {
      fprintf(stderr, "MSC UDP: payload size %zu too large (max %zu per datagram)\n",
              requested, maxpay);
      exit(MSC_EXIT_CLI);
   }
   if (g_pmtud && g_payload_probe > 0 && requested > g_payload_probe)
   {
      fprintf(stderr, "MSC UDP: payload %zu exceeds the probed path MTU payload; "
              "clamping to %zu\n", requested, g_payload_probe);
      return g_payload_probe;
   }
   return requested;
}

/* ===== PMTUD probe exchange (session open, after the HELLO) ===============
 * PLPMTUD-style (RFC 8899 spirit): real probe datagrams with DF set are the
 * only truth; the kernel's classical-PMTUD route MTU merely seeds the ladder.
 * An ICMP-black-holed size is simply never confirmed, so the failure mode is
 * "pick the smaller size", never a hang. */

/* candidate probe ladder: total UDP payload sizes worth confirming. Standards
 * (eth 1500, IPoIB-datagram 2044, 4k, jumbo 9000, max) plus the kernel's own
 * route-MTU guess and the exact ceiling we would use. All <=
 * min(MSC_UDP_MAX_DATAGRAM, cap + header): probing above the cap would confirm
 * sizes we would refuse to use anyway. */
static int pmtud_ladder(int seed_mtu, size_t *cand, int maxn)
{
   size_t hdr = sizeof(struct msc_udp_packet_header);
   /* A remote black hole need not appear in the local route MTU. Start below
    * normal tunnel MTUs, including the conservative IPv4 576-byte IP size. */
   size_t lid[] = { 548, 768, 1024, 1248,
                    sizeof(struct msc_udp_packet_header) + MSC_UDP_DEFAULT_PAYLOAD_SIZE,
                    1472, 2016, 4068, 8972, 65507 };
   size_t lim = g_pmtud_cap + hdr;
   int n = 0;
   unsigned i;
   if (lim > MSC_UDP_MAX_DATAGRAM) lim = MSC_UDP_MAX_DATAGRAM;
   for (i = 0; i < sizeof(lid) / sizeof(lid[0]); i++)
      if (lid[i] > hdr && lid[i] <= lim && n < maxn)
         cand[n++] = lid[i];
   if (seed_mtu > 28 && (size_t)(seed_mtu - 28) > hdr &&
       (size_t)(seed_mtu - 28) <= lim && n < maxn)
      cand[n++] = (size_t)seed_mtu - 28;
   if (n < maxn)
      cand[n++] = lim;   /* always probe the ceiling we would actually use */
   /* sort + dedup (n is tiny) */
   {
      int a, b;
      for (a = 0; a < n; a++)
         for (b = a + 1; b < n; b++)
            if (cand[b] < cand[a])
            { size_t t = cand[a]; cand[a] = cand[b]; cand[b] = t; }
      for (a = 1, b = 0; a < n; a++)
         if (cand[a] != cand[b]) cand[++b] = cand[a];
      n = b + 1;
   }
   return n;
}

static void pmtud_sleep_ms(long ms)
{
   struct timespec ts;
   ts.tv_sec = ms / 1000;
   ts.tv_nsec = (ms % 1000) * 1000000L;
   nanosleep(&ts, NULL);
}

/* sender side: fire the ladder at the receiver on flow 0 (DF set so nothing is
 * fragmented into a false positive), tell it over the control_channel, and adopt the
 * largest payload it confirms. A total probe loss leaves g_payload_probe at 0;
 * session opening then fails instead of assuming an unconfirmed payload. */
/* Watch the sender's UDP socket until `until_ns` for the ack to the RTT probe
 * identified by `probe_id`, retransmitting it every `retx_gap_ns`.
 *
 * This MUST be called before the control MTU exchange. The receiver answers
 * MSC_UDP_RTT_PROBE only inside pmtud_probe_receiver(), and that loop ends when
 * it consumes the control marker -- after which it sends the MTU confirmation
 * that unblocks the sender. So by the time the sender is past the exchange, the
 * receiver is provably gone and no probe can be answered. Reading the ack here,
 * while the receiver is still listening, is what makes the sample a round trip
 * instead of a measure of the sender's own startup: reading it only after the
 * exchange would make every sample carry the PMTUD ladder's 30 ms of sleeps
 * plus the receiver's 60 ms straggler grace, reading ~92-94 ms on every path.
 *
 * It also makes the retransmits real. They could never be answered from the
 * post-exchange loop, so their documented job -- stopping one lost probe from
 * burning the whole budget -- was silently not being done.
 *
 * Returns 1 and sets *sample_ns on the first matching ack; 0 if the window
 * closed with nothing. */
static int rtt_ack_drain(int udpfd, struct msc_udp_receive_demux *receive_demux,
                         uint64_t until_ns, uint64_t probe_id,
                         uint64_t *next_retx_ns, uint64_t retx_gap_ns,
                         uint64_t *sample_ns)
{
   char rbuf[sizeof(struct msc_udp_packet_header)];
   for (;;)
   {
      uint64_t now = msc_udp_now_ns();
      int got;
      if (now >= until_ns)
         return 0;
      if (now >= *next_retx_ns)
      {
         struct msc_udp_packet_header rp;
         memset(&rp, 0, sizeof(rp));
         rp.magic = htonl(MSC_UDP_MAGIC);
         rp.type = htonl(MSC_UDP_RTT_PROBE);
         rp.flow_id = htonl(0);
         rp.length = htonl(0);
         rp.xfer_id = msc_udp_hton64(0);
         rp.seq = msc_udp_hton64(probe_id);      /* stable id */
         rp.offset = msc_udp_hton64(0);
         rp.send_tsc = msc_udp_hton64(now);      /* fresh timestamp */
         (void)send(udpfd, &rp, sizeof(rp), 0);
         *next_retx_ns = now + retx_gap_ns;
      }
      if (receive_demux != NULL)
      {
         got = msc_udp_receive_demux_pop(receive_demux, 0, rbuf, sizeof(rbuf),
                                         1000, NULL);
      }
      else
      {
         struct pollfd pfd;
         int wait = (int)((until_ns - now) / 1000000ULL);
         if (wait > 10) wait = 10;
         if (wait < 1) wait = 1;
         pfd.fd = udpfd;
         pfd.events = POLLIN;
         if (poll(&pfd, 1, wait) <= 0)
            continue;
         got = (int)recv(udpfd, rbuf, sizeof(rbuf), MSG_DONTWAIT);
      }
      if (got < (int)sizeof(struct msc_udp_packet_header))
         continue;
      {
         const struct msc_udp_packet_header *h =
            (const struct msc_udp_packet_header *)rbuf;
         uint64_t echoed, t;
         if (ntohl(h->magic) != MSC_UDP_MAGIC ||
             ntohl(h->type) != MSC_UDP_RTT_ACK ||
             ntohl(h->flow_id) != 0 ||
             msc_udp_ntoh64(h->xfer_id) != 0 ||
             msc_udp_ntoh64(h->seq) != probe_id)
            continue;
         /* echoed is whichever send's timestamp came back; it is <= now and
          * >= probe_id, so the sample is that probe's true round trip. */
         echoed = msc_udp_ntoh64(h->send_tsc);
         t = msc_udp_now_ns();
         if (echoed >= probe_id && t > echoed)
         {
            *sample_ns = t - echoed;
            return 1;
         }
      }
   }
}

static void pmtud_probe_sender(int udpfd, int controlfd,
                               struct msc_udp_receive_demux *receive_demux,
                               int do_pmtud)
{
   size_t cand[12];
   int seed_mtu = 0, n, round, i;
   uint32_t tag = 0, confirmed = 0;
   size_t hdr = sizeof(struct msc_udp_packet_header);
   char *buf;
   int disc = IP_PMTUDISC_PROBE;   /* DF set, path MTU ignored: probe the wire */
   socklen_t sl = sizeof(seed_mtu);
   uint64_t rtt_probe_t0 = 0;
   uint64_t rtt_sample = 0;        /* set only by rtt_ack_drain(), pre-exchange */
   uint64_t rtt_next_retx = 0;
   uint64_t rtt_retx_gap = (uint64_t)MSC_UDP_DATA_PROBE_RETX_MS * 1000000ULL;

   if (do_pmtud)
   {
      setsockopt(udpfd, IPPROTO_IP, IP_MTU_DISCOVER, &disc, sizeof(disc));
      getsockopt(udpfd, IPPROTO_IP, IP_MTU, &seed_mtu, &sl);   /* route MTU seed */
      n = pmtud_ladder(seed_mtu, cand, (int)(sizeof(cand) / sizeof(cand[0])));
   }
   else
      n = 0;

   g_payload_probe = 0;   /* a failed probe must not inherit a stale ceiling */
   buf = msc_udp_alloc(MSC_UDP_MAX_DATAGRAM, "pmtud probe");
   memset(buf, 0, MSC_UDP_MAX_DATAGRAM);
   if (g_feat_agreed & MSC_UDP_FEAT_DATA_RTT)
   {
      /* Send the first probe BEFORE the control MTU exchange: the receiver holds
       * its MTU confirmation until it has acked a probe, so a probe sent only
       * after the exchange would deadlock both ends until the timeout. rtt_probe_t0
       * is a stable id (seq); the wait loop retransmits under it, each send
       * stamping a fresh send_tsc. */
      struct msc_udp_packet_header *h = (struct msc_udp_packet_header *)buf;
      uint64_t nowp = msc_udp_now_ns();
      rtt_probe_t0 = nowp;
      h->magic = htonl(MSC_UDP_MAGIC);
      h->type = htonl(MSC_UDP_RTT_PROBE);
      h->flow_id = htonl(0);
      h->length = htonl(0);
      h->xfer_id = msc_udp_hton64(0);
      h->seq = msc_udp_hton64(rtt_probe_t0);
      h->offset = msc_udp_hton64(0);
      h->send_tsc = msc_udp_hton64(nowp);
      (void)send(udpfd, buf, sizeof(*h), 0);
      rtt_next_retx = nowp + rtt_retx_gap;
   }
   for (round = 0; do_pmtud && round < 3; round++)
   {
      for (i = 0; i < n; i++)
      {
         struct msc_udp_packet_header *h = (struct msc_udp_packet_header *)buf;
         h->magic = htonl(MSC_UDP_MAGIC);
         h->type = htonl(MSC_UDP_PROBE);
         h->flow_id = htonl(0);
         h->length = htonl((uint32_t)(cand[i] - hdr));
         h->xfer_id = msc_udp_hton64(0);            /* never matches a transfer */
         h->seq = msc_udp_hton64((uint64_t)cand[i]); /* identifies the probe size */
         h->offset = msc_udp_hton64(0);
         h->send_tsc = msc_udp_hton64(msc_udp_now_ns());
         (void)send(udpfd, buf, cand[i], 0);    /* EMSGSIZE = size unsendable */
      }
      /* This inter-round pause used to be a blind sleep. Spend it draining
       * instead: the receiver is still inside its probe loop (it has not seen
       * the control marker, which we send below), so this is the window in
       * which our probe's ack can actually be read. On any path whose RTT is
       * under the ladder's ~30 ms that costs nothing and yields a true round
       * trip; the blind sleep is what made every sample ~92-94 ms. */
      if (rtt_probe_t0 != 0 && rtt_sample == 0)
         (void)rtt_ack_drain(udpfd, receive_demux,
                             msc_udp_now_ns() + 10ULL * 1000 * 1000,
                             rtt_probe_t0, &rtt_next_retx, rtt_retx_gap,
                             &rtt_sample);
      else
         pmtud_sleep_ms(10);
   }
   /* Paths slower than the ladder need a little longer, and the no-PMTUD path
    * never entered the loop above at all. Bounded and ABSOLUTE: a multiple of
    * the handshake RTT cannot be used here, because that value is deliberately
    * untrustworthy -- stdio reads a local pipe and `many` over-reads ~7x (see
    * path_profile_reconsider). Beyond this we give up and keep the control
    * seed rather than adopt a contaminated sample. */
   if (rtt_probe_t0 != 0 && rtt_sample == 0)
      (void)rtt_ack_drain(udpfd, receive_demux,
                          msc_udp_now_ns() +
                          (uint64_t)MSC_UDP_DATA_PROBE_WAIT_MS * 1000000ULL,
                          rtt_probe_t0, &rtt_next_retx, rtt_retx_gap,
                          &rtt_sample);
   free(buf);

   if (send_u32(controlfd, MSC_UDP_CTL_MTU) != 0 ||
       send_u32(controlfd, (uint32_t)(do_pmtud ? cand[n - 1] : 0)) != 0 ||
       recv_u32(controlfd, &tag) != 0 || tag != MSC_UDP_CTL_MTU ||
       recv_u32(controlfd, &confirmed) != 0)
   {
      fprintf(stderr, "%s; using default payload\n",
              do_pmtud ? "MSC UDP sender: PMTUD exchange failed"
                       : "MSC UDP sender: pre-data RTT exchange failed");
      confirmed = 0;
   }
   if (do_pmtud && confirmed > hdr)
      g_payload_probe = (size_t)confirmed - hdr;
   disc = IP_PMTUDISC_WANT;   /* back to the default for the data phase */
   if (do_pmtud)
      setsockopt(udpfd, IPPROTO_IP, IP_MTU_DISCOVER, &disc, sizeof(disc));

   /* The sample, if there is one, was taken by rtt_ack_drain() BEFORE the
    * control exchange above -- the only window in which the receiver is still
    * answering probes. There is deliberately no read of the ack here: a value
    * obtained at this point has the PMTUD ladder's sleeps and the receiver's
    * straggler grace baked into it, which is exactly the ~92-94 ms artifact
    * this replaced. Validity is therefore structural rather than a range
    * check; the absolute guard below stays only as a coarse backstop.
    *
    * The old range check (>= 4/5 of the probe budget) is kept but can no
    * longer be the thing that saves us: it was calibrated when the receiver
    * lingered for the whole 1000 ms budget, and once that linger shrank to
    * 60 ms the artifact sailed underneath it at ~94 ms. A guard anchored to
    * g_handshake_rtt_ns was considered and rejected -- that signal is
    * deliberately untrustworthy (stdio reads a local pipe, `many` over-reads
    * ~7x), so on a 50 ms path it would discard the VALID sample. */
   if (rtt_probe_t0 != 0)
   {
      if (rtt_sample != 0 &&
          rtt_sample >= (uint64_t)MSC_UDP_DATA_PROBE_TIMEOUT_MS * 1000000ULL * 4 / 5)
         rtt_sample = 0;
      if (rtt_sample != 0)
      {
         g_data_rtt_ns = rtt_sample;
         /* Make the path decision before sender_flows_alloc seeds its first RTO. */
         path_profile_reconsider(rtt_sample);
         if (g_stats)
            fprintf(stderr, "MSC UDP sender: initial data RTT %.3fms\n",
                    (double)rtt_sample / 1e6);
      }
      else if (g_stats)
         fprintf(stderr, "MSC UDP sender: initial data RTT probe timed out; "
                         "using control RTT seed\n");
   }
   if (g_stats && do_pmtud)
   {
      /* probe_losses: ladder rungs at/above the confirmed ceiling that never
       * came back -- i.e. the width of the black-hole above the working size.
       * A high count next to a low ceiling is the signature of a capped path. */
      int probe_losses = 0;
      for (i = 0; i < n; i++)
         if ((uint32_t)cand[i] > confirmed) probe_losses++;
      fprintf(stderr, "MSC UDP sender: PMTUD seed_mtu %d confirmed_payload %u -> "
              "payload ceiling %zu (ladder %d, probe_losses %d)\n",
              seed_mtu, confirmed, g_payload_probe, n, probe_losses);
   }
}

/* receiver side: drain probes off UDP flow 0 while waiting for the sender's
 * control_channel marker, linger briefly for stragglers, then report the largest probe
 * that actually arrived. Bounded by the pre-data probe budget; the marker is
 * always consumed (blocking if need be) or its bytes would desync every later
 * control-channel read.
 *
 * In one mode the demux is the SOLE reader of the shared fd, so both legs go
 * through it: probes are popped from the flow-0 queue (where the demux routes
 * MSC_UDP_PROBE) and the control marker is watched via the shim's wait_readable --
 * never a raw recv()/poll() racing the demux on the shared socket. In many
 * mode the flow-0 data socket is still private (raw recv is fine) and only
 * the control poll swaps to the shim. */
static void pmtud_probe_receiver(int udpfd, int controlfd,
                                 struct msc_udp_receive_demux *receive_demux,
                                 int do_pmtud)
{
   size_t hdr = sizeof(struct msc_udp_packet_header);
   char *buf = msc_udp_alloc(MSC_UDP_MAX_DATAGRAM, "pmtud recv");
   uint32_t max_seen = 0, tag = 0, highest_sent = 0;
   g_payload_probe = 0;   /* a failed probe must not inherit a stale ceiling */
   int ctl_seen = 0;
   uint64_t deadline = msc_udp_now_ns() +
      (uint64_t)MSC_UDP_DATA_PROBE_TIMEOUT_MS * 1000ULL * 1000ULL;
   uint64_t quiet_end = 0;
   uint64_t last_probe_ns = 0;   /* arrival of the most recent ladder probe */
   int rtt_ack_sent = 0;
   struct sockaddr_storage peer;
   socklen_t peerlen = sizeof(peer);
   int have_peer = 0;
   struct msc_udp_control_channel *channel = msc_udp_control_channel_lookup(controlfd);

   for (;;)
   {
      uint64_t now = msc_udp_now_ns();
      ssize_t got;
      if (now > deadline) break;
      /* Three ways out, all requiring the marker consumed and (when the peer
       * negotiated it) a probe acked:
       *   - max_seen >= highest_sent: the whole ladder landed. Free, but it can
       *     ONLY fire on a path that carries the largest candidate -- and
       *     pmtud_ladder() always ends with the MSC_UDP_PMTUD_CAP ceiling,
       *     which a below-cap path never delivers.
       *   - idle gap: no probe for MSC_UDP_PMTUD_IDLE_MS. The sender emits the
       *     control marker only AFTER the entire ladder is on the wire, so once
       *     the marker is here nothing more will be sent and a quiet socket
       *     means the ladder has finished arriving. This is what a below-cap
       *     path takes instead of waiting out the full grace -- it needs no
       *     knowledge of the path MTU, which is the point: a local send
       *     succeeding says nothing about remote arrival.
       *   - quiet_end: unchanged backstop, so this can only ever shorten. */
      if (ctl_seen && (!((g_feat_agreed & MSC_UDP_FEAT_DATA_RTT) && !rtt_ack_sent)) &&
          (now > quiet_end || max_seen >= highest_sent ||
           (last_probe_ns != 0 &&
            now > last_probe_ns + (uint64_t)MSC_UDP_PMTUD_IDLE_MS * 1000000ULL)))
         break;

      /* drain any probes queued on flow 0 (1 ms SO_RCVTIMEO / pop timeout
       * paces this loop) */
      if (receive_demux != NULL)
      {
         got = (ssize_t)msc_udp_receive_demux_pop(receive_demux, 0, buf,
                                                  MSC_UDP_MAX_DATAGRAM, 1000, NULL);
         if (!have_peer && msc_udp_receive_demux_peer(receive_demux, &peer, &peerlen) == 0)
            have_peer = 1;
      }
      else
      {
         peerlen = sizeof(peer);
         got = recvfrom(udpfd, buf, MSC_UDP_MAX_DATAGRAM, 0,
                        (struct sockaddr *)&peer, &peerlen);
         if (got >= 0)
            have_peer = 1;
      }
      if (got >= (ssize_t)hdr)
      {
         const struct msc_udp_packet_header *h = (const struct msc_udp_packet_header *)buf;
         uint32_t type = ntohl(h->type);
         if (ntohl(h->magic) == MSC_UDP_MAGIC && type == MSC_UDP_RTT_PROBE &&
             have_peer)
         {
            struct msc_udp_packet_header ack;
            memset(&ack, 0, sizeof(ack));
            ack.magic = htonl(MSC_UDP_MAGIC);
            ack.type = htonl(MSC_UDP_RTT_ACK);
            ack.flow_id = h->flow_id;
            ack.length = htonl(0);
            ack.xfer_id = h->xfer_id;
            ack.seq = h->seq;
            ack.send_tsc = h->send_tsc;
            (void)sendto(udpfd, &ack, sizeof(ack), 0,
                         (struct sockaddr *)&peer, peerlen);
            if (!rtt_ack_sent && ctl_seen)
               /* probe answered after the control marker: give pmtud stragglers
                * (sent right after the probe) a grace, then let the loop break */
               quiet_end = msc_udp_now_ns() + 60ULL * 1000 * 1000;
            rtt_ack_sent = 1;
            continue;
         }
         if (do_pmtud && ntohl(h->magic) == MSC_UDP_MAGIC && type == MSC_UDP_PROBE)
         {
            last_probe_ns = msc_udp_now_ns();   /* arms the idle-gap exit */
            if ((uint32_t)got > max_seen)
               max_seen = (uint32_t)got;
         }
         continue;   /* keep draining while data is flowing */
      }

      if (!ctl_seen)
      {
         int readable;
         if (channel != NULL)
            readable = msc_udp_control_channel_wait_readable(channel, 5) > 0;
         else
         {
            struct pollfd pfd;
            pfd.fd = controlfd;
            pfd.events = POLLIN;
            readable = poll(&pfd, 1, 5) > 0 && (pfd.revents & POLLIN);
         }
         if (readable)
         {
            if (recv_u32(controlfd, &tag) != 0 || tag != MSC_UDP_CTL_MTU ||
                recv_u32(controlfd, &highest_sent) != 0)
            {
               fprintf(stderr, "MSC UDP receiver: PMTUD exchange failed\n");
               free(buf);
               return;
            }
            ctl_seen = 1;
            /* Linger only a short straggler grace, NOT the whole RTT budget:
             * this send is what completes the sender's control exchange, and
             * the sender cannot read the (already-sent) RTT ack until it does.
             * Holding the confirmation for the budget was the stdio stall --
             * the sender blocked here for the budget and then read the ack as a
             * near-deadline (bogus) sample. The `!(DATA_RTT && !rtt_ack_sent)`
             * break gate, plus quiet_end being re-armed when the probe is acked,
             * already keeps the loop alive until the RTT probe has been answered. */
            quiet_end = msc_udp_now_ns() + 60ULL * 1000 * 1000;
         }
      }
   }
   free(buf);

   /* the marker MUST be consumed (and replied to) even if the drain timed out */
   if (!ctl_seen)
   {
      if (recv_u32(controlfd, &tag) != 0 || tag != MSC_UDP_CTL_MTU ||
          recv_u32(controlfd, &highest_sent) != 0)
      {
         fprintf(stderr, "MSC UDP receiver: PMTUD exchange failed\n");
         return;
      }
   }
   (void)(send_u32(controlfd, MSC_UDP_CTL_MTU) == 0 &&
          send_u32(controlfd, max_seen) == 0);
   if (do_pmtud && max_seen > hdr)
      g_payload_probe = (size_t)max_seen - hdr;
   if (g_stats && do_pmtud)
      fprintf(stderr, "MSC UDP receiver: PMTUD saw payload %u -> payload ceiling %zu\n",
              max_seen, g_payload_probe);
}

/* ===== the sender (sender flow) =========================================== */

/* Aggregate cross-flow pacer: one token bucket shared by every flow so the SUM
 * of fresh sends cannot align into one microburst. MSC_UDP_FAIR refills it from the
 * flows' combined cwnd/SRTT target; MSC_UDP_PACE_RATE_MBIT instead supplies a hard
 * known payload-rate ceiling. Lock-free: rates are per-flow slots (each flow
 * writes only its own), the refill is serialized by a CAS on last_ns, and a
 * flow RESERVES its budget with a CAS before it sends (refunding whatever it
 * did not use). */
struct msc_udp_pacer
{
   int nflows;
   _Atomic uint64_t *rates_ups; /* per-flow pace rate, units/second (published) */
   uint64_t cap_ups;            /* hard aggregate cap, 0 = sum the flow targets */
   _Atomic uint64_t last_ns;    /* last refill timestamp (0 = not started) */
   _Atomic int64_t tokens_uq;   /* shared allowance, micro-units */
   int64_t burst_uq;            /* bucket depth, micro-units */
   _Atomic uint64_t granted;    /* total whole units the shared bucket released */
   _Atomic uint64_t depletions; /* reserves that got 0 -- bucket ran empty (backpressure) */
   uint64_t start_ns;           /* set once at alloc; for the granted-rate summary */
};

/* MSC_UDP_CC=rate: one slot of the 3-slot windowed-max delivery-rate filter
 * (Linux lib/minmax.c layout: value + the round it was measured in). */
struct msc_udp_rate_win { double v; uint64_t t; };

enum msc_udp_rate_state
{
   MSC_UDP_RATE_STARTUP = 0,   /* exponential search for the pipe's ceiling */
   MSC_UDP_RATE_DRAIN,         /* pace below the estimate until the queue empties */
   MSC_UDP_RATE_PROBE_BW,      /* steady state: cruise + periodic gain probe */
   MSC_UDP_RATE_PROBE_RTT      /* window to the floor to re-measure min RTT */
};

struct sender_flow
{
   struct msc_udp_xfer *x;
   int udpfd;
   struct msc_udp_receive_demux *receive_demux;         /* shared socket: acknowledgments arrive via this
                                 * flow's demux queue, never a raw recv on
                                 * udpfd. NULL when this flow owns udpfd */
   int shared_flows;            /* flows sharing udpfd (1 = private socket) */
   int buf_owner;               /* the one flow allowed to autotune udpfd */
   int flow_id;
   struct msc_udp_part part;        /* local-seq -> global-unit mapping */
   uint64_t units;              /* part.units (loop bound) */
   uint64_t file_hint;          /* cached file index for xfer_file_of */

   size_t ring_slots;          /* power of two; includes one unused slot */
   uint64_t snd_una;            /* oldest unacked local unit */
   uint64_t snd_nxt;            /* next new local unit to send */
   uint64_t recover;            /* fast-recovery episode boundary */

   double cwnd;                 /* sender capacity (units) */
   double ssthresh;
   uint64_t rwnd;               /* receiver space advertised (units) */
   int dupack;
   uint64_t last_cum_ack;
   uint64_t last_delivered;     /* receiver's `delivered` on the previous ack:
                                 * a repeated cum_ack only counts as a duplicate
                                 * ack if this advanced (see sender_take_acknowledgment) */

   /* RTT / RTO (nanoseconds) */
   double srtt, rttvar;
   uint64_t rto;
   int have_rtt;
   int rto_streak;

   /* token-bucket send pacing (pacing), used only when g_pace */
   double pace_tokens;          /* fresh-send allowance (units) */
   uint64_t pace_last_ns;       /* last token refill (0 = uninitialized) */

   /* CUBIC state (MSC_UDP_CC=cubic; RFC 8312) */
   double w_max;                /* window at the last reduction */
   double origin;               /* cubic origin point for this epoch */
   double K;                    /* seconds from epoch start to the origin */
   double w_est;                /* TCP-friendly (Reno-equivalent) estimate */
   uint64_t epoch_start_ns;     /* 0 = epoch not started */

   /* ECN (MSC_UDP_ECN) */
   uint64_t ce_seen;            /* highest cumulative CE count echoed so far */
   uint64_t ecn_cuts;           /* window cuts taken on CE (telemetry) */

   /* send-buffer autotuning (MSC_UDP_SOCK_AUTOTUNE) */
   uint64_t next_buf_check_ns;
   uint64_t enobufs;            /* sendmmsg ENOBUFS/EAGAIN events */
   uint64_t enobufs_last;
   int sndbuf_now;              /* applied SO_SNDBUF (kernel-reported / 2) */
   int sndbuf_maxed;            /* hit the wmem_max clamp or MSC_UDP_AUTOTUNE_MAX */
   uint64_t buf_grows;          /* growth events (telemetry) */

   /* aggregate cross-flow pacer (MSC_UDP_FAIR); NULL when off */
   struct msc_udp_pacer *pacer;
   uint64_t fair_starved;       /* rounds the shared bucket returned 0 (telemetry) */

   /* window-ring metadata, indexed by local seq % ring_slots */
   uint64_t *sent_ns;
   uint64_t *gap_seen_ns; /* first SACK/dupack report for a hole; WAN guard only */
   uint8_t *sacked;
   uint8_t *retransmitted;

   char *retransmit_cache;           /* ring_slots * payload bytes */
   unsigned int seed;
   int gso_enabled;              /* fallback belongs to this worker */

   /* batch scratch for fresh sends (one pread + one sendmmsg per ~64 units) */
   char *readbuf;                       /* MSC_UDP_SEND_BATCH * payload bytes */
   struct msc_udp_packet_header *hdrs;      /* MSC_UDP_SEND_BATCH */
   struct iovec *iov;                   /* 2 * MSC_UDP_SEND_BATCH */
   struct mmsghdr *msgs;                /* MSC_UDP_SEND_BATCH */
   char *gso_buf;                       /* contiguous [hdr|payload] packets; GSO only */

   uint64_t tx_units, rtx_units, acknowledgments, rtt_ambiguous;
   uint64_t rtx_rto, rtx_sack, rtx_dupack; /* rtx_units by trigger; exact, unlike
                                            * the rate-limited stream events */

   /* DSACK (MSC_UDP_FEAT_DSACK): spurious-retransmit detection and response */
   uint64_t spurious_rtx;        /* retransmits the receiver reported as duplicates */
   uint64_t reorder_extra_ns;    /* DSACK-driven growth on the adaptive window,
                                  * added to SRTT/DIV in flow_reorder_wait_ns() */
   uint64_t reorder_grow_gate_ns;/* next growth allowed (one srtt/4 step per RTT) */
   double undo_cwnd, undo_ssthresh, undo_wmax; /* pre-cut state (RFC 3708-style undo) */
   uint64_t undo_retrans;        /* episode retransmits not yet reported spurious */
   int undo_marker;              /* an undo-eligible episode is open */
   uint64_t undo_done;           /* episodes whose cut was reverted (telemetry) */

   /* MSC_UDP_CC=rate: delivery-rate estimation + delay-aware window.
    * All zero-initialized by flows_alloc's memset; the two rings are only
    * allocated in rate mode. */
   uint64_t rate_delivered;      /* newest receiver `delivered` seen in any ack */
   uint64_t rate_delivered_ns;   /* when it last advanced (local clock) */
   uint64_t first_tx_ns;         /* start of the current send-side sample window */
   uint64_t *del_at_send;        /* ring: rate_delivered when the unit was (re)sent */
   uint64_t *del_ns_at_send;     /* ring: rate_delivered_ns at that moment */
   uint64_t *ftx_at_send;        /* ring: first_tx_ns at that moment */
   struct msc_udp_rate_win bw_win[3]; /* windowed-max delivery rate (units/sec,
                                  * keyed by round) -- Linux minmax layout */
   double btl_bw_ups;            /* bw_win[0].v cache: the current estimate */
   uint64_t min_rtt_ns;          /* windowed-min RTT (0 = no clean sample yet) */
   uint64_t min_rtt_stamp_ns;    /* when min_rtt_ns was last confirmed fresh */
   uint64_t probe_rtt_min_ns;    /* freshest min observed during PROBE_RTT */
   uint64_t next_round_delivered;/* round edge: delivered at the newest send */
   uint64_t round_count;         /* packet-timed round trips completed */
   int round_start;              /* this ack crossed a round edge */
   int rate_state;               /* enum msc_udp_rate_state */
   int rate_filled;              /* startup found the pipe full */
   double full_bw;               /* startup growth high-water */
   int full_bw_cnt;              /* rounds without >=25% filter growth */
   int pbw_phase;                /* PROBE_BW gain-cycle index */
   uint64_t phase_start_ns;      /* current PROBE_BW phase began */
   uint64_t probe_rtt_done_ns;   /* dwell deadline; 0 = not yet at the floor */
   double rate_pacing_gain;      /* multiplies btl_bw in pace_grant */
   int rate_app_limited;         /* out of fresh data: samples may only raise */
   uint64_t hi_sack_seen;        /* newest sacked unit already rate-sampled */
   double inflight_hi;           /* loss-bounded inflight cap (0 = unset):
                                  * BBRv2-style backstop for paths whose burst
                                  * rate exceeds their sustainable rate (shallow
                                  * buffers), where the bw max-filter alone
                                  * overruns into chronic loss */
   uint64_t rtx_at_round;        /* rtx_units at the last round edge */
   uint64_t spurious_at_round;   /* spurious_rtx at the last round edge (DSACK-proven) */
   uint64_t delivered_at_round;  /* rate_delivered at the last round edge */
   int hi_loss_rounds;           /* consecutive rounds over the loss threshold; the
                                  * inflight_hi clamp waits for 2 so a one-round
                                  * reordering burst (whose DSACK proof lands a round
                                  * later) does not look like congestion */
   int startup_queue_rounds;     /* consecutive STARTUP rounds with SRTT past
                                  * g_startup_queue_mult * min_rtt */
   int undersub_rounds;          /* consecutive rounds proving this flow is
                                  * cwnd-limited on a queue-free path, i.e. the
                                  * bandwidth estimate is too low (see
                                  * rate_pipe_not_full) */
   double cwnd_at_probe_rtt;     /* window saved on entering PROBE_RTT, restored
                                  * on exit (BBR's bbr_restore_cwnd) */
   uint32_t retired_seen;        /* x->flows_retired when this flow last looked */
   uint64_t rate_restarts;       /* times STARTUP was re-entered (telemetry) */
   uint64_t rate_samples;        /* telemetry */
   int error;
};

#define RING_AT(arr, seq) ((arr)[(seq) & (g->ring_slots - 1)])

static uint64_t sender_window(const struct sender_flow *g)
{
   uint64_t c = (uint64_t)g->cwnd;
   uint64_t w;
   if (c < 1) c = 1;
   w = c < g->rwnd ? c : g->rwnd;
   if (w < 1) w = 1;
   if (w > g->ring_slots - 1) w = g->ring_slots - 1;
   return w;
}

/* Token-bucket pacing (pacing). Refill this flow's fresh-send allowance from the
 * time elapsed since the last call at rate g_pace_gain*cwnd/srtt (units/sec),
 * cap it at g_pace_burst, and return the whole-unit budget for this round. The
 * sender_window() bound still caps inflight; this only spreads a freed window
 * over the SRTT so N flows stop microbursting the receiver into rcvbuf overflow.
 * Consumed units are subtracted by the caller after it sends. */
static uint64_t pace_grant(struct sender_flow *g, uint64_t now)
{
   double srtt = g->srtt;
   double rate;
   if (g->pace_last_ns == 0)
      g->pace_last_ns = now;   /* first call: start the clock, spend the seed bucket */
   if (srtt < (double)g_pace_bootstrap_ns)
   {
      srtt = (double)g_pace_bootstrap_ns;
      /* Default is the historic 1 us floor; MSC_UDP_WAN_GUARD raises it until a
       * real RTT sample arrives, so every flow cannot refill its seed bucket
       * immediately after startup. */
   }
   /* rate mode: once a bandwidth estimate exists the pace IS the controller's
    * output (pacing_gain * btl_bw), not a smoothing of the window's edge.
    * Before the first sample the cwnd/srtt formula stands in, as it does for
    * every mode. */
   if (cc_uses_rate_samples() && g->btl_bw_ups > 0.0)
   {
      rate = g->rate_pacing_gain * g->btl_bw_ups / 1e9;   /* units per ns */
      /* engaged loss bound: do not pace a small window out as one burst the
       * shallow buffer it exists for cannot absorb */
      if (g->inflight_hi > 0.0 && g->min_rtt_ns > 0)
      {
         double cap = g->inflight_hi / (double)g->min_rtt_ns;
         if (rate > cap)
            rate = cap;
      }
      /* Never pace below the floor window's ack-clock rate. Pacing at the
       * estimate means we only ever measure what we pace, so an estimate
       * that collapsed during a stall (its real samples aged out of the max
       * filter) is self-sealing: with a sub-unit BDP the 1.25 probe phase
       * cannot deliver even one extra unit to raise it. Measured: a flow
       * pinned at 14 Mbit/s for 37 s on loopback while its 7 siblings
       * finished in 0.3 s. Flooring at MSC_UDP_RATE_MIN_CWND per min-RTT
       * lets a stuck flow re-measure at the window rate the acks already
       * prove; on a genuine WAN the floor is a rounding error (4 units per
       * 100 ms). */
      if (g->min_rtt_ns > 0)
      {
         double fl = MSC_UDP_RATE_MIN_CWND / (double)g->min_rtt_ns;
         if (rate < fl)
            rate = fl;
      }
   }
   else
      rate = g_pace_gain * g->cwnd / srtt;   /* units per ns */
   if (now > g->pace_last_ns)
   {
      g->pace_tokens += rate * (double)(now - g->pace_last_ns);
      g->pace_last_ns = now;
      if (g->pace_tokens > (double)g_pace_burst)
         g->pace_tokens = (double)g_pace_burst;
   }
   if (g->pace_tokens < 0.0)
      g->pace_tokens = 0.0;
   return (uint64_t)g->pace_tokens;
}

/* A SACK proves a later unit arrived, but not that this hole was lost: a
 * delayed shaper can release a burst out of order. The default remains eager
 * fast recovery. With MSC_UDP_REORDER_WAIT_MS (or MSC_UDP_WAN_GUARD), record the first
 * hole report and let the original arrive before remaking it. */
/* This flow's effective reordering window.
 *
 * An explicit MSC_UDP_REORDER_WAIT_MS, or MSC_UDP_WAN_GUARD's 10 ms preset,
 * wins unchanged. Otherwise it is derived from the flow's own SRTT, because a
 * fixed default cannot serve both fabrics MSC runs on: the historic default of
 * 0 (retransmit the instant a SACK exposes a hole) is right on IB, where
 * reordering is negligible, and catastrophic on a jittery WAN, where netem's
 * 2 ms jitter alone reorders enough to make the SACK fast retransmit fire
 * continuously. Measured on a netem WAN path: ~10900 retransmissions
 * per 2 GiB transfer with zero real loss, each one dragging cc_on_loss() with
 * it, for 0.02 Gbit/s against 5.4 for an uncontrolled blast.
 *
 * Before any RTT sample exists we keep the old immediate behaviour: with no
 * measurement there is nothing to scale to, and the handshake seeds a sample
 * almost immediately (W1). */
static uint64_t flow_reorder_wait_ns(const struct sender_flow *g)
{
   uint64_t w;
   if (g_reorder_wait_set)
      return g_reorder_wait_ns;
   if (!g->have_rtt)
      return 0;
   w = (uint64_t)(g->srtt / (double)g_reorder_div) + g->reorder_extra_ns;
   if (w > MSC_UDP_MAX_REORDER_WAIT_NS)
      w = MSC_UDP_MAX_REORDER_WAIT_NS;
   return w;
}

/* RFC 6298 retransmission timeout: SRTT + max(G, 4*RTTVAR), floored by
 * g_min_rto_ns and capped by MSC_UDP_MAX_RTO_NS, with G the sender's tick.
 * The reordering window is deliberately NOT an input: folding it into the
 * floor (as `SRTT + window`) would make one constant do two jobs,
 * so tuning the window for reordering silently moved every flow's timeout --
 * that coupling let a 10 ms window produce 1142 spurious RTO
 * retransmissions. The "do not let the RTO race the reordering hold" rule
 * lives in reorder_rto_held(), per-unit and explicit, not in this arithmetic.
 * Only update_rtt and the ack-path un-ratchet may assign g->rto from here;
 * the RTO backoff doubling in the sender loop ratchets the stored value. */
static uint64_t flow_rto_ns(const struct sender_flow *g)
{
   double var = 4.0 * g->rttvar;
   double gran = (double)g_tick_us * 1000.0;
   uint64_t r;
   if (var < gran) var = gran;
   r = (uint64_t)(g->srtt + var);
   if (r < g_min_rto_ns) r = g_min_rto_ns;
   if (r > MSC_UDP_MAX_RTO_NS) r = MSC_UDP_MAX_RTO_NS;
   return r;
}

static int reorder_hole_ready(struct sender_flow *g, uint64_t seq, uint64_t now)
{
   uint64_t *seen;
   uint64_t wait = flow_reorder_wait_ns(g);
   if (wait == 0)
      return 1;
   seen = &RING_AT(g->gap_seen_ns, seq);
   if (*seen == 0)
   {
      *seen = now;
      return 0;
   }
   return now - *seen >= wait;
}

/* If a acknowledgment has just reported this oldest unit as a hole, do not let the
 * sender-side RTO race the reordering hold. A genuine loss without later SACK
 * coverage still follows the normal RTO path. */
static int reorder_rto_held(const struct sender_flow *g, uint64_t seq, uint64_t now)
{
   uint64_t seen;
   uint64_t wait = flow_reorder_wait_ns(g);
   if (wait == 0)
      return 0;
   seen = RING_AT(g->gap_seen_ns, seq);
   return seen != 0 && now - seen < wait;
}

/* A acknowledgment that fills an old hole can advance cum_ack across packets the
 * receiver received much earlier. Its echoed send timestamp then measures the
 * recovery delay, not the path RTT. In guarded mode, retain the last clean
 * estimator rather than feeding that ambiguous sample into the RTO. */
static int ack_sample_ambiguous(const struct sender_flow *g, uint64_t first,
                                uint64_t end)
{
   uint64_t u;
   for (u = first; u < end; u++)
      if (RING_AT(g->retransmitted, u) || RING_AT(g->sacked, u) ||
          RING_AT(g->gap_seen_ns, u) != 0)
         return 1;
   return 0;
}

/* ----- MSC_UDP_CC=rate: delivery-rate estimation + delay-aware window ---
 *
 * The loss-driven controllers cannot serve both fabrics MSC runs on: cubic
 * under-runs a lossy WAN (at 0.1% loss / 50 ms RTT the Mathis bound already
 * caps 8 flows at ~77 Mbit/s on a gigabit path) and over-runs a fast fabric
 * (cwnd 11474 on clean IPoIB built a 39 ms queue over a 0.64 ms path and
 * induced the very drops it then reacted to). Loss is the wrong input on both.
 *
 * This controller estimates two things instead, per BBR's design:
 *   - the path's delivery rate: the acknowledgment already carries the
 *     receiver's cumulative deduplicated `delivered` count, so a send-time
 *     snapshot of (delivered, time) per unit turns every ack into a rate
 *     sample -- valid even for retransmitted units, because delivery is
 *     counted at the receiver rather than inferred from which copy an echo
 *     answered (that is what sidesteps Karn's ambiguity for this signal);
 *   - the path's queue-free floor: a windowed-min RTT from the clean samples
 *     update_rtt already isolates. A min filter needs an occasional clean
 *     sample, not a stream, so sack-churn sample droughts do not starve it.
 *
 * cwnd is then bounded by gain * (btl_bw * min_rtt): the window stops growing
 * into a queue once the pipe is full, and stops collapsing on loss that
 * carries no congestion information. Pacing follows pacing_gain * btl_bw. */

static const double rate_pbw_gains[8] = { 1.25, 0.75, 1, 1, 1, 1, 1, 1 };

/* 3-slot windowed max over MSC_UDP_RATE_BW_RTTS rounds (Linux minmax_running_max):
 * slot 0 is the max, slots 1-2 are the best of progressively newer sub-windows,
 * so the estimate degrades gracefully as the true max ages out. */
static void rate_bw_update(struct sender_flow *g, double val, uint64_t t)
{
   struct msc_udp_rate_win *s = g->bw_win;
   const uint64_t win = MSC_UDP_RATE_BW_RTTS;
   if (val >= s[0].v || t - s[2].t > win)
   {
      s[0].v = s[1].v = s[2].v = val;
      s[0].t = s[1].t = s[2].t = t;
   }
   else if (val >= s[1].v)
   {
      s[1].v = s[2].v = val;
      s[1].t = s[2].t = t;
   }
   else if (val >= s[2].v)
   {
      s[2].v = val;
      s[2].t = t;
   }
   if (t - s[0].t > win)
   {
      s[0] = s[1];
      s[1] = s[2];
      s[2].v = val; s[2].t = t;
      if (t - s[0].t > win)
      {
         s[0] = s[1];
         s[1] = s[2];
      }
   }
   else if (s[1].t == s[0].t && t - s[0].t > win / 4)
   {
      s[1].v = s[2].v = val;
      s[1].t = s[2].t = t;
   }
   else if (s[2].t == s[1].t && t - s[0].t > win / 2)
   {
      s[2].v = val;
      s[2].t = t;
   }
   g->btl_bw_ups = s[0].v;
}

/* estimated bandwidth-delay product in units. Before the first clean RTT
 * sample the handshake-seeded srtt stands in; 0 means "no estimate yet"
 * (no bound is applied, startup growth carries on). */
static double rate_bdp_units(const struct sender_flow *g)
{
   double rtt = g->min_rtt_ns > 0 ? (double)g->min_rtt_ns : g->srtt;
   if (g->btl_bw_ups <= 0.0 || rtt <= 0.0)
      return 0.0;
   return g->btl_bw_ups * rtt / 1e9;
}

static void rate_note_delivered(struct sender_flow *g, uint64_t delivered,
                                uint64_t now)
{
   if (delivered > g->rate_delivered)
   {
      g->rate_delivered = delivered;
      g->rate_delivered_ns = now;
   }
}

/* send-time snapshot backing the per-ack rate sample (fresh sends and
 * retransmits both re-stamp, so an interval never spans a unit's earlier life) */
static void rate_note_sent(struct sender_flow *g, uint64_t seq, uint64_t now)
{
   if (g->rate_delivered_ns == 0)
      g->rate_delivered_ns = now;   /* flow's clock starts at the first send */
   /* an idle restart opens a new send-side window (Linux tcp_rate_skb_sent):
    * only the first fresh unit of an empty pipe can have seq == snd_una */
   if (g->first_tx_ns == 0 || seq == g->snd_una)
      g->first_tx_ns = now;
   RING_AT(g->del_at_send, seq) = g->rate_delivered;
   RING_AT(g->del_ns_at_send, seq) = g->rate_delivered_ns;
   RING_AT(g->ftx_at_send, seq) = g->first_tx_ns;
}

/* min-RTT filter, fed from update_rtt's clean (unambiguous, non-retransmit)
 * samples. `<=` keeps the stamp fresh while the floor holds steady. */
static void rate_note_rtt(struct sender_flow *g, uint64_t sample, uint64_t now)
{
   if (g->rate_state == MSC_UDP_RATE_PROBE_RTT &&
       (g->probe_rtt_min_ns == 0 || sample < g->probe_rtt_min_ns))
      g->probe_rtt_min_ns = sample;
   if (g->min_rtt_ns == 0 || sample <= g->min_rtt_ns)
   {
      g->min_rtt_ns = sample;
      g->min_rtt_stamp_ns = now;
   }
}

/* One delivery-rate sample from the newest acked unit `seq`: the delivered
 * delta over the interval since that unit's (re)send. Also advances the
 * packet-timed round counter that keys the bw filter's window. */
static void rate_sample(struct sender_flow *g, uint64_t seq, uint64_t now)
{
   uint64_t d0 = RING_AT(g->del_at_send, seq);
   uint64_t t0 = RING_AT(g->del_ns_at_send, seq);
   uint64_t sent = RING_AT(g->sent_ns, seq);
   uint64_t ftx = RING_AT(g->ftx_at_send, seq);
   uint64_t ack_ns, snd_ns, interval;
   double ups;
   if (t0 == 0 || now <= t0 || g->rate_delivered <= d0)
      return;
   if (d0 >= g->next_round_delivered)
   {
      g->round_count++;
      g->next_round_delivered = g->rate_delivered;
      g->round_start = 1;
   }
   /* the interval is the LONGER of the send-side and ack-side elapse (Linux
    * tcp_rate_gen): a burst of acks compressed by the return path -- routine
    * on loopback, where the receiver drains in GRO batches -- makes the
    * ack-side delta tiny and would inflate the estimate several-fold; the
    * send-side window it answers cannot be compressed after the fact.
    * Measured before this guard: btlbw "14 Gbit/s" against a receiver
    * sustaining 1.6, so pacing overran it into 8% loss and RTO stalls. */
   snd_ns = sent > ftx ? sent - ftx : 0;
   ack_ns = now - t0;
   interval = snd_ns > ack_ns ? snd_ns : ack_ns;
   /* the sampled window's end starts the next send-side window */
   if (sent > g->first_tx_ns)
      g->first_tx_ns = sent;
   if (interval == 0)
      return;
   /* an interval shorter than the path's floor cannot be a trustworthy rate
    * measurement (BBR discards these outright) */
   if (g->min_rtt_ns > 0 && interval < g->min_rtt_ns)
      return;
   ups = (double)(g->rate_delivered - d0) * 1e9 / (double)interval;
   g->rate_samples++;
   /* app-limited tail: once the flow has dispatched all fresh data a sample
    * can only prove we had nothing to send. Let it raise the estimate but
    * keep it out of the expiring window, where it would evict a real one. */
   if (g->rate_app_limited && ups <= g->btl_bw_ups)
      return;
   rate_bw_update(g, ups, g->round_count);
}

/* Startup is over: drain the queue it built, then cruise. */
static void rate_enter_drain(struct sender_flow *g)
{
   g->rate_filled = 1;
   g->rate_state = MSC_UDP_RATE_DRAIN;
   g->rate_pacing_gain = 1.0 / MSC_UDP_RATE_HIGH_GAIN;
   g->startup_queue_rounds = 0;
   g->undersub_rounds = 0;
}

/* Back to exponential search. The windowed-max bandwidth filter can only track
 * a capacity that CHANGES SLOWLY: it has no way to climb after it has latched
 * a value too low, because everything downstream of it -- the pacing rate, the
 * cwnd target, and therefore the delivery rate the next sample measures -- is
 * derived from that same value. PROBE_BW's 1.25 gain does climb, but one phase
 * in eight is ~2 s per 25% at a 250 ms RTT, which is slower than the transfers
 * msc runs.
 *
 * Two situations produce a latched-low estimate, and both are ordinary:
 *   - startup ended on evidence that was not about the pipe (an RTO's window
 *     cut, an app-limited stretch);
 *   - a sibling flow FINISHED. msc splits the work into N equal partitions up
 *     front, so flows retire at different times and every survivor's share of
 *     the path grows as a step change. Measured: after 8 flows fell to 2 on the
 *     250 ms path, the aggregate sat at 0.10-0.29 Gbit/s -- one eighth of the
 *     path each -- for 60% of the transfer's data phase.
 *
 * rate_pipe_not_full() below decides when to call this; the min-RTT filter is
 * kept, being a property of the path rather than of our share of it. */
static void rate_restart_startup(struct sender_flow *g)
{
   g->rate_state = MSC_UDP_RATE_STARTUP;
   g->rate_filled = 0;
   g->rate_pacing_gain = MSC_UDP_RATE_HIGH_GAIN;
   g->full_bw = g->btl_bw_ups;   /* the plateau test restarts from where we are */
   g->full_bw_cnt = 0;
   g->startup_queue_rounds = 0;
   g->undersub_rounds = 0;
   /* Release the loss-bounded inflight cap along with the estimate. It was set
    * by losses taken while N flows shared this path, so it is a statement about
    * the OLD share -- and it only lifts 1.25x per round, which makes it, not
    * the bandwidth filter, the thing that governs how fast the flow can grow.
    * Measured: the last surviving flow on a 250 ms path climbed at exactly
    * 1.25x per round trip for the final 3 seconds of a transfer, with the bw
    * estimate free to move the whole time. Both conditions that reach here --
    * a sibling retiring, or a queue-free path -- say the old bound is stale;
    * if it is not, the next lossy round re-derives it in two rounds. */
   g->inflight_hi = 0.0;
   g->hi_loss_rounds = 0;
   g->rate_restarts++;
}

/* Has this flow proved it is NOT the bottleneck? Then the estimate under it is
 * a floor rather than a ceiling, and cruising will never find that out.
 *
 * The test is the absence of a queue: SRTT within 25% of the path's own min
 * RTT, with the round under the loss threshold and not app-limited. A flow
 * pacing at its estimate keeps roughly one BDP in flight, so on a saturated
 * path SRTT sits visibly above min_rtt -- that is what stops this from firing
 * forever on a path that really is full. When SRTT collapses back onto min_rtt
 * while we are still pacing at the old estimate, the queue we were sharing has
 * gone somewhere else.
 *
 * Note what this deliberately does NOT test: whether cwnd is the binding limit.
 * In rate mode the window is 2x the BDP by construction and the pacer is
 * always what binds, so an "inflight >= cwnd" condition can never be true and
 * the rule would be dead code (it was, in the first cut of this change). */
/* May this flow re-enter STARTUP at all?
 *
 * Two conditions, and the second is deliberately NOT the profile. The preset
 * says what kind of path the operator or the handshake believes this is;
 * g->min_rtt_ns is what the flow's own acknowledged packets measured. When
 * those disagree, the measurement wins -- and they DO disagree in practice:
 * on a 0.13 ms IPoIB link the handshake reads the path correctly at
 * 0.126 ms and picks `lan`, and then the pre-data UDP RTT probe reports
 * 91.77 ms -- a 700x overestimate that is really the receiver process starting
 * up -- and path_profile_reconsider() promotes the whole transfer to `wan`.
 * That is a pre-existing defect (the old binary does it too), but it means a
 * profile-name gate would have left this mechanism running on a 0.15 ms fabric,
 * which is exactly what it must not do. The per-flow min RTT is taken from real
 * ACK samples and was correct there: 0.128-0.194 ms.
 *
 * The threshold is the same 5 ms that separates lan from wan, because the
 * question is the same one: is a round trip long enough that PROBE_BW's 25%
 * per 8 RTT is too slow to matter? */
static int rate_restart_allowed(const struct sender_flow *g)
{
   return g_bw_restart && g->min_rtt_ns >= g_wan_rtt_threshold_ns;
}

static int rate_pipe_not_full(const struct sender_flow *g, int round_congested)
{
   if (round_congested || g->rate_app_limited)
      return 0;
   if (g->min_rtt_ns == 0 || g->srtt <= 0.0)
      return 0;
   return g->srtt <= (double)g->min_rtt_ns * 1.25;
}

/* per-ack state machine (called after this ack's samples are in) */
static void rate_update(struct sender_flow *g, uint64_t now)
{
   double bdp = rate_bdp_units(g);
   uint64_t inflight = g->snd_nxt - g->snd_una;
   uint64_t mrtt = g->min_rtt_ns > 0 ? g->min_rtt_ns : (uint64_t)g->srtt;
   /* set at a round edge below; read by the STARTUP/PROBE_BW arms */
   int round_congested = 0;      /* the round's loss cleared the 2% threshold */
   int round_retransmitted = 0;  /* the round retransmitted anything at all */

   /* loss-bounded inflight (BBRv2's loss response, simplified): ignoring loss
    * entirely is only right when loss carries no congestion information. A
    * round losing >2% is not random -- it is the path's queue overflowing
    * (measured on clean loopback: burst rate 15 Gbit/s, sustainable 1.6,
    * rcvbuf 208 KiB -> 8-14% rtx and 200 ms RTO stalls without this cap).
    * Clamp inflight below the level that lost, re-probe 1.25x per below-
    * threshold round.
    *
    * Two corrections for the geo class (600 ms RTT, 0.7% loss, reordering --
    * an emulated geo path, where rate collapsed to 0.01 Gbit/s while an
    * uncontrolled blast reached 0.18):
    *   (A) Retransmits the receiver later proved SPURIOUS by DSACK are not
    *       congestion. Reordering under 8 ms of jitter manufactures fast-rtx
    *       that this clamp would otherwise read as a full pipe; subtract the
    *       round's spurious delta so pure reordering never trips it. (The DSACK
    *       proof lags the rtx by ~1 RTT, so the correction is approximate at a
    *       round edge but strictly removes the reordering false positive.)
    *   (B) Release when the round is BELOW the 2% congestion threshold, not only
    *       when it is exactly loss-free. On a path with genuine 0.1-0.7% random
    *       loss a zero-loss round essentially never occurs at a useful window,
    *       so the old `losses == 0` release let inflight_hi ratchet down to
    *       MIN_CWND and never recover -- a self-sealing collapse (cwnd ~45 over
    *       600 ms). BBRv2 grows the cap on any sub-threshold round; do the same. */
   if (g->round_start)
   {
      uint64_t gross = g->rtx_units - g->rtx_at_round;
      uint64_t spur = g->spurious_rtx - g->spurious_at_round;
      uint64_t losses = gross > spur ? gross - spur : 0;   /* (A) drop DSACK-proven */
      uint64_t dlv = g->rate_delivered - g->delivered_at_round;
      round_retransmitted = gross > 0;
      round_congested = losses > dlv / 50 + 2;
      if (round_congested)
      {
         /* (C) require the excess to persist: reordering under jitter makes one
          * round look lossy, but its retransmits are proven spurious a round
          * later; only a real queue overflow stays over threshold. Without this,
          * a geo path (600 ms, 0.7% loss, 8 ms jitter) pinned ~5/8 flows at
          * cwnd == inflight_hi ~150, far below their BDP. */
         if (++g->hi_loss_rounds >= 2)
         {
            double hi = (double)inflight * 0.85;
            if (hi < MSC_UDP_RATE_MIN_CWND)
               hi = MSC_UDP_RATE_MIN_CWND;
            if (g->inflight_hi == 0.0 || hi < g->inflight_hi)
               g->inflight_hi = hi;
         }
      }
      else
      {
         g->hi_loss_rounds = 0;
         if (g->inflight_hi > 0.0)                          /* (B) sub-threshold: re-probe */
         {
            g->inflight_hi *= 1.25;
            if (g->inflight_hi > (double)(g->ring_slots - 1))
               g->inflight_hi = 0.0;   /* fully recovered: cap dissolves */
         }
      }
      g->rtx_at_round = g->rtx_units;
      g->spurious_at_round = g->spurious_rtx;
      g->delivered_at_round = g->rate_delivered;
   }

   switch (g->rate_state)
   {
   case MSC_UDP_RATE_STARTUP:
      /* full pipe = the filter stopped growing >=25% for 3 consecutive rounds
       * (an app-limited round proves nothing about the pipe and is not
       * counted against startup) */
      if (g->round_start && g->btl_bw_ups > 0.0)
      {
         /* Already searching, so the retirement of a sibling needs no state
          * change -- but the plateau counter it may have accumulated was
          * measured against the old share of the path, so restart the count
          * rather than ending startup on it. */
         uint32_t retired = atomic_load(&g->x->flows_retired);
         if (retired != g->retired_seen)
         {
            g->retired_seen = retired;
            g->full_bw = g->btl_bw_ups;
            g->full_bw_cnt = 0;
            g->startup_queue_rounds = 0;
         }
         /* A round that RETRANSMITTED proves nothing either, and for the same
          * reason: loss recovery cut the window, so the filter had less in
          * flight to measure with -- the estimate did not stop growing because
          * the pipe is full, it stopped because we stopped filling it.
          * Measured on a 250 ms / 0.01%-loss path: one early RTO at t~2 s
          * collapsed cwnd 54 -> 4, three quiet rounds followed, the flow
          * latched rate_filled with a tiny estimate, and then spent the next
          * 13 seconds climbing back at PROBE_BW's 1.25-per-cycle rate --
          * 0.10 Gbit/s on a path that carries 0.93. */
         if (g->btl_bw_ups >= g->full_bw * 1.25)
         {
            g->full_bw = g->btl_bw_ups;
            g->full_bw_cnt = 0;
         }
         else if (!g->rate_app_limited && !round_retransmitted &&
                  ++g->full_bw_cnt >= 3)
         {
            rate_enter_drain(g);
            break;
         }
         /* Delay exit (profiles that set startup_queue_mult). The plateau test
          * only fires once the pipe is ALREADY overfull, and STARTUP's 2.885
          * gain overshoots hard before it does: eight flows put 92 MB in flight
          * against a 31 MB BDP on the 250 ms path, a ~450 ms standing queue.
          * A queue that deep is itself the answer the plateau test is waiting
          * for, so stop as soon as SRTT says so. Two rounds, not one, so a
          * single jitter spike does not end startup early. */
         if (g_startup_queue_mult > 0.0 && g->min_rtt_ns > 0 && g->srtt > 0.0 &&
             g->srtt >= (double)g->min_rtt_ns * g_startup_queue_mult)
         {
            if (++g->startup_queue_rounds >= 2)
            {
               rate_enter_drain(g);
               break;
            }
         }
         else
            g->startup_queue_rounds = 0;
      }
      break;
   case MSC_UDP_RATE_DRAIN:
      if (bdp > 0.0 && (double)inflight <= bdp)
      {
         g->rate_state = MSC_UDP_RATE_PROBE_BW;
         g->pbw_phase = 2;   /* enter on a cruise phase, not a probe */
         g->rate_pacing_gain = 1.0;
         g->phase_start_ns = now;
      }
      break;
   case MSC_UDP_RATE_PROBE_BW:
      if (g->round_start)
      {
         /* A sibling finished: the share of the path this flow's estimate was
          * measured against just changed, and no waiting will make that
          * clearer. Go straight back to exponential search rather than
          * rediscovering it from the queue three rounds later. */
         uint32_t retired = atomic_load(&g->x->flows_retired);
         if (rate_restart_allowed(g) && retired != g->retired_seen && !round_congested)
         {
            /* Consume the signal only when acting on it: a round that happened
             * to be lossy must not swallow the one notification this flow gets
             * that its share of the path changed. */
            g->retired_seen = retired;
            rate_restart_startup(g);
            break;
         }
         /* Three consecutive rounds, so a momentarily quiet path (one round
          * where the queue happened to drain) cannot restart the search. */
         if (rate_restart_allowed(g) && rate_pipe_not_full(g, round_congested))
         {
            if (++g->undersub_rounds >= 3)
            {
               rate_restart_startup(g);
               break;
            }
         }
         else
            g->undersub_rounds = 0;
      }
      if (mrtt > 0 && now - g->phase_start_ns >= mrtt)
      {
         g->pbw_phase = (g->pbw_phase + 1) & 7;
         g->rate_pacing_gain = rate_pbw_gains[g->pbw_phase];
         g->phase_start_ns = now;
      }
      break;
   case MSC_UDP_RATE_PROBE_RTT:
      /* rate_cwnd holds the window at the floor; once inflight has drained
       * to it, dwell MSC_UDP_RATE_PROBE_RTT_NS so the queue-free floor is
       * actually visible, then adopt what was measured and resume. */
      if (g->probe_rtt_done_ns == 0 &&
          inflight <= (uint64_t)MSC_UDP_RATE_MIN_CWND)
         g->probe_rtt_done_ns = now + MSC_UDP_RATE_PROBE_RTT_NS;
      if (g->probe_rtt_done_ns != 0 && now >= g->probe_rtt_done_ns)
      {
         if (g->probe_rtt_min_ns != 0)
            g->min_rtt_ns = g->probe_rtt_min_ns;
         g->min_rtt_stamp_ns = now;
         g->rate_state = g->rate_filled ? MSC_UDP_RATE_PROBE_BW
                                        : MSC_UDP_RATE_STARTUP;
         g->rate_pacing_gain = g->rate_filled ? 1.0 : MSC_UDP_RATE_HIGH_GAIN;
         g->pbw_phase = 2;
         g->phase_start_ns = now;
         g->undersub_rounds = 0;
         /* Give back the window the dwell drained (BBR's bbr_restore_cwnd).
          * Without this the flow leaves PROBE_RTT at the 4-unit floor and has
          * to re-climb at PROBE_BW's 1.25-per-cycle rate, which costs one
          * round trip per doubling: 4 of a 22-second transfer's seconds on the
          * 250 ms path, every 10 seconds. The window was measured before the
          * dwell and the dwell is 200 ms -- nothing about the path changed. */
         if (g->cwnd_at_probe_rtt > g->cwnd)
            g->cwnd = g->cwnd_at_probe_rtt;
         g->cwnd_at_probe_rtt = 0.0;
      }
      return;   /* the dwell owns the window; do not re-enter below */
   }
   /* a stale floor hides queue growth we may ourselves be causing: drain to
    * the window floor and re-measure (BBR's PROBE_RTT) */
   if (g->min_rtt_stamp_ns != 0 &&
       now - g->min_rtt_stamp_ns > MSC_UDP_RATE_MINRTT_WIN_NS)
   {
      g->cwnd_at_probe_rtt = g->cwnd;   /* restored on the way out */
      g->rate_state = MSC_UDP_RATE_PROBE_RTT;
      g->probe_rtt_done_ns = 0;
      g->probe_rtt_min_ns = 0;
      g->rate_pacing_gain = 1.0;
      g->undersub_rounds = 0;
   }
}

/* the cwnd side: grow by what was acked, bounded by gain * BDP once the pipe
 * is known full. min(cwnd, rwnd) in sender_window still applies above this. */
static void rate_on_ack_cwnd(struct sender_flow *g, uint64_t acked)
{
   double gain = g->rate_filled ? MSC_UDP_RATE_CWND_GAIN
                                : MSC_UDP_RATE_HIGH_GAIN;
   double bdp = rate_bdp_units(g);
   if (g->rate_state == MSC_UDP_RATE_PROBE_RTT)
   {
      if (g->cwnd > MSC_UDP_RATE_MIN_CWND)
         g->cwnd = MSC_UDP_RATE_MIN_CWND;
      return;
   }
   g->cwnd += (double)acked;
   if (bdp > 0.0)
   {
      double target = gain * bdp;
      /* ack-quantum floor: the receiver stretch-acks (g_ack_every units or
       * the FEEDBACK_MS timer), so on a path whose RTT is shorter than that
       * timer the window must hold at least ~2 ack intervals of the current
       * estimate -- otherwise the flow only ever measures its own window
       * rate and the estimate can never climb back after a collapse (the
       * loopback slow mode: bdp < 1 unit, cwnd at the 4-unit floor, one ack
       * per ms, sealed). Negligible whenever RTT >> FEEDBACK_MS. */
      double ackq = 2.0 * g->btl_bw_ups
                    * ((double)MSC_UDP_FEEDBACK_MS / 1e3);
      if (target < ackq)
         target = ackq;
      if (target < MSC_UDP_RATE_MIN_CWND)
         target = MSC_UDP_RATE_MIN_CWND;
      if (g->cwnd > target)
         g->cwnd = target;
   }
   if (g->inflight_hi > 0.0 && g->cwnd > g->inflight_hi)
      g->cwnd = g->inflight_hi;
   /* Floor at the initial window, NOT the 4-unit minimum, whenever we are not
    * draining for PROBE_RTT (which returned above). On a clean sub-ms path the
    * BDP is a few units, so once cwnd reaches the 4-unit floor the flow delivers
    * ~4 units/RTT, the btlbw max-filter collapses, and every target that would
    * re-open the window (gain*bdp AND the ack-quantum floor, both proportional
    * to btlbw) collapses with it -- a self-sealing slow mode that stranded whole
    * flows at 0.24 Gbit/s on IPoIB. A
    * btlbw-INDEPENDENT floor keeps enough in flight to measure the path and
    * climb back out. Harmless on high-BDP paths, where bdp >> INIT_CWND and this
    * never binds. */
   if (g->cwnd < (double)MSC_UDP_INIT_CWND)
      g->cwnd = (double)MSC_UDP_INIT_CWND;
   if (g->cwnd > (double)(g->ring_slots - 1))
      g->cwnd = (double)(g->ring_slots - 1);
}

/* Publish this flow's rate, refill the shared bucket (one winner per timestamp
 * via CAS -- losers just read), then atomically RESERVE up to `want` whole
 * units. The caller refunds whatever it did not actually send. */
static uint64_t fair_reserve(struct sender_flow *g, uint64_t want, uint64_t now)
{
   struct msc_udp_pacer *p = g->pacer;
   double srtt = g->srtt < (double)g_pace_bootstrap_ns
               ? (double)g_pace_bootstrap_ns : g->srtt;
   uint64_t last;
   int64_t avail, take;

   atomic_store(&p->rates_ups[g->flow_id],
                (uint64_t)(g_pace_gain * g->cwnd / srtt * 1e9)); /* units/sec */
   last = atomic_load(&p->last_ns);
   if (last == 0)
      atomic_compare_exchange_strong(&p->last_ns, &last, now);
   else if (now > last && atomic_compare_exchange_strong(&p->last_ns, &last, now))
   {
      double total_ups = (double)p->cap_ups;
      int i;
      if (total_ups == 0.0)
         for (i = 0; i < p->nflows; i++)
            total_ups += (double)atomic_load(&p->rates_ups[i]);
      {
         /* elapsed ns * units/sec -> micro-units: * 1e6 / 1e9 */
         int64_t add = (int64_t)(total_ups * (double)(now - last) / 1e3);
         /* CAS-SET the capped total rather than fetch_add-then-trim: an
          * add-then-subtract briefly parks tokens_uq above burst_uq, and a
          * concurrent reserver could grab that overshoot -- so the aggregate
          * burst cap FAIR exists to enforce would be momentarily exceeded.
          * Publishing only the clamped value keeps the ceiling hard. add >= 0
          * (total_ups >= 0, now > last), so this never inflates the bucket. */
         int64_t cur = atomic_load(&p->tokens_uq);
         for (;;)
         {
            int64_t next = cur + add;
            if (next > p->burst_uq) next = p->burst_uq;
            if (atomic_compare_exchange_weak(&p->tokens_uq, &cur, next))
               break;
            /* cur reloaded by the failed CAS (a reserver spent tokens); retry */
         }
      }
   }

   if (want == 0)
      return 0;
   avail = atomic_load(&p->tokens_uq);
   for (;;)
   {
      if (avail < 1000000)
      {
         atomic_fetch_add(&p->depletions, 1);   /* bucket empty: flow stalls */
         return 0;                       /* less than one whole unit left */
      }
      take = (int64_t)want * 1000000;
      if (take > avail)
         take = avail / 1000000 * 1000000;   /* whole units only */
      if (atomic_compare_exchange_weak(&p->tokens_uq, &avail, avail - take))
      {
         atomic_fetch_add(&p->granted, (uint64_t)(take / 1000000));
         return (uint64_t)(take / 1000000);
      }
      /* avail was reloaded by the failed CAS; retry */
   }
}

/* best-effort: send one packet for local unit `seq`. A failed/ENOBUFS send is
 * treated as a drop -- the unit stays unacked and is retransmitted. `reason` names
 * why a retransmit fired (sack|dupack|rto) for the visual stream; NULL when fresh. */
static void fire_packet(struct sender_flow *g, uint64_t seq, int is_retransmit,
                        const char *reason)
{
   struct msc_udp_xfer *x = g->x;
   uint64_t global = part_global(&g->part, seq);
   struct msc_udp_file *fl = &x->files[xfer_file_of(x, global, &g->file_hint)];
   uint64_t off = unit_off(x, &g->part, fl, global);  /* within file */
   uint64_t len = unit_len(x, &g->part, fl, global);
   struct msc_udp_packet_header h;
   struct iovec iov[2];
   struct msghdr msg;
   uint64_t now = msc_udp_now_ns();

   RING_AT(g->sent_ns, seq) = now;
   cc_on_sent(g, seq, now);
   if (is_retransmit)
   {
      RING_AT(g->retransmitted, seq) = 1;
      g->rtx_units++;
      if (g->undo_marker) g->undo_retrans++;
      if (reason != NULL && strcmp(reason, "sack") == 0) g->rtx_sack++;
      else if (reason != NULL && strcmp(reason, "dupack") == 0) g->rtx_dupack++;
      else g->rtx_rto++;
   }
   else
   {
      RING_AT(g->retransmitted, seq) = 0;
      RING_AT(g->sacked, seq) = 0;
      RING_AT(g->gap_seen_ns, seq) = 0;
      g->tx_units++;
   }

   if (msc_udp_stats_on && is_retransmit && msc_udp_stats_retransmit_ok(g->flow_id))
      msc_udp_stats_emit("retransmit", ",\"flow\":%d,\"seq\":%llu,\"reason\":\"%s\"",
                      g->flow_id, (unsigned long long)seq,
                      reason != NULL ? reason : "rto");

   /* test-only loss injection: pretend we sent it */
   if (g_drop_pct && (int)(rand_r(&g->seed) % 100) < g_drop_pct)
      return;

   /* on a retransmit, the payload is already cached in the retransmission cache from its initial
    * fresh send, so we never re-pread from the source fd here. */
   h.magic = htonl(MSC_UDP_MAGIC);
   h.type = htonl(MSC_UDP_DATA);
   h.flow_id = htonl((uint32_t)g->flow_id);
   h.length = htonl((uint32_t)len);
   h.xfer_id = msc_udp_hton64(x->transfer_id);
   h.seq = msc_udp_hton64(seq);
   h.offset = msc_udp_hton64(off);
   h.send_tsc = msc_udp_hton64(now);
   iov[0].iov_base = &h; iov[0].iov_len = sizeof(h);
   iov[1].iov_base = g->retransmit_cache + (size_t)(seq & (g->ring_slots - 1)) * g->x->payload_size; iov[1].iov_len = (size_t)len;
   memset(&msg, 0, sizeof(msg));
   msg.msg_iov = iov;
   msg.msg_iovlen = 2;
   for (;;)
   {
      ssize_t s = sendmsg(g->udpfd, &msg, 0);
      if (s >= 0) return;
      if (errno == EINTR) continue;
      /* local queue full / transient: drop, it will be retransmitted */
      return;
   }
}

static void update_rtt(struct sender_flow *g, uint64_t sample)
{
   double s = (double)sample;
   if (!g->have_rtt)
   {
      g->srtt = s;
      g->rttvar = s / 2.0;
      g->have_rtt = 1;
      /* First RTT that actually crossed the data path: the profile was picked
       * from the control channel's round trip, which two of the three control
       * modes get wrong. Correct it now, before the window opens far. */
      path_profile_reconsider(sample);
   }
   else
   {
      double diff = g->srtt - s;
      if (diff < 0) diff = -diff;
      g->rttvar = 0.75 * g->rttvar + 0.25 * diff;
      g->srtt = 0.875 * g->srtt + 0.125 * s;
   }
   g->rto = flow_rto_ns(g);
   cc_on_rtt(g, sample, msc_udp_now_ns());
}

/* ----- congestion control dispatch (MSC_UDP_CC=reno|cubic) --------------------
 * The three places TCP's cwnd math lives, factored so a second algorithm drops
 * in: growth on new acks, the multiplicative decrease on a loss episode, and
 * the RTO collapse. The reno paths reproduce the original NewReno math exactly. */

/* CUBIC: remember where the last reduction happened and reset the epoch */
static void cubic_note_loss(struct sender_flow *g)
{
   if (g->cwnd < g->w_max)   /* fast convergence: release share faster */
      g->w_max = g->cwnd * (1.0 + g_cc_beta) / 2.0;
   else
      g->w_max = g->cwnd;
   g->epoch_start_ns = 0;
}

static void loss_cc_on_loss(struct sender_flow *g, int cubic)
{
   /* one decrease per loss episode (NewReno-style recover boundary). The factor
    * is g_cc_beta (default 0.5 = classic halving, 0.7 under cubic) and the
    * result is floored by g_min_cwnd so a lossy episode can't crawl the window
    * down to nothing. */
   if (g->snd_una >= g->recover)
   {
      /* remember the pre-cut state: if DSACKs later report every retransmit
       * of this episode as a duplicate, the loss was invented and the cut is
       * reverted (see the DSACK block in sender_take_acknowledgment) */
      g->undo_cwnd = g->cwnd;
      g->undo_ssthresh = g->ssthresh;
      g->undo_wmax = g->w_max;
      g->undo_retrans = 0;
      g->undo_marker = 1;
      if (cubic) cubic_note_loss(g);
      g->ssthresh = g->cwnd * g_cc_beta;
      if (g->ssthresh < 2.0) g->ssthresh = 2.0;
      if (g->ssthresh < g_min_cwnd) g->ssthresh = g_min_cwnd;
      g->cwnd = g->ssthresh;
      g->recover = g->snd_nxt;
   }
}

/* the RTO cut (called from the flow_worker loop; recover/dupack/backoff stay there) */
static void loss_cc_on_rto(struct sender_flow *g, int cubic)
{
   if (!g->undo_marker)   /* repeated RTOs keep the original pre-episode state */
   {
      g->undo_cwnd = g->cwnd;
      g->undo_ssthresh = g->ssthresh;
      g->undo_wmax = g->w_max;
      g->undo_retrans = 0;
      g->undo_marker = 1;
   }
   if (cubic) cubic_note_loss(g);
   /* ssthresh is halved on RTO regardless of g_cc_beta -- intentional, not an
    * oversight. An RTO is a strictly more severe signal than a fast-recovery /
    * ECN episode (cc_on_loss, which does honor g_cc_beta): the pipe drained to
    * empty, so we distrust the prior window more. This mirrors Linux TCP, whose
    * tcp_enter_loss cut is harsher than its fast-recovery cut for both reno and
    * cubic. cwnd itself still follows g_cc_beta under MSC_UDP_RTO_GENTLE below. */
   g->ssthresh = g->cwnd / 2.0;
   if (g->ssthresh < 2.0) g->ssthresh = 2.0;
   /* classic RTO collapses to 1 (safe but glacial on a low-RTT fabric
    * where spurious timeouts are common). MSC_UDP_RTO_GENTLE makes the RTO
    * a multiplicative decrease like fast-recovery instead. Either way,
    * floor by g_min_cwnd so the window never crawls. */
   g->cwnd = g_rto_gentle ? g->cwnd * g_cc_beta : 1.0;
   if (g->cwnd < g_min_cwnd) g->cwnd = g_min_cwnd;
}

/* growth on `acked` new units. Reno: +1/ack in slow start, +1/cwnd per ack in
 * avoidance. CUBIC: slow start unchanged; in avoidance chase the cubic target
 * W(t) = origin + C*(t-K)^3 (t since the epoch started, RFC 8312), floored by
 * the TCP-friendly Reno estimate so cubic never does worse than reno would. */
static void loss_cc_on_ack(struct sender_flow *g, uint64_t newest_seq,
                           uint64_t acked, uint64_t now, int cubic)
{
   uint64_t k;
   (void)newest_seq;
   for (k = 0; k < acked && g->cwnd < g->ssthresh; k++)
      g->cwnd += 1.0;           /* opening rush (both algorithms) */
   acked -= k;
   if (acked == 0) return;
   if (!cubic)
   {
      for (k = 0; k < acked; k++)
         g->cwnd += 1.0 / g->cwnd;                    /* steady service */
      return;
   }
   if (g->epoch_start_ns == 0)
   {
      g->epoch_start_ns = now;
      if (g->w_max > g->cwnd)
      {
         g->K = cbrt((g->w_max - g->cwnd) / MSC_UDP_CUBIC_C);
         g->origin = g->w_max;
      }
      else
      {
         g->K = 0.0;
         g->origin = g->cwnd;
      }
      g->w_est = g->cwnd;
   }
   {
      double t = (double)(now - g->epoch_start_ns) / 1e9 + g->srtt / 1e9;
      double d = t - g->K;
      double target = g->origin + MSC_UDP_CUBIC_C * d * d * d;
      double grow;
      /* TCP-friendly region: what reno would have by now (RFC 8312 eq. 4) */
      g->w_est += 3.0 * (1.0 - g_cc_beta) / (1.0 + g_cc_beta)
                  * (double)acked / g->cwnd;
      if (target < g->w_est) target = g->w_est;
      if (target > g->cwnd)
      {
         grow = (target - g->cwnd) * (double)acked / g->cwnd;
         if (grow > target - g->cwnd) grow = target - g->cwnd;
         g->cwnd += grow;
      }
      else
         g->cwnd += 0.01 * (double)acked / g->cwnd;   /* minimal probing */
      if (g->cwnd > (double)(g->ring_slots - 1))
         g->cwnd = (double)(g->ring_slots - 1);
   }
}

/* ----- congestion-control implementations and dispatch ------------------ */
static void cc_noop_init(struct sender_flow *g) { (void)g; }
static void cc_noop_destroy(struct sender_flow *g) { (void)g; }
static void cc_noop_sent(struct sender_flow *g, uint64_t seq, uint64_t now)
{ (void)g; (void)seq; (void)now; }
static void cc_noop_delivered(struct sender_flow *g, uint64_t delivered, uint64_t now)
{ (void)g; (void)delivered; (void)now; }
static void cc_noop_rtt(struct sender_flow *g, uint64_t sample, uint64_t now)
{ (void)g; (void)sample; (void)now; }
static void cc_noop_sack(struct sender_flow *g, uint64_t hi_sack, uint64_t now)
{ (void)g; (void)hi_sack; (void)now; }
static void cc_noop_loss(struct sender_flow *g) { (void)g; }

static void rate_cc_init(struct sender_flow *g)
{
   g->del_at_send = msc_udp_alloc(sizeof(uint64_t) * g->ring_slots,
                                  "rate del_at_send");
   g->del_ns_at_send = msc_udp_alloc(sizeof(uint64_t) * g->ring_slots,
                                     "rate del_ns_at_send");
   g->ftx_at_send = msc_udp_alloc(sizeof(uint64_t) * g->ring_slots,
                                  "rate ftx_at_send");
   memset(g->del_at_send, 0, sizeof(uint64_t) * g->ring_slots);
   memset(g->del_ns_at_send, 0, sizeof(uint64_t) * g->ring_slots);
   memset(g->ftx_at_send, 0, sizeof(uint64_t) * g->ring_slots);
   g->rate_pacing_gain = MSC_UDP_RATE_HIGH_GAIN;
}

static void rate_cc_destroy(struct sender_flow *g)
{
   free(g->del_at_send);
   free(g->del_ns_at_send);
   free(g->ftx_at_send);
   g->del_at_send = NULL;
   g->del_ns_at_send = NULL;
   g->ftx_at_send = NULL;
}

static void rate_cc_on_sent(struct sender_flow *g, uint64_t seq, uint64_t now)
{ rate_note_sent(g, seq, now); }

static void rate_cc_on_delivered(struct sender_flow *g, uint64_t delivered,
                                 uint64_t now)
{ rate_note_delivered(g, delivered, now); }

static void rate_cc_on_rtt(struct sender_flow *g, uint64_t sample, uint64_t now)
{ rate_note_rtt(g, sample, now); }

static void rate_cc_on_ack(struct sender_flow *g, uint64_t newest_seq,
                           uint64_t acked, uint64_t now)
{
   rate_sample(g, newest_seq, now);
   rate_on_ack_cwnd(g, acked);
}

static void rate_cc_on_sack(struct sender_flow *g, uint64_t hi_sack, uint64_t now)
{
   if (hi_sack > g->hi_sack_seen)
   {
      rate_sample(g, hi_sack - 1, now);
      g->hi_sack_seen = hi_sack;
   }
   rate_update(g, now);
   g->round_start = 0;
}

static void rate_cc_on_rto(struct sender_flow *g)
{
   /* The pipe drained to empty, but retain the delivery and RTT estimators so
    * subsequent ACKs can re-inflate cwnd directly toward the measured BDP. */
   g->cwnd = MSC_UDP_RATE_MIN_CWND;
}

static void reno_cc_on_ack(struct sender_flow *g, uint64_t newest_seq,
                           uint64_t acked, uint64_t now)
{ loss_cc_on_ack(g, newest_seq, acked, now, 0); }

static void cubic_cc_on_ack(struct sender_flow *g, uint64_t newest_seq,
                            uint64_t acked, uint64_t now)
{ loss_cc_on_ack(g, newest_seq, acked, now, 1); }

static void reno_cc_on_loss(struct sender_flow *g)
{ loss_cc_on_loss(g, 0); }

static void cubic_cc_on_loss(struct sender_flow *g)
{ loss_cc_on_loss(g, 1); }

static void reno_cc_on_ecn(struct sender_flow *g)
{ loss_cc_on_loss(g, 0); }

static void cubic_cc_on_ecn(struct sender_flow *g)
{ loss_cc_on_loss(g, 1); }

static void reno_cc_on_rto(struct sender_flow *g)
{ loss_cc_on_rto(g, 0); }

static void cubic_cc_on_rto(struct sender_flow *g)
{ loss_cc_on_rto(g, 1); }

static const struct msc_udp_cc_ops cc_rate_ops = {
   .name = "rate",
   .uses_rate_samples = 1,
   .init = rate_cc_init,
   .destroy = rate_cc_destroy,
   .on_sent = rate_cc_on_sent,
   .on_delivered = rate_cc_on_delivered,
   .on_rtt = rate_cc_on_rtt,
   .on_ack = rate_cc_on_ack,
   .on_sack = rate_cc_on_sack,
   .on_loss = cc_noop_loss,
   .on_ecn = cc_noop_loss,
   .on_rto = rate_cc_on_rto
};

static const struct msc_udp_cc_ops cc_loss_ops = {
   .name = "reno",
   .uses_rate_samples = 0,
   .init = cc_noop_init,
   .destroy = cc_noop_destroy,
   .on_sent = cc_noop_sent,
   .on_delivered = cc_noop_delivered,
   .on_rtt = cc_noop_rtt,
   .on_ack = reno_cc_on_ack,
   .on_sack = cc_noop_sack,
   .on_loss = reno_cc_on_loss,
   .on_ecn = reno_cc_on_ecn,
   .on_rto = reno_cc_on_rto
};

static const struct msc_udp_cc_ops cc_cubic_ops = {
   .name = "cubic",
   .uses_rate_samples = 0,
   .init = cc_noop_init,
   .destroy = cc_noop_destroy,
   .on_sent = cc_noop_sent,
   .on_delivered = cc_noop_delivered,
   .on_rtt = cc_noop_rtt,
   .on_ack = cubic_cc_on_ack,
   .on_sack = cc_noop_sack,
   .on_loss = cubic_cc_on_loss,
   .on_ecn = cubic_cc_on_ecn,
   .on_rto = cubic_cc_on_rto
};

static const struct msc_udp_cc_ops *cc_ops_for(enum msc_udp_cc_algorithm algorithm)
{
   switch (algorithm)
   {
   case MSC_UDP_CC_ALG_CUBIC: return &cc_cubic_ops;
   case MSC_UDP_CC_ALG_RENO:  return &cc_loss_ops;
   case MSC_UDP_CC_ALG_RATE:  return &cc_rate_ops;
   }
   return &cc_rate_ops;
}

static const struct msc_udp_cc_ops *cc_current(void)
{
   if (g_cc_ops == NULL)
      g_cc_ops = cc_ops_for(g_cc_algorithm);
   return g_cc_ops;
}

static int cc_uses_rate_samples(void)
{ return cc_current()->uses_rate_samples; }

static void cc_on_sent(struct sender_flow *g, uint64_t seq, uint64_t now)
{ cc_current()->on_sent(g, seq, now); }

static void cc_on_delivered(struct sender_flow *g, uint64_t delivered, uint64_t now)
{ cc_current()->on_delivered(g, delivered, now); }

static void cc_on_rtt(struct sender_flow *g, uint64_t sample, uint64_t now)
{ cc_current()->on_rtt(g, sample, now); }

static void cc_on_ack(struct sender_flow *g, uint64_t newest_seq,
                      uint64_t acked, uint64_t now)
{
   if (!g_no_cc)
      cc_current()->on_ack(g, newest_seq, acked, now);
}

static void cc_on_sack(struct sender_flow *g, uint64_t hi_sack, uint64_t now)
{ cc_current()->on_sack(g, hi_sack, now); }

static void cc_on_loss(struct sender_flow *g)
{
   if (!g_no_cc)
      cc_current()->on_loss(g);
}

static void cc_on_ecn(struct sender_flow *g)
{
   if (!g_no_cc)
      cc_current()->on_ecn(g);
}

static void cc_on_rto(struct sender_flow *g)
{
   if (!g_no_cc)
      cc_current()->on_rto(g);
}

static void sender_take_acknowledgment(struct sender_flow *g, const char *buf, ssize_t n)
{
   const struct msc_udp_acknowledgment_header *r = (const struct msc_udp_acknowledgment_header *)buf;
   uint32_t sackcnt;
   uint64_t cum, echo, delivered;
   uint64_t hi_sack = 0;
   int did_fast = 0;
   uint32_t i;

   if (n < (ssize_t)sizeof(*r) || ntohl(r->magic) != MSC_UDP_MAGIC ||
       ntohl(r->type) != MSC_UDP_ACK ||
       ntohl(r->flow_id) != (uint32_t)g->flow_id ||
       msc_udp_ntoh64(r->xfer_id) != g->x->transfer_id)
      return;
   sackcnt = ntohl(r->sack_count);
   if (sackcnt > MSC_UDP_MAX_SACK ||
       n < (ssize_t)(sizeof(*r) + (size_t)sackcnt * sizeof(struct msc_udp_sack_block)))
      return;
   cum = msc_udp_ntoh64(r->cum_ack);
   echo = msc_udp_ntoh64(r->echo_tsc);
   delivered = msc_udp_ntoh64(r->delivered);
   { uint64_t w = msc_udp_ntoh64(r->rwnd_units);
     if (w < 1) w = 1;
     if (w > g->ring_slots - 1) w = g->ring_slots - 1;
     g->rwnd = w; }
   g->acknowledgments++;
   cc_on_delivered(g, delivered, msc_udp_now_ns());

   if (msc_udp_stats_on && msc_udp_stats_due(g->flow_id, MSC_UDP_STATS_ACK))
      msc_udp_stats_emit("acknowledgment",
                      ",\"flow\":%d,\"cum_ack\":%llu,\"sacks\":%u,"
                      "\"delivered\":%llu,\"rwnd\":%llu",
                      g->flow_id, (unsigned long long)cum, sackcnt,
                      (unsigned long long)delivered,
                      (unsigned long long)msc_udp_ntoh64(r->rwnd_units));

   if (cum > g->snd_una && cum <= g->snd_nxt)
   {
      uint64_t acked = cum - g->snd_una;
      uint64_t now = msc_udp_now_ns();
      int ambiguous = flow_reorder_wait_ns(g) > 0 &&
         ack_sample_ambiguous(g, g->snd_una, cum);
      if (echo > 0 && now > echo)
      {
         if (!ambiguous && !RING_AT(g->retransmitted, cum - 1))
            update_rtt(g, now - echo);
         else if (ambiguous)
            g->rtt_ambiguous++;
      }
      cc_on_ack(g, cum - 1, acked, now);
      g->snd_una = cum;
      g->dupack = 0;
      g->rto_streak = 0;
      /* forward progress: un-ratchet any exponential RTO backoff so recovery
       * does not stay glacial after a burst of timeouts (Karn skips RTT samples
       * on retransmitted units, so without this the doubled RTO never comes back down) */
      if (g->have_rtt)
         g->rto = flow_rto_ns(g);
   }
   else if (cum == g->last_cum_ack && delivered > g->last_delivered &&
            g->snd_una < g->snd_nxt)
   {
      /* A repeated cum_ack is a LOSS signal only when the receiver actually got
       * something new while the hole stayed open -- that is what `delivered`
       * advancing means. The receiver also emits unsolicited acks on every 1 ms
       * recvmmsg timeout (see the idle arms in the receiver loop), and on a
       * high-RTT path it spends most of each RTT idle, so those repeats vastly
       * outnumber real ones. Counting them fires the dupack fast retransmit
       * below on idleness, whose cc_on_loss() pins cwnd at its 2.0 clamp and
       * keeps the receiver idle -- a self-sustaining collapse with zero packet
       * loss.
       * TCP needs no such guard: a TCP receiver only acks in response to a
       * segment, so it cannot manufacture a duplicate out of silence. */
      g->dupack++;
   }
   g->last_cum_ack = cum;
   g->last_delivered = delivered;

   /* ECN: a rise in the receiver's cumulative CE count is congestion without
    * loss -- take one multiplicative decrease per window (the recover boundary
    * inside cc_on_loss dedups an episode), no retransmit needed. Checked AFTER the
    * ack advance so a acknowledgment that both closes the old episode and reports CE
    * still gets its cut. */
   {
      uint64_t ce = msc_udp_ntoh64(r->ce_units);
      if (ce > g->ce_seen)
      {
         g->ce_seen = ce;
         /* rate mode v1 ignores CE like it ignores loss (neither fabric
          * marks); if a marking path appears, wire CE into the bw filter
          * rather than through a window cut */
         if (g_ecn && g->snd_una >= g->recover)
         {
            cc_on_ecn(g);
            g->ecn_cuts++;
         }
      }
   }

   /* DSACK: by convention the first block may report a range that arrived in
    * duplicate -- entirely below the cum-ack, or contained in a later block.
    * Every duplicate of a unit we retransmitted is a retransmit the path did
    * not need: the original was merely reordered or delayed. React three ways:
    * count it, widen this flow's reordering window (RACK-style, one srtt/4
    * step per RTT, so the misreading stops), and if every retransmit of the
    * current loss episode turns out spurious, revert the cwnd cut the invented
    * loss caused (RFC 3708). A peer that never emits DSACK leaves all of this
    * dormant. */
   i = 0;
   if (g_dsack && (g_feat_agreed & MSC_UDP_FEAT_DSACK) && sackcnt > 0)
   {
      const struct msc_udp_sack_block *b0 =
         (const struct msc_udp_sack_block *)(buf + sizeof(*r));
      uint64_t s0 = msc_udp_ntoh64(b0->start);
      uint64_t e0 = msc_udp_ntoh64(b0->end);
      int is_dsack = e0 > s0 && e0 <= cum;
      unsigned int j;
      for (j = 1; !is_dsack && j < sackcnt; j++)
      {
         const struct msc_udp_sack_block *bj =
            (const struct msc_udp_sack_block *)(buf + sizeof(*r) + (size_t)j * sizeof(*bj));
         if (e0 > s0 && s0 >= msc_udp_ntoh64(bj->start) &&
             e0 <= msc_udp_ntoh64(bj->end))
            is_dsack = 1;
      }
      /* ring-reuse guard: a slot's `retransmitted` flag is only this unit's
       * while the window has not lapped it */
      if (is_dsack && e0 - s0 <= g->ring_slots &&
          s0 + g->ring_slots >= g->snd_nxt)
      {
         uint64_t u, spur = 0;
         for (u = s0; u < e0; u++)
            if (RING_AT(g->retransmitted, u)) spur++;
         if (spur > 0)
         {
            uint64_t now2 = msc_udp_now_ns();
            g->spurious_rtx += spur;
            if (!g_reorder_wait_set && g->have_rtt &&
                now2 >= g->reorder_grow_gate_ns)
            {
               g->reorder_extra_ns += (uint64_t)(g->srtt / 4.0);
               if (g->reorder_extra_ns > MSC_UDP_MAX_REORDER_WAIT_NS)
                  g->reorder_extra_ns = MSC_UDP_MAX_REORDER_WAIT_NS;
               g->reorder_grow_gate_ns = now2 + (uint64_t)g->srtt;
            }
            if (g->undo_marker)
            {
               g->undo_retrans = g->undo_retrans > spur ? g->undo_retrans - spur : 0;
               if (g->undo_retrans == 0 && g->snd_una <= g->recover)
               {
                  if (g->cwnd < g->undo_cwnd) g->cwnd = g->undo_cwnd;
                  if (g->ssthresh < g->undo_ssthresh) g->ssthresh = g->undo_ssthresh;
                  g->w_max = g->undo_wmax;
                  g->epoch_start_ns = 0;   /* cubic: re-derive from restored w_max */
                  g->undo_marker = 0;
                  g->undo_done++;
               }
            }
         }
         i = 1;   /* the DSACK block is not forward SACK coverage */
      }
   }

   /* record SACK coverage and find the highest sacked unit below snd_nxt */
   for (; i < sackcnt; i++)
   {
      const struct msc_udp_sack_block *b =
         (const struct msc_udp_sack_block *)(buf + sizeof(*r) + (size_t)i * sizeof(*b));
      uint64_t s = msc_udp_ntoh64(b->start);
      uint64_t e = msc_udp_ntoh64(b->end);
      uint64_t u;
      if (s < g->snd_una) s = g->snd_una;
      if (e > g->snd_nxt) e = g->snd_nxt;
      for (u = s; u < e; u++) RING_AT(g->sacked, u) = 1;
      if (e > hi_sack) hi_sack = e;
   }

   cc_on_sack(g, hi_sack, msc_udp_now_ns());

   /* Fast retransmit every hole below the highest SACKed unit. In the default
    * mode this is immediate. The opt-in reordering hold records its first SACK
    * report and waits briefly, so a delayed shaper cannot turn one reordered
    * burst into a storm of spurious retransmissions. fire_packet refreshes sent_ns, so
    * a real hole is still retransmitted at most once per RTO. */
   if (hi_sack > g->snd_una)
   {
      uint64_t now = msc_udp_now_ns();
      uint64_t u;
      for (u = g->snd_una; u < hi_sack; u++)
         if (!RING_AT(g->sacked, u) &&
             (!RING_AT(g->retransmitted, u) || now - RING_AT(g->sent_ns, u) >= g->rto) &&
             reorder_hole_ready(g, u, now))
         {
            fire_packet(g, u, 1, "sack");
            did_fast = 1;
         }
   }
   /* duplicate-ack fast retransmit of snd_una when nothing above it is sacked */
   if (g->dupack >= g_dupack_thresh && g->snd_una < g->snd_nxt &&
       !RING_AT(g->retransmitted, g->snd_una) &&
       reorder_hole_ready(g, g->snd_una, msc_udp_now_ns()))
   {
      fire_packet(g, g->snd_una, 1, "dupack");
      did_fast = 1;
      g->dupack = 0;
   }
   if (did_fast) cc_on_loss(g);
}

static void sender_drain_acknowledgments(struct sender_flow *g)
{
   char buf[sizeof(struct msc_udp_acknowledgment_header) +
            MSC_UDP_MAX_SACK * sizeof(struct msc_udp_sack_block)];
   if (g->receive_demux != NULL)
   {
      /* one mode: the demux owns the socket; drain this flow's queue */
      int n;
      while ((n = msc_udp_receive_demux_pop(g->receive_demux, g->flow_id, buf, sizeof(buf), 0, NULL)) > 0)
         sender_take_acknowledgment(g, buf, (ssize_t)n);
      return;
   }
   for (;;)
   {
      ssize_t n = recv(g->udpfd, buf, sizeof(buf), MSG_DONTWAIT);
      if (n < 0)
      {
         if (errno == EINTR) continue;
         break;   /* EAGAIN: drained */
      }
      sender_take_acknowledgment(g, buf, n);
   }
}

/* push msgs[0..ndg) with sendmmsg, retrying partial sends; un-sent units stay
 * unacked and are recovered by SACK/RTO, so transient errors just stop early. */
static void sender_send_batch(struct sender_flow *g, unsigned ndg)
{
   unsigned done = 0;
   while (done < ndg)
   {
      int sent = sendmmsg(g->udpfd, g->msgs + done, ndg - done, 0);
      if (sent > 0) { done += (unsigned)sent; continue; }
      if (sent < 0 && errno == EINTR) continue;
      if (sent < 0 && (errno == ENOBUFS || errno == EAGAIN))
         g->enobufs++;   /* send-buffer pressure: the autotune growth signal */
      break;   /* ENOBUFS/EAGAIN/other: leave the rest to loss recovery */
   }
}

/* fill one packet header in network byte order */
static void fill_header(struct msc_udp_packet_header *h, struct sender_flow *g,
                        uint64_t seq, uint64_t off, uint64_t len, uint64_t now)
{
   h->magic = htonl(MSC_UDP_MAGIC);
   h->type = htonl(MSC_UDP_DATA);
   h->flow_id = htonl((uint32_t)g->flow_id);
   h->length = htonl((uint32_t)len);
   h->xfer_id = msc_udp_hton64(g->x->transfer_id);
   h->seq = msc_udp_hton64(seq);
   h->offset = msc_udp_hton64(off);
   h->send_tsc = msc_udp_hton64(now);
}

/* the largest UDP GSO super-packet we hand the kernel in one sendmsg. The kernel
 * (esp. 4.18) bounds UDP GSO to ~64 segments / a datagram's worth, so we stay
 * well under that and split a batch into several sends if needed. */
#define MSC_UDP_GSO_MAX_BYTES 60000

static atomic_int g_gso_logged = 0;   /* log the first failure once */

/* GSO send: lay the k packets out contiguously as [hdr|payload], each exactly
 * gso_size except a partial final unit (which lands last), then hand the kernel
 * one sendmsg per <=MSC_UDP_GSO_MAX_BYTES chunk with UDP_SEGMENT so it slices them
 * into MTU datagrams in one trip through the egress stack. Returns 0 on success,
 * -1 on any GSO error (errno logged once) so the caller disables GSO and falls
 * back to sendmmsg rather than limping on single-unit retransmits. */
static int fire_send_gso(struct sender_flow *g, uint64_t start, unsigned k, uint64_t now)
{
   struct msc_udp_xfer *x = g->x;
   size_t payload = x->payload_size;
   size_t hdrsz = sizeof(struct msc_udp_packet_header);
   size_t gso_size = hdrsz + payload;
   uint64_t base_global = part_global(&g->part, start);
   struct msc_udp_file *fl = &x->files[xfer_file_of(x, base_global, &g->file_hint)];
   uint64_t base_off = unit_off(x, &g->part, fl, base_global); /* within file */
   unsigned max_seg = (unsigned)(MSC_UDP_GSO_MAX_BYTES / gso_size);
   unsigned i, done;
   char control[CMSG_SPACE(sizeof(uint16_t))];

   if (max_seg < 2) max_seg = 2;
   if (max_seg > 64) max_seg = 64;

   /* lay out all k packets contiguously once (all within file fl) */
   for (i = 0; i < k; i++)
   {
      uint64_t seq = start + i;
      uint64_t off = base_off + (uint64_t)i * payload;
      uint64_t len = unit_len(x, &g->part, fl, base_global + i);
      char *slot = g->gso_buf + (size_t)i * gso_size;
      fill_header((struct msc_udp_packet_header *)slot, g, seq, off, len, now);
      memcpy(slot + hdrsz, g->readbuf + (size_t)(off - base_off), (size_t)len);
   }

   for (done = 0; done < k; )
   {
      unsigned seg = k - done;
      size_t total = 0;
      unsigned j;
      struct msghdr msg;
      struct iovec iov;

      if (seg > max_seg) seg = max_seg;
      for (j = done; j < done + seg; j++)
         total += hdrsz + (size_t)unit_len(x, &g->part, fl, base_global + j);

      memset(&msg, 0, sizeof(msg));
      iov.iov_base = g->gso_buf + (size_t)done * gso_size;
      iov.iov_len = total;
      msg.msg_iov = &iov;
      msg.msg_iovlen = 1;
      if (seg > 1)   /* a single packet needs no segmentation */
      {
         struct cmsghdr *cmsg;
         memset(control, 0, sizeof(control));   /* no uninitialized padding */
         msg.msg_control = control;
         msg.msg_controllen = sizeof(control);
         cmsg = CMSG_FIRSTHDR(&msg);
         cmsg->cmsg_level = SOL_UDP;
         cmsg->cmsg_type = UDP_SEGMENT;
         cmsg->cmsg_len = CMSG_LEN(sizeof(uint16_t));
         *(uint16_t *)CMSG_DATA(cmsg) = (uint16_t)gso_size;
      }
      for (;;)
      {
         ssize_t s = sendmsg(g->udpfd, &msg, 0);
         if (s >= 0) break;
         if (errno == EINTR) continue;
         if (!atomic_exchange(&g_gso_logged, 1))
         {
            fprintf(stderr, "MSC UDP sender: UDP GSO sendmsg failed (%s); "
                    "falling back to sendmmsg\n", strerror(errno));
         }
         return -1;   /* caller disables GSO and resends via sendmmsg */
      }
      done += seg;
   }
   return 0;
}

/* send the contiguous run of fresh units [start, start+k): one pread, then one
 * sendmmsg (default) or one UDP_SEGMENT sendmsg (MSC_UDP_GSO). Each unit is recorded
 * in-flight; a drop-injected unit (sendmmsg path only) is recorded but not
 * transmitted, and is recovered like any real loss. */
static void fire_batch(struct sender_flow *g, uint64_t start, unsigned k)
{
   struct msc_udp_xfer *x = g->x;
   size_t payload = x->payload_size;
   uint64_t base_global = part_global(&g->part, start);
   /* fire_fresh clamps every fresh batch to a single file (and, in affine mode, a
    * single stripe), so all k units here belong to fl, map to consecutive global
    * units, and read contiguously from one source fd */
   struct msc_udp_file *fl = &x->files[xfer_file_of(x, base_global, &g->file_hint)];
   uint64_t base_off = unit_off(x, &g->part, fl, base_global); /* within file */
   uint64_t end_off = base_off + (uint64_t)k * payload;
   uint64_t now = msc_udp_now_ns();
   unsigned i, ndg = 0;
   size_t read_len;

   if (end_off > fl->size) end_off = fl->size;   /* partial final unit of fl */
   read_len = (size_t)(end_off - base_off);
   if (msc_udp_pread_all(fl->fd, g->readbuf, read_len, fl->base + (off_t)base_off) != 0)
   {
      fprintf(stderr, "MSC UDP sender: source read failed at %llu: %s\n",
              (unsigned long long)base_off, strerror(errno));
      g->error = 1;
      return;
   }

   /* record every unit in-flight up front (independent of the send method) */
   for (i = 0; i < k; i++)
   {
      uint64_t seq = start + i;
      size_t len = unit_len(x, &g->part, fl, base_global + i);
      memcpy(g->retransmit_cache + (size_t)(seq & (g->ring_slots - 1)) * payload,
             g->readbuf + (size_t)i * payload, len);
      RING_AT(g->sent_ns, seq) = now;
      cc_on_sent(g, seq, now);
      RING_AT(g->retransmitted, seq) = 0;
      RING_AT(g->sacked, seq) = 0;
      RING_AT(g->gap_seen_ns, seq) = 0;
      g->tx_units++;
   }

   if (g->gso_enabled)
   {
      if (fire_send_gso(g, start, k, now) == 0)
         return;
      g->gso_enabled = 0;
   }

   for (i = 0; i < k; i++)
   {
      uint64_t seq = start + i;
      uint64_t off = base_off + (uint64_t)i * payload;
      uint64_t len = unit_len(x, &g->part, fl, base_global + i);
      struct msc_udp_packet_header *h;

      if (g_drop_pct && (int)(rand_r(&g->seed) % 100) < g_drop_pct)
         continue;
      h = &g->hdrs[ndg];
      fill_header(h, g, seq, off, len, now);
      g->iov[2 * ndg].iov_base = h;
      g->iov[2 * ndg].iov_len = sizeof(*h);
      g->iov[2 * ndg + 1].iov_base = g->readbuf + (size_t)(off - base_off);
      g->iov[2 * ndg + 1].iov_len = (size_t)len;
      memset(&g->msgs[ndg].msg_hdr, 0, sizeof(g->msgs[ndg].msg_hdr));
      g->msgs[ndg].msg_hdr.msg_iov = &g->iov[2 * ndg];
      g->msgs[ndg].msg_hdr.msg_iovlen = 2;
      ndg++;
   }
   sender_send_batch(g, ndg);
}

/* fire fresh packets in batches while the window is open.
 *
 * The sendmmsg path packs units from CONSECUTIVE files into one batch of up to
 * MSC_UDP_SEND_BATCH datagrams: each file-run is one pread into its slots of the
 * shared readbuf, and the whole batch goes out in a single sendmmsg. This is the
 * many-tiny-files win -- without it each sub-MSC_UDP_SEND_BATCH file cost its own
 * syscall. The GSO path keeps one file per batch: UDP_SEGMENT slices a buffer
 * into equal-size datagrams (only the last may be short), so a batch spanning
 * files (with a partial unit at each file's tail) cannot be expressed. */
static void fire_fresh(struct sender_flow *g)
{
   struct msc_udp_xfer *x = g->x;
   size_t payload = x->payload_size;
   uint64_t win = sender_window(g);
   uint64_t now = msc_udp_now_ns();
   /* pacing budget: cap fresh units this round to the token bucket (pacing). When
    * pacing is off, (uint64_t)-1 makes the gate a no-op. MSC_UDP_FAIR additionally
    * bounds the round by the shared cross-flow bucket, so the SUM of all flows'
    * fresh sends is rate-limited (aligned per-flow bursts can't stack up). */
   uint64_t budget = g_pace ? pace_grant(g, now) : (uint64_t)-1;
   uint64_t nfresh = 0; /* fresh units released this round (charged to the bucket) */
   uint64_t fair_take = 0; /* units reserved from the shared bucket this round */
   if (g->pacer != NULL)
   {
      uint64_t inflight = g->snd_nxt - g->snd_una;
      uint64_t room = win > inflight ? win - inflight : 0;
      uint64_t left = g->units - g->snd_nxt;
      uint64_t want = room < left ? room : left;
      if (want > budget) want = budget;
      fair_take = fair_reserve(g, want, now);
      budget = fair_take;
      if (want > 0 && fair_take == 0)
         g->fair_starved++;   /* window open but the shared bucket said wait */
   }
   unsigned slot = 0;   /* units placed in the current batch (readbuf slots used) */
   unsigned ndg = 0;    /* datagrams queued for sendmmsg (<= slot under MSC_UDP_DROP) */

   if (g->gso_enabled)
   {
      while (g->snd_nxt < g->units && (g->snd_nxt - g->snd_una) < win &&
             nfresh < budget && !g->error)
      {
         uint64_t global = part_global(&g->part, g->snd_nxt);
         struct msc_udp_file *fl = &x->files[xfer_file_of(x, global, &g->file_hint)];
         uint64_t room_win = win - (g->snd_nxt - g->snd_una);
         uint64_t room_units = g->units - g->snd_nxt;
         uint64_t room_file = fl->unit_base + fl->nunits - global;
         uint64_t room_pace = budget - nfresh;
         uint64_t k = MSC_UDP_SEND_BATCH;
         if (k > room_win) k = room_win;
         if (k > room_units) k = room_units;
         if (k > room_file) k = room_file;
         if (k > room_pace) k = room_pace;
         /* affine: keep each batch within one stripe (consecutive global units) */
         if (g->part.affine)
         {
            uint64_t room_stripe = g->part.ustripe - (g->snd_nxt % g->part.ustripe);
            if (k > room_stripe) k = room_stripe;
         }
         fire_batch(g, g->snd_nxt, (unsigned)k);
         g->snd_nxt += k;
         nfresh += k;
      }
      if (g_pace) g->pace_tokens -= (double)nfresh;
      if (g->pacer != NULL && fair_take > nfresh)   /* refund the unsent part */
         atomic_fetch_add(&g->pacer->tokens_uq,
                          (int64_t)(fair_take - nfresh) * 1000000);
      return;
   }

   while (g->snd_nxt < g->units && (g->snd_nxt - g->snd_una) < win &&
          nfresh < budget && !g->error)
   {
      uint64_t global = part_global(&g->part, g->snd_nxt);
      struct msc_udp_file *fl = &x->files[xfer_file_of(x, global, &g->file_hint)];
      uint64_t base_off = unit_off(x, &g->part, fl, global); /* within file */
      uint64_t room_win = win - (g->snd_nxt - g->snd_una);
      uint64_t room_units = g->units - g->snd_nxt;
      uint64_t room_file = fl->unit_base + fl->nunits - global;
      uint64_t room_batch = (uint64_t)MSC_UDP_SEND_BATCH - slot;
      uint64_t room_pace = budget - nfresh;
      uint64_t k = room_file, end_off, i;
      size_t read_len;

      if (k > room_win) k = room_win;
      if (k > room_units) k = room_units;
      if (k > room_batch) k = room_batch;
      if (k > room_pace) k = room_pace;
      /* affine: a flow's units are contiguous within a stripe but jump across
       * stripe boundaries, so one pread batch must stay within one stripe */
      if (g->part.affine)
      {
         uint64_t room_stripe = g->part.ustripe - (g->snd_nxt % g->part.ustripe);
         if (k > room_stripe) k = room_stripe;
      }

      /* one pread of this file-run into slots [slot, slot+k) of the readbuf */
      end_off = base_off + k * (uint64_t)payload;
      if (end_off > fl->size) end_off = fl->size;   /* partial final unit of fl */
      read_len = (size_t)(end_off - base_off);
      if (msc_udp_pread_all(fl->fd, g->readbuf + (size_t)slot * payload, read_len,
                   fl->base + (off_t)base_off) != 0)
      {
         fprintf(stderr, "MSC UDP sender: source read failed at %llu: %s\n",
                 (unsigned long long)base_off, strerror(errno));
         g->error = 1;
         break;
      }

      for (i = 0; i < k; i++)
      {
         uint64_t seq = g->snd_nxt + i;
         unsigned s = slot + (unsigned)i;             /* readbuf slot for this unit */
         uint64_t off = base_off + i * (uint64_t)payload;
         uint64_t len = unit_len(x, &g->part, fl, global + i);
         struct msc_udp_packet_header *h;

         memcpy(g->retransmit_cache + (size_t)(seq & (g->ring_slots - 1)) * payload,
                g->readbuf + (size_t)s * payload, len);

         RING_AT(g->sent_ns, seq) = now;
         cc_on_sent(g, seq, now);
         RING_AT(g->retransmitted, seq) = 0;
         RING_AT(g->sacked, seq) = 0;
         RING_AT(g->gap_seen_ns, seq) = 0;
         g->tx_units++;
         if (g_drop_pct && (int)(rand_r(&g->seed) % 100) < g_drop_pct)
            continue;   /* recorded in-flight but not transmitted; recovered later */
         h = &g->hdrs[ndg];
         fill_header(h, g, seq, off, len, now);
         g->iov[2 * ndg].iov_base = h;
         g->iov[2 * ndg].iov_len = sizeof(*h);
         g->iov[2 * ndg + 1].iov_base = g->readbuf + (size_t)s * payload;
         g->iov[2 * ndg + 1].iov_len = (size_t)len;
         memset(&g->msgs[ndg].msg_hdr, 0, sizeof(g->msgs[ndg].msg_hdr));
         g->msgs[ndg].msg_hdr.msg_iov = &g->iov[2 * ndg];
         g->msgs[ndg].msg_hdr.msg_iovlen = 2;
         ndg++;
      }
      slot += (unsigned)k;
      g->snd_nxt += k;
      nfresh += k;

      if (slot == MSC_UDP_SEND_BATCH)   /* batch full: flush and start a fresh one */
      {
         sender_send_batch(g, ndg);
         slot = 0;
         ndg = 0;
      }
   }
   if (ndg > 0)   /* trailing partial batch (drops may leave slot > 0, ndg == 0) */
      sender_send_batch(g, ndg);
   if (g_pace) g->pace_tokens -= (double)nfresh;
   if (g->pacer != NULL && fair_take > nfresh)   /* refund the unsent part */
      atomic_fetch_add(&g->pacer->tokens_uq,
                       (int64_t)(fair_take - nfresh) * 1000000);
}

static void *sender_flow_worker(void *arg)
{
   struct sender_flow *g = arg;
   struct pollfd pfd;
   uint64_t last_progress = 0;
   uint64_t last_una = g->snd_una;
   pfd.fd = g->udpfd;
   pfd.events = POLLIN;

   while (g->snd_una < g->units && !g->error)
   {
      if (g->x->cancel_flag != NULL && *g->x->cancel_flag)
      { g->error = 1; break; }
      uint64_t now;
      int rto_held;

      fire_fresh(g);   /* fresh packets, batched (one pread + sendmmsg per ~64) */
      if (cc_uses_rate_samples())   /* out of fresh data: rate samples stop proving anything */
         g->rate_app_limited = g->snd_nxt >= g->units;
      /* RTO: retransmit the oldest unacked unit if its retransmit timer expired */
      now = msc_udp_now_ns();
      rto_held = g->snd_una < g->snd_nxt &&
         reorder_rto_held(g, g->snd_una, now);
      if (g->snd_una < g->snd_nxt &&
          !rto_held && now - RING_AT(g->sent_ns, g->snd_una) > g->rto)
      {
         fire_packet(g, g->snd_una, 1, "rto");
         cc_on_rto(g);
         g->recover = g->snd_nxt;
         g->dupack = 0;
         g->rto *= 2;
         if (g->rto > MSC_UDP_MAX_RTO_NS) g->rto = MSC_UDP_MAX_RTO_NS;
         g->rto_streak++;
      }
      if (g->receive_demux != NULL)
      {
         /* one mode: the timed wait moves from ppoll on a private socket to
          * this flow's queue condvar -- same tick budget, so the CC/RTO clock
          * keeps ticking when idle. A popped acknowledgment is consumed here and the
          * drain below empties whatever else queued meanwhile. */
         char rbuf[sizeof(struct msc_udp_acknowledgment_header) +
                   MSC_UDP_MAX_SACK * sizeof(struct msc_udp_sack_block)];
         int n = msc_udp_receive_demux_pop(g->receive_demux, g->flow_id, rbuf, sizeof(rbuf),
                             g_tick_us, NULL);
         if (n > 0)
            sender_take_acknowledgment(g, rbuf, (ssize_t)n);
      }
      else
      {
         struct timespec ts;
         ts.tv_sec = g_tick_us / 1000000;
         ts.tv_nsec = (g_tick_us % 1000000) * 1000L;
         ppoll(&pfd, 1, &ts, NULL);   /* returns early on a acknowledgment (POLLIN) */
      }
      sender_drain_acknowledgments(g);
      now = msc_udp_now_ns();
      if (g->snd_una != last_una)
      {
         last_una = g->snd_una;
         last_progress = now;
      }
      else if (last_progress == 0 && g->snd_nxt > g->snd_una)
         last_progress = now;
      else if (g_stall_timeout_ns != 0 && last_progress != 0 &&
               now - last_progress >= g_stall_timeout_ns)
      {
         fprintf(stderr,
                 "MSC UDP sender flow %d: no acknowledgement progress for %.1f seconds, giving up\n",
                 g->flow_id, (double)g_stall_timeout_ns / 1e9);
         g->error = 1;
         break;
      }

      if (msc_udp_stats_on && msc_udp_stats_due(g->flow_id, MSC_UDP_STATS_FLOW))
      {
         char ratebuf[128] = "";
         if (cc_uses_rate_samples())
            snprintf(ratebuf, sizeof(ratebuf),
                     ",\"btlbw_mbps\":%.1f,\"minrtt_us\":%.0f,\"cc_state\":%d",
                     g->btl_bw_ups * 8.0 * (double)g->x->payload_size / 1e6,
                     (double)g->min_rtt_ns / 1e3, g->rate_state);
         msc_udp_stats_emit("flow_sample",
                         ",\"flow\":%d,\"units\":%llu,\"snd_una\":%llu,"
                         "\"snd_nxt\":%llu,\"cwnd\":%.1f,\"rwnd\":%llu,"
                         "\"in_flight\":%llu,\"tx_units\":%llu,\"retransmitted\":%llu,"
                         "\"acknowledgments\":%llu,\"srtt_us\":%.0f,\"rto_us\":%llu%s",
                         g->flow_id, (unsigned long long)g->units,
                         (unsigned long long)g->snd_una,
                         (unsigned long long)g->snd_nxt, g->cwnd,
                         (unsigned long long)g->rwnd,
                         (unsigned long long)(g->snd_nxt - g->snd_una),
                         (unsigned long long)g->tx_units,
                         (unsigned long long)g->rtx_units,
                         (unsigned long long)g->acknowledgments,
                         g->srtt / 1000.0,
                         (unsigned long long)(g->rto / 1000), ratebuf);
      }

      /* send-buffer autotuning: grow SO_SNDBUF toward 2x the window's byte
       * volume (~BDP) or on ENOBUFS pressure, in doubling steps, stopping at
       * the wmem_max clamp or MSC_UDP_AUTOTUNE_MAX. Checked at a coarse cadence.
       * When flows share a socket only its buf_owner may autotune -- several
       * of them would race setsockopt on the same buffer -- and the target
       * scales by the flows sharing it to stay an aggregate estimate. */
      if (g_autotune && !g->sndbuf_maxed && now >= g->next_buf_check_ns &&
          g->buf_owner)
      {
         int64_t want = (int64_t)(2.0 * g->cwnd * (double)g->x->payload_size) + 65536;
         want *= g->shared_flows;
         g->next_buf_check_ns = now + 100ULL * 1000 * 1000;
         if (want > g_autotune_max) want = g_autotune_max;
         if (want > g->sndbuf_now || g->enobufs > g->enobufs_last)
         {
            int64_t target = g->sndbuf_now > 0 ? g->sndbuf_now : 65536;
            int applied;
            while (target < want) target *= 2;
            if (target > g_autotune_max) target = g_autotune_max;
            applied = set_buf_size(g->udpfd, SO_SNDBUF, (int)target);
            if (applied > g->sndbuf_now) g->buf_grows++;
            if (applied >= 0 && applied < (int)target) g->sndbuf_maxed = 1;
            if (applied > 0) g->sndbuf_now = applied;
         }
         g->enobufs_last = g->enobufs;
      }
   }

   /* every unit is acked: tell the receiver so it can stop lingering immediately
    * instead of waiting out MSC_UDP_FLOW_LINGER_MS. Header-only, best-effort -- send
    * a few so a single drop does not push the receiver back onto its linger timer.
    * (We only reach here with snd_una >= units when !error, i.e. the receiver
    * already delivered everything and acked it, so a FIN can never arrive early.) */
   /* one closing snapshot so the UI always sees the flow's final state */
   if (msc_udp_stats_on)
      msc_udp_stats_emit("flow_sample",
                      ",\"flow\":%d,\"units\":%llu,\"snd_una\":%llu,"
                      "\"snd_nxt\":%llu,\"cwnd\":%.1f,\"rwnd\":%llu,"
                      "\"in_flight\":%llu,\"tx_units\":%llu,\"retransmitted\":%llu,"
                      "\"acknowledgments\":%llu,\"srtt_us\":%.0f,\"rto_us\":%llu,"
                      "\"final\":1",
                      g->flow_id, (unsigned long long)g->units,
                      (unsigned long long)g->snd_una,
                      (unsigned long long)g->snd_nxt, g->cwnd,
                      (unsigned long long)g->rwnd,
                      (unsigned long long)(g->snd_nxt - g->snd_una),
                      (unsigned long long)g->tx_units,
                      (unsigned long long)g->rtx_units,
                      (unsigned long long)g->acknowledgments,
                      g->srtt / 1000.0,
                      (unsigned long long)(g->rto / 1000));

   /* Whatever happens next (FIN, or an error teardown), this flow has stopped
    * competing for the path. Tell the siblings before the FIN handshake, not
    * after it. */
   if (cc_uses_rate_samples())
      atomic_fetch_add(&g->x->flows_retired, 1);

   if (!g->error && g->snd_una >= g->units)
   {
      struct msc_udp_packet_header fin;
      int i;
      memset(&fin, 0, sizeof(fin));
      fin.magic = htonl(MSC_UDP_MAGIC);
      fin.type = htonl(MSC_UDP_FIN);
      fin.flow_id = htonl((uint32_t)g->flow_id);
      fin.xfer_id = msc_udp_hton64(g->x->transfer_id);
      for (i = 0; i < 5; i++)
         (void)send(g->udpfd, &fin, sizeof(fin), 0);
   }

   if (g_stats)
      fprintf(stderr, "MSC UDP sender flow %d: units %llu tx %llu retransmitted %llu "
              "rtxrto %llu rtxsack %llu rtxdupack %llu spurious %llu undo %llu "
              "acknowledgments %llu cwnd %.0f srtt %.0fus rto %lluus reorder %.1fms "
              "cc %s wmax %.0f "
              "ce %llu ecncut %llu rttskip %llu sndbuf %dKiB%s grows %llu fairwait %llu\n",
              g->flow_id, (unsigned long long)g->units,
              (unsigned long long)g->tx_units, (unsigned long long)g->rtx_units,
              (unsigned long long)g->rtx_rto, (unsigned long long)g->rtx_sack,
              (unsigned long long)g->rtx_dupack,
              (unsigned long long)g->spurious_rtx, (unsigned long long)g->undo_done,
              (unsigned long long)g->acknowledgments, g->cwnd,
              g->srtt / 1000.0, (unsigned long long)(g->rto / 1000),
              (double)flow_reorder_wait_ns(g) / 1e6,
              g_no_cc ? "none" : cc_current()->name,
              g->w_max,
              (unsigned long long)g->ce_seen, (unsigned long long)g->ecn_cuts,
              (unsigned long long)g->rtt_ambiguous,
              g->sndbuf_now / 1024, g->sndbuf_maxed ? " (maxed)" : "",
              (unsigned long long)g->buf_grows,
              (unsigned long long)g->fair_starved);
   if (g_stats && cc_uses_rate_samples())
   {
      static const char *const state_names[] =
         { "startup", "drain", "probe_bw", "probe_rtt" };
      fprintf(stderr, "MSC UDP sender flow %d: rate btlbw %.3f Gbit/s "
              "minrtt %.3fms bdp %.0f inflhi %.0f state %s filled %d "
              "restarts %llu samples %llu applimited %d\n",
              g->flow_id,
              g->btl_bw_ups * 8.0 * (double)g->x->payload_size / 1e9,
              (double)g->min_rtt_ns / 1e6, rate_bdp_units(g), g->inflight_hi,
              state_names[g->rate_state & 3], g->rate_filled,
              (unsigned long long)g->rate_restarts,
              (unsigned long long)g->rate_samples, g->rate_app_limited);
   }
   return NULL;
}

/* ===== the receiver (receiver flow) ================================= */
struct receiver_flow
{
   struct msc_udp_xfer *x;
   int udpfd;
   struct msc_udp_receive_demux *receive_demux;         /* shared socket: packets arrive via this
                                 * flow's demux queue, never a raw recv on
                                 * udpfd. NULL when this flow owns udpfd */
   int shared_flows;            /* flows sharing udpfd (1 = private socket) */
   int buf_owner;               /* the one flow allowed to autotune udpfd */
   int flow_id;
   struct msc_udp_part part;        /* local-seq -> global-unit mapping */
   uint64_t units;              /* part.units (loop bound) */
   uint64_t file_hint;          /* cached file index for xfer_file_of */

   uint64_t rcv_nxt;
   uint8_t *receive_bitmap;          /* order receive_bitmap: one bit per unit owned */
   uint64_t received;
   uint64_t last_echo_tsc;
   uint64_t acked_at_last_acknowledgment;  /* rcv_nxt when we last sent a acknowledgment */
   uint64_t last_acknowledgment_ns;        /* for the stretch-ACK timer */

   struct sockaddr_storage sender_addr;
   socklen_t sender_addrlen;
   int have_addr;

   uint64_t rx_units, dup_units, acknowledgments_sent;
   /* Only the sampler sees this snapshot; publish once per receive batch. */
   pthread_mutex_t sample_lock;
   uint64_t sampled_rx_units;
   uint64_t dsack_start, dsack_end; /* duplicate range to report in the next
                                     * acknowledgment's first SACK block (DSACK) */
   int dsack_pending;
   uint64_t gro_segs;       /* packets delivered inside coalesced (>1 seg) slots */
   uint64_t gro_slots;      /* recvmsg slots the kernel actually coalesced */
   uint64_t writes;         /* flush_run calls that hit the file (1 pwritev/memcpy run) */
   uint64_t wbytes;         /* bytes those writes moved -> avg run size = wbytes/writes */
   uint64_t flush_ewma_ns;  /* EWMA of one flush_run's wall time: the write-stall
                             * signal that shrinks the advertised window (MSC_UDP_RWND) */
   uint64_t adv_rwnd;       /* last window advertised in a acknowledgment (telemetry) */
   uint64_t ce_units;       /* cumulative CE-marked data units seen (ECN) */
   int wpath;               /* visual: last write path (0 none, 1 mmap, 2 pwritev) */

   /* receive-buffer autotuning (MSC_UDP_SOCK_AUTOTUNE) */
   uint64_t next_buf_check_ns;
   uint32_t sk_drops_last;  /* per-socket drop count at the last check */
   int rcvbuf_now;          /* applied SO_RCVBUF (kernel-reported / 2) */
   int rcvbuf_maxed;        /* hit the rmem_max clamp or MSC_UDP_AUTOTUNE_MAX */
   int meminfo_bad;         /* SO_MEMINFO unsupported: skip autotune growth */
   uint64_t buf_grows;      /* growth events (telemetry) */
   uint64_t clamp_drops;    /* SK_MEMINFO_DROPS accrued AFTER rcvbuf_maxed:
                             * losses no amount of autotune can absorb (raise
                             * net.core.rmem_max, or the sender must pace) */
   int clamp_warned;
   int error;
   int error_code;             /* stable receiver-local failure, when known */
};

static int board_test(const uint8_t *b, uint64_t u) { return (b[u >> 3] >> (u & 7)) & 1; }
static int board_set(uint8_t *b, uint64_t u)
{
   uint8_t mask = (uint8_t)(1u << (u & 7));
   if (b[u >> 3] & mask) return 0;
   b[u >> 3] |= mask;
   return 1;
}

/* build and send one acknowledgment snapshot to the sender */
static void send_acknowledgment(struct receiver_flow *cf, uint64_t echo_tsc)
{
   char buf[sizeof(struct msc_udp_acknowledgment_header) +
            MSC_UDP_MAX_SACK * sizeof(struct msc_udp_sack_block)];
   struct msc_udp_acknowledgment_header *r = (struct msc_udp_acknowledgment_header *)buf;
   uint32_t nb = 0;
   uint64_t u = cf->rcv_nxt;
   uint64_t scan_end = cf->rcv_nxt + MSC_UDP_WIN_RING; /* holes live within the window */

   if (!cf->have_addr) return;
   if (scan_end > cf->units) scan_end = cf->units;
   /* DSACK (RFC 2883 convention): the first block reports a range that
    * arrived in duplicate, telling the sender that retransmit was wasted.
    * The regular blocks that follow keep reporting what is held above the
    * cum-ack, so an endpoint that ignores DSACK sees a normal acknowledgment. */
   if (cf->dsack_pending)
   {
      struct msc_udp_sack_block *b = (struct msc_udp_sack_block *)(buf + sizeof(*r));
      b->start = msc_udp_hton64(cf->dsack_start);
      b->end = msc_udp_hton64(cf->dsack_end);
      nb = 1;
      cf->dsack_pending = 0;
   }
   while (u < scan_end && nb < MSC_UDP_MAX_SACK)
   {
      uint64_t start;
      if (!board_test(cf->receive_bitmap, u)) { u++; continue; }
      start = u;
      while (u < cf->units && board_test(cf->receive_bitmap, u)) u++;
      {
         struct msc_udp_sack_block *b =
            (struct msc_udp_sack_block *)(buf + sizeof(*r) + (size_t)nb * sizeof(*b));
         b->start = msc_udp_hton64(start);
         b->end = msc_udp_hton64(u);
      }
      nb++;
   }
   r->magic = htonl(MSC_UDP_MAGIC);
   r->type = htonl(MSC_UDP_ACK);
   r->flow_id = htonl((uint32_t)cf->flow_id);
   r->sack_count = htonl(nb);
   r->xfer_id = msc_udp_hton64(cf->x->transfer_id);
   r->cum_ack = msc_udp_hton64(cf->rcv_nxt);
   /* the real receiver window (MSC_UDP_RWND): ring space minus the out-of-order
    * units this flow is holding above the cum-ack, halved while the write path
    * is stalling. A slow receiver now throttles the sender through the window
    * instead of through rcvbuf overflow + retransmissions. Floored at the initial cwnd
    * so the ack clock (and hole repair, which is not window-gated) never stalls. */
   {
      uint64_t rwnd = MSC_UDP_WIN_RING - 1;
      if (g_rwnd)
      {
         uint64_t occ = cf->received - cf->rcv_nxt;   /* units held out of order */
         uint64_t floor_u = (uint64_t)g_init_cwnd;
         rwnd = occ < rwnd ? rwnd - occ : 0;
         if (cf->flush_ewma_ns > g_rwnd_stall_ns)
            rwnd /= 2;
         if (rwnd < floor_u) rwnd = floor_u;
      }
      cf->adv_rwnd = rwnd;
      r->rwnd_units = msc_udp_hton64(rwnd);
   }
   r->echo_tsc = msc_udp_hton64(echo_tsc);
   r->delivered = msc_udp_hton64(cf->received);
   r->ce_units = msc_udp_hton64(cf->ce_units);
   {
      size_t len = sizeof(*r) + (size_t)nb * sizeof(struct msc_udp_sack_block);
      sendto(cf->udpfd, buf, len, 0,
             (struct sockaddr *)&cf->sender_addr, cf->sender_addrlen);
      cf->acknowledgments_sent++;
   }
   cf->acked_at_last_acknowledgment = cf->rcv_nxt;
   cf->last_acknowledgment_ns = msc_udp_now_ns();
}

/* Unsolicited ("idle") acknowledgment, sent when the receive path times out
 * rather than in response to data. It must be rate limited: SO_RCVTIMEO is 1 ms
 * and a high-RTT sender leaves the receiver idle for most of every RTT, so an
 * unrated idle ack floods the reverse path -- 13005 acks for 1104 units on the
 * 53 ms netem path, which before the sender-side guard in
 * sender_take_acknowledgment() was read as a storm of duplicate acks.
 *
 * An open hole keeps the prompt cadence so repair is never delayed; with
 * nothing outstanding this is only a window update / keepalive and can be lazy.
 *
 * echo_tsc is deliberately 0. The sender samples RTT only when echo > 0, and an
 * idle ack would otherwise echo a stale timestamp from an earlier batch and
 * inflate srtt and rto. */
static void send_idle_acknowledgment(struct receiver_flow *cf, uint64_t now)
{
   /* Prompt when there is anything the sender is still waiting to hear about:
    * a hole (received > rcv_nxt, the original case) OR in-order data that has
    * not been acknowledged yet (rcv_nxt > acked_at_last). The second arm is
    * the delayed-ack timer the arrival path lacks: a batch smaller than
    * g_ack_every whose FEEDBACK_MS timer had not expired at arrival time was
    * never acked at all if the sender then went window-blocked -- so every
    * small-window round (any controller's post-RTO recovery; the rate
    * controller's 4-unit floor) cost a full IDLE_ACK_MS. Measured: 8 rate
    * flows pinned at exactly 4 units/20 ms = 0.11 Gbit/s aggregate. Pure
    * keepalives (everything acked) stay lazy, which is what protects the
    * reverse path of a high-RTT link from the idle-ack flood. */
   uint64_t gap = (cf->received > cf->rcv_nxt ||
                   cf->rcv_nxt > cf->acked_at_last_acknowledgment)
                  ? (uint64_t)MSC_UDP_FEEDBACK_MS * 1000000ULL
                  : (uint64_t)MSC_UDP_IDLE_ACK_MS * 1000000ULL;
   if (now - cf->last_acknowledgment_ns >= gap)
      send_acknowledgment(cf, 0);
}

/* one validated, not-yet-written packet; data points into the recv buffer */
struct recv_item
{
   uint64_t seq;
   uint64_t fi;      /* destination file index in the table */
   uint64_t off;     /* absolute offset within that file (base + within) */
   uint64_t len;
   const char *data;
};

/* write a run of packets at strictly contiguous offsets with one pwritev, then
 * mark the order receive_bitmap. Marking only after the write means a failed write never
 * leaves a receive_bitmap bit set for data that is not on disk. board_set's return de-
 * dupes, so a duplicate inside a run is counted once. */
static int flush_run(struct receiver_flow *cf, const struct recv_item *run,
                     unsigned n, struct iovec *wiov, int *new_units)
{
   struct msc_udp_xfer *x = cf->x;
   struct msc_udp_file *fl = &x->files[run[0].fi];
   uint64_t t0 = msc_udp_now_ns();   /* write-stall EWMA feeds the advertised rwnd */
   unsigned i;
   if (n == 0)
      return 0;
   /* MSC_UDP_RX_NOWRITE discards instead of writing -- a diagnostic to isolate the
    * dest write path's cost from the network/recv path (produces wrong output). */
   if (g_rx_nowrite)
   {
      /* fall through to receive_bitmap marking without touching the file */
   }
   else if (fl->map != NULL)
   {
      /* lock-free: memcpy each packet into the shared mapping. run offsets are
       * absolute within the file and base == 0 whenever a file is mapped. */
      for (i = 0; i < n; i++)
         memcpy((char *)fl->map + (size_t)run[i].off, run[i].data, (size_t)run[i].len);
      cf->wpath = 1;
   }
   else
   {
      cf->wpath = 2;
      /* a run is contiguous within one file (the coalescer breaks at file edges) */
      for (i = 0; i < n; i++)
      {
         wiov[i].iov_base = (void *)run[i].data;
         wiov[i].iov_len = (size_t)run[i].len;
      }
      if (msc_udp_pwritev_all(fl->fd, wiov, (int)n, (off_t)run[0].off) != 0)
      {
         fprintf(stderr, "MSC UDP receiver flow %d: write failed: %s\n",
                 cf->flow_id, strerror(errno));
         cf->error = 1;
         cf->error_code = MSC_EXIT_DESTINATION;
         return -1;
      }
   }
   if (!g_rx_nowrite)
   {
      uint64_t rb = 0, dt = msc_udp_now_ns() - t0;
      for (i = 0; i < n; i++) rb += run[i].len;
      cf->writes++;
      cf->wbytes += rb;   /* one contiguous run == one pwritev / one memcpy sweep */
      cf->flush_ewma_ns = cf->flush_ewma_ns
         ? (cf->flush_ewma_ns * 7 + dt) / 8 : dt;
   }
   for (i = 0; i < n; i++)
      if (board_set(cf->receive_bitmap, run[i].seq))
      {
         cf->received++;
         cf->rx_units++;
         *new_units = 1;
      }
   return 0;
}

/* with MSC_UDP_GRO the kernel hands us one recvmsg buffer holding many coalesced
 * packets, so each slot must be big enough for a full GRO super-datagram and we
 * need fewer of them. Without GRO we keep the old one-packet-per-slot layout. */
#define MSC_UDP_GRO_SLOT  65536   /* >= max coalesced UDP packet (64KB) */
#define MSC_UDP_GRO_BATCH 16      /* slots per recvmmsg when coalescing */

/* idle-resend cadence for the final acknowledgment during MSC_UDP_FLOW_LINGER_MS: a few
 * attempts spread across the linger window (not MSC_UDP_FEEDBACK_MS's 1ms active-
 * transfer cadence, which would flood it with ~300 redundant sends). */
#define MSC_UDP_LINGER_RESEND_MS (MSC_UDP_FLOW_LINGER_MS / 4)

static void *receiver_flow_worker(void *arg)
{
   struct receiver_flow *cf = arg;
   struct msc_udp_xfer *x = cf->x;
   size_t bufsize = sizeof(struct msc_udp_packet_header) + x->payload_size;
   /* slot geometry depends on GRO: big slots / few of them when coalescing.
    * One slot can yield up to slotsz/bufsize packets, so items/wiov are sized
    * for that worst case rather than for the slot count. */
   int nslots = g_gro ? MSC_UDP_GRO_BATCH : g_recv_batch;
   size_t slotsz = g_gro ? MSC_UDP_GRO_SLOT : bufsize;
   size_t per_slot = slotsz / bufsize + 1;          /* max packets in one slot */
   size_t maxitems = (size_t)nslots * per_slot;
   /* room for both possible cmsgs: UDP_GRO's int stride and IP_TOS's byte */
   size_t ctllen = CMSG_SPACE(sizeof(int)) + CMSG_SPACE(sizeof(int));
   char *bufs = msc_udp_alloc(slotsz * (size_t)nslots, "receiver bufs");
   struct iovec *iov = msc_udp_alloc(sizeof(struct iovec) * (size_t)nslots, "iov");
   struct mmsghdr *msgs = msc_udp_alloc(sizeof(struct mmsghdr) * (size_t)nslots, "msgs");
   struct sockaddr_storage *names =
      msc_udp_alloc(sizeof(struct sockaddr_storage) * (size_t)nslots, "names");
   /* one mode: the demux already split GRO super-datagrams and captured each
    * datagram's TOS byte, so no cmsg space is needed -- per-slot TOS arrives
    * out-of-band in slot_tos instead */
   char *ctl = (cf->receive_demux == NULL && (g_gro || g_ecn))
      ? msc_udp_alloc(ctllen * (size_t)nslots, "recv ctl") : NULL;
   unsigned char *slot_tos = cf->receive_demux != NULL
      ? msc_udp_alloc((size_t)nslots, "slot tos") : NULL;
   struct recv_item *items =
      msc_udp_alloc(sizeof(struct recv_item) * maxitems, "recv items");
   struct iovec *wiov = msc_udp_alloc(sizeof(struct iovec) * maxitems, "wiov");
   uint64_t *echo_seq = msc_udp_alloc(sizeof(uint64_t) * maxitems, "echo seq");
   uint64_t *echo_tsc = msc_udp_alloc(sizeof(uint64_t) * maxitems, "echo tsc");
   int done = 0;
   int got_fin = 0;                 /* sender signalled this flow is fully acked */
   uint64_t quiet_deadline = 0;
   uint64_t last_data = msc_udp_now_ns();
   int i;

   for (i = 0; i < nslots; i++)
   {
      iov[i].iov_base = bufs + (size_t)i * slotsz;
      iov[i].iov_len = slotsz;
      memset(&msgs[i].msg_hdr, 0, sizeof(msgs[i].msg_hdr));
      msgs[i].msg_hdr.msg_iov = &iov[i];
      msgs[i].msg_hdr.msg_iovlen = 1;
      msgs[i].msg_hdr.msg_name = &names[i];
      msgs[i].msg_hdr.msg_namelen = sizeof(names[i]);
      if (ctl != NULL)
         msgs[i].msg_hdr.msg_control = ctl + (size_t)i * ctllen;
   }

   if (cf->units == 0) { done = 1; }   /* nothing to receive */

   while (!done || msc_udp_now_ns() <= quiet_deadline)
   {
      if (x->cancel_flag != NULL && *x->cancel_flag)
      { cf->error = 1; break; }
      int got;
      /* the kernel shrinks msg_controllen to what it wrote, so re-arm every slot
       * before each recvmmsg or the cmsgs stop arriving after the first batch */
      if (ctl != NULL)
         for (i = 0; i < nslots; i++)
            msgs[i].msg_hdr.msg_controllen = ctllen;
      if (cf->receive_demux != NULL)
      {
         /* one mode: the timed wait moves from recvmmsg+SO_RCVTIMEO to this
          * flow's queue condvar (same MSC_UDP_FEEDBACK_MS budget, so the acknowledgment
          * clock keeps ticking when idle), then a nonblocking drain fills the
          * rest of the batch. got == 0 marks a timeout (recvmmsg would have
          * said -1/EAGAIN); the demux is only gone after session teardown, so
          * -1 mid-flow is an error. */
         unsigned char t0 = 0;
         int n = msc_udp_receive_demux_pop(cf->receive_demux, cf->flow_id, bufs, slotsz,
                             (long)MSC_UDP_FEEDBACK_MS * 1000, &t0);
         if (n > 0)
         {
            msgs[0].msg_len = (unsigned)n;
            slot_tos[0] = t0;
            got = 1;
            while (got < nslots &&
                   (n = msc_udp_receive_demux_pop(cf->receive_demux, cf->flow_id,
                                    bufs + (size_t)got * slotsz, slotsz, 0,
                                    &slot_tos[got])) > 0)
            {
               msgs[got].msg_len = (unsigned)n;
               got++;
            }
            /* stdio1: no control handshake pre-seeded the sender address at
             * flows_alloc, but a datagram just arrived, so the demux has
             * stashed its source by now -- learn it here (once) so acknowledgments
             * have their sendto target, mirroring the raw path's msg_name
             * learning below */
            if (!cf->have_addr &&
                msc_udp_receive_demux_peer(cf->receive_demux, &cf->sender_addr, &cf->sender_addrlen) == 0)
               cf->have_addr = 1;
         }
         else if (n == 0)
            got = 0;
         else
         {
            fprintf(stderr, "MSC UDP receiver flow %d: demux exited mid-flow\n",
                    cf->flow_id);
            cf->error = 1;
            break;
         }
      }
      else
         got = recvmmsg(cf->udpfd, msgs, nslots, MSG_WAITFORONE, NULL);
      uint64_t now = msc_udp_now_ns();
      uint64_t batch_echo = cf->last_echo_tsc;
      uint64_t prev_nxt = cf->rcv_nxt;
      int new_units = 0;
      int k;

      /* receive-buffer autotuning: double SO_RCVBUF when this socket is
       * dropping (SK_MEMINFO_DROPS moved) or running >3/4 full, stopping at
       * the rmem_max clamp or MSC_UDP_AUTOTUNE_MAX. Coarse cadence, off the fast
       * path; an old kernel without SO_MEMINFO just disables it. */
      if (g_autotune && !cf->meminfo_bad &&
          now >= cf->next_buf_check_ns &&
          cf->buf_owner)   /* a shared socket gets ONE autotuner, never
                            * several racing setsockopt on it */
      {
         uint32_t mi[MSC_UDP_SKMEM_VARS];
         socklen_t ml = sizeof(mi);
         cf->next_buf_check_ns = now + 100ULL * 1000 * 1000;
         if (getsockopt(cf->udpfd, SOL_SOCKET, SO_MEMINFO, mi, &ml) == 0 &&
             ml >= sizeof(mi))
         {
            int pressure = mi[MSC_UDP_SKMEM_DROPS] > cf->sk_drops_last ||
                           mi[MSC_UDP_SKMEM_RMEM_ALLOC] > mi[MSC_UDP_SKMEM_RCVBUF] / 4 * 3;
            if (cf->rcvbuf_maxed)
            {
               /* Growth is exhausted, but keep watching the drop counter:
                * losses at the clamp are the invisible 3%-of-transfer waste
                * measured on clean IPoIB, and only the operator
                * (rmem_max) or the sender (pacing) can fix them. */
               if (mi[MSC_UDP_SKMEM_DROPS] > cf->sk_drops_last)
               {
                  cf->clamp_drops += mi[MSC_UDP_SKMEM_DROPS] - cf->sk_drops_last;
                  if (g_stats && !cf->clamp_warned)
                  {
                     cf->clamp_warned = 1;
                     fprintf(stderr, "MSC UDP receiver flow %d: still dropping "
                             "datagrams at the SO_RCVBUF ceiling (%d bytes) -- "
                             "raise net.core.rmem_max\n",
                             cf->flow_id, cf->rcvbuf_now);
                  }
               }
            }
            else if (pressure)
            {
               int64_t target = (int64_t)(cf->rcvbuf_now > 0
                                          ? cf->rcvbuf_now : 65536) * 2;
               int applied;
               if (target > g_autotune_max) target = g_autotune_max;
               applied = set_buf_size(cf->udpfd, SO_RCVBUF, (int)target);
               if (applied > cf->rcvbuf_now) cf->buf_grows++;
               if (applied >= 0 && applied < (int)target) cf->rcvbuf_maxed = 1;
               if (applied > 0) cf->rcvbuf_now = applied;
            }
            cf->sk_drops_last = mi[MSC_UDP_SKMEM_DROPS];
         }
         else
            cf->meminfo_bad = 1;
      }

      if (got == 0)
      {
         /* one mode: queue-pop timeout -- identical idle handling to the
          * EAGAIN path below (recvmmsg itself never returns 0 here) */
         if (done && now > quiet_deadline) break;
         if (!done && cf->have_addr) send_idle_acknowledgment(cf, now);
         /* Lingering after completion: idle-resend the final acknowledgment on the
          * same feedback cadence as the active path, not just when a fresh
          * (retransmitted) packet happens to arrive. Without this, a final
          * acknowledgment dropped by a full flow queue only gets a second chance if
          * the sender's own RTO fires before this flow's short linger expires
          * -- and a backed-off RTO can easily exceed MSC_UDP_FLOW_LINGER_MS,
          * spuriously failing an otherwise-complete transfer. */
         if (done && cf->have_addr &&
             now - cf->last_acknowledgment_ns >= (uint64_t)MSC_UDP_LINGER_RESEND_MS * 1000000ULL)
            send_acknowledgment(cf, cf->last_echo_tsc);
         if (!done && g_stall_timeout_ns != 0 &&
             now - last_data > g_stall_timeout_ns)
         {
            fprintf(stderr, "MSC UDP receiver flow %d: no data, giving up\n", cf->flow_id);
            cf->error = 1;
            break;
         }
         continue;
      }
      if (got < 0)
      {
         if (errno == EINTR) continue;
         if (errno == EAGAIN || errno == EWOULDBLOCK)
         {
            if (done && now > quiet_deadline) break;
            if (!done && cf->have_addr) send_idle_acknowledgment(cf, now);
            /* see the identical one-mode comment above */
            if (done && cf->have_addr &&
                now - cf->last_acknowledgment_ns >= (uint64_t)MSC_UDP_FEEDBACK_MS * 1000000ULL)
               send_acknowledgment(cf, cf->last_echo_tsc);
            if (!done && g_stall_timeout_ns != 0 &&
                now - last_data > g_stall_timeout_ns)
            {
               fprintf(stderr, "MSC UDP receiver flow %d: no data, giving up\n", cf->flow_id);
               cf->error = 1;
               break;
            }
            continue;
         }
         fprintf(stderr, "MSC UDP receiver flow %d: recv failed: %s\n",
                 cf->flow_id, strerror(errno));
         cf->error = 1;
         break;
      }

      /* parse the batch into validated, not-yet-seen packets. Each slot may carry
       * a single datagram or, under GRO, a coalesced run we walk in gso strides;
       * the kernel reports the per-segment stride in a SOL_UDP/UDP_GRO cmsg. */
      unsigned nit = 0;
      unsigned nvalid = 0;   /* valid packets this batch, arrival order, for echo */
      for (k = 0; k < got; k++)
      {
         const char *slot = bufs + (size_t)k * slotsz;
         ssize_t total = (ssize_t)msgs[k].msg_len;
         size_t stride = (size_t)total;   /* not coalesced: one segment == slot */
         int slot_ce = 0;                 /* this datagram arrived CE-marked */
         ssize_t pos;

         if (ctl != NULL)
         {
            struct cmsghdr *c;
            for (c = CMSG_FIRSTHDR(&msgs[k].msg_hdr); c;
                 c = CMSG_NXTHDR(&msgs[k].msg_hdr, c))
            {
               if (g_gro && c->cmsg_level == SOL_UDP && c->cmsg_type == UDP_GRO)
                  stride = (size_t)(*(int *)CMSG_DATA(c));
               if (g_ecn && c->cmsg_level == IPPROTO_IP && c->cmsg_type == IP_TOS &&
                   (*(const unsigned char *)CMSG_DATA(c) & 0x3) == 0x3)
                  slot_ce = 1;   /* ECN CE: the fabric marked congestion */
            }
            if (stride == 0) stride = (size_t)total;   /* guard against a 0 stride */
            if (g_gro && total > 0 && stride < (size_t)total) /* >1 seg == coalesced */
            {
               cf->gro_slots++;
               cf->gro_segs += ((size_t)total + stride - 1) / stride;
            }
         }
         /* one mode: the demux delivered exactly one (GRO-pre-split) packet
          * per slot and its TOS byte out-of-band */
         if (slot_tos != NULL && g_ecn && (slot_tos[k] & 0x3) == 0x3)
            slot_ce = 1;

         for (pos = 0; pos < total; pos += (ssize_t)stride)
         {
            const char *buf = slot + pos;
            const struct msc_udp_packet_header *h = (const struct msc_udp_packet_header *)buf;
            ssize_t n = total - pos;   /* last coalesced segment may be short */
            uint64_t seq, off, len, global, exp_off, exp_len, fi;
            struct msc_udp_file *fl;

            if (n > (ssize_t)stride) n = (ssize_t)stride;

            if (n < (ssize_t)sizeof(*h) || ntohl(h->magic) != MSC_UDP_MAGIC ||
                ntohl(h->flow_id) != (uint32_t)cf->flow_id ||
                msc_udp_ntoh64(h->xfer_id) != x->transfer_id)
               continue;
            if (ntohl(h->type) == MSC_UDP_FIN) { got_fin = 1; continue; }
            if (ntohl(h->type) != MSC_UDP_DATA)
               continue;
            seq = msc_udp_ntoh64(h->seq);
            len = ntohl(h->length);
            off = msc_udp_ntoh64(h->offset);   /* offset within the destination file */
            if (seq >= cf->units) continue;
            /* record for the echo scan (arrival order) before dedup drops it */
            echo_seq[nvalid] = seq;
            echo_tsc[nvalid] = msc_udp_ntoh64(h->send_tsc);
            nvalid++;
            global = part_global(&cf->part, seq);
            fi = xfer_file_of(x, global, &cf->file_hint);
            fl = &x->files[fi];
            exp_off = unit_off(x, &cf->part, fl, global);
            exp_len = unit_len(x, &cf->part, fl, global);
            if (off != exp_off || len != exp_len || n != (ssize_t)(sizeof(*h) + len))
               continue;

            if (!cf->have_addr)
            {
               memcpy(&cf->sender_addr, &names[k], msgs[k].msg_hdr.msg_namelen);
               cf->sender_addrlen = msgs[k].msg_hdr.msg_namelen;
               cf->have_addr = 1;
            }
            if (board_test(cf->receive_bitmap, seq))
            {
               cf->dup_units++;
               /* DSACK: remember the duplicate range; the next acknowledgment
                * reports it so the sender learns the retransmit was wasted.
                * A later duplicate before the ack goes out extends a
                * contiguous range or replaces a disjoint one -- an ack is a
                * snapshot, and dup runs are contiguous in practice. */
               if (g_dsack && (g_feat_agreed & MSC_UDP_FEAT_DSACK))
               {
                  if (cf->dsack_pending && seq == cf->dsack_end)
                     cf->dsack_end = seq + 1;
                  else
                  {
                     cf->dsack_start = seq;
                     cf->dsack_end = seq + 1;
                     cf->dsack_pending = 1;
                  }
               }
               continue;
            }
            if (slot_ce) cf->ce_units++;   /* echo congestion back in the acknowledgment */
            items[nit].seq = seq;
            items[nit].fi = fi;
            items[nit].off = fl->base + exp_off;   /* absolute file offset */
            items[nit].len = len;
            items[nit].data = buf + sizeof(*h);
            nit++;
         }
      }

      /* coalesce strictly-contiguous packets (arrival order) into one pwritev
       * each; out-of-order arrivals simply break the run */
      {
         unsigned r = 0;
         while (r < nit)
         {
            unsigned runlen = 1;
            while (r + runlen < nit &&
                   items[r + runlen].fi == items[r + runlen - 1].fi &&
                   items[r + runlen].off ==
                      items[r + runlen - 1].off + items[r + runlen - 1].len)
               runlen++;
            if (flush_run(cf, &items[r], runlen, wiov, &new_units) != 0)
               goto out;
            r += runlen;
         }
      }
      if (new_units) last_data = now;

      /* advance the cumulative ack across any now-contiguous prefix */
      while (cf->rcv_nxt < cf->units && board_test(cf->receive_bitmap, cf->rcv_nxt))
         cf->rcv_nxt++;

      if (g_stats)
      {
         pthread_mutex_lock(&cf->sample_lock);
         cf->sampled_rx_units = cf->rx_units;
         pthread_mutex_unlock(&cf->sample_lock);
      }

      /* echo the send time of the newest in-order packet we saw this batch
       * (latest arrival with seq < rcv_nxt), scanning the recorded arrival list */
      if (cf->rcv_nxt > prev_nxt)
      {
         int j;
         for (j = (int)nvalid - 1; j >= 0; j--)
            if (echo_seq[j] < cf->rcv_nxt)
            {
               batch_echo = echo_tsc[j];
               break;
            }
         cf->last_echo_tsc = batch_echo;
      }

      if (cf->rcv_nxt == cf->units)
      {
         /* complete: (re)send the final acknowledgment and (re)arm the linger so a lost
          * last acknowledgment is recovered when the sender retransmissions the tail */
         done = 1;
         send_acknowledgment(cf, batch_echo);
         quiet_deadline = now + (uint64_t)MSC_UDP_FLOW_LINGER_MS * 1000000ULL;
      }
      else if (new_units)
      {
         /* stretch-ACK the clean path, but feed back promptly the moment a hole
          * appears (so the sender can fast-retransmit) or the timer expires */
         int gap = (cf->received > cf->rcv_nxt);
         int advanced = (cf->rcv_nxt - cf->acked_at_last_acknowledgment) >= g_ack_every;
         int timer = (now - cf->last_acknowledgment_ns) >=
                     (uint64_t)MSC_UDP_FEEDBACK_MS * 1000000ULL;
         if (gap || advanced || timer)
            send_acknowledgment(cf, batch_echo);
      }

      if (msc_udp_stats_on)
      {
         if (msc_udp_stats_due(cf->flow_id, MSC_UDP_STATS_FLOW))
            msc_udp_stats_emit("flow_sample",
                            ",\"flow\":%d,\"units\":%llu,\"rcv_nxt\":%llu,"
                            "\"rx_units\":%llu,\"received\":%llu,\"dup\":%llu,"
                            "\"acknowledgments\":%llu,\"rwnd\":%llu,\"ce\":%llu",
                            cf->flow_id, (unsigned long long)cf->units,
                            (unsigned long long)cf->rcv_nxt,
                            (unsigned long long)cf->rx_units,
                            (unsigned long long)cf->received,
                            (unsigned long long)cf->dup_units,
                            (unsigned long long)cf->acknowledgments_sent,
                            (unsigned long long)cf->adv_rwnd,
                            (unsigned long long)cf->ce_units);
         if (cf->writes > 0 && msc_udp_stats_due(cf->flow_id, MSC_UDP_STATS_WRITE))
            msc_udp_stats_emit("write_sample",
                            ",\"flow\":%d,\"path\":\"%s\",\"writes\":%llu,"
                            "\"avg_write\":%llu,\"stall_us\":%llu,\"drops\":%u",
                            cf->flow_id,
                            cf->wpath == 1 ? "mmap"
                               : cf->wpath == 2 ? "pwritev" : "none",
                            (unsigned long long)cf->writes,
                            (unsigned long long)(cf->wbytes / cf->writes),
                            (unsigned long long)(cf->flush_ewma_ns / 1000),
                            cf->sk_drops_last);
      }

      /* the sender has every ack for this flow and asked us to leave: skip the
       * rest of the MSC_UDP_FLOW_LINGER_MS wait. Only once done, so a stray FIN can
       * never cut a receive short; the final acknowledgment has already been sent. */
      if (got_fin && done)
         break;
   }

out:
   /* one closing snapshot so the UI always sees the flow's final state */
   if (msc_udp_stats_on)
      msc_udp_stats_emit("flow_sample",
                      ",\"flow\":%d,\"units\":%llu,\"rcv_nxt\":%llu,"
                      "\"rx_units\":%llu,\"received\":%llu,\"dup\":%llu,"
                      "\"acknowledgments\":%llu,\"rwnd\":%llu,\"ce\":%llu,\"final\":1",
                      cf->flow_id, (unsigned long long)cf->units,
                      (unsigned long long)cf->rcv_nxt,
                      (unsigned long long)cf->rx_units,
                      (unsigned long long)cf->received,
                      (unsigned long long)cf->dup_units,
                      (unsigned long long)cf->acknowledgments_sent,
                      (unsigned long long)cf->adv_rwnd,
                      (unsigned long long)cf->ce_units);
   if (g_stats)
      fprintf(stderr, "MSC UDP receiver flow %d: units %llu rx %llu dup %llu "
              "acknowledgments %llu gro_slots %llu gro_segs %llu writes %llu avgwrite %lluB "
              "rwnd %llu wstall %lluus ce %llu rcvbuf %dKiB%s grows %llu "
              "clampdrops %llu\n",
              cf->flow_id, (unsigned long long)cf->units,
              (unsigned long long)cf->rx_units, (unsigned long long)cf->dup_units,
              (unsigned long long)cf->acknowledgments_sent,
              (unsigned long long)cf->gro_slots, (unsigned long long)cf->gro_segs,
              (unsigned long long)cf->writes,
              (unsigned long long)(cf->writes ? cf->wbytes / cf->writes : 0),
              (unsigned long long)cf->adv_rwnd,
              (unsigned long long)(cf->flush_ewma_ns / 1000),
              (unsigned long long)cf->ce_units,
              cf->rcvbuf_now / 1024, cf->rcvbuf_maxed ? " (maxed)" : "",
              (unsigned long long)cf->buf_grows,
              (unsigned long long)cf->clamp_drops);
   free(bufs); free(iov); free(msgs); free(names); free(ctl); free(slot_tos);
   free(items); free(wiov); free(echo_seq); free(echo_tsc);
   return NULL;
}

/* ===== flow allocation / lifecycle ======================================= */
static struct sender_flow *sender_flows_alloc(struct msc_udp_xfer *x,
                                            const struct msc_udp_sockset *ss)
{
   struct sender_flow *flows = msc_udp_alloc(sizeof(*flows) * (size_t)x->flow_count, "sender flows");
   struct msc_udp_pacer *pacer = NULL;
   int f;
   memset(flows, 0, sizeof(*flows) * (size_t)x->flow_count);
   if (g_fair || g_pace_rate_mbit > 0.0)
   {
      /* one shared bucket for the whole flow set (MSC_UDP_FAIR or a hard cap) */
      pacer = msc_udp_alloc(sizeof(*pacer), "fair pacer");
      memset(pacer, 0, sizeof(*pacer));
      pacer->nflows = x->flow_count;
      pacer->rates_ups = msc_udp_alloc(sizeof(_Atomic uint64_t) * (size_t)x->flow_count,
                                     "fair rates");
      memset((void *)pacer->rates_ups, 0,
             sizeof(_Atomic uint64_t) * (size_t)x->flow_count);
      if (g_pace_rate_mbit > 0.0)
      {
         double ups = g_pace_rate_mbit * 1000000.0 / (8.0 * (double)x->payload_size);
         pacer->cap_ups = ups < 1.0 ? 1 : (uint64_t)ups;
      }
      pacer->burst_uq = (int64_t)g_pace_burst * 1000000;
      atomic_store(&pacer->tokens_uq, pacer->burst_uq);   /* seed a full bucket */
      atomic_store(&pacer->last_ns, 0);
      pacer->start_ns = msc_udp_now_ns();   /* for the aggregate granted-rate summary */
   }
   for (f = 0; f < x->flow_count; f++)
   {
      flows[f].x = x;
      flows[f].udpfd = ss->udp_fds[f];
      flows[f].flow_id = f;
      make_part(&flows[f].part, x->total_units, x->flow_count, f,
                x->payload_size, x->stripe_size, x->stripe_count,
                x->my_ost_start, x->my_ost_count);
      flows[f].units = flows[f].part.units;
      /* Bound the cache AND its metadata, even for many jumbo-payload flows.
       * Small transfers only allocate the ring they can actually use. */
      {
         uint64_t unit_bytes = x->payload_size + 5 * sizeof(uint64_t) + 2;
         uint64_t limit = g_retransmit_budget / (uint64_t)x->flow_count / unit_bytes;
         size_t slots = 2;
         if (limit < 2)
         {
            fprintf(stderr, "MSC UDP: retransmission budget too small for %d flows\n",
                    x->flow_count);
            exit(MSC_EXIT_CLI);
         }
         while (slots < MSC_UDP_WIN_RING && slots < flows[f].units + 1 &&
                slots * 2 <= limit)
            slots *= 2;
         flows[f].ring_slots = slots;
      }
      flows[f].cwnd = g_no_cc ? (double)(flows[f].ring_slots - 1) : g_init_cwnd;
      flows[f].ssthresh = flows[f].ring_slots - 1;
      flows[f].rwnd = flows[f].ring_slots - 1;
      /* W0: start from the data-path probe when available, rather than have_rtt=0
       * plus a blind 200 ms RTO. Fall back to HELLO only when the probe was
       * unavailable. have_rtt stays 0, so the seed is provisional -- the first
       * real data ACK replaces it outright instead of blending. */
      if (g_data_rtt_ns > 0 || g_handshake_rtt_ns > 0)
      {
         uint64_t seed_rtt = g_data_rtt_ns > 0 ? g_data_rtt_ns : g_handshake_rtt_ns;
         flows[f].srtt = (double)seed_rtt;
         flows[f].rttvar = (double)seed_rtt / 2.0;
         flows[f].rto = flow_rto_ns(&flows[f]);
      }
      else
         flows[f].rto = MSC_UDP_INIT_RTO_NS;
      flows[f].last_cum_ack = (uint64_t)-1;
      cc_current()->init(&flows[f]);
      flows[f].pace_tokens = (double)g_pace_burst;  /* seed a full bucket */
      flows[f].pace_last_ns = 0;                    /* lazy: first pace_grant sets it */
      flows[f].pacer = pacer;
      flows[f].receive_demux = ss->receive_demux_of[f];
      flows[f].shared_flows = sock_share(ss->flow_count, ss->nports,
                                         f % ss->nports);
      /* the lowest flow on each socket owns its buffer: with f -> f % K that
       * is exactly the flows below K, and with one socket per flow, all of
       * them */
      flows[f].buf_owner = f < ss->nports;
      /* a shared socket already carries the aggregate buffer set at session
       * open -- re-applying the per-flow size would shrink it */
      flows[f].sndbuf_now = flows[f].receive_demux != NULL
         ? get_buf_size(ss->udp_fds[f], SO_SNDBUF)
         : set_buf_size(ss->udp_fds[f], SO_SNDBUF, g_sock_buffer);
      flows[f].seed = (unsigned int)(f * 2654435761u) ^ (unsigned int)msc_udp_now_ns();
      flows[f].gso_enabled = g_gso;
      flows[f].sent_ns = msc_udp_alloc(sizeof(uint64_t) * flows[f].ring_slots, "sent_ns");
      flows[f].gap_seen_ns = msc_udp_alloc(sizeof(uint64_t) * flows[f].ring_slots, "gap_seen_ns");
      flows[f].sacked = msc_udp_alloc(flows[f].ring_slots, "sacked");
      flows[f].retransmitted = msc_udp_alloc(flows[f].ring_slots, "retransmitted");
      memset(flows[f].gap_seen_ns, 0, sizeof(uint64_t) * flows[f].ring_slots);
      memset(flows[f].sacked, 0, flows[f].ring_slots);
      memset(flows[f].retransmitted, 0, flows[f].ring_slots);
      flows[f].retransmit_cache = msc_udp_alloc((size_t)flows[f].ring_slots * x->payload_size, "retransmission cache");
      flows[f].readbuf = msc_udp_alloc(x->payload_size * MSC_UDP_SEND_BATCH, "send readbuf");
      flows[f].hdrs = msc_udp_alloc(sizeof(struct msc_udp_packet_header) * MSC_UDP_SEND_BATCH, "send hdrs");
      flows[f].iov = msc_udp_alloc(sizeof(struct iovec) * 2 * MSC_UDP_SEND_BATCH, "send iov");
      flows[f].msgs = msc_udp_alloc(sizeof(struct mmsghdr) * MSC_UDP_SEND_BATCH, "send msgs");
      flows[f].gso_buf = g_gso
         ? msc_udp_alloc((sizeof(struct msc_udp_packet_header) + x->payload_size) * MSC_UDP_SEND_BATCH, "gso buf")
         : NULL;
   }
   return flows;
}

static void sender_flows_free(struct sender_flow *flows, int n)
{
   int f;
   if (n > 0 && flows[0].pacer != NULL)
   {
      struct msc_udp_pacer *p = flows[0].pacer;
      if (g_stats)
      {
         /* aggregate pacer summary: total units the shared bucket released, the
          * mean granted-rate over the run, and how often a flow found the bucket
          * empty (depletions = aggregate pacing actively holding it down). */
         uint64_t elapsed = msc_udp_now_ns() - p->start_ns;
         double secs = elapsed / 1e9;
         uint64_t granted = atomic_load(&p->granted);
         if (p->cap_ups > 0)
            fprintf(stderr, "MSC UDP sender: aggregate cap %.1f Mbit/s granted %llu units "
                    "in %.2fs (%.0f units/s) depletions %llu\n",
                    (double)p->cap_ups * 8.0 * (double)flows[0].x->payload_size / 1e6,
                    (unsigned long long)granted, secs,
                    secs > 0.0 ? granted / secs : 0.0,
                    (unsigned long long)atomic_load(&p->depletions));
         else
            fprintf(stderr, "MSC UDP sender: FAIR pacer granted %llu units in %.2fs "
                    "(%.0f units/s aggregate) depletions %llu\n",
                    (unsigned long long)granted, secs,
                    secs > 0.0 ? granted / secs : 0.0,
                    (unsigned long long)atomic_load(&p->depletions));
      }
      free((void *)p->rates_ups);
      free(p);
   }
   for (f = 0; f < n; f++)
   {
      free(flows[f].sent_ns); free(flows[f].sacked);
      free(flows[f].gap_seen_ns); free(flows[f].retransmitted); free(flows[f].retransmit_cache);
      cc_current()->destroy(&flows[f]);
      free(flows[f].readbuf); free(flows[f].hdrs);
      free(flows[f].iov); free(flows[f].msgs); free(flows[f].gso_buf);
   }
   free(flows);
}

static struct receiver_flow *receiver_flows_alloc(struct msc_udp_xfer *x,
                                                const struct msc_udp_sockset *ss)
{
   struct receiver_flow *flows = msc_udp_alloc(sizeof(*flows) * (size_t)x->flow_count, "receiver flows");
   int f;
   memset(flows, 0, sizeof(*flows) * (size_t)x->flow_count);
   for (f = 0; f < x->flow_count; f++)
   {
      pthread_mutex_init(&flows[f].sample_lock, NULL);
      flows[f].x = x;
      flows[f].udpfd = ss->udp_fds[f];
      flows[f].receive_demux = ss->receive_demux_of[f];
      flows[f].shared_flows = sock_share(ss->flow_count, ss->nports,
                                         f % ss->nports);
      flows[f].buf_owner = f < ss->nports;   /* lowest flow on each socket */
      flows[f].flow_id = f;
      make_part(&flows[f].part, x->total_units, x->flow_count, f,
                x->payload_size, x->stripe_size, x->stripe_count,
                x->my_ost_start, x->my_ost_count);
      flows[f].units = flows[f].part.units;
      flows[f].receive_bitmap = msc_udp_alloc(flows[f].units / 8 + 1, "order receive_bitmap");
      memset(flows[f].receive_bitmap, 0, flows[f].units / 8 + 1);
      /* shared socket: (a) don't shrink its aggregate buffer with a per-flow
       * set, and (b) the demux (not this flow) reads msg_name, so pre-seed the
       * sender address it learned at the handshake -- every acknowledgment sendto()s
       * it on the unconnected shared socket. */
      if (flows[f].receive_demux != NULL)
      {
         flows[f].rcvbuf_now = get_buf_size(ss->udp_fds[f], SO_RCVBUF);
         if (msc_udp_receive_demux_peer(flows[f].receive_demux, &flows[f].sender_addr,
                          &flows[f].sender_addrlen) == 0)
            flows[f].have_addr = 1;
      }
      else
         flows[f].rcvbuf_now = set_buf_size(ss->udp_fds[f], SO_RCVBUF,
                                            rcvbuf_seed_bytes());
   }
   return flows;
}

static void receiver_flows_free(struct receiver_flow *flows, int n)
{
   int f;
   for (f = 0; f < n; f++)
   {
      pthread_mutex_destroy(&flows[f].sample_lock);
      free(flows[f].receive_bitmap);
   }
   free(flows);
}

/* parallel fsync of a slice of the file table. Many concurrent fsyncs to one
 * filesystem coalesce into shared journal commits, so flushing a big tree across
 * threads is far cheaper than a serial fsync-per-file barrier (which dominates
 * many-small-files transfers on journaling disks). */
struct fsync_job
{
   struct msc_udp_file *files;
   uint64_t lo, hi;
   int rc;
};

static void *fsync_worker(void *arg)
{
   struct fsync_job *j = arg;
   uint64_t i;
   for (i = j->lo; i < j->hi; i++)
      if (fsync(j->files[i].fd) != 0 && errno != EINVAL)   /* EINVAL: e.g. tmpfs */
      {
         fprintf(stderr, "MSC UDP receiver: fsync failed: %s\n", strerror(errno));
         j->rc = -1;
      }
   return NULL;
}

static int fsync_table(struct msc_udp_file *files, uint64_t nfiles)
{
   uint64_t nworkers = (uint64_t)(g_fsync_threads > 0 ? g_fsync_threads : 1);
   struct fsync_job *jobs;
   pthread_t *threads;
   uint64_t per, rem, i, next, created = 0;
   int rc = 0;

   if (nfiles == 0) return 0;
   if (nworkers > nfiles) nworkers = nfiles;
   if (nworkers < 1) nworkers = 1;
   jobs = msc_udp_alloc(sizeof(*jobs) * (size_t)nworkers, "fsync jobs");
   threads = msc_udp_alloc(sizeof(*threads) * (size_t)nworkers, "fsync threads");
   per = nfiles / nworkers;
   rem = nfiles % nworkers;
   next = 0;
   for (i = 0; i < nworkers; i++)
   {
      jobs[i].files = files;
      jobs[i].lo = next;
      jobs[i].hi = next + per + (i < rem ? 1 : 0);
      jobs[i].rc = 0;
      next = jobs[i].hi;
   }
   /* A failed create must leave valid ranges for EVERY inline fallback. */
   for (i = 0; i < nworkers; i++)
   {
      if (pthread_create(&threads[i], NULL, fsync_worker, &jobs[i]) != 0) break;
      created++;
   }
   /* anything not handed to a thread (creation failure): flush inline */
   for (i = created; i < nworkers; i++)
      { fsync_worker(&jobs[i]); if (jobs[i].rc != 0) rc = -1; }
   for (i = 0; i < created; i++)
   {
      pthread_join(threads[i], NULL);
      if (jobs[i].rc != 0) rc = -1;
   }
   free(jobs);
   free(threads);
   return rc;
}

/* spawn one flow_worker thread per flow, run the global unit stream to completion,
 * join, and report whether any flow failed. Shared by the single-file and the
 * multi-file (recursive) paths -- the only difference between them is how the
 * file table and the control channel handshake are set up. Returns 0 on success. */
static int sender_run_flows(struct msc_udp_xfer *x, const struct msc_udp_sockset *ss)
{
   struct sender_flow *flows = sender_flows_alloc(x, ss);
   pthread_t *threads = msc_udp_alloc(sizeof(pthread_t) * (size_t)x->flow_count, "threads");
   int created = 0, f, rc = 0;
   if (g_stats && g_wan_guard)
      fprintf(stderr, "MSC UDP sender: %s guard init_cwnd %.0f pace gain %.2f burst %llu "
              "bootstrap %llums reorder_wait %llums dupack_thresh %d "
              "min_rto %llums startup_queue_mult %.2f bw_restart %d\n",
              g_profile_active != NULL ? g_profile_active->name : "WAN",
              g_init_cwnd, g_pace_gain, (unsigned long long)g_pace_burst,
              (unsigned long long)(g_pace_bootstrap_ns / 1000000ULL),
              (unsigned long long)(g_reorder_wait_ns / 1000000ULL),
              g_dupack_thresh, (unsigned long long)(g_min_rto_ns / 1000000ULL),
              g_startup_queue_mult, g_bw_restart);
   if (g_stats && g_pace_rate_mbit > 0.0)
      fprintf(stderr, "MSC UDP sender: aggregate fresh-payload cap %.1f Mbit/s\n",
              g_pace_rate_mbit);
   for (f = 0; f < x->flow_count; f++)
   {
      if (pthread_create(&threads[f], NULL, sender_flow_worker, &flows[f]) != 0) break;
      created++;
   }
   for (f = 0; f < created; f++) pthread_join(threads[f], NULL);
   if (created < x->flow_count) rc = -1;
   for (f = 0; f < x->flow_count; f++) if (flows[f].error) rc = -1;
   free(threads);
   sender_flows_free(flows, x->flow_count);
   if (rc != 0) fprintf(stderr, "MSC UDP sender: a flow failed\n");
   return rc;
}

/* Read one named receiver from the "Udp:" block of /proc/net/snmp (host-wide, not
 * per-socket). The block is a label line then a value line with matching columns;
 * we find `name`'s column in the labels and pull that field from the values.
 * Returns -1 if unavailable (non-Linux, no procfs) so the caller can skip it. */
static long snmp_udp_stat(const char *name)
{
   FILE *fp = fopen("/proc/net/snmp", "r");
   char hdr[4096], val[4096];
   long result = -1;
   if (fp == NULL) return -1;
   while (fgets(hdr, sizeof(hdr), fp) != NULL)
   {
      char *save, *tok;
      int idx = 0, found = -1;
      if (strncmp(hdr, "Udp:", 4) != 0) continue;
      if (fgets(val, sizeof(val), fp) == NULL || strncmp(val, "Udp:", 4) != 0)
         break;
      strtok_r(hdr, " \t\n", &save);   /* consume the "Udp:" label */
      for (tok = strtok_r(NULL, " \t\n", &save); tok != NULL;
           tok = strtok_r(NULL, " \t\n", &save), idx++)
         if (strcmp(tok, name) == 0) { found = idx; break; }
      if (found < 0) break;
      strtok_r(val, " \t\n", &save);   /* consume the "Udp:" label */
      for (idx = 0, tok = strtok_r(NULL, " \t\n", &save); tok != NULL;
           tok = strtok_r(NULL, " \t\n", &save), idx++)
         if (idx == found) { result = atol(tok); break; }
      break;
   }
   fclose(fp);
   return result;
}

/* Read snapshots without racing the workers' private packet accounting. */
static uint64_t receiver_rx_units_total(struct receiver_flow *flows, int n)
{
   uint64_t t = 0;
   int f;
   for (f = 0; f < n; f++)
   {
      pthread_mutex_lock(&flows[f].sample_lock);
      t += flows[f].sampled_rx_units;
      pthread_mutex_unlock(&flows[f].sample_lock);
   }
   return t;
}

/* Background sampler (MSC_UDP_STATS only): every interval_ns, snapshot the aggregate
 * received bytes and record the peak interval rate. Together with the avg (total
 * bytes / wall time) this exposes the avg-vs-peak idle gap the pacing work is
 * meant to close, straight out of the transfer instead of a side-channel. */
struct rx_sampler
{
   struct receiver_flow *flows;
   int nflows;
   size_t payload_size;
   uint64_t interval_ns;
   pthread_mutex_t lock;
   pthread_cond_t wake;
   int stop;          /* protected by lock */
   double peak_bps;   /* out */
};

static void *rx_sampler_thread(void *arg)
{
   struct rx_sampler *s = arg;
   uint64_t last_ns = msc_udp_now_ns();
   uint64_t last_units = receiver_rx_units_total(s->flows, s->nflows);
   double peak = 0.0;
   pthread_mutex_lock(&s->lock);
   while (!s->stop)
   {
      struct timespec ts;
      uint64_t now, units, dns;
      uint64_t deadline = msc_udp_now_ns() + s->interval_ns;
      ts.tv_sec = (time_t)(deadline / 1000000000ULL);
      ts.tv_nsec = (long)(deadline % 1000000000ULL);
      while (!s->stop)
         if (pthread_cond_timedwait(&s->wake, &s->lock, &ts) == ETIMEDOUT)
            break;
      now = msc_udp_now_ns();
      units = receiver_rx_units_total(s->flows, s->nflows);
      dns = now - last_ns;
      if (dns > 0 && units >= last_units)
      {
         double bps = (double)(units - last_units) * (double)s->payload_size * 8.0
                      * 1e9 / (double)dns;
         if (bps > peak) peak = bps;
      }
      last_ns = now;
      last_units = units;
   }
   pthread_mutex_unlock(&s->lock);
   s->peak_bps = peak;
   return NULL;
}

static int receiver_run_flows(struct msc_udp_xfer *x, const struct msc_udp_sockset *ss)
{
   struct receiver_flow *flows;
   pthread_t *threads;
   int created = 0, f, rc = 0;
   uint64_t i;

   /* mmap each destination so flows memcpy in parallel instead of serializing on
    * the per-inode pwritev lock (measured ~2.25x on a single big file). Only when
    * base == 0 (a byte-range write into a shared file keeps the pwrite path) AND
    * the dest is NOT Lustre: mmap writeback on Lustre can't issue large RPCs and is
    * ~7x slower than the coalesced pwritev path (1.6 vs 11 Gbit/s single-node),
    * so Lustre dests take pwritev. MSC_UDP_FORCE_MMAP overrides for the A/B.
    * The file is already sized (ftruncate) by the caller, so the mapping spans it. */
   for (i = 0; i < x->nfiles; i++)
   {
      x->files[i].map = NULL;
      if (x->allow_mmap && x->files[i].size > 0 &&
          x->files[i].base == 0 && !g_no_mmap &&
          (g_force_mmap || !msc_udp_fd_is_lustre(x->files[i].fd)))
      {
         /* Reserve storage before mmap: ftruncate alone can leave sparse pages
          * whose first write kills the process with SIGBUS on a full device.
          * Unsupported allocation falls back to pwritev, which reports errno. */
         void *m = MAP_FAILED;
         if (ftruncate(x->files[i].fd, (off_t)x->files[i].size) == 0)
         {
            if (fallocate(x->files[i].fd, 0, 0, (off_t)x->files[i].size) == 0)
               m = mmap(NULL, (size_t)x->files[i].size, PROT_READ | PROT_WRITE,
                        MAP_SHARED, x->files[i].fd, 0);
            else if (errno == ENOSPC || errno == EDQUOT || errno == EIO)
            {
               uint64_t j;
               fprintf(stderr, "MSC UDP receiver: cannot reserve destination storage: %s\n",
                       strerror(errno));
               for (j = 0; j < i; j++)
                  if (x->files[j].map != NULL)
                  {
                     munmap(x->files[j].map, (size_t)x->files[j].size);
                     x->files[j].map = NULL;
                  }
               return MSC_EXIT_DESTINATION;
            }
         }
         if (m != MAP_FAILED) x->files[i].map = m;   /* else fall back to pwritev */
      }
   }
   if (g_stats)
   {
      uint64_t mapped = 0;
      for (i = 0; i < x->nfiles; i++)
         if (x->files[i].map != NULL) mapped++;
      fprintf(stderr, "MSC UDP receiver write-path: mmap %llu pwritev %llu\n",
              (unsigned long long)mapped,
              (unsigned long long)(x->nfiles - mapped));
   }

   flows = receiver_flows_alloc(x, ss);
   threads = msc_udp_alloc(sizeof(pthread_t) * (size_t)x->flow_count, "threads");

   /* MSC_UDP_STATS: snapshot host-wide UDP receive drops and start the rx-rate
    * sampler so the whole verification set (throughput avg/peak, rcvbuf drops)
    * falls out of this one run. */
   struct rx_sampler samp;
   pthread_t samp_thread;
   int samp_on = 0;
   long rcvbuf_before = 0, inerr_before = 0;
   uint64_t rx_start_ns = 0;
   if (g_stats)
   {
      pthread_condattr_t attr;
      pthread_mutex_init(&samp.lock, NULL);
      pthread_condattr_init(&attr);
      pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
      pthread_cond_init(&samp.wake, &attr);
      pthread_condattr_destroy(&attr);
      rcvbuf_before = snmp_udp_stat("RcvbufErrors");
      inerr_before = snmp_udp_stat("InErrors");
      samp.flows = flows;
      samp.nflows = x->flow_count;
      samp.payload_size = x->payload_size;
      samp.interval_ns = 50ULL * 1000 * 1000;   /* 50 ms */
      samp.stop = 0;
      samp.peak_bps = 0.0;
      rx_start_ns = msc_udp_now_ns();
      if (pthread_create(&samp_thread, NULL, rx_sampler_thread, &samp) == 0)
         samp_on = 1;
   }

   for (f = 0; f < x->flow_count; f++)
   {
      if (pthread_create(&threads[f], NULL, receiver_flow_worker, &flows[f]) != 0) break;
      created++;
   }
   for (f = 0; f < created; f++) pthread_join(threads[f], NULL);
   if (created < x->flow_count) rc = -1;
   for (f = 0; f < x->flow_count; f++)
      if (flows[f].error)
         rc = flows[f].error_code == MSC_EXIT_DESTINATION
            ? MSC_EXIT_DESTINATION : (rc == 0 ? -1 : rc);

   if (samp_on)
   {
      uint64_t elapsed = msc_udp_now_ns() - rx_start_ns;
      double secs = (double)elapsed / 1e9;
      long rcvbuf_after = snmp_udp_stat("RcvbufErrors");
      long inerr_after = snmp_udp_stat("InErrors");
      pthread_mutex_lock(&samp.lock);
      samp.stop = 1;
      pthread_cond_signal(&samp.wake);
      pthread_mutex_unlock(&samp.lock);
      pthread_join(samp_thread, NULL);
      fprintf(stderr, "MSC UDP receiver rx: %llu bytes in %.0f ms, avg %.2f Gbit/s, "
              "peak %.2f Gbit/s, UdpRcvbufErrors +%ld, UdpInErrors +%ld\n",
              (unsigned long long)x->total_bytes, secs * 1000.0,
              secs > 0 ? (double)x->total_bytes * 8.0 / secs / 1e9 : 0.0,
              samp.peak_bps / 1e9,
              rcvbuf_before >= 0 && rcvbuf_after >= 0 ? rcvbuf_after - rcvbuf_before : -1,
              inerr_before >= 0 && inerr_after >= 0 ? inerr_after - inerr_before : -1);
   }
   if (g_stats)
   {
      pthread_cond_destroy(&samp.wake);
      pthread_mutex_destroy(&samp.lock);
   }
   free(threads);
   receiver_flows_free(flows, x->flow_count);

   /* unmap; dirty pages stay in the page cache (coherent with read()/checksum),
    * and a later fsync on the fd flushes them for durability */
   for (i = 0; i < x->nfiles; i++)
      if (x->files[i].map != NULL)
      {
         munmap(x->files[i].map, (size_t)x->files[i].size);
         x->files[i].map = NULL;
      }
   if (rc != 0) fprintf(stderr, "MSC UDP receiver: a flow failed\n");
   return rc;
}

/* Sender side: hold the receiver's advertised port list to what this end's env
 * asked for. The receiver owns the decision, but both ends see the same env, so
 * a list that does not match the request means the receiver never honored it --
 * an old binary, or one launched without the env. Failing here (loudly, before
 * any data) is the point: the alternative is a transfer that quietly opens N
 * unpredictable ports after the admin opened K specific ones. Returns 0 when
 * the list is acceptable. */
static int sender_check_ports(int flow_count, int count, const unsigned int *ports)
{
   int want = resolve_nports(flow_count);
   const char *skew = " -- it did not honor the request (old msc on that end, "
                      "or the MSC_UDP_PORT_* env did not reach it?)";
   int i;

   if (count != want)
   {
      fprintf(stderr, "MSC UDP sender: asked for %d data port%s but the receiver "
              "advertised %d%s\n", want, want == 1 ? "" : "s", count, skew);
      return -1;
   }
   /* Exact modes pin the slot as well as the set: flow f uses socket f % K, so
    * a permuted list would put flows on ports the user did not intend. */
   if (g_ports.mode == MSC_PORT_MODE_LIST || g_ports.mode == MSC_PORT_MODE_BLOCK)
   {
      for (i = 0; i < count; i++)
      {
         unsigned int expect = g_ports.mode == MSC_PORT_MODE_LIST
                                  ? g_ports.list[i] : g_ports.base + (unsigned)i;
         if (ports[i] != expect)
         {
            fprintf(stderr, "MSC UDP sender: asked for data port %u at slot %d but "
                    "the receiver advertised %u%s\n", expect, i, ports[i], skew);
            return -1;
         }
      }
      return 0;
   }
   /* Scan mode -- including the default window. Checking the default too is
    * what catches a receiver that fell back to kernel-assigned ports: the
    * transfer would work, but on ports nobody opened a firewall for. */
   for (i = 0; i < count; i++)
      if (!msc_port_spec_contains(&g_ports, ports[i]))
      {
         fprintf(stderr, "MSC UDP sender: asked for %d data port%s in %u-%u%s but "
                 "the receiver advertised %u%s\n", want, want == 1 ? "" : "s",
                 g_ports.base, g_ports.base + g_ports.span - 1,
                 g_ports.base_explicit ? "" : " (the default window)",
                 ports[i], skew);
         return -1;
      }
   return 0;
}

/* ===== public entry points =============================================== */
void *msc_udp_sender_open(struct msc_udp_config *cfg)
{
   struct msc_udp_sender *s;
   unsigned int *ports = NULL;
   int f;
   uint32_t tag, count;

   read_env();
   msc_udp_stats_init("sender");

   s = msc_udp_alloc(sizeof(*s), "sender session");
   memset(s, 0, sizeof(*s));
   s->controlfd = -1;
   s->flow_count = cfg->flow_count;
   s->next_transfer_id = random_transfer_id();

   /* tcp keeps the TCP control_channel; many/one swap in the UDP shim (SYN handshake
    * with wire-negotiated mode, loud bounded failure); the stdio modes ride
    * the SSH session the CLI already opened -- an AF_UNIX socketpair end, a
    * plain connected byte stream, so msc_udp_send_all/recvall use it directly with no
    * shim registration and no rendezvous at all. */
   if (msc_udp_ctl_is_stdio(g_ctl))
   {
      if (cfg->ctl_fd <= 0)
      {
         fprintf(stderr, "MSC UDP sender: MSC_UDP_CTL=%s needs the host:dest CLI "
                 "(control rides its ssh session; there is no port to dial)\n",
                 msc_udp_ctl_mode_name(g_ctl));
         free(s);
         return NULL;
      }
      s->controlfd = cfg->ctl_fd;
   }
   else
      s->controlfd = g_ctl == MSC_UDP_CTLMODE_TCP
         ? connect_tcp(cfg->receiver_host, cfg->control_port)
         : msc_udp_control_channel_connect(cfg->receiver_host, cfg->control_port);

   if (g_ctl == MSC_UDP_CTLMODE_ONE)
   {
      /* single-socket funnel: the control socket IS every data flow, so
       * there is no MSC_UDP_CTL_PORTS exchange (both ends gate on the SYN-
       * negotiated mode) and no per-flow connect. The demux spawns right
       * after the handshake and owns every read from here through teardown
       * -- HELLO and PMTUD below already ride its queues. */
      int *shared = msc_udp_alloc(sizeof(int), "shared fd");
      s->one_mode = 1;
      shared[0] = s->controlfd;
      if (g_ecn)
      {
         int tos = 0x02;   /* ECT(0): let the fabric mark CE instead of drop */
         setsockopt(s->controlfd, IPPROTO_IP, IP_TOS, &tos, sizeof(tos));
      }
      (void)resolve_nports(cfg->flow_count);   /* warns on MSC_UDP_PORTS>1 here */
      sockset_build(&s->ss, cfg->flow_count, 1, shared,
                    msc_udp_control_channel_lookup(s->controlfd), "sender");
   }
   else
   {
      /* Every other mode advertises its data ports over the control channel.
       * The receiver decides K, so the list length IS K -- accept anything from
       * a single funnelled socket up to one per flow. */
      int *shared;
      if (recv_u32(s->controlfd, &tag) != 0)
      {
         fprintf(stderr, "MSC UDP sender: control channel closed before port list\n");
         msc_udp_sender_close(s);
         return NULL;
      }
      if (tag == MSC_UDP_CTL_ERROR)
      {
         /* The receiver failed before it ever opened a session (e.g. the
          * destination already exists) and reported why instead of just
          * vanishing -- see msc_udp_send_transfer_error(). Surface its real
          * exit code instead of the generic network failure this used to
          * fall through to. */
         uint32_t status;
         if (recv_u32(s->controlfd, &status) == 0 && transfer_status_valid(status))
         {
            fprintf(stderr, "MSC UDP sender: receiver failed before opening the "
                    "session (exit code %u)\n", status);
            cfg->error_code = (int)status;
         }
         else
            fprintf(stderr, "MSC UDP sender: receiver failed before opening the "
                    "session (reason unknown)\n");
         msc_udp_sender_close(s);
         return NULL;
      }
      if (tag != MSC_UDP_CTL_PORTS ||
          recv_u32(s->controlfd, &count) != 0 ||
          (int)count < 1 || (int)count > cfg->flow_count)
      {
         fprintf(stderr, "MSC UDP sender: bad port list from receiver\n");
         msc_udp_sender_close(s);
         return NULL;
      }
      ports = msc_udp_alloc(sizeof(unsigned int) * (size_t)count, "ports");
      for (f = 0; f < (int)count; f++)
         if (recv_u32(s->controlfd, &ports[f]) != 0)
         {
            fprintf(stderr, "MSC UDP sender: short port list\n");
            free(ports);
            msc_udp_sender_close(s);
            return NULL;
         }
      /* The receiver is the one that acts on MSC_UDP_PORTS/MSC_UDP_PORT_BASE, but both
       * ends get the same env (msc forwards MSC_UDP_* to the receiver it launches,
       * as do the harnesses), so check that it actually honored the request.
       * Without this an old receiver -- or one launched without the env --
       * would silently fall back to N ports and blow the firewall contract the
       * user asked for. */
      if (sender_check_ports(cfg->flow_count, (int)count, ports) != 0)
      {
         free(ports);
         msc_udp_sender_close(s);
         return NULL;
      }
      report_data_ports("sender", cfg->receiver_host, ports, (int)count,
                        cfg->flow_count);
      shared = connect_udp_sender(cfg->receiver_host, (int)count, ports);
      free(ports);
      sockset_build(&s->ss, cfg->flow_count, (int)count, shared, NULL, "sender");
   }

   /* proto/feature negotiation: fail a mismatched pair loudly at session open */
   if (hello_sender(s->controlfd, s->next_transfer_id) != 0)
   {
      msc_udp_sender_close(s);
      return NULL;
   }
   /* PMTUD probe on flow 0 -- before any transfer, so a discovered payload sizes
    * every buffer. Only when both ends agreed in the HELLO. */
   if ((g_pmtud && (g_feat_agreed & MSC_UDP_FEAT_PMTUD)) ||
       (g_feat_agreed & MSC_UDP_FEAT_DATA_RTT))
      pmtud_probe_sender(s->ss.udp_fds[0], s->controlfd,
                         s->ss.receive_demux_of[0],
                         g_pmtud && (g_feat_agreed & MSC_UDP_FEAT_PMTUD));
   if (g_pmtud && (g_feat_agreed & MSC_UDP_FEAT_PMTUD) && g_payload_probe == 0)
   {
      fprintf(stderr, "MSC UDP sender: no usable payload confirmed by PMTUD\n");
      msc_udp_sender_close(s);
      return NULL;
   }
   return s;
}

int msc_udp_sender_fd(void *session)
{
   struct msc_udp_sender *s = session;
   return s == NULL ? -1 : s->controlfd;
}

int msc_udp_sender_send(void *session, struct msc_udp_config *cfg)
{
   struct msc_udp_sender *s = session;
   struct msc_udp_xfer x;
   struct msc_udp_file onefile;
   int srcfd = -1, rc = 0, error_code = 0;
   uint32_t tag;
   uint64_t filesize, ack_total, local_sum = 0, remote_sum = 0;
   uint64_t vis_t0 = 0;
   off_t base;
   struct stat st;

   if (s == NULL)
   {
      cfg->error_code = -1;
      return -1;
   }

   if (cfg->source_fd >= 0) srcfd = cfg->source_fd;
   else srcfd = open(cfg->source_path, O_RDONLY);
   if (srcfd < 0 || fstat(srcfd, &st) != 0)
   {
      fprintf(stderr, "MSC UDP sender: cannot open source %s: %s\n",
              cfg->source_path ? cfg->source_path : "(fd)", strerror(errno));
      cfg->error_code = -1;
      return -1;
   }
   base = cfg->src_offset;
   filesize = (uint64_t)st.st_size > (uint64_t)base ? (uint64_t)st.st_size - (uint64_t)base : 0;
   if (cfg->xferlen != 0 && cfg->xferlen < filesize) filesize = cfg->xferlen;

   /* query the source's Lustre layout so the receiver can match the destination's
    * stripe count and the transport can OST-affinity partition. 0/0 when not on
    * Lustre.
    *
    * In OST-partitioned multi-machine mode (my_ost_count > 0), each worker owns
    * a disjoint subset of the file's OSTs. We query the real stripe geometry so
    * the affine partition can route flow_workers to only the assigned OSTs. The worker
    * sees the WHOLE file but only touches its assigned stripes.
    *
    * In legacy byte-range multi mode (my_ost_count == 0), we leave layout 0/0
    * so each worker partitions its slice contiguously (backward compat). */
   cfg->lustre_stripe_size = 0;
   cfg->lustre_stripe_count = 0;
   if (!cfg->multi || cfg->my_ost_count > 0)
   {
      if (cfg->source_path != NULL)
         msc_udp_query_stripe(cfg->source_path, &cfg->lustre_stripe_size,
                          &cfg->lustre_stripe_count);
      if (g_stripe_count > 0)   /* test/benchmark override of the queried layout */
      {
         cfg->lustre_stripe_size = g_stripe_size;
         cfg->lustre_stripe_count = g_stripe_count;
      }
   }

   /* OST-partitioned multi: the worker sees the whole file; stripe filtering
    * happens inside make_part via the restricted OST range. Override any
    * offset/length the orchestrator may have passed. */
   if (cfg->multi && cfg->my_ost_count > 0)
   {
      base = 0;
      filesize = (uint64_t)st.st_size;
   }

   /* one-entry file table: a single file is just a tree of one */
   onefile.fd = srcfd;
   onefile.base = base;
   onefile.size = filesize;
   x.flow_count = cfg->flow_count;
   x.payload_size = pick_payload(cfg->payload_size);
   x.transfer_id = s->next_transfer_id++;
   x.files = &onefile;
   x.nfiles = 1;
   x.allow_mmap = 0;
   x.stripe_size = cfg->lustre_stripe_size;
   x.stripe_count = cfg->lustre_stripe_count;
   x.my_ost_start = cfg->my_ost_count > 0 ? cfg->my_ost_start : -1;
   x.my_ost_count = cfg->my_ost_count;
   x.cancel_flag = cfg->cancel_flag;
   atomic_init(&x.flows_retired, 0);   /* stack xfer: field-initialized, not zeroed */
   xfer_index(&x);   /* unit_base/nunits + total_units/total_bytes (stripe-aware) */

   /* greeting: announce session parameters the receiver cannot know on its own,
    * including the source's Lustre stripe layout (size then count) and, since
    * protocol v5, the destination stripe count the user asked for. The last is
    * carried separately precisely so it cannot disturb the source geometry above,
    * which both ends map units with; it travels as a two's-complement u32 so -1
    * ("every OST") needs no further protocol revision. */
   if (send_u32(s->controlfd, MSC_UDP_CTL_GREETING) != 0 ||
       send_u32(s->controlfd, MSC_UDP_PROTO_VERSION) != 0 ||
       send_u32(s->controlfd, (uint32_t)x.flow_count) != 0 ||
       send_u32(s->controlfd, (uint32_t)x.payload_size) != 0 ||
       send_u64(s->controlfd, x.total_bytes) != 0 ||
       send_u64(s->controlfd, x.transfer_id) != 0 ||
       send_u64(s->controlfd, x.stripe_size) != 0 ||
       send_u32(s->controlfd, x.stripe_count) != 0 ||
       send_u32(s->controlfd, (uint32_t)(int32_t)cfg->dest_stripe_count) != 0)
   {
      fprintf(stderr, "MSC UDP sender: failed sending greeting\n");
      rc = -1;
      goto out;
   }
   /* Under the UDP control modes the greeting is the last control_channel traffic
    * before sender_run_flows() hands off to the raw-UDP flow_worker threads,
    * which never touch this shim again until the tail DONE ack -- possibly
    * seconds to minutes later. Confirm every greeting byte actually reached
    * the receiver now, or a lost tail byte would never get retransmitted and
    * the receiver would block forever reading it. No-op in TCP mode. */
   if (msc_udp_control_channel_drain(msc_udp_control_channel_lookup(s->controlfd)) != 0)
   {
      fprintf(stderr, "MSC UDP sender: greeting was not confirmed by the receiver\n");
      rc = -1;
      goto out;
   }

   if (msc_udp_stats_on)
   {
      vis_t0 = msc_udp_now_ns();
      msc_udp_stats_emit("session_start",
                      ",\"xfer_id\":%llu,\"bytes\":%llu,\"units\":%llu,"
                      "\"flows\":%d,\"payload\":%zu,\"stripe_size\":%llu,"
                      "\"stripe_count\":%u,\"dest_stripe_count\":%ld",
                      (unsigned long long)x.transfer_id,
                      (unsigned long long)x.total_bytes,
                      (unsigned long long)x.total_units,
                      x.flow_count, x.payload_size,
                      (unsigned long long)x.stripe_size, x.stripe_count,
                      cfg->dest_stripe_count);
   }

   rc = sender_run_flows(&x, &s->ss);
   if (rc != 0)
   {
      error_code = receive_transfer_error_if_ready(s->controlfd);
      if (error_code > 0)
         fprintf(stderr, "MSC UDP sender: receiver reported exit %d\n",
                 error_code);
      goto out;
   }

   /* transfer complete: the receiver confirms the whole byte count over the control_channel */
   if (recv_u32(s->controlfd, &tag) != 0)
   {
      fprintf(stderr, "MSC UDP sender: receiver did not acknowledge transfer\n");
      rc = -1;
      goto out;
   }
   if (tag == MSC_UDP_CTL_ERROR)
   {
      uint32_t status;
      if (recv_u32(s->controlfd, &status) == 0 &&
          transfer_status_valid(status))
         error_code = (int)status;
      fprintf(stderr, "MSC UDP sender: receiver reported exit %d\n",
              error_code > 0 ? error_code : MSC_EXIT_NETWORK);
      rc = -1;
      goto out;
   }
   if (tag != MSC_UDP_CTL_DONE ||
       recv_u64(s->controlfd, &ack_total) != 0 ||
       ack_total != x.total_bytes)
   {
      fprintf(stderr,
              "MSC UDP sender: receiver sent an invalid completion record\n");
      rc = -1;
      goto out;
   }
   if (cfg->verify_checksum)
   {
      if (recv_u64(s->controlfd, &remote_sum) != 0)
      {
         fprintf(stderr, "MSC UDP sender: receiver did not send checksum\n");
         rc = -1;
         goto out;
      }
      if (msc_udp_checksum_fd(srcfd, base, x.total_bytes, &local_sum) != 0)
      {
         fprintf(stderr, "MSC UDP sender: failed checksumming source\n");
         send_u32(s->controlfd, MSC_UDP_CTL_ABORT);
         rc = -1;
         goto out;
      }
      if (local_sum != remote_sum)
      {
         fprintf(stderr, "MSC UDP sender: checksum mismatch local=%016llx remote=%016llx\n",
                 (unsigned long long)local_sum, (unsigned long long)remote_sum);
         send_u32(s->controlfd, MSC_UDP_CTL_ABORT);
         rc = -1;
         goto out;
      }
      if (send_u32(s->controlfd, MSC_UDP_CTL_PUBLISH) != 0)
      {
         fprintf(stderr, "MSC UDP sender: failed sending publish ack\n");
         rc = -1;
         goto out;
      }
      cfg->checksum = local_sum;
   }
   cfg->bytes_done = x.total_bytes;

out:
   if (msc_udp_stats_on && vis_t0 != 0)
   {
      double secs = (double)(msc_udp_now_ns() - vis_t0) / 1e9;
      msc_udp_stats_emit("session_done",
                      ",\"xfer_id\":%llu,\"ok\":%d,\"bytes\":%llu,"
                      "\"checksum\":\"%016llx\",\"secs\":%.3f,\"gbit\":%.3f",
                      (unsigned long long)x.transfer_id, rc == 0,
                      (unsigned long long)(rc == 0 ? x.total_bytes : 0),
                      (unsigned long long)local_sum, secs,
                      rc == 0 && secs > 0
                         ? (double)x.total_bytes * 8.0 / 1e9 / secs : 0.0);
   }
   if (rc != 0 && s->controlfd >= 0)
      send_u32(s->controlfd, MSC_UDP_CTL_ABORT);
   if (cfg->source_fd < 0 && srcfd >= 0) close(srcfd);
   cfg->error_code = rc == 0 ? 0 : (error_code > 0 ? error_code : -1);
   return rc;
}

static int table_specs_to_files(const struct msc_udp_tree_xfer *t,
                                struct msc_udp_file *files,
                                const char *role)
{
   struct stat st;
   uint64_t i, total = 0;
   if (t->nfiles > SIZE_MAX / sizeof(*files))
      return -1;
   for (i = 0; i < t->nfiles; i++)
   {
      uint64_t end;
      if (t->files == NULL || t->files[i].fd < 0 ||
          t->files[i].size == 0 ||
          t->files[i].base > (uint64_t)INT64_MAX ||
          t->files[i].size - 1U >
             (uint64_t)INT64_MAX - t->files[i].base ||
          UINT64_MAX - total < t->files[i].size ||
          fstat(t->files[i].fd, &st) != 0 || !S_ISREG(st.st_mode))
      {
         fprintf(stderr, "MSC UDP %s: invalid table file slice\n", role);
         return -1;
      }
      end = t->files[i].base + t->files[i].size;
      if (st.st_size < 0 || end > (uint64_t)st.st_size)
      {
         fprintf(stderr,
                 "MSC UDP %s: table slice exceeds its backing file\n", role);
         return -1;
      }
      total += t->files[i].size;
      if (total > (uint64_t)INT64_MAX)
      {
         fprintf(stderr, "MSC UDP %s: table byte total is out of range\n",
                 role);
         return -1;
      }
      files[i].fd = t->files[i].fd;
      files[i].base = (off_t)t->files[i].base;
      files[i].size = t->files[i].size;
   }
   return 0;
}

/* multi-file (recursive) send: the file table + manifest are already agreed by
 * both ends over the control_channel, so there is no per-file greeting. The whole tree
 * is one global unit stream; the only control_channel traffic is the single tail ack. */
int msc_udp_sender_send_table(void *session, struct msc_udp_tree_xfer *t)
{
   struct msc_udp_sender *s = session;
   struct msc_udp_xfer x;
   struct msc_udp_file *files;
   uint64_t ack_total;
   uint32_t tag;
   int rc = 0, error_code = 0;

   if (t != NULL) t->error_code = -1;
   if (s == NULL || t == NULL || t->transfer_id == 0 ||
       t->transfer_id != s->next_transfer_id ||
       t->nfiles > SIZE_MAX / sizeof(*files))
   {
      fprintf(stderr, "MSC UDP sender: invalid or repeated table transfer id\n");
      return -1;
   }
   files = msc_udp_alloc(sizeof(*files) * (t->nfiles ? t->nfiles : 1), "sender files");
   if (table_specs_to_files(t, files, "sender") != 0)
   {
      free(files);
      return -1;
   }
   x.flow_count = t->flow_count;
   x.payload_size = pick_payload(t->payload_size);
   x.transfer_id = t->transfer_id;
   x.files = files;
   x.nfiles = t->nfiles;
   x.allow_mmap = 0;
   x.stripe_size = 0;     /* a tree spans many files: no single layout to affine by */
   x.stripe_count = 0;    /* -> contiguous unit count + partition */
   x.cancel_flag = t->cancel_flag;
   atomic_init(&x.flows_retired, 0);   /* stack xfer: field-initialized, not zeroed */
   xfer_index(&x);
   t->total_bytes = x.total_bytes;

   /* msc.c already streamed the whole manifest (TREE_DIR/TREE_FILE records,
    * then TREE_DONE) over this control_channel before calling here, and
    * TABLE_BEGIN below is the last control traffic before sender_run_flows()
    * hands off to the raw-UDP flow_worker threads for potentially a very long
    * stretch -- after which nothing pumps this shim. So SEND TABLE_BEGIN FIRST,
    * THEN drain: the drain confirms the receiver acked the manifest AND
    * TABLE_BEGIN (go-back-N, so acking TABLE_BEGIN implies the whole manifest
    * arrived), which is the only thing that keeps a lost TABLE_BEGIN datagram
    * getting retransmitted instead of stranding the receiver's blocking recv
    * forever. Draining only the manifest (leaving TABLE_BEGIN un-drained before
    * going silent) was the recursive+UDP-control hang. No-op in TCP mode. */
   /* Tests only: force the TABLE_BEGIN record to be lost on first transmission,
    * so only the post-send drain's retransmit can deliver it. Fixed code
    * recovers; the un-drained regression wedges the receiver's recv. */
   {
      const char *drop = getenv("MSC_TEST_DROP_TABLE_BEGIN");
      if (drop != NULL)
         msc_udp_control_channel_test_drop_next(
            msc_udp_control_channel_lookup(s->controlfd),
            (uint32_t)strtoul(drop, NULL, 10));
   }
   if (send_u32(s->controlfd, MSC_UDP_CTL_TABLE_BEGIN) != 0 ||
       send_u64(s->controlfd, x.transfer_id) != 0 ||
       send_u64(s->controlfd, x.nfiles) != 0 ||
       send_u64(s->controlfd, x.total_bytes) != 0)
   {
      fprintf(stderr, "MSC UDP sender: could not synchronize table transfer\n");
      free(files);
      return -1;
   }
   if (msc_udp_control_channel_drain(msc_udp_control_channel_lookup(s->controlfd)) != 0)
   {
      fprintf(stderr, "MSC UDP sender: table transfer was not confirmed by the receiver\n");
      free(files);
      return -1;
   }
   s->next_transfer_id++;

   if (x.total_units > 0)
      rc = sender_run_flows(&x, &s->ss);
   if (rc != 0)
   {
      error_code = receive_transfer_error_if_ready(s->controlfd);
      if (error_code > 0)
         fprintf(stderr, "MSC UDP sender: recursive receiver reported exit %d\n",
                 error_code);
      send_u32(s->controlfd, MSC_UDP_CTL_ABORT);
      free(files);
      t->error_code = error_code > 0 ? error_code : -1;
      return -1;
   }

   /* one tail barrier for the whole tree: byte-count accounting, no checksum */
   if (recv_u32(s->controlfd, &tag) != 0)
   {
      fprintf(stderr, "MSC UDP sender: receiver did not acknowledge tree transfer\n");
      free(files);
      return -1;
   }
   if (tag == MSC_UDP_CTL_ERROR)
   {
      uint32_t status;
      if (recv_u32(s->controlfd, &status) == 0 &&
          transfer_status_valid(status))
         t->error_code = (int)status;
      fprintf(stderr, "MSC UDP sender: recursive receiver reported exit %d\n",
              t->error_code > 0 ? t->error_code : MSC_EXIT_NETWORK);
      free(files);
      return -1;
   }
   if (tag != MSC_UDP_CTL_DONE ||
       recv_u64(s->controlfd, &ack_total) != 0 ||
       ack_total != x.total_bytes)
   {
      fprintf(stderr,
              "MSC UDP sender: receiver sent an invalid tree completion record\n");
      free(files);
      return -1;
   }
   free(files);
   t->error_code = 0;
   return 0;
}

void msc_udp_sender_close(void *session)
{
   struct msc_udp_sender *s = session;
   int f;
   if (s == NULL) return;
   /* two-phase shutdown. Phase 1: the control channel FIN/ACK exchange runs while the
    * demux (one mode) is still alive feeding it -- joining the demux first
    * would strand the peer's ACKs and defeat FIN reliability. Phase 2: join
    * the demuxes, then close each socket exactly once. Only sock_fds may be
    * closed -- udp_fds[] is an alias array and in `one` mode sock_fds[0] is
    * controlfd, closed at the end. msc_udp_control_channel_close never closes the fd. */
   if (s->controlfd >= 0)
      msc_udp_control_channel_close(msc_udp_control_channel_lookup(s->controlfd));
   sockset_teardown(&s->ss);
   if (s->ss.sock_fds != NULL)
   {
      for (f = 0; f < s->ss.nports; f++)
         if (s->ss.sock_fds[f] != s->controlfd)
            close(s->ss.sock_fds[f]);
      free(s->ss.sock_fds);
   }
   if (s->controlfd >= 0) close(s->controlfd);
   free(s);
}

void *msc_udp_receiver_open(struct msc_udp_config *cfg, int controlfd)
{
   struct msc_udp_receiver *s;
   unsigned int *ports = NULL;
   int f;

   read_env();
   msc_udp_stats_init("receiver");

   s = msc_udp_alloc(sizeof(*s), "receiver session");
   memset(s, 0, sizeof(*s));
   s->controlfd = controlfd;
   s->flow_count = cfg->flow_count;
   s->next_transfer_id = 0;   /* announced by the sender in HELLO */
   if (g_ctl == MSC_UDP_CTLMODE_TCP)
      set_nodelay(controlfd);   /* mirror the sender; meaningless for UDP */

   if (g_ctl == MSC_UDP_CTLMODE_ONE)
   {
      /* single-socket funnel: no per-flow ports exist, so the MSC_UDP_CTL_PORTS
       * exchange is skipped on both ends (gated on the SYN-negotiated mode).
       * Per-packet metadata options go on BEFORE the demux takes over
       * reading so no packet misses its TOS/GRO cmsg. */
      int *shared = msc_udp_alloc(sizeof(int), "shared fd");
      s->one_mode = 1;
      shared[0] = controlfd;
      if (g_gro)
      {
         int one = 1;
         setsockopt(controlfd, SOL_UDP, UDP_GRO, &one, sizeof(one)); /* best-effort */
      }
      if (g_ecn)
      {
         int one = 1;   /* deliver each datagram's TOS byte so CE marks count */
         setsockopt(controlfd, IPPROTO_IP, IP_RECVTOS, &one, sizeof(one));
      }
      (void)resolve_nports(cfg->flow_count);   /* warns on MSC_UDP_PORTS>1 here */
      sockset_build(&s->ss, cfg->flow_count, 1, shared,
                    msc_udp_control_channel_lookup(controlfd), "receiver");
   }
   else
   {
      /* Every other mode binds ordinary data sockets and advertises them as a
       * PORTS list. K and the ports themselves come from the resolved port
       * spec (default: min(flows, 8) ports scanned from MSC_UDP_DEFAULT_PORT).
       * bind_udp_receiver already applies GRO/RECVTOS before any demux takes
       * over reading, so no packet misses its cmsg; a demux over a shared
       * socket runs with no control-queue consumer and learns the sender
       * address from the first packet (acknowledgments sendto it, as in `one`). */
      int nports = resolve_nports(cfg->flow_count);
      int *shared;
      if (cfg->actual_control_port != 0 &&
          msc_port_spec_contains(&g_ports, cfg->actual_control_port))
         fprintf(stderr, "MSC UDP receiver: warning: the data-port window "
                 "contains the control port %u\n", cfg->actual_control_port);
      ports = msc_udp_alloc(sizeof(unsigned int) * (size_t)nports, "ports");
      shared = bind_udp_receiver(&g_ports, nports, ports);
      report_data_ports("receiver", NULL, ports, nports, cfg->flow_count);
      if (send_u32(controlfd, MSC_UDP_CTL_PORTS) != 0 ||
          send_u32(controlfd, (uint32_t)nports) != 0)
      {
         fprintf(stderr, "MSC UDP receiver: failed sending port list\n");
         free(ports);
         free(shared);
         msc_udp_receiver_close(s);
         return NULL;
      }
      for (f = 0; f < nports; f++)
         if (send_u32(controlfd, ports[f]) != 0)
         {
            fprintf(stderr, "MSC UDP receiver: failed sending port list\n");
            free(ports);
            free(shared);
            msc_udp_receiver_close(s);
            return NULL;
         }
      free(ports);   /* the ports themselves are reported by report_data_ports */
      sockset_build(&s->ss, cfg->flow_count, nports, shared, NULL, "receiver");
   }

   /* proto/feature negotiation: mirror of the sender's session-open sequence */
   if (hello_receiver(controlfd, &s->next_transfer_id) != 0)
   {
      msc_udp_receiver_close(s);
      return NULL;
   }
   /* PMTUD probe exchange, mirroring the sender (only when both ends agreed) */
   if ((g_pmtud && (g_feat_agreed & MSC_UDP_FEAT_PMTUD)) ||
       (g_feat_agreed & MSC_UDP_FEAT_DATA_RTT))
      pmtud_probe_receiver(s->ss.udp_fds[0], controlfd,
                           s->ss.receive_demux_of[0],
                           g_pmtud && (g_feat_agreed & MSC_UDP_FEAT_PMTUD));
   if (g_pmtud && (g_feat_agreed & MSC_UDP_FEAT_PMTUD) && g_payload_probe == 0)
   {
      fprintf(stderr, "MSC UDP receiver: no usable payload confirmed by PMTUD\n");
      msc_udp_receiver_close(s);
      return NULL;
   }
   return s;
}

int msc_udp_receiver_fd(void *session)
{
   struct msc_udp_receiver *s = session;
   return s == NULL ? -1 : s->controlfd;
}

uint64_t msc_udp_sender_next_transfer_id(void *session)
{
   struct msc_udp_sender *s = session;
   return s == NULL ? 0 : s->next_transfer_id;
}

uint64_t msc_udp_receiver_next_transfer_id(void *session)
{
   struct msc_udp_receiver *s = session;
   return s == NULL ? 0 : s->next_transfer_id;
}

int msc_udp_receiver_recv(void *session, struct msc_udp_config *cfg)
{
   struct msc_udp_receiver *s = session;
   struct msc_udp_xfer x;
   struct msc_udp_file onefile;
   int destfd = -1, rc = 0, error_code = 0;
   uint32_t tag, version, nflip, payload, stripe_count = 0, dest_stripes = 0;
   long dest_stripe_count;
   uint64_t filesize, transfer_id, stripe_size = 0, sum = 0;
   uint64_t vis_t0 = 0, vis_xfer_id = 0, vis_bytes = 0;

   if (s == NULL)
   {
      cfg->error_code = -1;
      return -1;
   }

   /* greeting: adopt the sender's payload size, learn the file size, and pick up
    * the source's Lustre stripe layout (size then count) to match it here */
   if (recv_u32(s->controlfd, &tag) != 0 || tag != MSC_UDP_CTL_GREETING ||
       recv_u32(s->controlfd, &version) != 0 || version != MSC_UDP_PROTO_VERSION ||
       recv_u32(s->controlfd, &nflip) != 0 || (int)nflip != cfg->flow_count ||
       recv_u32(s->controlfd, &payload) != 0 || payload == 0 ||
       payload > MSC_UDP_MAX_DATAGRAM - sizeof(struct msc_udp_packet_header) ||
       recv_u64(s->controlfd, &filesize) != 0 ||
       recv_u64(s->controlfd, &transfer_id) != 0 ||
       recv_u64(s->controlfd, &stripe_size) != 0 ||
       recv_u32(s->controlfd, &stripe_count) != 0 ||
       recv_u32(s->controlfd, &dest_stripes) != 0)
   {
      fprintf(stderr, "MSC UDP receiver: bad greeting from sender\n");
      rc = -1;
      goto out;
   }
   /* The greeting is authoritative when it carries a request: the sender resolved
    * the flag and the forwarded MSC_DEST_STRIPE_COUNT to one value before
    * launching this receiver, so cfg->dest_stripe_count is only the fallback for
    * setup paths that ran before a greeting existed. */
   dest_stripe_count = (long)(int32_t)dest_stripes;
   if (dest_stripe_count == 0)
      dest_stripe_count = cfg->dest_stripe_count;

   /* A preset fd cannot be relaid out -- a Lustre layout is fixed at creation --
    * so those paths apply and verify the request where the file is still being
    * created: findzero_process() for a shared multimachine destination, and the
    * resume hello for a partial file (udp_transport.c). */
   if (cfg->dest_fd >= 0) destfd = cfg->dest_fd;
   else destfd = msc_udp_open_dest(cfg->dest_path, stripe_size, stripe_count,
                                   dest_stripe_count, cfg->force);
   if (destfd < 0)
   {
      /* An explicit destination layout that could not be applied is reported as
       * such: the transfer would otherwise land on a default-striped file the
       * caller believes is striped as requested. */
      if (dest_stripe_count != 0)
         fprintf(stderr, "MSC UDP receiver: cannot create dest %s with stripe "
                         "count %ld: %s\n",
                 cfg->dest_path ? cfg->dest_path : "(fd)", dest_stripe_count,
                 strerror(errno));
      else
         fprintf(stderr, "MSC UDP receiver: cannot open dest %s: %s\n",
                 cfg->dest_path ? cfg->dest_path : "(fd)", strerror(errno));
      error_code = MSC_EXIT_DESTINATION;
      rc = -1;
      goto out;
   }

   onefile.fd = destfd;
   if (cfg->multi && cfg->my_ost_count > 0)
      onefile.base = 0;  /* write the whole file; stripe filter does the rest */
   else
      onefile.base = cfg->dst_offset;
   onefile.size = filesize;
   x.flow_count = cfg->flow_count;
   x.payload_size = payload;
   x.transfer_id = transfer_id;
   x.files = &onefile;
   x.nfiles = 1;
   /* Only a receiver that owns the whole file may size or map it.  In multi
    * mode the destination is shared, and ftruncate()ing it to this worker's
    * slice would cut off bytes another worker has already written. */
   x.allow_mmap = !cfg->multi;
   x.stripe_size = stripe_size;
   x.stripe_count = stripe_count;
   x.my_ost_start = cfg->my_ost_count > 0 ? cfg->my_ost_start : -1;
   x.my_ost_count = cfg->my_ost_count;
   x.cancel_flag = cfg->cancel_flag;
   atomic_init(&x.flows_retired, 0);   /* stack xfer: field-initialized, not zeroed */
   xfer_index(&x);

   if (msc_udp_stats_on)
   {
      vis_t0 = msc_udp_now_ns();
      vis_xfer_id = x.transfer_id;
      vis_bytes = x.total_bytes;
      msc_udp_stats_emit("session_start",
                      ",\"xfer_id\":%llu,\"bytes\":%llu,\"units\":%llu,"
                      "\"flows\":%d,\"payload\":%zu,\"stripe_size\":%llu,"
                      "\"stripe_count\":%u,\"dest_stripe_count\":%ld",
                      (unsigned long long)x.transfer_id,
                      (unsigned long long)x.total_bytes,
                      (unsigned long long)x.total_units,
                      x.flow_count, x.payload_size,
                      (unsigned long long)stripe_size, stripe_count,
                      dest_stripe_count);
   }

   rc = receiver_run_flows(&x, &s->ss);
   if (rc != 0)
   {
      if (rc == MSC_EXIT_DESTINATION)
         error_code = MSC_EXIT_DESTINATION;
      rc = -1;
      goto out;
   }

   if (fsync(destfd) != 0 && errno != EINVAL)   /* EINVAL: e.g. a pipe dest */
   {
      fprintf(stderr, "MSC UDP receiver: fsync failed: %s\n", strerror(errno));
      error_code = MSC_EXIT_DESTINATION;
      rc = -1;
      goto out;
   }

   /* transfer complete: confirm the whole byte count over the reliable control_channel */
   if (send_u32(s->controlfd, MSC_UDP_CTL_DONE) != 0 || send_u64(s->controlfd, x.total_bytes) != 0)
   { fprintf(stderr, "MSC UDP receiver: failed sending final ack\n"); rc = -1; goto out; }
   if (cfg->verify_checksum)
   {
      if (msc_udp_checksum_fd(destfd, cfg->dst_offset, x.total_bytes, &sum) != 0)
      {
         fprintf(stderr, "MSC UDP receiver: failed checksumming destination\n");
         rc = -1;
         goto out;
      }
      if (send_u64(s->controlfd, sum) != 0)
      { fprintf(stderr, "MSC UDP receiver: failed sending checksum\n"); rc = -1; goto out; }
      if (recv_u32(s->controlfd, &tag) != 0 || tag != MSC_UDP_CTL_PUBLISH)
      {
         fprintf(stderr, "MSC UDP receiver: sender did not approve publish\n");
         rc = -1;
         goto out;
      }
      cfg->checksum = sum;
   }
   if (cfg->final_dest_path != NULL && cfg->dest_path != NULL)
   {
      if (cfg->force)
      {
         if (rename(cfg->dest_path, cfg->final_dest_path) != 0)
         {
            fprintf(stderr, "MSC UDP receiver: publish rename failed: %s\n", strerror(errno));
            error_code = MSC_EXIT_DESTINATION;
            rc = -1;
            goto out;
         }
      }
      else if (link(cfg->dest_path, cfg->final_dest_path) != 0 ||
               unlink(cfg->dest_path) != 0)
      {
         fprintf(stderr, "MSC UDP receiver: publish link failed: %s\n", strerror(errno));
         error_code = MSC_EXIT_DESTINATION;
         rc = -1;
         goto out;
      }
      if (msc_udp_fsync_parent(cfg->final_dest_path) != 0)
      {
         fprintf(stderr, "MSC UDP receiver: syncing published directory failed: %s\n",
                 strerror(errno));
         error_code = MSC_EXIT_DESTINATION;
         rc = -1;
         goto out;
      }
   }
   cfg->bytes_done = x.total_bytes;

out:
   if (rc != 0 && error_code == MSC_EXIT_DESTINATION &&
       s != NULL && s->controlfd >= 0)
      (void)msc_udp_send_transfer_error(s->controlfd, error_code);
   if (msc_udp_stats_on && vis_t0 != 0)
   {
      double secs = (double)(msc_udp_now_ns() - vis_t0) / 1e9;
      msc_udp_stats_emit("session_done",
                      ",\"xfer_id\":%llu,\"ok\":%d,\"bytes\":%llu,"
                      "\"checksum\":\"%016llx\",\"secs\":%.3f,\"gbit\":%.3f",
                      (unsigned long long)vis_xfer_id, rc == 0,
                      (unsigned long long)(rc == 0 ? vis_bytes : 0),
                      (unsigned long long)sum, secs,
                      rc == 0 && secs > 0
                         ? (double)vis_bytes * 8.0 / 1e9 / secs : 0.0);
   }
   if (cfg->dest_fd < 0 && destfd >= 0) close(destfd);
   cfg->error_code = rc == 0 ? 0 : (error_code > 0 ? error_code : -1);
   return rc;
}

/* multi-file (recursive) receive: the file table is already built from the
 * manifest the caller read off the control_channel (every fd open, every dir created).
 * Run the whole tree as one global unit stream, flush every file, then send the
 * single tail ack -- byte-count accounting, no per-file checksum. */
int msc_udp_receiver_recv_table(void *session, struct msc_udp_tree_xfer *t)
{
   struct msc_udp_receiver *s = session;
   struct msc_udp_xfer x;
   struct msc_udp_file *files;
   uint64_t transfer_id, nfiles, total_bytes;
   uint32_t tag;
   int rc = 0, error_code = 0;

   if (t != NULL) t->error_code = -1;
   if (s == NULL || t == NULL || t->transfer_id == 0 ||
       t->transfer_id != s->next_transfer_id ||
       t->nfiles > SIZE_MAX / sizeof(*files))
   {
      fprintf(stderr, "MSC UDP receiver: invalid or repeated table transfer id\n");
      return -1;
   }
   files = msc_udp_alloc(sizeof(*files) * (t->nfiles ? t->nfiles : 1), "receiver files");
   if (table_specs_to_files(t, files, "receiver") != 0)
   {
      free(files);
      return -1;
   }
   x.flow_count = t->flow_count;
   x.payload_size = pick_payload(t->payload_size);
   x.transfer_id = t->transfer_id;
   x.files = files;
   x.nfiles = t->nfiles;
   x.allow_mmap = 0;
   x.stripe_size = 0;     /* tree: contiguous count + partition (matches the sender) */
   x.stripe_count = 0;
   x.cancel_flag = t->cancel_flag;
   atomic_init(&x.flows_retired, 0);   /* stack xfer: field-initialized, not zeroed */
   xfer_index(&x);
   t->total_bytes = x.total_bytes;
   /* Bounded: the sender has gone silent on the shim between TREE_READY and the
    * data flows, so a lost TABLE_BEGIN (or a vanished sender) must trip the
    * idle watchdog and fail with exit 6, never orphan this receiver forever. */
   if (recv_u32_stall_bounded(s->controlfd, &tag) != 0 ||
       recv_u64_stall_bounded(s->controlfd, &transfer_id) != 0 ||
       recv_u64_stall_bounded(s->controlfd, &nfiles) != 0 ||
       recv_u64_stall_bounded(s->controlfd, &total_bytes) != 0 ||
       tag != MSC_UDP_CTL_TABLE_BEGIN || transfer_id != x.transfer_id ||
       nfiles != x.nfiles || total_bytes != x.total_bytes)
   {
      fprintf(stderr, "MSC UDP receiver: table transfer synchronization failed\n");
      free(files);
      return -1;
   }
   s->next_transfer_id++;

   if (x.total_units > 0)
      rc = receiver_run_flows(&x, &s->ss);
   if (rc != 0)
   {
      if (rc == MSC_EXIT_DESTINATION)
         error_code = MSC_EXIT_DESTINATION;
      goto out;
   }

   /* durability: flush every destination file before acking the tree, in
    * parallel so the journal coalesces commits instead of one fsync at a time */
   if (fsync_table(files, t->nfiles) != 0)
   {
      error_code = MSC_EXIT_DESTINATION;
      rc = -1;
      goto out;
   }

   if (send_u32(s->controlfd, MSC_UDP_CTL_DONE) != 0 ||
       send_u64(s->controlfd, x.total_bytes) != 0)
   {
      fprintf(stderr, "MSC UDP receiver: failed sending tree ack\n");
      free(files);
      return -1;
   }
   t->error_code = 0;
   free(files);
   return 0;

out:
   if (error_code == MSC_EXIT_DESTINATION)
      (void)msc_udp_send_transfer_error(s->controlfd, error_code);
   t->error_code = error_code > 0 ? error_code : -1;
   free(files);
   return -1;
}

void msc_udp_receiver_close(void *session)
{
   struct msc_udp_receiver *s = session;
   int f;
   if (s == NULL) return;
   /* two-phase shutdown, mirroring the sender: FIN/ACK over the still-live
    * demux, then join it. The control fd itself belongs to the CALLER (msc.c
    * accepted it) and is closed there exactly once -- in one mode it is also
    * the data socket, so sock_fds[0] must be left alone here. */
   msc_udp_control_channel_close(msc_udp_control_channel_lookup(s->controlfd));
   sockset_teardown(&s->ss);
   if (s->ss.sock_fds != NULL)
   {
      for (f = 0; f < s->ss.nports; f++)
         if (s->ss.sock_fds[f] != s->controlfd)
            close(s->ss.sock_fds[f]);
      free(s->ss.sock_fds);
   }
   free(s);
}

int msc_udp_sender_run(struct msc_udp_config *cfg)
{
   void *session = msc_udp_sender_open(cfg);
   int rc;
   if (session == NULL)
   {
      /* msc_udp_sender_open() already set cfg->error_code when the receiver
       * reported a real reason (MSC_UDP_CTL_ERROR); don't clobber it. */
      if (!transfer_status_valid((uint32_t)cfg->error_code))
         cfg->error_code = -1;
      return -1;
   }
   rc = msc_udp_sender_send(session, cfg);
   msc_udp_sender_close(session);
   return rc;
}

int msc_udp_receiver_run(struct msc_udp_config *cfg, int controlfd)
{
   void *session = msc_udp_receiver_open(cfg, controlfd);
   int rc;
   if (session == NULL)
   {
      cfg->error_code = -1;
      return -1;
   }
   rc = msc_udp_receiver_recv(session, cfg);
   msc_udp_receiver_close(session);
   return rc;
}
