/*
 * wan_shim.c - LD_PRELOAD in-process WAN emulator for MSC's UDP data path.
 *
 * Kernel netem needs root (tc/ip), which test machines and shared clusters
 * often do not grant. This shim is the privilege-free alternative: it
 * intercepts the process's outbound UDP send syscalls and applies delay +
 * jitter + independent loss *before* the datagram ever reaches the kernel, so
 * no privilege and no separate relay node are needed. It uses the same
 * dlsym(RTLD_NEXT, ...) technique as udp_offload_shim.c, extended from one hook
 * (sendmsg) to the four send calls MSC's UDP engine actually uses.
 *
 * Because it works at the libc syscall boundary it is oblivious to *which*
 * binary is running: the same wan_shim.so impairs `msc` and its loopback test
 * harness (udp_test) identically. Usage is a one-liner with no source changes:
 *
 *   LD_PRELOAD=./build/wan_shim.so MSC_UDP_WANSHIM_DELAY_MS=35 \
 *     MSC_UDP_WANSHIM_JITTER_MS=5 MSC_UDP_WANSHIM_LOSS_PCT=0.5 \
 *     msc -l nodeA -r nodeB -i src -o dst
 *
 * Config (read once at library load; all default 0 = transparent passthrough):
 *   MSC_UDP_WANSHIM_DELAY_MS   base one-way delay in ms
 *   MSC_UDP_WANSHIM_JITTER_MS  uniform jitter magnitude (+/-) around the delay
 *   MSC_UDP_WANSHIM_LOSS_PCT   independent per-datagram drop probability (%)
 *   MSC_UDP_WANSHIM_DUP_PCT    independent per-datagram duplication probability
 *   MSC_UDP_WANSHIM_DROP_ACKS  drop the first N MSC UDP ACK datagrams
 *   MSC_UDP_WANSHIM_DROP_FINS  drop the first N MSC UDP FIN datagrams
 *   MSC_UDP_WANSHIM_MTU        remote MTU black hole: swallow any datagram
 *                              larger than this many bytes, as a downstream
 *                              link with a smaller MTU does.  NOT an EMSGSIZE --
 *                              a local send succeeding proves only the LOCAL
 *                              interface could carry it, which is the whole
 *                              distinction PMTUD exists to discover
 *   MSC_UDP_WANSHIM_SEED       RNG seed for a reproducible run; unset => per-run
 *   MSC_UDP_WANSHIM_TRACE      1 => log the applied config at start and a
 *                              send/delay/drop tally at exit
 *
 * UDP-only scope is a CORRECTNESS requirement, not a nicety: every hooked call
 * first checks SO_TYPE == SOCK_DGRAM and passes anything else straight through.
 * The default control channel is a TCP stream; dropping bytes at its send()
 * boundary would silently corrupt the stream (the kernel would never retransmit
 * "lost" bytes it was never told about), unlike real loss on a TCP link which
 * the kernel recovers invisibly. The same SO_TYPE filter also cleanly excludes
 * msc's plain-TCP (-T) data path. The UDP control modes (MSC_UDP_CTL=many|one)
 * DO ride a connected UDP socket, so this shim correctly subjects them to the
 * same WAN conditions.
 *
 * Operating externally at the syscall boundary makes this a black-box test of
 * the real loss-recovery path, and -- applied on the receiver -- it can drop
 * ACKs as well as data.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <sys/mman.h>
#include <errno.h>
#include <limits.h>
#include <linux/udp.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/uio.h>

/* ===== real libc entry points ============================================= */
static ssize_t (*real_sendmsg)(int, const struct msghdr *, int);
static int     (*real_sendmmsg)(int, struct mmsghdr *, unsigned, int);
static ssize_t (*real_sendto)(int, const void *, size_t, int,
                              const struct sockaddr *, socklen_t);
static ssize_t (*real_send)(int, const void *, size_t, int);
static int     (*real_getsockopt)(int, int, int, void *, socklen_t *);
static int     (*real_close)(int);

static void init_reals(void)
{
   real_sendmsg   = dlsym(RTLD_NEXT, "sendmsg");
   real_sendmmsg  = dlsym(RTLD_NEXT, "sendmmsg");
   real_sendto    = dlsym(RTLD_NEXT, "sendto");
   real_send      = dlsym(RTLD_NEXT, "send");
   real_getsockopt = dlsym(RTLD_NEXT, "getsockopt");
   real_close     = dlsym(RTLD_NEXT, "close");
}

/* ===== config (parsed once at load) ======================================= */
static int      g_active;         /* any impairment configured at all */
static uint64_t g_delay_ns;       /* base one-way delay */
static uint64_t g_jitter_ns;      /* uniform +/- magnitude */
static double   g_loss_frac;      /* per-datagram drop probability [0,1] */
static double   g_dup_frac;       /* per-datagram duplicate probability [0,1] */
static unsigned g_drop_acks;      /* targeted MSC UDP ACK drops */
static unsigned g_drop_fins;      /* targeted MSC UDP FIN drops */
/* Remote MTU black hole: silently swallow any datagram larger than this, as a
 * downstream link with a smaller MTU does to a DF-set packet.  Deliberately
 * NOT an EMSGSIZE: the local kernel accepting a send tells you only that the
 * LOCAL interface could carry it, which is exactly the distinction PMTUD has
 * to discover and exactly what a local-only check cannot emulate. */
static size_t   g_mtu;            /* 0 = unlimited */
static atomic_uint g_acks_seen;
static atomic_uint g_fins_seen;
static int      g_trace;

/* Counters live in a MAP_SHARED anonymous page, not plain statics, because the
 * processes that do the sending are usually not the process the shim was
 * constructed in. msc's own test harness (udp_test.c) forks a sender and a
 * receiver; with per-process statics each child incremented its own
 * copy-on-write copy and the parent -- which sends nothing -- reported
 * "sent=0 delayed=0 dropped=0" at exit. That looked exactly like a shim that
 * had failed to load, while the shim was in fact working (the impaired
 * transfer took 15x as long as the clean one). Sharing the page makes the
 * summary the aggregate across the whole process tree, so it can be trusted
 * and asserted on. */
struct wan_shim_counters
{
   atomic_ullong immediate;
   atomic_ullong delayed;
   atomic_ullong dropped;
   atomic_ullong duplicated;
   atomic_ullong reordered;
   atomic_ullong forced_ack_drops;
   atomic_ullong forced_fin_drops;
   atomic_ullong mtu_drops;
};
static struct wan_shim_counters *g_ctr;      /* shared page; never NULL once init ran */
static struct wan_shim_counters g_ctr_local; /* fallback if the mmap fails */
static pid_t g_ctr_owner;                    /* only this pid prints the summary */

#define g_n_immediate (g_ctr->immediate)
#define g_n_delayed   (g_ctr->delayed)
#define g_n_dropped   (g_ctr->dropped)
#define g_n_duplicated (g_ctr->duplicated)
#define g_n_reordered (g_ctr->reordered)
#define g_n_forced_ack_drops (g_ctr->forced_ack_drops)
#define g_n_forced_fin_drops (g_ctr->forced_fin_drops)
#define g_n_mtu_drops (g_ctr->mtu_drops)

/* ===== monotonic time ===================================================== */
static uint64_t now_ns(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* ===== RNG (SplitMix64, its own lock so drop/jitter rolls on the engine's
 * several sender threads stay thread-safe without contending the delay heap) ========= */
static pthread_mutex_t g_rng_lock = PTHREAD_MUTEX_INITIALIZER;
static uint64_t        g_rng_state;
static int             g_seed_explicit;

static uint64_t splitmix64(void)
{
   uint64_t z = (g_rng_state += 0x9e3779b97f4a7c15ull);
   z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ull;
   z = (z ^ (z >> 27)) * 0x94d049bb133111ebull;
   return z ^ (z >> 31);
}

/* uniform double in [0,1) */
static double rng_unit(void)
{
   return (splitmix64() >> 11) * (1.0 / 9007199254740992.0);
}

/* Roll loss and, for a survivor, its jitter in one locked step so a fixed seed
 * reproduces the exact same sequence (each datagram consumes one roll if
 * dropped, two if kept). Returns 1 if the datagram should be dropped; otherwise
 * *jitter_out is the signed jitter to add. */
static int roll(int64_t *jitter_out, int *duplicate_out)
{
   int drop;
   pthread_mutex_lock(&g_rng_lock);
   drop = g_loss_frac > 0.0 && rng_unit() < g_loss_frac;
   if (!drop)
   {
      *jitter_out = g_jitter_ns
         ? (int64_t)((rng_unit() * 2.0 - 1.0) * (double)g_jitter_ns) : 0;
      *duplicate_out = g_dup_frac > 0.0 && rng_unit() < g_dup_frac;
   }
   pthread_mutex_unlock(&g_rng_lock);
   return drop;
}

/* MSC UDP's common packet header begins with network-order magic and type.
 * Kept independent of udp_session.h so the shim builds on its own. */
#define MSC_UDP_WIRE_MAGIC 0x4d534331U
#define MSC_UDP_WIRE_ACK   2U
#define MSC_UDP_WIRE_FIN   3U

static uint32_t packet_type(const struct iovec *iov, int iovcnt)
{
   unsigned char header[8];
   size_t copied = 0;
   int i;
   uint32_t magic, type;
   for (i = 0; i < iovcnt && copied < sizeof(header); i++)
   {
      size_t take = iov[i].iov_len;
      if (take > sizeof(header) - copied) take = sizeof(header) - copied;
      memcpy(header + copied, iov[i].iov_base, take);
      copied += take;
   }
   if (copied != sizeof(header)) return 0;
   memcpy(&magic, header, sizeof(magic));
   memcpy(&type, header + sizeof(magic), sizeof(type));
   return ntohl(magic) == MSC_UDP_WIRE_MAGIC ? ntohl(type) : 0;
}

static int force_drop(const struct iovec *iov, int iovcnt)
{
   uint32_t type = packet_type(iov, iovcnt);
   unsigned seen;
   if (type == MSC_UDP_WIRE_ACK && g_drop_acks)
   {
      seen = atomic_fetch_add(&g_acks_seen, 1);
      if (seen < g_drop_acks)
      {
         atomic_fetch_add(&g_n_forced_ack_drops, 1);
         atomic_fetch_add(&g_n_dropped, 1);
         return 1;
      }
   }
   if (type == MSC_UDP_WIRE_FIN && g_drop_fins)
   {
      seen = atomic_fetch_add(&g_fins_seen, 1);
      if (seen < g_drop_fins)
      {
         atomic_fetch_add(&g_n_forced_fin_drops, 1);
         atomic_fetch_add(&g_n_dropped, 1);
         return 1;
      }
   }
   return 0;
}

/* ===== per-fd SOCK_DGRAM cache ============================================ */
/* 0 = unknown, 1 = dgram, 2 = other. Caching matters for measurement fidelity:
 * an extra getsockopt on every send would inflate the very per-thread CPU the
 * benchmark measures. close() invalidates the slot so a reused fd number is
 * re-classified (a TCP fd dup2'd directly over a cached dgram number without a
 * close is the one uncovered corner, not exercised by msc's hot path). */
#define FD_CACHE 65536
static volatile signed char g_fdtype[FD_CACHE];

static int is_dgram(int fd)
{
   int type;
   socklen_t len = sizeof(type);
   if (fd < 0)
      return 0;
   if (fd < FD_CACHE)
   {
      signed char t = g_fdtype[fd];
      if (t == 1) return 1;
      if (t == 2) return 0;
   }
   if (real_getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &len) == 0
       && type == SOCK_DGRAM)
   {
      if (fd < FD_CACHE) g_fdtype[fd] = 1;
      return 1;
   }
   if (fd < FD_CACHE) g_fdtype[fd] = 2;
   return 0;
}

/* ===== delay heap + background release thread ============================= */
struct pkt
{
   uint64_t release_ns;
   uint64_t seq;                 /* FIFO tiebreak among equal release times */
   int      fd;
   int      flags;
   struct sockaddr_storage addr; /* destination; addrlen 0 => connected send() */
   socklen_t addrlen;
   void    *ctrl;                /* preserved cmsg blob (e.g. UDP_SEGMENT GSO) */
   size_t   ctrllen;
   void    *buf;
   size_t   len;
};

static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_cond;                 /* CLOCK_MONOTONIC (see init) */
static struct pkt    **g_heap;
static size_t          g_heap_len, g_heap_cap;
static uint64_t        g_seq;
static uint64_t        g_release_max_seq;
static int             g_release_any;
static int             g_thread_started;
static int             g_shutdown;
static pthread_t       g_thread;

static void heap_swap(size_t a, size_t b)
{
   struct pkt *t = g_heap[a]; g_heap[a] = g_heap[b]; g_heap[b] = t;
}

static int heap_less(size_t a, size_t b)
{
   if (g_heap[a]->release_ns != g_heap[b]->release_ns)
      return g_heap[a]->release_ns < g_heap[b]->release_ns;
   return g_heap[a]->seq < g_heap[b]->seq;
}

/* caller holds g_lock; returns 0 on success, -1 if the heap can't grow */
static int heap_push(struct pkt *p)
{
   size_t i;
   if (g_heap_len == g_heap_cap)
   {
      size_t ncap = g_heap_cap ? g_heap_cap * 2 : 256;
      struct pkt **nh = realloc(g_heap, ncap * sizeof(*nh));
      if (!nh) return -1;
      g_heap = nh;
      g_heap_cap = ncap;
   }
   i = g_heap_len++;
   g_heap[i] = p;
   while (i > 0)
   {
      size_t parent = (i - 1) / 2;
      if (!heap_less(i, parent)) break;
      heap_swap(i, parent);
      i = parent;
   }
   return 0;
}

/* caller holds g_lock */
static struct pkt *heap_pop(void)
{
   struct pkt *top = g_heap[0];
   size_t i = 0;
   g_heap[0] = g_heap[--g_heap_len];
   for (;;)
   {
      size_t l = 2 * i + 1, r = 2 * i + 2, m = i;
      if (l < g_heap_len && heap_less(l, m)) m = l;
      if (r < g_heap_len && heap_less(r, m)) m = r;
      if (m == i) break;
      heap_swap(i, m);
      i = m;
   }
   return top;
}

/* Fire one queued datagram through the real kernel path. Everything is replayed
 * as a sendmsg: a connected send() becomes a nameless sendmsg (sends to the
 * connected peer), a sendto/sendmmsg element carries its saved name, and a
 * sendmsg carries its saved cmsg blob so GSO still slices. */
static void pkt_send(struct pkt *p)
{
   struct msghdr m;
   struct iovec iov;
   memset(&m, 0, sizeof(m));
   iov.iov_base = p->buf;
   iov.iov_len = p->len;
   if (p->addrlen)
   {
      m.msg_name = &p->addr;
      m.msg_namelen = p->addrlen;
   }
   m.msg_iov = &iov;
   m.msg_iovlen = 1;
   if (p->ctrllen)
   {
      m.msg_control = p->ctrl;
      m.msg_controllen = p->ctrllen;
   }
   real_sendmsg(p->fd, &m, p->flags);
}

static void pkt_free(struct pkt *p)
{
   free(p->buf);
   free(p->ctrl);
   free(p);
}

/* Drain up to this many due datagrams per lock acquisition. Popping one at a
 * time and re-locking between each let a bursty sender (a window's worth of
 * fresh units dumped at once) outrun the release thread, so the backlog -- not
 * the configured delay -- set the tail latency. Batching the drain keeps the
 * emitter ahead of any realistic in-flight window. */
#define DRAIN_BATCH 512

static void *release_thread(void *arg)
{
   struct pkt *batch[DRAIN_BATCH];
   (void)arg;
   pthread_mutex_lock(&g_lock);
   for (;;)
   {
      uint64_t now, rel;
      size_t nb = 0, k;
      if (g_heap_len == 0)
      {
         if (g_shutdown) break;
         pthread_cond_wait(&g_cond, &g_lock);
         continue;
      }
      rel = g_heap[0]->release_ns;
      now = now_ns();
      if (!g_shutdown && rel > now)
      {
         struct timespec ts;
         ts.tv_sec = (time_t)(rel / 1000000000ull);
         ts.tv_nsec = (long)(rel % 1000000000ull);
         pthread_cond_timedwait(&g_cond, &g_lock, &ts);
         continue;   /* re-evaluate: a nearer packet may have arrived */
      }
      /* pop every datagram already due (all of them, on shutdown drain) in one
       * lock hold, then release the lock before touching the kernel */
      while (nb < DRAIN_BATCH && g_heap_len > 0
             && (g_shutdown || g_heap[0]->release_ns <= now))
         batch[nb++] = heap_pop();
      pthread_mutex_unlock(&g_lock);
      for (k = 0; k < nb; k++)
      {
         if (g_release_any && batch[k]->seq < g_release_max_seq)
            atomic_fetch_add(&g_n_reordered, 1);
         if (!g_release_any || batch[k]->seq > g_release_max_seq)
            g_release_max_seq = batch[k]->seq;
         g_release_any = 1;
         pkt_send(batch[k]);
         pkt_free(batch[k]);
      }
      pthread_mutex_lock(&g_lock);
   }
   pthread_mutex_unlock(&g_lock);
   return NULL;
}

/* caller holds g_lock; lazily start the release thread (deferred to first use
 * so passthrough runs and non-sending children never spawn one). */
static int ensure_thread_locked(void)
{
   if (g_thread_started) return 0;
   if (pthread_create(&g_thread, NULL, release_thread, NULL) != 0)
      return -1;
   g_thread_started = 1;
   return 0;
}

/* Copy an iovec-scattered datagram plus its addressing/cmsg into the heap for
 * later release. Returns 0 if queued; on any allocation failure returns -1 and
 * the caller falls back to sending immediately. */
static int enqueue(int fd, const struct iovec *iov, int iovcnt,
                   const void *name, socklen_t namelen,
                   const void *ctrl, size_t ctrllen, int flags, uint64_t rel)
{
   struct pkt *p;
   size_t total = 0, off = 0;
   int i;
   for (i = 0; i < iovcnt; i++)
      total += iov[i].iov_len;

   p = calloc(1, sizeof(*p));
   if (!p) return -1;
   p->buf = malloc(total ? total : 1);
   if (!p->buf) { free(p); return -1; }
   for (i = 0; i < iovcnt; i++)
   {
      memcpy((char *)p->buf + off, iov[i].iov_base, iov[i].iov_len);
      off += iov[i].iov_len;
   }
   p->len = total;
   if (namelen && name)
   {
      if (namelen > sizeof(p->addr)) namelen = sizeof(p->addr);
      memcpy(&p->addr, name, namelen);
      p->addrlen = namelen;
   }
   if (ctrllen && ctrl)
   {
      p->ctrl = malloc(ctrllen);
      if (!p->ctrl) { free(p->buf); free(p); return -1; }
      memcpy(p->ctrl, ctrl, ctrllen);
      p->ctrllen = ctrllen;
   }
   p->fd = fd;
   p->flags = flags;
   p->release_ns = rel;

   pthread_mutex_lock(&g_lock);
   if (ensure_thread_locked() != 0)
   {
      pthread_mutex_unlock(&g_lock);
      pkt_send(p);        /* no timer thread: best-effort immediate send */
      pkt_free(p);
      atomic_fetch_add(&g_n_immediate, 1);
      return 0;
   }
   p->seq = g_seq++;
   if (heap_push(p) != 0)
   {
      pthread_mutex_unlock(&g_lock);
      pkt_send(p);
      pkt_free(p);
      atomic_fetch_add(&g_n_immediate, 1);
      return 0;
   }
   pthread_cond_signal(&g_cond);
   pthread_mutex_unlock(&g_lock);
   atomic_fetch_add(&g_n_delayed, 1);
   return 0;
}

/* Decide the fate of one outbound datagram. Returns:
 *   1  drop it (caller synthesizes success without sending)
 *   0  send immediately (release_time already passed)
 *  -1  queued for delayed release (caller synthesizes success)
 * On the queued path the datagram bytes are copied out of the caller's buffers,
 * so the caller may reuse/free them the instant it returns -- matching real UDP,
 * where sendto() success has never meant "left the NIC," only "kernel accepted."
 */
static int classify(int fd, const struct iovec *iov, int iovcnt,
                    const void *name, socklen_t namelen,
                    const void *ctrl, size_t ctrllen, int flags)
{
   int64_t jitter = 0;
   int64_t rel;
   int duplicate = 0;
   /* MTU black hole first: a datagram too big for a downstream link is gone
    * before any delay or loss model applies to it.  All four intercepted send
    * calls funnel through here, so this covers send/sendto/sendmsg/sendmmsg. */
   if (g_mtu != 0)
   {
      size_t total = 0;
      int i;
      for (i = 0; i < iovcnt; i++)
         total += iov[i].iov_len;
      if (total > g_mtu)
      {
         atomic_fetch_add(&g_n_mtu_drops, 1);
         return 1;
      }
   }
   if (force_drop(iov, iovcnt))
      return 1;
   if (roll(&jitter, &duplicate))
   {
      atomic_fetch_add(&g_n_dropped, 1);
      return 1;
   }
   rel = (int64_t)now_ns() + (int64_t)g_delay_ns + jitter;
   if (duplicate &&
       enqueue(fd, iov, iovcnt, name, namelen, ctrl, ctrllen, flags,
               (uint64_t)(rel > (int64_t)now_ns()
                           ? rel + 1000 : (int64_t)now_ns() + 1000)) == 0)
      atomic_fetch_add(&g_n_duplicated, 1);
   if (rel <= (int64_t)now_ns())
      return 0;   /* delay collapsed to now (e.g. negative jitter): send live */
   if (enqueue(fd, iov, iovcnt, name, namelen, ctrl, ctrllen, flags,
               (uint64_t)rel) == 0)
      return -1;
   return 0;      /* enqueue failed: fall back to a live send */
}

/* ===== intercepted syscalls =============================================== */
/* Impair wire datagrams, not GSO superpackets. Preserve every other ancillary
 * setting (notably IP_TOS/ECN) when splitting a scatter/gather UDP_SEGMENT send.
 * Allocations finish before any packet is accepted, so allocation failure does
 * not silently bypass impairment or leave a partially queued batch. */
static ssize_t impaired_sendmsg(int fd, const struct msghdr *msg, int flags)
{
   size_t total = 0, i, off;
   uint16_t segment = 0;
   struct cmsghdr *c;
   for (i = 0; i < msg->msg_iovlen; i++)
   {
      if (msg->msg_iov[i].iov_len > (size_t)SSIZE_MAX - total)
      { errno = EMSGSIZE; return -1; }
      total += msg->msg_iov[i].iov_len;
   }
   for (c = CMSG_FIRSTHDR(msg); c != NULL; c = CMSG_NXTHDR((struct msghdr *)msg, c))
      if (c->cmsg_level == IPPROTO_UDP && c->cmsg_type == UDP_SEGMENT)
      {
         if (c->cmsg_len != CMSG_LEN(sizeof(segment)))
         { errno = EINVAL; return -1; }
         memcpy(&segment, CMSG_DATA(c), sizeof(segment));
      }
   if (segment != 0 && total > segment)
   {
      unsigned char *data = malloc(total);
      unsigned char *control = malloc(msg->msg_controllen);
      struct msghdr part = *msg;
      struct iovec iov;
      if (data == NULL || control == NULL)
      { free(data); free(control); errno = ENOMEM; return -1; }
      for (i = 0, off = 0; i < msg->msg_iovlen; i++)
      {
         memcpy(data + off, msg->msg_iov[i].iov_base, msg->msg_iov[i].iov_len);
         off += msg->msg_iov[i].iov_len;
      }
      memcpy(control, msg->msg_control, msg->msg_controllen);
      part.msg_control = control;
      /* Explicitly disable segmentation on these individual datagrams. This
       * also overrides a socket-level UDP_SEGMENT setting if one is present. */
      for (c = CMSG_FIRSTHDR(&part); c != NULL; c = CMSG_NXTHDR(&part, c))
         if (c->cmsg_level == IPPROTO_UDP && c->cmsg_type == UDP_SEGMENT)
         {
            uint16_t zero = 0;
            memcpy(CMSG_DATA(c), &zero, sizeof(zero));
         }
      part.msg_iov = &iov;
      part.msg_iovlen = 1;
      for (off = 0; off < total; off += iov.iov_len)
      {
         iov.iov_base = data + off;
         iov.iov_len = total - off < segment ? total - off : segment;
         if (classify(fd, &iov, 1, part.msg_name, part.msg_namelen,
                      control, part.msg_controllen, flags) == 0)
         {
            if (real_sendmsg(fd, &part, flags) < 0)
            {
               int saved = errno;
               free(control); free(data); errno = saved;
               return -1;
            }
            atomic_fetch_add(&g_n_immediate, 1);
         }
      }
      free(control); free(data);
      return (ssize_t)total;
   }
   if (classify(fd, msg->msg_iov, (int)msg->msg_iovlen,
                msg->msg_name, msg->msg_namelen,
                msg->msg_control, msg->msg_controllen, flags) != 0)
      return (ssize_t)total;
   atomic_fetch_add(&g_n_immediate, 1);
   return real_sendmsg(fd, msg, flags);
}

ssize_t sendto(int fd, const void *buf, size_t len, int flags,
               const struct sockaddr *addr, socklen_t addrlen)
{
   struct iovec iov;
   if (!real_sendto) init_reals();
   if (!g_active || !is_dgram(fd))
      return real_sendto(fd, buf, len, flags, addr, addrlen);
   iov.iov_base = (void *)buf;
   iov.iov_len = len;
   switch (classify(fd, &iov, 1, addr, addrlen, NULL, 0, flags))
   {
   case 1:  return (ssize_t)len;   /* dropped */
   case -1: return (ssize_t)len;   /* queued  */
   default: break;
   }
   atomic_fetch_add(&g_n_immediate, 1);
   return real_sendto(fd, buf, len, flags, addr, addrlen);
}

ssize_t send(int fd, const void *buf, size_t len, int flags)
{
   struct iovec iov;
   if (!real_send) init_reals();
   if (!g_active || !is_dgram(fd))
      return real_send(fd, buf, len, flags);
   iov.iov_base = (void *)buf;
   iov.iov_len = len;
   switch (classify(fd, &iov, 1, NULL, 0, NULL, 0, flags))
   {
   case 1:  return (ssize_t)len;
   case -1: return (ssize_t)len;
   default: break;
   }
   atomic_fetch_add(&g_n_immediate, 1);
   return real_send(fd, buf, len, flags);
}

ssize_t sendmsg(int fd, const struct msghdr *msg, int flags)
{
   if (!real_sendmsg) init_reals();
   if (!g_active || !is_dgram(fd))
      return real_sendmsg(fd, msg, flags);
   return impaired_sendmsg(fd, msg, flags);
}

/* sendmmsg is MSC UDP's default (non-GSO) fresh-send batch path. Each element is an
 * independent datagram, so classify each one; report every element accepted
 * (dropped ones "left the NIC" invisibly). Only a live send of an immediate
 * element can genuinely fail (ENOBUFS): mirror the kernel and return the count
 * fully handled so far, or -1 with errno if that was the very first element. */
int sendmmsg(int fd, struct mmsghdr *msgvec, unsigned vlen, int flags)
{
   unsigned i;
   if (!real_sendmmsg) init_reals();
   if (!g_active || !is_dgram(fd))
      return real_sendmmsg(fd, msgvec, vlen, flags);
   for (i = 0; i < vlen; i++)
   {
      ssize_t s = impaired_sendmsg(fd, &msgvec[i].msg_hdr, flags);
      if (s < 0)
         return i > 0 ? (int)i : -1;
      msgvec[i].msg_len = (unsigned)s;
   }
   return (int)vlen;
}

int close(int fd)
{
   if (!real_close) init_reals();
   if (fd >= 0 && fd < FD_CACHE)
      g_fdtype[fd] = 0;    /* re-classify if this fd number is reused */
   return real_close(fd);
}

/* ===== fork safety ======================================================== */
/* The release thread does not survive fork(); the loopback harnesses (and any
 * msc worker fork) fork the sender. Reset the child so it starts a fresh thread
 * on its first delayed send, and drop the parent's inherited queue (those bytes
 * belong to the parent's sockets). */
static void atfork_prepare(void)
{
   pthread_mutex_lock(&g_lock);
   pthread_mutex_lock(&g_rng_lock);
}
static void atfork_parent(void)
{
   pthread_mutex_unlock(&g_rng_lock);
   pthread_mutex_unlock(&g_lock);
}
static void atfork_child(void)
{
   size_t i;
   for (i = 0; i < g_heap_len; i++)
      pkt_free(g_heap[i]);
   g_heap_len = 0;
   g_thread_started = 0;
   g_shutdown = 0;
   g_release_max_seq = 0;
   g_release_any = 0;
   /* An auto seed was derived from the parent's pid/time; decorrelate the child
    * so forked flows don't share a drop pattern. An explicit seed is honored. */
   if (!g_seed_explicit)
      g_rng_state ^= (uint64_t)getpid() * 0x9e3779b97f4a7c15ull;
   pthread_mutex_unlock(&g_rng_lock);
   pthread_mutex_unlock(&g_lock);
}

/* ===== load / unload ====================================================== */
static double env_double(const char *name)
{
   const char *value = getenv(name);
   return value != NULL ? strtod(value, NULL) : 0.0;
}

__attribute__((constructor))
static void wan_shim_init(void)
{
   pthread_condattr_t ca;
   const char *seed;
   double delay_ms, jitter_ms, loss_pct, dup_pct;

   init_reals();

   /* Shared before any fork, so every descendant increments the same counters. */
   g_ctr = mmap(NULL, sizeof(*g_ctr), PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_ANONYMOUS, -1, 0);
   if (g_ctr == MAP_FAILED)
      g_ctr = &g_ctr_local;   /* degrade to per-process counts, still correct locally */
   g_ctr_owner = getpid();

   delay_ms  = env_double("MSC_UDP_WANSHIM_DELAY_MS");
   jitter_ms = env_double("MSC_UDP_WANSHIM_JITTER_MS");
   loss_pct  = env_double("MSC_UDP_WANSHIM_LOSS_PCT");
   dup_pct   = env_double("MSC_UDP_WANSHIM_DUP_PCT");
   g_drop_acks = (unsigned)env_double("MSC_UDP_WANSHIM_DROP_ACKS");
   g_drop_fins = (unsigned)env_double("MSC_UDP_WANSHIM_DROP_FINS");
   g_mtu     = (size_t)env_double("MSC_UDP_WANSHIM_MTU");
   g_trace   = getenv("MSC_UDP_WANSHIM_TRACE") != NULL;

   if (delay_ms  < 0) delay_ms  = 0;
   if (jitter_ms < 0) jitter_ms = 0;
   if (loss_pct  < 0) loss_pct  = 0;
   if (loss_pct  > 100) loss_pct = 100;
   if (dup_pct   < 0) dup_pct   = 0;
   if (dup_pct   > 100) dup_pct = 100;

   g_delay_ns  = (uint64_t)(delay_ms  * 1e6);
   g_jitter_ns = (uint64_t)(jitter_ms * 1e6);
   g_loss_frac = loss_pct / 100.0;
   g_dup_frac = dup_pct / 100.0;
   g_active = (g_delay_ns || g_jitter_ns || g_loss_frac > 0.0 ||
               g_dup_frac > 0.0 || g_drop_acks || g_drop_fins || g_mtu != 0);

   seed = getenv("MSC_UDP_WANSHIM_SEED");
   if (seed && *seed)
   {
      g_rng_state = strtoull(seed, NULL, 0);
      g_seed_explicit = 1;
   }
   else
   {
      g_rng_state = (uint64_t)now_ns() ^ ((uint64_t)getpid() << 32);
   }

   pthread_condattr_init(&ca);
   pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
   pthread_cond_init(&g_cond, &ca);
   pthread_condattr_destroy(&ca);

   pthread_atfork(atfork_prepare, atfork_parent, atfork_child);

   if (g_trace)
      fprintf(stderr, "wan_shim: delay=%.3gms jitter=%.3gms loss=%.3g%% "
              "dup=%.3g%% drop_acks=%u drop_fins=%u seed=%s%llu %s\n",
              delay_ms, jitter_ms, loss_pct, dup_pct, g_drop_acks, g_drop_fins,
              g_seed_explicit ? "" : "auto:",
              (unsigned long long)g_rng_state,
              g_active ? "" : "(inactive: transparent passthrough)");
}

__attribute__((destructor))
static void wan_shim_fini(void)
{
   if (g_thread_started)
   {
      pthread_mutex_lock(&g_lock);
      g_shutdown = 1;               /* drain remaining queue, then exit */
      pthread_cond_signal(&g_cond);
      pthread_mutex_unlock(&g_lock);
      pthread_join(g_thread, NULL);
   }
   /* Only the constructing process prints, so a forking harness gets one
    * aggregate line rather than one line per child. Children still contribute
    * to the shared counters before they exit. */
   if (g_trace && getpid() == g_ctr_owner)
      fprintf(stderr, "wan_shim: sent=%llu delayed=%llu dropped=%llu "
              "duplicated=%llu reordered=%llu forced_ack_drops=%llu "
              "forced_fin_drops=%llu mtu_drops=%llu\n",
              (unsigned long long)atomic_load(&g_n_immediate),
              (unsigned long long)atomic_load(&g_n_delayed),
              (unsigned long long)atomic_load(&g_n_dropped),
              (unsigned long long)atomic_load(&g_n_duplicated),
              (unsigned long long)atomic_load(&g_n_reordered),
              (unsigned long long)atomic_load(&g_n_forced_ack_drops),
              (unsigned long long)atomic_load(&g_n_forced_fin_drops),
              (unsigned long long)atomic_load(&g_n_mtu_drops));
}
