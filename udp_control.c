#define _GNU_SOURCE   /* recvmmsg, struct mmsghdr */
/*
 * udp_control.c - a reliable, ordered byte-stream over UDP datagrams (a
 * mini-TCP) for MSC UDP's control channel, plus the `one`-mode RX demux that lets
 * control and all N data flows share a single UDP socket/port.
 *
 * Modes (MSC_UDP_CTL): stdio/stdio1 (the default; control rides the SSH
 * session) and tcp leave this file idle, many (control gets
 * its own UDP socket + this shim, data unchanged), one (control AND data share
 * one socket; a demux thread owns every read from right after the handshake
 * until teardown and routes datagrams by leading magic / flow_id into bounded
 * per-flow queues -- PMTUD included, nothing else ever recv()s the fd).
 *
 * The shim hides behind the msc_udp_send_all/msc_udp_recv_all contract (deliver exactly N
 * bytes, in order, or error), so every existing control call site works
 * unchanged: udp_io.c dispatches here whenever the fd is registered.
 *
 * Reliability: go-back-N over a byte stream with cumulative ACKs -- control
 * volume is tiny, so there is no SACK and no RTT estimation, just a fixed
 * initial RTO with exponential backoff that resets on progress. The 3-way
 * handshake doubles as mode/version negotiation (SYN payload = [version,
 * ctl_mode]); a many/one mismatch is refused with RST before any control
 * record flows. The wire header is a fixed 24-byte big-endian frame produced
 * by explicit pack/unpack helpers -- no C struct ever goes on the wire.
 *
 * Threading: each control endpoint is single-threaded (the sender has one
 * control thread, the receiver one; test harness forks whole processes), so
 * the fd registry and shim state need no locks. The one concurrent path is
 * the `one`-mode inbound queue, where the demux thread produces into
 * mutex+condvar rings that the control thread / flow threads consume.
 */
#include "udp_session.h"
#include "msc.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* UDP_GRO cmsg bits, mirrored from udp_session.c so this file compiles on
 * toolchains without the kernel headers (an old kernel just never sends the
 * cmsg and every slot is a single datagram). */
#ifndef SOL_UDP
#define SOL_UDP 17
#endif
#ifndef UDP_GRO
#define UDP_GRO 104
#endif

/* ===== MSC_UDP_CTL mode ======================================================= */
int msc_udp_ctl_mode(void)
{
   static int mode = -1;
   const char *v;
   if (mode >= 0)
      return mode;
   v = getenv("MSC_UDP_CTL");
   /* Default: stdio. Control rides the stdin/stdout of the SSH session that
    * already launches the receiver, so a transfer opens NO listening TCP port
    * on either end. Where opening a port is an administrative negotiation,
    * that is the difference between a deployable tool and an undeployable one
    * -- and msc already uses SSH for the port rendezvous even under `tcp`, so
    * the TCP control port was pure extra surface.
    *
    * It is free. Measured on a clean IPoIB link at a matched four data
    * sockets, tcp and stdio control gave medians of 23.7 and 24.4 Gbit/s over
    * 8 interleaved reps: paired mean difference 0.12 Gbit/s, 95% CI
    * [-1.55, +1.80], indistinguishable.
    * What costs throughput is the SOCKET count, not the control plane -- 4 -> 8
    * sockets is worth +6.3 Gbit/s on the same pair.
    *
    * Both ends read this same function, so both default to stdio and agree
    * with no forwarding and no negotiation. (Negotiation is impossible in
    * principle anyway: every carrier of the mode -- the greeting, the shim's
    * SYN payload -- travels over the control channel whose location is the
    * question.) A caller that hands over no control descriptor still fails
    * loudly rather than silently: the sender bails on cfg->ctl_fd <= 0. That
    * is why udp_test.c stands up an AF_UNIX socketpair for its in-process
    * pair; there is no SSH on a loopback test, but the mode only ever wanted a
    * connected byte stream.
    *
    * stdio cannot measure the path: its control channel is a pipe to the local
    * ssh process, so a handshake RTT taken on it reflects the pipe (~0.24 ms)
    * rather than the network (~52 ms on a 50 ms pair). Already handled --
    * path_profile_reconsider() treats the handshake value from every control
    * mode as provisional and re-derives the profile from the first data-plane
    * ACK. That correction is now on the common path, not an opt-in arm.
    *
    * tcp/many/one/stdio1 stay selectable: existing scripts use them, and
    * one/stdio1 are the instruments that measure the
    * single-socket funnel penalty. Nothing falls back to them automatically. */
   if (v == NULL || v[0] == '\0' || strcmp(v, "stdio") == 0)
      mode = MSC_UDP_CTLMODE_STDIO;
   else if (strcmp(v, "tcp") == 0)
      mode = MSC_UDP_CTLMODE_TCP;
   else if (strcmp(v, "many") == 0)
      mode = MSC_UDP_CTLMODE_MANY;
   else if (strcmp(v, "one") == 0)
      mode = MSC_UDP_CTLMODE_ONE;
   else if (strcmp(v, "stdio1") == 0)
      mode = MSC_UDP_CTLMODE_STDIO1;
   else
   {
      fprintf(stderr, "MSC UDP: unknown MSC_UDP_CTL=%s (want tcp|many|one|stdio|stdio1),"
              " using stdio\n", v);
      mode = MSC_UDP_CTLMODE_STDIO;
   }
   return mode;
}

const char *msc_udp_ctl_mode_name(int mode)
{
   switch (mode)
   {
   case MSC_UDP_CTLMODE_MANY:   return "many";
   case MSC_UDP_CTLMODE_ONE:    return "one";
   case MSC_UDP_CTLMODE_STDIO:  return "stdio";
   case MSC_UDP_CTLMODE_STDIO1: return "stdio1";
   default:                 return "tcp";
   }
}

/* MSC_UDP_CONTROL_DROP: probabilistically drop every OUTGOING shim frame -- SYN,
 * SYN|ACK, ACK, DATA, and FIN alike (RST is exempt so a mode mismatch stays
 * deterministic). Exporting it on both endpoints covers both directions; the
 * shim's own retransmission must recover everything. Companion to the data
 * path's MSC_UDP_DROP. */
static int hs_drop_pct(void)
{
   static int pct = -1;
   if (pct < 0)
   {
      const char *v = getenv("MSC_UDP_CONTROL_DROP");
      pct = 0;
      if (v != NULL)
      {
         pct = atoi(v);
         if (pct < 0) pct = 0;
         if (pct > 100) pct = 100;
      }
   }
   return pct;
}

static int hs_stats_on(void)
{
   static int on = -1;
   if (on < 0)
   {
      const char *v = getenv("MSC_UDP_STATS");
      on = (v != NULL && atoi(v) != 0) ? 1 : 0;
   }
   return on;
}

/* MSC_UDP_CONTROL_TRACE=1: per-frame shim trace on stderr (debug only, very noisy) */
static int hs_trace_on(void)
{
   static int on = -1;
   if (on < 0)
   {
      const char *v = getenv("MSC_UDP_CONTROL_TRACE");
      on = (v != NULL && atoi(v) != 0) ? 1 : 0;
   }
   return on;
}

/* ===== wire format: fixed 24-byte big-endian header ======================= */
struct hs_hdr
{
   uint32_t conn_id;
   uint16_t flags;
   uint16_t length;
   uint32_t seq;
   uint32_t ack;
   uint32_t win;
};

static void put32(uint8_t *b, uint32_t v) { uint32_t n = htonl(v); memcpy(b, &n, 4); }
static uint32_t get32(const uint8_t *b) { uint32_t n; memcpy(&n, b, 4); return ntohl(n); }

static void hs_hdr_encode(uint8_t *b, const struct hs_hdr *h)
{
   put32(b, MSC_UDP_CONTROL_MAGIC);
   put32(b + 4, h->conn_id);
   b[8] = (uint8_t)(h->flags >> 8);
   b[9] = (uint8_t)(h->flags & 0xff);
   b[10] = (uint8_t)(h->length >> 8);
   b[11] = (uint8_t)(h->length & 0xff);
   put32(b + 12, h->seq);
   put32(b + 16, h->ack);
   put32(b + 20, h->win);
}

static int hs_hdr_decode(struct hs_hdr *h, const uint8_t *b, size_t n)
{
   if (n < MSC_UDP_CONTROL_HDR_BYTES || get32(b) != MSC_UDP_CONTROL_MAGIC)
      return -1;
   h->conn_id = get32(b + 4);
   h->flags = (uint16_t)(((uint16_t)b[8] << 8) | b[9]);
   h->length = (uint16_t)(((uint16_t)b[10] << 8) | b[11]);
   h->seq = get32(b + 12);
   h->ack = get32(b + 16);
   h->win = get32(b + 20);
   if ((size_t)MSC_UDP_CONTROL_HDR_BYTES + h->length > n)
      return -1;   /* truncated, or the header lies about its payload */
   return 0;
}

/* ===== shim state ========================================================= */
struct msc_udp_control_channel
{
   int fd;
   int is_stdio;                /* stdio kind: fd is the read side of an inherited
                                 * pipe pair (the SSH session), stdio_wfd the write
                                 * side. SSH's TCP already delivers reliably, so
                                 * every UDP shim field below stays idle. */
   int stdio_wfd;
   int connected;               /* fd is connect()ed: send(); else sendto(peer) */
   int responder;               /* accepted (vs initiated) this connection */
   struct sockaddr_storage peer;
   socklen_t peerlen;
   uint32_t conn_id;            /* initiator's nonce, echoed on every frame */
   int established;
   int dead;                    /* RST / retry exhaustion / refused: no more I/O */
   int peer_fin;                /* peer's stream ended cleanly */
   int fin_sent, fin_acked, fin_tx_pending;
   char reason[160];            /* why dead, for the caller-facing message */

   /* send stream: go-back-N over a byte ring. snd_una <= snd_tx <= snd_nxt;
    * [snd_una, snd_tx) is in flight, [snd_tx, snd_nxt) buffered unsent. */
   uint8_t sndring[MSC_UDP_CONTROL_WIN_BYTES];
   uint32_t snd_una, snd_tx, snd_nxt;
   uint32_t peer_win;           /* peer's advertised free buffer, bytes */
   uint64_t rto_ns, rto_deadline;   /* deadline 0 = timer off */
   int retries;
   /* In-flight DATAGRAM cap. The byte window alone is not enough: control
    * records are tiny (a 4-byte msc_udp_send_all = one datagram), so 64 KiB of window
    * could otherwise become ~16k datagrams in flight -- which overflows the
    * peer's rcvbuf by skb count long before the byte window closes, and the
    * loss storm degenerates the stream into RTO-paced trickle (observed on
    * the 2000-file manifest). Each sent frame's start seq enters this ring;
    * cumulative ACKs retire entries; sends stall at the cap so the flight
    * stays ACK-clocked. */
   uint32_t txseq[128];
   uint32_t txr_head, txr_count;

   /* receive stream: in-order only (a gap is dropped and re-ACKed; go-back-N
    * resends it). [rcv_out, rcv_nxt) is buffered for the app. */
   uint8_t rcvring[MSC_UDP_CONTROL_WIN_BYTES];
   uint32_t rcv_out, rcv_nxt;

   struct msc_udp_receive_demux *receive_demux;         /* one-mode: inbound rides the control queue */
   unsigned int seed;           /* MSC_UDP_CONTROL_DROP rand state */
   uint32_t test_drop_frames;   /* tests: drop this many upcoming TX frames, once */

   uint64_t tx_frames, rx_frames, rtx_events, drops_injected;
};

/* fd -> shim registry. Single-threaded per control endpoint (see the file
 * header), so a plain array with no locking is enough; TCP-mode fds are simply
 * never registered, which is what keeps the default path zero-cost. */
#define CONTROL_REG_MAX 16
static struct { int fd; struct msc_udp_control_channel *channel; } g_reg[CONTROL_REG_MAX];

struct msc_udp_control_channel *msc_udp_control_channel_lookup(int fd)
{
   int i;
   if (fd < 0)
      return NULL;
   for (i = 0; i < CONTROL_REG_MAX; i++)
      if (g_reg[i].channel != NULL && g_reg[i].fd == fd)
         return g_reg[i].channel;
   return NULL;
}

static void hs_register(struct msc_udp_control_channel *channel)
{
   int i;
   for (i = 0; i < CONTROL_REG_MAX; i++)
      if (g_reg[i].channel == NULL)
      {
         g_reg[i].fd = channel->fd;
         g_reg[i].channel = channel;
         return;
      }
   fprintf(stderr, "MSC UDP control channel: registry full\n");
   exit(MSC_EXIT_INTERNAL);
}

static void hs_unregister(struct msc_udp_control_channel *channel)
{
   int i;
   for (i = 0; i < CONTROL_REG_MAX; i++)
      if (g_reg[i].channel == channel)
         g_reg[i].channel = NULL;
}

static struct msc_udp_control_channel *hs_new(int fd)
{
   struct msc_udp_control_channel *channel = msc_udp_alloc(sizeof(*channel), "control channel");
   memset(channel, 0, sizeof(*channel));
   channel->fd = fd;
   channel->peer_win = MSC_UDP_CONTROL_WIN_BYTES;
   channel->rto_ns = (uint64_t)MSC_UDP_CONTROL_INIT_RTO_MS * 1000000ULL;
   channel->seed = (unsigned int)(msc_udp_now_ns() ^ (uint64_t)getpid());
   return channel;
}

static void hs_die(struct msc_udp_control_channel *channel, const char *why)
{
   if (channel->dead)
      return;
   channel->dead = 1;
   snprintf(channel->reason, sizeof(channel->reason), "%s", why);
   fprintf(stderr, "MSC UDP control channel: %s\n", why);
}

static uint32_t hs_adv_win(const struct msc_udp_control_channel *channel)
{
   return MSC_UDP_CONTROL_WIN_BYTES - (channel->rcv_nxt - channel->rcv_out);
}

/* cumulative ack we report: the peer's FIN consumes one sequence slot */
static uint32_t hs_ack_out(const struct msc_udp_control_channel *channel)
{
   return channel->peer_fin ? channel->rcv_nxt + 1 : channel->rcv_nxt;
}

/* ===== datagram I/O ======================================================= */
static int hs_dgram_send(struct msc_udp_control_channel *channel, const uint8_t *frame, size_t len)
{
   if (hs_trace_on())
   {
      struct hs_hdr h;
      if (hs_hdr_decode(&h, frame, len) == 0)
         fprintf(stderr, "channel>%d TX f=%02x len=%u seq=%u ack=%u win=%u | "
                 "una=%u tx=%u nxt=%u rnxt=%u rout=%u pw=%u\n",
                 channel->fd, h.flags, h.length, h.seq, h.ack, h.win,
                 channel->snd_una, channel->snd_tx, channel->snd_nxt, channel->rcv_nxt,
                 channel->rcv_out, channel->peer_win);
   }
   if (channel->test_drop_frames > 0)
   {
      /* deterministic one-shot loss (tests): drop exactly the next N TX frames
       * so a fix that must retransmit a specific record is exercised without
       * depending on probabilistic MSC_UDP_CONTROL_DROP. */
      channel->test_drop_frames--;
      channel->drops_injected++;
      return 0;
   }
   if (hs_drop_pct() && (int)(rand_r(&channel->seed) % 100) < hs_drop_pct())
   {
      channel->drops_injected++;
      return 0;   /* injected loss: pretend it went out; retransmit recovers */
   }
   for (;;)
   {
      ssize_t s = channel->connected
         ? send(channel->fd, frame, len, 0)
         : sendto(channel->fd, frame, len, 0,
                  (const struct sockaddr *)&channel->peer, channel->peerlen);
      if (s >= 0)
      {
         channel->tx_frames++;
         return 0;
      }
      if (errno == EINTR)
         continue;
      if (errno == ECONNREFUSED)
      {
         /* ICMP port-unreachable bounced off a connected socket: fail fast */
         hs_die(channel, "control peer unreachable (connection refused)");
         return -1;
      }
      return 0;   /* ENOBUFS and friends: treated as a drop, RTO recovers */
   }
}

/* wait up to timeout_ms for one inbound datagram (0 = nonblocking look).
 * Returns len > 0, 0 on timeout, -1 once the channel is dead. In one mode the
 * demux owns the socket, so this pops the control queue instead of recv()ing. */
static int hs_dgram_recv(struct msc_udp_control_channel *channel, uint8_t *buf, size_t cap,
                         int timeout_ms)
{
   if (channel->receive_demux != NULL)
   {
      int n = msc_udp_receive_demux_pop(channel->receive_demux, -1, buf, cap, (long)timeout_ms * 1000, NULL);
      if (n < 0)
         hs_die(channel, "one-mode demux shut down under the control channel");
      return n;
   }
   for (;;)
   {
      ssize_t n;
      if (timeout_ms > 0)
      {
         struct pollfd p;
         int r;
         p.fd = channel->fd;
         p.events = POLLIN;
         r = poll(&p, 1, timeout_ms);
         if (r < 0 && errno == EINTR)
            continue;
         if (r <= 0)
            return 0;
      }
      n = recvfrom(channel->fd, buf, cap, MSG_DONTWAIT, NULL, NULL);
      if (n >= 0)
         return (int)n;
      if (errno == EINTR)
         continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK)
         return 0;
      if (errno == ECONNREFUSED)
      {
         hs_die(channel, "control peer unreachable (connection refused)");
         return -1;
      }
      return 0;
   }
}

/* ===== frame senders ====================================================== */
static int hs_send_ctl(struct msc_udp_control_channel *channel, uint16_t flags, uint32_t seq,
                       const void *payload, size_t plen)
{
   uint8_t frame[MSC_UDP_CONTROL_HDR_BYTES + MSC_UDP_CONTROL_MSS];
   struct hs_hdr h;
   h.conn_id = channel->conn_id;
   h.flags = flags;
   h.length = (uint16_t)plen;
   h.seq = seq;
   h.ack = hs_ack_out(channel);
   h.win = hs_adv_win(channel);
   hs_hdr_encode(frame, &h);
   if (plen > 0)
      memcpy(frame + MSC_UDP_CONTROL_HDR_BYTES, payload, plen);
   return hs_dgram_send(channel, frame, MSC_UDP_CONTROL_HDR_BYTES + plen);
}

static int hs_send_ack(struct msc_udp_control_channel *channel)
{
   return hs_send_ctl(channel, MSC_UDP_CONTROL_ACK, channel->snd_nxt, NULL, 0);
}

/* transmit whatever the peer's window allows from [snd_tx, snd_nxt) in
 * MSS-sized DATA frames (each piggybacks the current cumulative ack and
 * window), then the FIN once every data byte is out, and arm the retransmit
 * timer if anything is outstanding. When data is pending against a zero
 * window with nothing in flight, one segment is forced out anyway -- that IS
 * the zero-window probe: the peer drops what it has no room for and re-ACKs
 * with its current window, so the stream can never deadlock on a stale
 * window. */
static void hs_flush(struct msc_udp_control_channel *channel)
{
   if (channel->dead)
      return;
   for (;;)
   {
      uint32_t inflight = channel->snd_tx - channel->snd_una;
      uint32_t pending = channel->snd_nxt - channel->snd_tx;
      uint32_t allowed = channel->peer_win;
      uint32_t len, off, first;
      uint8_t frame[MSC_UDP_CONTROL_HDR_BYTES + MSC_UDP_CONTROL_MSS];
      struct hs_hdr h;

      if (pending == 0)
         break;
      if (allowed == 0 && inflight == 0)
         allowed = 1;              /* zero-window probe */
      if (inflight >= allowed)
         break;
      /* retire ACKed frames, then honor the in-flight datagram cap */
      while (channel->txr_count > 0 &&
             (int32_t)(channel->snd_una -
                       channel->txseq[channel->txr_head]) > 0)
      {
         channel->txr_head = (channel->txr_head + 1) %
                        (uint32_t)(sizeof(channel->txseq) / sizeof(channel->txseq[0]));
         channel->txr_count--;
      }
      if (channel->txr_count >= sizeof(channel->txseq) / sizeof(channel->txseq[0]))
         break;                    /* ACK clock reopens the flight */
      len = allowed - inflight;
      if (len > pending) len = pending;
      if (len > MSC_UDP_CONTROL_MSS) len = MSC_UDP_CONTROL_MSS;

      h.conn_id = channel->conn_id;
      h.flags = (uint16_t)(MSC_UDP_CONTROL_DATA | MSC_UDP_CONTROL_ACK);
      h.length = (uint16_t)len;
      h.seq = channel->snd_tx;
      h.ack = hs_ack_out(channel);
      h.win = hs_adv_win(channel);
      hs_hdr_encode(frame, &h);
      off = channel->snd_tx % MSC_UDP_CONTROL_WIN_BYTES;
      first = MSC_UDP_CONTROL_WIN_BYTES - off;
      if (first > len) first = len;
      memcpy(frame + MSC_UDP_CONTROL_HDR_BYTES, channel->sndring + off, first);
      if (first < len)
         memcpy(frame + MSC_UDP_CONTROL_HDR_BYTES + first, channel->sndring, len - first);
      if (hs_dgram_send(channel, frame, MSC_UDP_CONTROL_HDR_BYTES + len) != 0)
         return;
      channel->txseq[(channel->txr_head + channel->txr_count) %
                (uint32_t)(sizeof(channel->txseq) / sizeof(channel->txseq[0]))] = h.seq;
      channel->txr_count++;
      channel->snd_tx += len;
   }
   if (channel->fin_sent && !channel->fin_acked && channel->fin_tx_pending &&
       channel->snd_tx == channel->snd_nxt)
   {
      channel->fin_tx_pending = 0;
      hs_send_ctl(channel, (uint16_t)(MSC_UDP_CONTROL_FIN | MSC_UDP_CONTROL_ACK), channel->snd_nxt, NULL, 0);
   }
   if (channel->rto_deadline == 0 &&
       (channel->snd_una != channel->snd_tx || channel->snd_tx != channel->snd_nxt ||
        (channel->fin_sent && !channel->fin_acked)))
      channel->rto_deadline = msc_udp_now_ns() + channel->rto_ns;
}

/* ===== inbound frame processing =========================================== */
static void hs_input(struct msc_udp_control_channel *channel, const uint8_t *dgram, size_t n)
{
   struct hs_hdr h;
   const uint8_t *payload = dgram + MSC_UDP_CONTROL_HDR_BYTES;

   if (hs_hdr_decode(&h, dgram, n) != 0)
      return;
   if (h.conn_id != channel->conn_id)
      return;                      /* stale connection / stray sender */
   channel->rx_frames++;
   if (hs_trace_on())
      fprintf(stderr, "channel<%d RX f=%02x len=%u seq=%u ack=%u win=%u | "
              "una=%u tx=%u nxt=%u rnxt=%u rout=%u pw=%u\n",
              channel->fd, h.flags, h.length, h.seq, h.ack, h.win,
              channel->snd_una, channel->snd_tx, channel->snd_nxt, channel->rcv_nxt, channel->rcv_out,
              channel->peer_win);

   if (h.flags & MSC_UDP_CONTROL_RST)
   {
      char msg[sizeof(channel->reason)];
      if (h.length > 0)
      {
         size_t m = h.length < sizeof(msg) - 1 ? h.length : sizeof(msg) - 1;
         memcpy(msg, payload, m);
         msg[m] = '\0';
      }
      else
         snprintf(msg, sizeof(msg), "peer sent RST");
      hs_die(channel, msg);
      return;
   }

   if ((h.flags & MSC_UDP_CONTROL_SYN) && !(h.flags & MSC_UDP_CONTROL_ACK))
   {
      /* duplicate SYN: our SYN|ACK was lost -- repeat it */
      if (channel->responder)
      {
         uint8_t pay[2];
         pay[0] = MSC_UDP_CONTROL_VERSION;
         pay[1] = (uint8_t)msc_udp_ctl_mode();
         hs_send_ctl(channel, (uint16_t)(MSC_UDP_CONTROL_SYN | MSC_UDP_CONTROL_ACK), 0, pay, sizeof(pay));
      }
      return;
   }
   if ((h.flags & MSC_UDP_CONTROL_SYN) && (h.flags & MSC_UDP_CONTROL_ACK))
   {
      /* duplicate SYN|ACK: our final handshake ACK was lost -- repeat it */
      if (!channel->responder)
      {
         channel->peer_win = h.win;
         hs_send_ack(channel);
      }
      return;
   }

   channel->established = 1;

   if (h.flags & MSC_UDP_CONTROL_ACK)
   {
      uint32_t max_ack = channel->snd_nxt + (channel->fin_sent ? 1u : 0u);
      int32_t adv = (int32_t)(h.ack - channel->snd_una);
      int32_t lim = (int32_t)(max_ack - channel->snd_una);
      if (adv > 0 && adv <= lim)
      {
         uint32_t una = h.ack;
         if (channel->fin_sent && h.ack == max_ack)
         {
            channel->fin_acked = 1;
            una = channel->snd_nxt;
         }
         channel->snd_una = una;
         if ((int32_t)(channel->snd_tx - channel->snd_una) < 0)
            channel->snd_tx = channel->snd_una;
         channel->retries = 0;
         channel->rto_ns = (uint64_t)MSC_UDP_CONTROL_INIT_RTO_MS * 1000000ULL;
         channel->rto_deadline =
            (channel->snd_una != channel->snd_tx || channel->snd_tx != channel->snd_nxt ||
             (channel->fin_sent && !channel->fin_acked))
            ? msc_udp_now_ns() + channel->rto_ns : 0;
      }
      channel->peer_win = h.win;       /* every ack is also a window update */
   }

   if (h.flags & MSC_UDP_CONTROL_DATA)
   {
      if (h.seq == channel->rcv_nxt && h.length > 0)
      {
         uint32_t space = hs_adv_win(channel);
         uint32_t take = h.length < space ? h.length : space;
         uint32_t off = channel->rcv_nxt % MSC_UDP_CONTROL_WIN_BYTES;
         uint32_t first = MSC_UDP_CONTROL_WIN_BYTES - off;
         if (first > take) first = take;
         memcpy(channel->rcvring + off, payload, first);
         if (first < take)
            memcpy(channel->rcvring, payload + first, take - first);
         channel->rcv_nxt += take;
      }
      /* in-order, duplicate, or a gap: always (re)ACK so go-back-N converges
       * (a zero-length in-order DATA is the peer's window probe -- same reply) */
      hs_send_ack(channel);
   }

   if (h.flags & MSC_UDP_CONTROL_FIN)
   {
      if ((int32_t)(h.seq - channel->rcv_nxt) <= 0)
         channel->peer_fin = 1;         /* in-order (or duplicate) end of stream */
      hs_send_ack(channel);             /* acks rcv_nxt+1 once peer_fin is set;
                                    * a FIN beyond a hole just re-ACKs and
                                    * go-back-N resends the missing data */
   }
}

/* one bounded engine step: wait up to wait_ms for traffic (less if the RTO
 * fires sooner), process every queued frame, run the retransmit timer, then
 * transmit whatever the window allows. Returns -1 once the channel is dead. */
static int hs_service(struct msc_udp_control_channel *channel, int wait_ms)
{
   uint8_t dg[MSC_UDP_CONTROL_HDR_BYTES + MSC_UDP_CONTROL_MSS];
   uint64_t now;
   int n, t = wait_ms;

   if (channel->dead)
      return -1;
   now = msc_udp_now_ns();
   if (channel->rto_deadline != 0)
   {
      int64_t left = ((int64_t)(channel->rto_deadline - now)) / 1000000;
      if (left < 0) left = 0;
      if (left < t) t = (int)left;
   }
   n = hs_dgram_recv(channel, dg, sizeof(dg), t);
   while (n > 0)
   {
      hs_input(channel, dg, (size_t)n);
      n = hs_dgram_recv(channel, dg, sizeof(dg), 0);
   }
   if (n < 0 || channel->dead)
      return -1;

   if (channel->rto_deadline != 0 && now >= channel->rto_deadline)
   {
      if (++channel->retries > MSC_UDP_CONTROL_MAX_RETRIES)
      {
         hs_die(channel, "control peer unresponsive (retransmit limit), giving up");
         return -1;
      }
      channel->rtx_events++;
      channel->snd_tx = channel->snd_una;              /* go-back-N */
      channel->txr_head = 0;
      channel->txr_count = 0;   /* the rewound flight no longer occupies the cap */
      if (channel->fin_sent && !channel->fin_acked)
         channel->fin_tx_pending = 1;
      channel->rto_ns *= 2;
      if (channel->rto_ns > (uint64_t)MSC_UDP_CONTROL_MAX_RTO_MS * 1000000ULL)
         channel->rto_ns = (uint64_t)MSC_UDP_CONTROL_MAX_RTO_MS * 1000000ULL;
      channel->rto_deadline = 0;                  /* hs_flush re-arms it */
   }
   hs_flush(channel);
   return 0;
}

/* ===== byte-stream API (the msc_udp_send_all/msc_udp_recv_all contract) =============== */
int msc_udp_control_channel_send(struct msc_udp_control_channel *channel, const void *buf, size_t n)
{
   const uint8_t *p = buf;
   size_t done = 0;

   if (channel == NULL)
      return -1;
   if (channel->is_stdio)
   {
      while (done < n)
      {
         ssize_t w = write(channel->stdio_wfd, p + done, n - done);
         if (w < 0 && errno == EINTR)
            continue;
         if (w <= 0)
         {
            fprintf(stderr, "MSC UDP control channel send failed: %s\n",
                    w < 0 ? strerror(errno) : "ssh channel closed");
            return -1;
         }
         done += (size_t)w;
      }
      return 0;
   }
   while (done < n)
   {
      uint32_t space;
      uint32_t take, off, first;
      if (channel->dead)
         goto dead;
      space = MSC_UDP_CONTROL_WIN_BYTES - (channel->snd_nxt - channel->snd_una);
      if (space == 0)
      {
         /* window-full: block only here, pumping ACKs until room frees */
         if (hs_service(channel, 50) != 0)
            goto dead;
         continue;
      }
      take = (uint32_t)(n - done < space ? n - done : space);
      off = channel->snd_nxt % MSC_UDP_CONTROL_WIN_BYTES;
      first = MSC_UDP_CONTROL_WIN_BYTES - off;
      if (first > take) first = take;
      memcpy(channel->sndring + off, p + done, first);
      if (first < take)
         memcpy(channel->sndring, p + done + first, take - first);
      channel->snd_nxt += take;
      done += take;
      hs_flush(channel);
      if (hs_service(channel, 0) != 0)   /* opportunistic ACK drain, nonblocking */
         goto dead;
   }
   /* Never return with UNTRANSMITTED bytes: a corked tail would starve the
    * peer's blocking read of bytes that were never even attempted. Unacked
    * is fine here -- ordinary pipelining across many small records (e.g. the
    * manifest) relies on NOT stopping-and-waiting for each one's ACK, per the
    * plan's go-back-N design. The one place this is NOT enough -- the last
    * send before the caller goes silent on this control_channel for a long stretch
    * (greeting/manifest immediately followed by the data-flow threads, which
    * never touch the shim) -- is handled explicitly by msc_udp_control_channel_drain(),
    * called at those transition points; see its call sites. */
   while (channel->snd_tx != channel->snd_nxt)
      if (hs_service(channel, 5) != 0)
         goto dead;
   return 0;
dead:
   fprintf(stderr, "MSC UDP control channel send failed: %s\n",
           channel->reason[0] ? channel->reason : "connection lost");
   return -1;
}

/* Block until every byte handed to msc_udp_control_channel_send so far is ACKED (not
 * merely transmitted). Call this at the specific points where the caller is
 * about to go quiet on the control_channel for a long stretch -- immediately before
 * handing off to the raw-UDP data-flow threads, which never touch this shim
 * -- so a lost tail byte still gets its retransmissions instead of stranding
 * the peer's blocking read forever. Ordinary control traffic (the manifest,
 * HELLO, PORTS, ...) does NOT call this: those stay pipelined, each further
 * send/recv keeps servicing any earlier unacked bytes along the way. */
int msc_udp_control_channel_drain(struct msc_udp_control_channel *channel)
{
   if (channel == NULL)
      return 0;   /* TCP mode: the kernel already guarantees this */
   if (channel->is_stdio)
      return 0;   /* SSH/TCP guarantees delivery, same as tcp mode */
   while (channel->snd_una != channel->snd_nxt)
      if (hs_service(channel, 20) != 0)
      {
         fprintf(stderr, "MSC UDP control channel drain failed: %s\n",
                 channel->reason[0] ? channel->reason : "connection lost");
         return -1;
      }
   return 0;
}

/* Tests only: arm a deterministic one-shot drop of the next `frames` outgoing
 * shim frames, so a caller can force a specific record to be lost and confirm
 * the retransmit path recovers it. No-op on TCP/stdio (no shim). */
void msc_udp_control_channel_test_drop_next(struct msc_udp_control_channel *channel,
                                            uint32_t frames)
{
   if (channel != NULL && !channel->is_stdio)
      channel->test_drop_frames = frames;
}

int msc_udp_control_channel_recv(struct msc_udp_control_channel *channel, void *buf, size_t n)
{
   uint8_t *p = buf;
   size_t got = 0;

   if (channel == NULL)
      return -1;
   if (channel->is_stdio)
   {
      while (got < n)
      {
         ssize_t r = read(channel->fd, p + got, n - got);
         if (r < 0 && errno == EINTR)
            continue;
         if (r <= 0)
         {
            if (r < 0)
               fprintf(stderr, "MSC UDP control channel receive failed: %s\n",
                       strerror(errno));
            else
               fprintf(stderr, "MSC UDP control channel closed mid-record\n");
            return -1;
         }
         got += (size_t)r;
      }
      return 0;
   }
   while (got < n)
   {
      uint32_t avail = channel->rcv_nxt - channel->rcv_out;
      if (avail > 0)
      {
         int pinched = hs_adv_win(channel) < MSC_UDP_CONTROL_MSS;
         uint32_t take = (uint32_t)(n - got < avail ? n - got : avail);
         uint32_t off = channel->rcv_out % MSC_UDP_CONTROL_WIN_BYTES;
         uint32_t first = MSC_UDP_CONTROL_WIN_BYTES - off;
         if (first > take) first = take;
         memcpy(p + got, channel->rcvring + off, first);
         if (first < take)
            memcpy(p + got + first, channel->rcvring, take - first);
         channel->rcv_out += take;
         got += take;
         /* consuming just reopened a pinched window: tell the peer now
          * instead of making its zero-window probe wait out an RTO */
         if (pinched && hs_adv_win(channel) >= MSC_UDP_CONTROL_MSS)
            hs_send_ack(channel);
         continue;
      }
      if (channel->dead)
      {
         fprintf(stderr, "MSC UDP control channel receive failed: %s\n",
                 channel->reason[0] ? channel->reason : "connection lost");
         return -1;
      }
      if (channel->peer_fin)
      {
         fprintf(stderr, "MSC UDP control channel closed mid-record\n");
         return -1;
      }
      if (hs_service(channel, 100) != 0)
      {
         fprintf(stderr, "MSC UDP control channel receive failed: %s\n",
                 channel->reason[0] ? channel->reason : "connection lost");
         return -1;
      }
   }
   return 0;
}

int msc_udp_control_channel_wait_readable(struct msc_udp_control_channel *channel, int timeout_ms)
{
   uint64_t deadline;
   if (channel == NULL)
      return -1;
   if (channel->is_stdio)
   {
      struct pollfd p;
      int r;
      p.fd = channel->fd;
      p.events = POLLIN;
      do
         r = poll(&p, 1, timeout_ms);
      while (r < 0 && errno == EINTR);
      if (r < 0)
         return -1;
      /* POLLHUP = ssh channel gone: readable like TCP EOF, recv then errors */
      return r > 0 && (p.revents & (POLLIN | POLLHUP)) ? 1 : 0;
   }
   deadline = msc_udp_now_ns() +
              (uint64_t)(timeout_ms > 0 ? timeout_ms : 0) * 1000000ULL;
   for (;;)
   {
      uint64_t now;
      int left;
      if (channel->rcv_nxt != channel->rcv_out)
         return 1;
      if (channel->peer_fin)
         return 1;      /* like poll on TCP EOF: readable, recv then errors */
      if (channel->dead)
         return -1;
      now = msc_udp_now_ns();
      if (now >= deadline)
         return 0;
      left = (int)((deadline - now) / 1000000ULL);
      if (left < 1) left = 1;
      if (left > 50) left = 50;
      if (hs_service(channel, left) != 0)
         return -1;
   }
}

/* ===== rendezvous ========================================================= */
int msc_udp_control_channel_bind_range(unsigned int base, unsigned int tries,
                           unsigned int *actual)
{
   struct sockaddr_in addr;
   unsigned int i;

   if (tries == 0)
      tries = 1;
   memset(&addr, 0, sizeof(addr));
   addr.sin_family = AF_INET;
   addr.sin_addr.s_addr = INADDR_ANY;

   for (i = 0; i < tries; i++)
   {
      unsigned int p = base == 0 ? 0 : base + i;
      int fd;
      if (base > 0 && p > 65535)
         break;
      fd = socket(AF_INET, SOCK_DGRAM, 0);
      if (fd < 0)
      {
         perror("MSC UDP: control socket");
         return -1;
      }
      /* EXCLUSIVE bind -- deliberately no SO_REUSEADDR. TCP leaned on
       * listen() to reject a second binder on the same port; UDP has no such
       * gate, so the plain-bind EADDRINUSE is what advances a second receiver
       * to the next port and keeps the one-receiver-per-port guarantee. */
      addr.sin_port = htons((uint16_t)p);
      if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) == 0)
      {
         if (base == 0)
         {
            socklen_t alen = sizeof(addr);
            if (getsockname(fd, (struct sockaddr *)&addr, &alen) != 0)
            {
               perror("MSC UDP: control getsockname");
               close(fd);
               return -1;
            }
            *actual = ntohs(addr.sin_port);
         }
         else
            *actual = p;
         return fd;
      }
      close(fd);
      if (base == 0)
         break;
   }
   fprintf(stderr, "MSC UDP: could not bind any control port in %u..%u\n",
           base, base + tries - 1);
   return -1;
}

/* deterministic refusal: RST carries the reason so the sender prints the same
 * message, and dup SYNs are re-RSTed briefly in case the first RST is lost.
 * RSTs bypass MSC_UDP_CONTROL_DROP so a mismatch under loss injection stays prompt. */
static void hs_reject(int fd, const struct sockaddr_storage *src,
                      socklen_t srclen, uint32_t conn_id, const char *msg)
{
   uint8_t frame[MSC_UDP_CONTROL_HDR_BYTES + MSC_UDP_CONTROL_MSS];
   struct hs_hdr h;
   size_t mlen = strlen(msg);
   uint64_t lend = msc_udp_now_ns() + 700ULL * 1000 * 1000;

   if (mlen > MSC_UDP_CONTROL_MSS)
      mlen = MSC_UDP_CONTROL_MSS;
   h.conn_id = conn_id;
   h.flags = MSC_UDP_CONTROL_RST;
   h.length = (uint16_t)mlen;
   h.seq = 0;
   h.ack = 0;
   h.win = 0;
   hs_hdr_encode(frame, &h);
   memcpy(frame + MSC_UDP_CONTROL_HDR_BYTES, msg, mlen);
   sendto(fd, frame, MSC_UDP_CONTROL_HDR_BYTES + mlen, 0,
          (const struct sockaddr *)src, srclen);
   while (msc_udp_now_ns() < lend)
   {
      struct pollfd p;
      uint8_t dg[MSC_UDP_CONTROL_HDR_BYTES + MSC_UDP_CONTROL_MSS];
      struct hs_hdr in;
      ssize_t n;
      p.fd = fd;
      p.events = POLLIN;
      if (poll(&p, 1, 100) <= 0)
         continue;
      n = recvfrom(fd, dg, sizeof(dg), MSG_DONTWAIT, NULL, NULL);
      if (n > 0 && hs_hdr_decode(&in, dg, (size_t)n) == 0 &&
          (in.flags & MSC_UDP_CONTROL_SYN) && in.conn_id == conn_id)
         sendto(fd, frame, MSC_UDP_CONTROL_HDR_BYTES + mlen, 0,
                (const struct sockaddr *)src, srclen);
   }
}

int msc_udp_control_channel_accept(int fd, int timeout_ms)
{
   int my_mode = msc_udp_ctl_mode();
   uint64_t deadline = msc_udp_now_ns() + (uint64_t)timeout_ms * 1000000ULL;

   for (;;)
   {
      uint8_t dg[MSC_UDP_CONTROL_HDR_BYTES + MSC_UDP_CONTROL_MSS];
      struct sockaddr_storage src;
      socklen_t srclen = sizeof(src);
      struct hs_hdr h;
      struct pollfd p;
      uint64_t now = msc_udp_now_ns();
      ssize_t n;
      int wait, ver, mode;

      if (now >= deadline)
      {
         fprintf(stderr, "MSC UDP: no sender connected within %d s\n",
                 timeout_ms / 1000);
         return -1;
      }
      wait = (int)((deadline - now) / 1000000ULL);
      if (wait > 500) wait = 500;
      if (wait < 1) wait = 1;
      p.fd = fd;
      p.events = POLLIN;
      if (poll(&p, 1, wait) <= 0)
         continue;
      n = recvfrom(fd, dg, sizeof(dg), MSG_DONTWAIT,
                   (struct sockaddr *)&src, &srclen);
      if (n < 0)
         continue;
      if (hs_hdr_decode(&h, dg, (size_t)n) != 0)
         continue;                 /* stray junk on the fixed port: not ours */
      if (!(h.flags & MSC_UDP_CONTROL_SYN) || (h.flags & MSC_UDP_CONTROL_ACK) || h.length < 2)
         continue;                 /* only a fresh SYN opens a connection */

      ver = dg[MSC_UDP_CONTROL_HDR_BYTES];
      mode = dg[MSC_UDP_CONTROL_HDR_BYTES + 1];
      if (ver != MSC_UDP_CONTROL_VERSION || mode != my_mode)
      {
         char msg[128];
         if (ver != MSC_UDP_CONTROL_VERSION)
            snprintf(msg, sizeof(msg), "control shim version mismatch: "
                     "sender v%d, receiver v%d -- rebuild both ends",
                     ver, MSC_UDP_CONTROL_VERSION);
         else
            snprintf(msg, sizeof(msg), "control mode mismatch: sender=%s, "
                     "receiver=%s -- set MSC_UDP_CTL to match on both ends",
                     msc_udp_ctl_mode_name(mode), msc_udp_ctl_mode_name(my_mode));
         fprintf(stderr, "MSC UDP receiver: %s\n", msg);
         hs_reject(fd, &src, srclen, h.conn_id, msg);
         return -1;
      }

      {
         struct msc_udp_control_channel *channel = hs_new(fd);
         uint8_t pay[2];
         channel->responder = 1;
         channel->conn_id = h.conn_id;
         memcpy(&channel->peer, &src, srclen);
         channel->peerlen = srclen;
         channel->peer_win = h.win;
         if (my_mode == MSC_UDP_CTLMODE_MANY)
         {
            /* a dedicated control socket has exactly one peer: connect it so
             * send() works and ICMP unreachable fails fast. The one-mode
             * shared socket must stay UNCONNECTED -- acknowledgments and control
             * both sendto() the learned sender address instead. */
            if (connect(fd, (struct sockaddr *)&src, srclen) == 0)
               channel->connected = 1;
         }
         pay[0] = MSC_UDP_CONTROL_VERSION;
         pay[1] = (uint8_t)my_mode;
         hs_send_ctl(channel, (uint16_t)(MSC_UDP_CONTROL_SYN | MSC_UDP_CONTROL_ACK), 0, pay, sizeof(pay));
         channel->established = 1;      /* dup SYNs re-elicit the SYN|ACK */
         hs_register(channel);
         return fd;
      }
   }
}

int msc_udp_control_channel_connect(const char *host, unsigned int port)
{
   struct sockaddr_in addr;
   struct hostent *server = gethostbyname(host);
   struct msc_udp_control_channel *channel;
   uint8_t pay[2];
   uint64_t deadline, next_syn;
   int fd;

   if (server == NULL)
   {
      fprintf(stderr, "MSC UDP: no such host %s\n", host);
      exit(MSC_EXIT_NETWORK);
   }
   fd = socket(AF_INET, SOCK_DGRAM, 0);
   if (fd < 0)
   {
      perror("MSC UDP control channel socket");
      exit(MSC_EXIT_INTERNAL);
   }
   memset(&addr, 0, sizeof(addr));
   addr.sin_family = AF_INET;
   memcpy(&addr.sin_addr.s_addr, server->h_addr, (size_t)server->h_length);
   addr.sin_port = htons((uint16_t)port);
   /* the initiator always has exactly one peer: connect in both modes (in
    * `one` mode this is the sender's single shared data+control socket) */
   if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
   {
      fprintf(stderr, "MSC UDP: control channel connect to %s:%u failed: %s\n",
              host, port, strerror(errno));
      exit(MSC_EXIT_NETWORK);
   }

   channel = hs_new(fd);
   channel->connected = 1;
   memcpy(&channel->peer, &addr, sizeof(addr));
   channel->peerlen = sizeof(addr);
   channel->conn_id = ((uint32_t)rand_r(&channel->seed) << 16) ^
                 (uint32_t)rand_r(&channel->seed) ^ (uint32_t)getpid();
   if (channel->conn_id == 0)
      channel->conn_id = 1;

   pay[0] = MSC_UDP_CONTROL_VERSION;
   pay[1] = (uint8_t)msc_udp_ctl_mode();
   deadline = msc_udp_now_ns() +
              (uint64_t)MSC_UDP_CONTROL_CONNECT_TIMEOUT_MS * 1000000ULL;
   hs_send_ctl(channel, MSC_UDP_CONTROL_SYN, 0, pay, sizeof(pay));
   next_syn = msc_udp_now_ns() + channel->rto_ns;

   for (;;)
   {
      uint8_t dg[MSC_UDP_CONTROL_HDR_BYTES + MSC_UDP_CONTROL_MSS];
      struct hs_hdr h;
      uint64_t now = msc_udp_now_ns();
      int64_t left;
      int n, wait;

      if (now >= deadline)
      {
         fprintf(stderr, "MSC UDP: control channel connect to %s:%u failed: no answer in "
                 "%d s (receiver down, port blocked, or MSC_UDP_CTL mismatch -- a "
                 "tcp-mode receiver cannot answer a UDP control handshake)\n",
                 host, port, MSC_UDP_CONTROL_CONNECT_TIMEOUT_MS / 1000);
         exit(MSC_EXIT_NETWORK);
      }
      left = (int64_t)(next_syn > deadline ? deadline - now : next_syn - now);
      wait = (int)(left / 1000000);
      if (wait < 1) wait = 1;
      n = hs_dgram_recv(channel, dg, sizeof(dg), wait);
      if (n < 0)
      {
         /* ICMP port-unreachable: nothing is listening there at all */
         fprintf(stderr, "MSC UDP: control channel connect to %s:%u failed: %s\n",
                 host, port, channel->reason);
         exit(MSC_EXIT_NETWORK);
      }
      if (n > 0 && hs_hdr_decode(&h, dg, (size_t)n) == 0 &&
          h.conn_id == channel->conn_id)
      {
         if (h.flags & MSC_UDP_CONTROL_RST)
         {
            char msg[sizeof(channel->reason)];
            if (h.length > 0)
            {
               size_t m = h.length < sizeof(msg) - 1 ? h.length
                                                     : sizeof(msg) - 1;
               memcpy(msg, dg + MSC_UDP_CONTROL_HDR_BYTES, m);
               msg[m] = '\0';
            }
            else
               snprintf(msg, sizeof(msg), "connection refused (RST)");
            /* RST is the receiver rejecting our control mode: a
             * configuration mismatch, not a transient network fault. */
            fprintf(stderr, "MSC UDP: control channel connect to %s:%u refused: %s\n",
                    host, port, msg);
            exit(MSC_EXIT_INCOMPATIBLE);
         }
         if ((h.flags & MSC_UDP_CONTROL_SYN) && (h.flags & MSC_UDP_CONTROL_ACK))
         {
            channel->peer_win = h.win;
            channel->established = 1;
            hs_send_ack(channel);
            hs_register(channel);
            return fd;
         }
      }
      if (msc_udp_now_ns() >= next_syn)
      {
         hs_send_ctl(channel, MSC_UDP_CONTROL_SYN, 0, pay, sizeof(pay));
         channel->rto_ns *= 2;
         if (channel->rto_ns > (uint64_t)MSC_UDP_CONTROL_MAX_RTO_MS * 1000000ULL)
            channel->rto_ns = (uint64_t)MSC_UDP_CONTROL_MAX_RTO_MS * 1000000ULL;
         next_syn = msc_udp_now_ns() + channel->rto_ns;
      }
   }
}

/* Wrap an already-connected pair of byte streams as a control channel. Both
 * ends of a stdio-mode session ride the SSH session the CLI already opened, so
 * there is no rendezvous and no listener: the receiver passes its own
 * stdin/stdout, the sender passes its ends of the pipes it spawned ssh with.
 * Both fds are duplicated, so the caller keeps ownership of the originals. */
int msc_udp_control_channel_pipe(int rfd_in, int wfd_in)
{
   struct msc_udp_control_channel *channel;
   int rfd, wfd;
   if (rfd_in < 0 || wfd_in < 0)
   {
      fprintf(stderr, "MSC UDP control channel: invalid stdio fds\n");
      return -1;
   }
   rfd = dup(rfd_in);
   wfd = dup(wfd_in);
   if (rfd < 0 || wfd < 0)
   {
      perror("MSC UDP control channel: dup stdio");
      if (rfd >= 0) close(rfd);
      if (wfd >= 0) close(wfd);
      return -1;
   }
   channel = hs_new(rfd);
   channel->is_stdio = 1;
   channel->stdio_wfd = wfd;
   channel->established = 1;
   hs_register(channel);
   return rfd;
}

int msc_udp_control_channel_stdio(void)
{
   return msc_udp_control_channel_pipe(STDIN_FILENO, STDOUT_FILENO);
}

void msc_udp_control_channel_close(struct msc_udp_control_channel *channel)
{
   if (channel == NULL)
      return;
   if (channel->is_stdio)
   {
      /* no FIN machinery: SSH/TCP closes reliably on its own. The write dup
       * is ours to close; the read dup is the caller's controlfd and is
       * closed there exactly once, same ownership rule as every other mode. */
      close(channel->stdio_wfd);
      hs_unregister(channel);
      free(channel);
      return;
   }
   if (channel->established && !channel->dead)
   {
      /* Phase 1 of the two-phase shutdown: run the FIN/ACK exchange while the
       * transport (and, in one mode, the demux) is still alive. FIN is a
       * courtesy -- the app-level end markers already delimit every exchange
       * -- so the budget is small and the whole close is bounded (~1s worst
       * case when the peer is already gone). */
      uint64_t fin_deadline = msc_udp_now_ns() + 1000ULL * 1000 * 1000;
      uint64_t linger;
      channel->fin_sent = 1;
      channel->fin_tx_pending = 1;
      channel->retries = 0;
      channel->rto_ns = (uint64_t)MSC_UDP_CONTROL_INIT_RTO_MS * 1000000ULL;
      channel->rto_deadline = 0;
      hs_flush(channel);
      while (!channel->fin_acked && !channel->dead && msc_udp_now_ns() < fin_deadline)
         if (hs_service(channel, 25) != 0)
            break;
      /* mini TIME_WAIT: briefly re-ACK duplicate FINs so the peer's own close
       * does not have to wait out its retransmit budget */
      linger = msc_udp_now_ns() +
               (channel->peer_fin ? 30ULL : 120ULL) * 1000 * 1000;
      while (!channel->dead && msc_udp_now_ns() < linger)
         if (hs_service(channel, 10) != 0)
            break;
   }
   if (hs_stats_on())
      fprintf(stderr, "MSC UDP control channel: fd %d tx %llu rx %llu rtx %llu "
              "hsdrop %llu (%s)\n", channel->fd,
              (unsigned long long)channel->tx_frames,
              (unsigned long long)channel->rx_frames,
              (unsigned long long)channel->rtx_events,
              (unsigned long long)channel->drops_injected,
              channel->dead ? channel->reason : "clean close");
   hs_unregister(channel);
   free(channel);
}

/* ===== one-mode RX demux ==================================================
 * One thread per side owns EVERY read on the shared socket from right after
 * the handshake until teardown: control frames, data packets, acknowledgments, and
 * PMTUD probes all flow through here into bounded queues. Enqueues never
 * block -- a full flow queue drops the datagram (the data reliability layer
 * retransmissions it; a dropped acknowledgment is superseded by the next one) and counts the
 * drop so a pathological rate is visible under MSC_UDP_STATS, not silent. The
 * control queue is sized so it effectively never drops (control is low-rate
 * and small; the shim's retransmit is the last-resort backstop). */

#define DMX_BATCH      16        /* recvmmsg slots per wakeup */
#define DMX_SLOT_BYTES 65536     /* >= max UDP datagram / UDP_GRO super-datagram */
#define DMX_CTLQ_CAP   512
#define DMX_FLOWQ_CAP  2048      /* per-flow datagram ring */

struct dmx_slot
{
   uint32_t len;
   uint32_t cap;
   unsigned char tos;            /* per-datagram IP TOS byte (ECN CE marks) */
   uint8_t *data;                /* grown to fit, then reused across wraps */
};

struct dmx_queue
{
   pthread_mutex_t mu;
   pthread_cond_t cv;
   struct dmx_slot *ring;
   uint32_t cap, head, tail;     /* count = tail - head */
   uint64_t enq, drops, maxdepth;
   int eof;
};

struct msc_udp_receive_demux
{
   int fd;
   int nflows;        /* TOTAL flows in the transfer (global flow_id space) */
   int qbase, qstride; /* this demux owns flows where f % qstride == qbase */
   int nq;            /* owned-flow count = queues allocated */
   pthread_t tid;
   int started;
   atomic_int stop;   /* plain volatile has no C11 cross-thread ordering guarantee */
   struct sockaddr_storage peer; /* sender addr: acknowledgments + control sendto it */
   socklen_t peerlen;
   int have_peer;
   struct dmx_queue ctlq;
   struct dmx_queue *flowq;      /* nq entries; flow f lives at f / qstride */
   uint64_t rx_dgrams, gro_slots, gro_segs, drop_unroutable;
};

/* global flow_id -> queue index, or -1 if this demux does not own the flow */
static int dmx_qidx(const struct msc_udp_receive_demux *d, uint32_t flow)
{
   if (flow >= (uint32_t)d->nflows || (int)(flow % (uint32_t)d->qstride) != d->qbase)
      return -1;
   return (int)(flow / (uint32_t)d->qstride);
}

static void dmxq_init(struct dmx_queue *q, uint32_t cap)
{
   pthread_condattr_t ca;
   memset(q, 0, sizeof(*q));
   q->ring = msc_udp_alloc(sizeof(*q->ring) * cap, "demux queue ring");
   memset(q->ring, 0, sizeof(*q->ring) * cap);
   q->cap = cap;
   pthread_mutex_init(&q->mu, NULL);
   pthread_condattr_init(&ca);
   pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
   pthread_cond_init(&q->cv, &ca);
   pthread_condattr_destroy(&ca);
}

static void dmxq_free(struct dmx_queue *q)
{
   uint32_t i;
   for (i = 0; i < q->cap; i++)
      free(q->ring[i].data);
   free(q->ring);
   pthread_mutex_destroy(&q->mu);
   pthread_cond_destroy(&q->cv);
}

static void dmxq_push(struct dmx_queue *q, const uint8_t *p, size_t n,
                      unsigned char tos)
{
   struct dmx_slot *s;
   uint32_t depth;
   pthread_mutex_lock(&q->mu);
   depth = q->tail - q->head;
   if (depth >= q->cap)
   {
      q->drops++;                /* never block: drop and let RTO/acknowledgments recover */
      pthread_mutex_unlock(&q->mu);
      return;
   }
   s = &q->ring[q->tail % q->cap];
   if (s->cap < n)
   {
      free(s->data);
      s->cap = n < 2048 ? 2048 : (uint32_t)n;
      s->data = msc_udp_alloc(s->cap, "demux slot");
   }
   memcpy(s->data, p, n);
   s->len = (uint32_t)n;
   s->tos = tos;
   q->tail++;
   q->enq++;
   if (depth + 1 > q->maxdepth)
      q->maxdepth = depth + 1;
   pthread_cond_signal(&q->cv);
   pthread_mutex_unlock(&q->mu);
}

static int dmxq_pop(struct dmx_queue *q, void *buf, size_t cap,
                    long timeout_us, unsigned char *tos_out)
{
   struct dmx_slot *s;
   uint32_t n;
   pthread_mutex_lock(&q->mu);
   if (q->head == q->tail && timeout_us > 0 && !q->eof)
   {
      struct timespec ts;
      clock_gettime(CLOCK_MONOTONIC, &ts);
      ts.tv_sec += timeout_us / 1000000;
      ts.tv_nsec += (timeout_us % 1000000) * 1000L;
      if (ts.tv_nsec >= 1000000000L)
      {
         ts.tv_sec++;
         ts.tv_nsec -= 1000000000L;
      }
      while (q->head == q->tail && !q->eof)
         if (pthread_cond_timedwait(&q->cv, &q->mu, &ts) == ETIMEDOUT)
            break;
   }
   if (q->head == q->tail)
   {
      int r = q->eof ? -1 : 0;
      pthread_mutex_unlock(&q->mu);
      return r;
   }
   s = &q->ring[q->head % q->cap];
   n = s->len < cap ? s->len : (uint32_t)cap;
   memcpy(buf, s->data, n);
   if (tos_out != NULL)
      *tos_out = s->tos;
   q->head++;
   pthread_mutex_unlock(&q->mu);
   return (int)n;
}

static void dmxq_eof(struct dmx_queue *q)
{
   pthread_mutex_lock(&q->mu);
   q->eof = 1;
   pthread_cond_broadcast(&q->cv);
   pthread_mutex_unlock(&q->mu);
}

/* route one datagram (or one GRO segment) by its leading magic. Packet,
 * acknowledgment, and probe headers all carry flow_id at byte offset 8. */
static void dmx_route(struct msc_udp_receive_demux *d, const uint8_t *p, size_t n,
                      unsigned char tos)
{
   uint32_t magic;
   if (n < 4)
   {
      d->drop_unroutable++;
      return;
   }
   magic = get32(p);
   if (magic == MSC_UDP_CONTROL_MAGIC)
   {
      dmxq_push(&d->ctlq, p, n, tos);
      return;
   }
   if (magic == MSC_UDP_MAGIC && n >= 12)
   {
      int q = dmx_qidx(d, get32(p + 8));
      if (q >= 0)
      {
         dmxq_push(&d->flowq[q], p, n, tos);
         return;
      }
   }
   d->drop_unroutable++;
}

static void *dmx_thread(void *arg)
{
   struct msc_udp_receive_demux *d = arg;
   size_t ctllen = CMSG_SPACE(sizeof(int)) + CMSG_SPACE(sizeof(int));
   uint8_t *bufs = msc_udp_alloc((size_t)DMX_BATCH * DMX_SLOT_BYTES, "demux bufs");
   struct iovec *iov = msc_udp_alloc(sizeof(struct iovec) * DMX_BATCH, "demux iov");
   struct mmsghdr *msgs = msc_udp_alloc(sizeof(struct mmsghdr) * DMX_BATCH,
                                      "demux msgs");
   struct sockaddr_storage *names =
      msc_udp_alloc(sizeof(struct sockaddr_storage) * DMX_BATCH, "demux names");
   char *ctl = msc_udp_alloc(ctllen * DMX_BATCH, "demux cmsg");
   int i;

   for (i = 0; i < DMX_BATCH; i++)
   {
      iov[i].iov_base = bufs + (size_t)i * DMX_SLOT_BYTES;
      iov[i].iov_len = DMX_SLOT_BYTES;
      memset(&msgs[i].msg_hdr, 0, sizeof(msgs[i].msg_hdr));
      msgs[i].msg_hdr.msg_iov = &iov[i];
      msgs[i].msg_hdr.msg_iovlen = 1;
      msgs[i].msg_hdr.msg_name = &names[i];
      msgs[i].msg_hdr.msg_namelen = sizeof(names[i]);
      msgs[i].msg_hdr.msg_control = ctl + (size_t)i * ctllen;
   }

   while (!atomic_load(&d->stop))
   {
      struct pollfd p;
      int got, k;
      int r;
      p.fd = d->fd;
      p.events = POLLIN;
      r = poll(&p, 1, 20);   /* short slices so stop is honored promptly */
      if (r < 0 && errno != EINTR)
         break;
      if (r <= 0 || !(p.revents & POLLIN))
         continue;
      for (k = 0; k < DMX_BATCH; k++)
      {
         /* the kernel shrinks these to what it wrote: re-arm every batch */
         msgs[k].msg_hdr.msg_controllen = ctllen;
         msgs[k].msg_hdr.msg_namelen = sizeof(names[k]);
      }
      got = recvmmsg(d->fd, msgs, DMX_BATCH, MSG_DONTWAIT, NULL);
      if (got < 0)
      {
         if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK ||
             errno == ECONNREFUSED)
            continue;   /* ECONNREFUSED: the flows detect peer death themselves */
         fprintf(stderr, "MSC UDP demux: recv failed: %s\n", strerror(errno));
         break;
      }
      for (k = 0; k < got; k++)
      {
         const uint8_t *slot = bufs + (size_t)k * DMX_SLOT_BYTES;
         size_t total = msgs[k].msg_len;
         size_t stride = total;
         unsigned char tos = 0;
         struct cmsghdr *c;
         size_t pos;

         d->rx_dgrams++;
         if (!d->have_peer && msgs[k].msg_hdr.msg_namelen > 0)
         {
            /* all flows share one sender peer: stash the source once so the
             * receiver's acknowledgments have a sendto target */
            memcpy(&d->peer, &names[k], msgs[k].msg_hdr.msg_namelen);
            d->peerlen = msgs[k].msg_hdr.msg_namelen;
            d->have_peer = 1;
         }
         for (c = CMSG_FIRSTHDR(&msgs[k].msg_hdr); c != NULL;
              c = CMSG_NXTHDR(&msgs[k].msg_hdr, c))
         {
            if (c->cmsg_level == SOL_UDP && c->cmsg_type == UDP_GRO)
               stride = (size_t)(*(int *)CMSG_DATA(c));
            if (c->cmsg_level == IPPROTO_IP && c->cmsg_type == IP_TOS)
               tos = *(const unsigned char *)CMSG_DATA(c);
         }
         if (stride == 0)
            stride = total;
         if (total > 0 && stride < total)
         {
            /* UDP_GRO coalesced several same-sized packets -- possibly from
             * DIFFERENT flows now that they share one socket -- so split the
             * super-datagram and route each segment on its own */
            d->gro_slots++;
            d->gro_segs += (total + stride - 1) / stride;
         }
         for (pos = 0; pos < total; pos += stride)
         {
            size_t seg = total - pos;
            if (seg > stride)
               seg = stride;
            dmx_route(d, slot + pos, seg, tos);
         }
      }
   }

   dmxq_eof(&d->ctlq);
   for (i = 0; i < d->nq; i++)
      dmxq_eof(&d->flowq[i]);
   free(bufs);
   free(iov);
   free(msgs);
   free(names);
   free(ctl);
   return NULL;
}

struct msc_udp_receive_demux *msc_udp_receive_demux_start(int fd, int nflows, int qbase, int qstride,
                              struct msc_udp_control_channel *channel)
{
   struct msc_udp_receive_demux *d = msc_udp_alloc(sizeof(*d), "demux");
   int f;
   if (qstride < 1 || qbase < 0 || qbase >= qstride || nflows <= qbase)
   {
      fprintf(stderr, "MSC UDP demux: bad stride %d/%d for %d flows\n",
              qbase, qstride, nflows);
      exit(MSC_EXIT_INTERNAL);
   }
   memset(d, 0, sizeof(*d));
   d->fd = fd;
   d->nflows = nflows;
   d->qbase = qbase;
   d->qstride = qstride;
   d->nq = (nflows - qbase + qstride - 1) / qstride;
   dmxq_init(&d->ctlq, DMX_CTLQ_CAP);
   d->flowq = msc_udp_alloc(sizeof(*d->flowq) * (size_t)d->nq, "demux flow queues");
   for (f = 0; f < d->nq; f++)
      dmxq_init(&d->flowq[f], DMX_FLOWQ_CAP);
   if (channel != NULL && channel->peerlen > 0)
   {
      memcpy(&d->peer, &channel->peer, sizeof(d->peer));
      d->peerlen = channel->peerlen;
      d->have_peer = 1;
   }
   if (pthread_create(&d->tid, NULL, dmx_thread, d) != 0)
   {
      fprintf(stderr, "MSC UDP demux: cannot spawn the RX thread\n");
      exit(MSC_EXIT_INTERNAL);
   }
   d->started = 1;
   /* from this point the demux owns every read on fd; the shim's inbound
    * switches to the control queue (its sends still go straight out) */
   if (channel != NULL)
      channel->receive_demux = d;
   return d;
}

int msc_udp_receive_demux_pop(struct msc_udp_receive_demux *d, int flow, void *buf, size_t cap,
                long timeout_us, unsigned char *tos_out)
{
   struct dmx_queue *q;
   if (d == NULL)
      return -1;
   if (flow < 0)
      q = &d->ctlq;
   else
   {
      int idx = dmx_qidx(d, (uint32_t)flow);
      if (idx < 0)
         return -1;
      q = &d->flowq[idx];
   }
   return dmxq_pop(q, buf, cap, timeout_us, tos_out);
}

int msc_udp_receive_demux_peer(struct msc_udp_receive_demux *d, struct sockaddr_storage *ss, socklen_t *len)
{
   if (d == NULL || !d->have_peer)
      return -1;
   memcpy(ss, &d->peer, sizeof(*ss));
   *len = d->peerlen;
   return 0;
}

void msc_udp_receive_demux_stop(struct msc_udp_receive_demux *d)
{
   int f;
   if (d == NULL)
      return;
   atomic_store(&d->stop, 1);
   if (d->started)
      pthread_join(d->tid, NULL);
   if (hs_stats_on())
   {
      fprintf(stderr, "MSC UDP demux[%d/%d]: dgrams %llu gro_slots %llu gro_segs %llu "
              "unroutable %llu | ctlq enq %llu drop %llu maxdepth %llu\n",
              d->qbase, d->qstride,
              (unsigned long long)d->rx_dgrams,
              (unsigned long long)d->gro_slots,
              (unsigned long long)d->gro_segs,
              (unsigned long long)d->drop_unroutable,
              (unsigned long long)d->ctlq.enq,
              (unsigned long long)d->ctlq.drops,
              (unsigned long long)d->ctlq.maxdepth);
      for (f = 0; f < d->nq; f++)
         if (d->flowq[f].enq > 0 || d->flowq[f].drops > 0)
            fprintf(stderr, "MSC UDP demux flow %d: enq %llu drop %llu "
                    "maxdepth %llu\n", d->qbase + f * d->qstride,
                    (unsigned long long)d->flowq[f].enq,
                    (unsigned long long)d->flowq[f].drops,
                    (unsigned long long)d->flowq[f].maxdepth);
   }
   dmxq_free(&d->ctlq);
   for (f = 0; f < d->nq; f++)
      dmxq_free(&d->flowq[f]);
   free(d->flowq);
   free(d);
}
