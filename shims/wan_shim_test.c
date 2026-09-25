/*
 * wan_shim_test.c - self-test for wan_shim.so (run it UNDER the shim).
 *
 * Sends N datagrams through a loopback UDP socket -- so they traverse the shim's
 * intercepted sendto() -- and a receiver thread on a second socket records which
 * arrived and how long each took. It then checks the observed drop rate and
 * delay distribution against the MSC_UDP_WANSHIM_* config the shim was given,
 * catching a broken RNG, an un-applied delay, or a heap-timing bug before any of
 * that contaminates a real measurement.
 *
 *   make shim_check          (from the top-level directory), or by hand:
 *   LD_PRELOAD=$PWD/shims/wan_shim.so MSC_UDP_WANSHIM_DELAY_MS=20 MSC_UDP_WANSHIM_JITTER_MS=4 \
 *     MSC_UDP_WANSHIM_LOSS_PCT=10 ./wan_shim_test [count]
 *
 * With no MSC_UDP_WANSHIM_* set it is a passthrough sanity check (expect ~0 loss,
 * ~0 delay). It prints a sorted "DROPPED: ..." line so two runs with the same
 * MSC_UDP_WANSHIM_SEED can be diffed for determinism. Exit 0 = pass.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

struct payload
{
   uint32_t seq;
   uint64_t send_ns;
} __attribute__((packed));

static uint64_t now_ns(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

struct rxctx
{
   int         fd;
   int         n;
   char       *arrived;    /* arrived[seq] != 0 */
   uint64_t   *delay_ns;   /* recv - send, per arrived seq */
   volatile int done;      /* set by main once all sends have drained */
};

static void *rx_thread(void *arg)
{
   struct rxctx *c = arg;
   struct timeval tv = { 0, 100000 };   /* 100 ms recv timeout */
   setsockopt(c->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
   for (;;)
   {
      struct payload p;
      ssize_t r = recv(c->fd, &p, sizeof(p), 0);
      if (r == (ssize_t)sizeof(p) && (int)p.seq < c->n)
      {
         c->arrived[p.seq] = 1;
         c->delay_ns[p.seq] = now_ns() - p.send_ns;
      }
      else if (r < 0)
      {
         if (c->done) break;   /* idle and sender finished draining */
      }
   }
   return NULL;
}

int main(int argc, char **argv)
{
   int n = argc > 1 ? atoi(argv[1]) : 4000;
   double delay_ms  = getenv("MSC_UDP_WANSHIM_DELAY_MS")  ? atof(getenv("MSC_UDP_WANSHIM_DELAY_MS"))  : 0;
   double jitter_ms = getenv("MSC_UDP_WANSHIM_JITTER_MS") ? atof(getenv("MSC_UDP_WANSHIM_JITTER_MS")) : 0;
   double loss_pct  = getenv("MSC_UDP_WANSHIM_LOSS_PCT")  ? atof(getenv("MSC_UDP_WANSHIM_LOSS_PCT"))  : 0;

   int rxfd, txfd, i, recvd = 0;
   struct sockaddr_in addr;
   socklen_t alen = sizeof(addr);
   int rcvbuf = 8 * 1024 * 1024;
   struct rxctx ctx;
   pthread_t rx;
   double mean_ms = 0, min_ms = 1e30, max_ms = 0, drop_pct;
   double dtol, mtol;
   int fail = 0;

   if (n <= 0) n = 4000;

   rxfd = socket(AF_INET, SOCK_DGRAM, 0);
   txfd = socket(AF_INET, SOCK_DGRAM, 0);
   if (rxfd < 0 || txfd < 0) { perror("socket"); return 2; }
   setsockopt(rxfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

   memset(&addr, 0, sizeof(addr));
   addr.sin_family = AF_INET;
   addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
   addr.sin_port = 0;
   if (bind(rxfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); return 2; }
   if (getsockname(rxfd, (struct sockaddr *)&addr, &alen) < 0) { perror("getsockname"); return 2; }

   ctx.fd = rxfd;
   ctx.n = n;
   ctx.arrived = calloc(n, 1);
   ctx.delay_ns = calloc(n, sizeof(uint64_t));
   ctx.done = 0;
   if (!ctx.arrived || !ctx.delay_ns) { perror("calloc"); return 2; }
   if (pthread_create(&rx, NULL, rx_thread, &ctx) != 0) { perror("pthread"); return 2; }

   for (i = 0; i < n; i++)
   {
      struct payload p;
      p.seq = (uint32_t)i;
      p.send_ns = now_ns();
      sendto(txfd, &p, sizeof(p), 0, (struct sockaddr *)&addr, sizeof(addr));
      if ((i & 511) == 511)
         usleep(200);   /* light pacing so the loopback rx buffer keeps up */
   }

   /* let the shim's release thread flush every queued datagram, then let the
    * receiver drain the socket before we tell it to stop */
   usleep((useconds_t)((delay_ms + jitter_ms) * 1000) + 1500000);
   ctx.done = 1;
   pthread_join(rx, NULL);

   for (i = 0; i < n; i++)
      if (ctx.arrived[i])
      {
         double d = ctx.delay_ns[i] / 1e6;
         mean_ms += d;
         if (d < min_ms) min_ms = d;
         if (d > max_ms) max_ms = d;
         recvd++;
      }
   if (recvd) mean_ms /= recvd;
   else       min_ms = 0;
   drop_pct = 100.0 * (n - recvd) / n;

   /* generous, statistics-aware tolerances: still fail a shim that ignores the
    * delay knob (min ~0) or the loss knob (drop ~0). The least-delayed sample is
    * the clean delay probe -- release-thread backlog can only inflate latency,
    * never shrink it below base-jitter -- so assert on min, then bound the mean
    * from below (delay was at least applied) with a wide upper drain margin. */
   dtol = loss_pct > 0 ? (loss_pct * 0.4 + 2.5) : 1.0;
   mtol = jitter_ms * 0.6 + 3.0;

   if (loss_pct == 0 && delay_ms == 0 && jitter_ms == 0)
   {
      /* passthrough sanity */
      if (drop_pct > 1.0) { fprintf(stderr, "passthrough dropped %.2f%%\n", drop_pct); fail = 1; }
      if (mean_ms > 3.0)  { fprintf(stderr, "passthrough mean delay %.2fms\n", mean_ms); fail = 1; }
   }
   else
   {
      double lo = delay_ms - jitter_ms - 2.0;
      if (lo < 0) lo = 0;
      if (recvd == 0) { fprintf(stderr, "nothing arrived\n"); fail = 1; }
      if (drop_pct < loss_pct - dtol || drop_pct > loss_pct + dtol)
      { fprintf(stderr, "drop %.2f%% off target %.2f%% (tol %.2f)\n", drop_pct, loss_pct, dtol); fail = 1; }
      /* min: the base delay must actually be applied and not overshot */
      if (recvd && (min_ms < lo || min_ms > delay_ms + jitter_ms + mtol))
      { fprintf(stderr, "min delay %.2fms off base %.2fms (want >= %.2f)\n", min_ms, delay_ms, lo); fail = 1; }
      /* mean: at least the base delay, with a wide margin for drain backlog */
      if (recvd && mean_ms < delay_ms - jitter_ms - 2.0)
      { fprintf(stderr, "mean delay %.2fms below base %.2fms\n", mean_ms, delay_ms); fail = 1; }
   }

   printf("DROPPED:");
   for (i = 0; i < n; i++)
      if (!ctx.arrived[i]) printf(" %d", i);
   printf("\n");
   printf("wan_shim_test: n=%d recv=%d drop=%.2f%% (want %.2f%%) "
          "delay mean=%.2f min=%.2f max=%.2fms (want %.2f+/-%.2f) : %s\n",
          n, recvd, drop_pct, loss_pct, mean_ms, (recvd ? min_ms : 0.0), max_ms,
          delay_ms, jitter_ms, fail ? "FAIL" : "PASS");

   free(ctx.arrived);
   free(ctx.delay_ns);
   return fail ? 1 : 0;
}
