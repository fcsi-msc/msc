/*
 * udp_stats.c - the MSC-UDP-STATS NDJSON telemetry stream (see udp_stats.h).
 *
 * Design constraints, in order:
 *   1. Zero cost when off: every hook in the transport is guarded by the
 *      msc_udp_stats_on int, so the disabled path is one predictable branch.
 *   2. Never perturb the transfer when on: emissions are rate-gated per flow
 *      (default 20 Hz), formatted into a stack buffer, and written with one
 *      locked fwrite -- no allocation, no syscall storm, no partial lines.
 *   3. Never block the flow_workers on a full pipe... within reason: the stream is
 *      line-buffered stdio; the consumer is required to drain it
 *      continuously. A stalled consumer degrades the transfer rather
 *      than corrupting it, exactly like MSC_UDP_STATS would.
 */
#include "udp_session.h"
#include "udp_stats.h"

#include <pthread.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

int msc_udp_stats_on = 0;

static FILE *vis_fp = NULL;
static const char *vis_side = "?";
static pthread_mutex_t vis_lock = PTHREAD_MUTEX_INITIALIZER;
static uint64_t vis_tick_ns = 50ULL * 1000 * 1000;   /* 20 Hz default */

/* last-emit stamp per flow per rate-gated kind. Each slot is only touched by
 * the flow_worker thread that owns the flow, so plain uint64_t is race-free. */
static uint64_t vis_last[MSC_UDP_MAX_FLOWS][MSC_UDP_STATS_NKINDS];

/* retransmit flood control: a per-flow token bucket (burst 50, refill 200/s).
 * Loss-injected runs (MSC_UDP_DROP=20) can retransmit thousands of units a second;
 * a consumer only needs enough discrete events to follow the transfer, and the
 * flow_sample `retransmitted` receiver carries the exact total either way. */
#define VIS_RETRANSMIT_BURST  50.0
#define VIS_RETRANSMIT_RATE   200.0   /* tokens per second */
static double vis_rtokens[MSC_UDP_MAX_FLOWS];
static uint64_t vis_rlast[MSC_UDP_MAX_FLOWS];

void msc_udp_stats_init(const char *side)
{
   const char *on, *rate, *stream;
   if (vis_fp != NULL)   /* both roles' session opens call this; first wins */
   {
      vis_side = side;
      return;
   }
   on = getenv("MSC_UDP_STATS_STREAM");
   if (on == NULL || atoi(on) == 0)
      return;

   rate = getenv("MSC_UDP_STATS_STREAM_RATE_HZ");
   if (rate != NULL && atof(rate) > 0.0)
   {
      double hz = atof(rate);
      if (hz > 1000.0) hz = 1000.0;
      vis_tick_ns = (uint64_t)(1e9 / hz);
   }

   stream = getenv("MSC_UDP_STATS_STREAM_OUTPUT");
   if (stream == NULL || strcmp(stream, "stderr") == 0)
      vis_fp = stderr;
   else if (strcmp(stream, "stdout") == 0)
   {
      if (msc_udp_ctl_is_stdio(msc_udp_ctl_mode()))
      {
         /* under MSC_UDP_CTL=stdio* the receiver's stdout IS the control channel;
          * NDJSON on it would corrupt the stream (the sender's stdout is the
          * user's terminal, but one rule for both roles keeps this safe) */
         fprintf(stderr, "MSC UDP visual: stdout is the control channel under "
                 "MSC_UDP_CTL=%s; using stderr\n",
                 msc_udp_ctl_mode_name(msc_udp_ctl_mode()));
         vis_fp = stderr;
      }
      else
         vis_fp = stdout;
   }
   else
   {
      vis_fp = fopen(stream, "a");
      if (vis_fp == NULL)
      {
         fprintf(stderr, "MSC UDP visual: cannot open %s, using stderr\n", stream);
         vis_fp = stderr;
      }
   }
   vis_side = side;
   msc_udp_stats_on = 1;
   {
      int f;
      for (f = 0; f < MSC_UDP_MAX_FLOWS; f++)
         vis_rtokens[f] = VIS_RETRANSMIT_BURST;
   }
}

int msc_udp_stats_due(int flow_id, int kind)
{
   uint64_t now;
   if (flow_id < 0 || flow_id >= MSC_UDP_MAX_FLOWS)
      return 0;
   now = msc_udp_now_ns();
   if (now - vis_last[flow_id][kind] < vis_tick_ns)
      return 0;
   vis_last[flow_id][kind] = now;
   return 1;
}

int msc_udp_stats_retransmit_ok(int flow_id)
{
   uint64_t now;
   if (flow_id < 0 || flow_id >= MSC_UDP_MAX_FLOWS)
      return 0;
   now = msc_udp_now_ns();
   if (vis_rlast[flow_id] != 0)
      vis_rtokens[flow_id] += (double)(now - vis_rlast[flow_id]) / 1e9
                              * VIS_RETRANSMIT_RATE;
   vis_rlast[flow_id] = now;
   if (vis_rtokens[flow_id] > VIS_RETRANSMIT_BURST)
      vis_rtokens[flow_id] = VIS_RETRANSMIT_BURST;
   if (vis_rtokens[flow_id] < 1.0)
      return 0;
   vis_rtokens[flow_id] -= 1.0;
   return 1;
}

void msc_udp_stats_emit(const char *event, const char *fmt, ...)
{
   char tail[896];
   char line[1024];
   int n, m = 0;
   va_list ap;

   if (!msc_udp_stats_on || vis_fp == NULL)
      return;
   /* format the variable tail OUTSIDE the lock (it may be slow), but stamp
    * ts_ns INSIDE it: with the stamp taken first, two flow_worker threads could
    * serialize their writes in the opposite order and the stream would not be
    * monotonic per process -- a property consumers may rely on. */
   if (fmt != NULL && fmt[0] != '\0')
   {
      va_start(ap, fmt);
      m = vsnprintf(tail, sizeof(tail), fmt, ap);
      va_end(ap);
      if (m < 0 || m >= (int)sizeof(tail))
         return;   /* a truncated line would be invalid JSON: drop it whole */
   }
   else
      tail[0] = '\0';

   pthread_mutex_lock(&vis_lock);
   n = snprintf(line, sizeof(line),
                "MSC-UDP-STATS {\"v\":1,\"ts_ns\":%llu,\"side\":\"%s\","
                "\"event\":\"%s\"%s}\n",
                (unsigned long long)msc_udp_now_ns(), vis_side, event, tail);
   if (n > 0 && n < (int)sizeof(line))
   {
      fwrite(line, 1, (size_t)n, vis_fp);
      fflush(vis_fp);
   }
   pthread_mutex_unlock(&vis_lock);
}
