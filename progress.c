#define _POSIX_C_SOURCE 200809L
#include "msc.h"

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static uint64_t progress_now_ns(void)
{
   struct timespec ts;
   clock_gettime(CLOCK_MONOTONIC, &ts);
   return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

static void progress_print(struct msc_progress *p, const char *state)
{
   uint64_t sent = __atomic_load_n(p->current_bytes, __ATOMIC_RELAXED);
   uint64_t logical = p->reused_bytes + sent;
   double elapsed = (double)(progress_now_ns() - p->start_ns) / 1e9;
   double rate = elapsed > 0 ? (double)sent / elapsed : 0;
   double percent;
   double eta;
   if (logical > p->total_bytes) logical = p->total_bytes;
   percent = p->total_bytes ? (double)logical * 100.0 / (double)p->total_bytes : 100.0;
   eta = rate > 0 && logical < p->total_bytes ? (double)(p->total_bytes - logical) / rate : 0;
   fprintf(stderr,
           "msc progress: %llu/%llu bytes %.1f%%, reused %llu, sent %llu, elapsed %.1fs, %.2f MiB/s, ETA %.1fs, retry %u, %s\n",
           (unsigned long long)logical, (unsigned long long)p->total_bytes, percent,
           (unsigned long long)p->reused_bytes, (unsigned long long)sent,
           elapsed, rate / 1048576.0, eta, p->retry_count, state);
}

static void *progress_worker(void *arg)
{
   struct msc_progress *p = arg;
   while (!p->stop && !msc_cancelled())
   {
      if (msc_interruptible_delay(p->interval_ms) != 0) break;
      if (!p->stop) progress_print(p, "transferring");
   }
   return NULL;
}

int msc_progress_start(struct msc_progress *p, struct argdata *AD,
                       volatile uint64_t *current, uint64_t reused, uint64_t total)
{
   memset(p, 0, sizeof(*p)); p->current_bytes = current;
   p->reused_bytes = reused; p->total_bytes = total;
   p->retry_count = AD->retry_current;
   p->interval_ms = AD->progress_interval_ms ? AD->progress_interval_ms : 1000;
   p->enabled = AD->progress_mode == MSC_PROGRESS_ALWAYS ||
                (AD->progress_mode == MSC_PROGRESS_AUTO && isatty(STDERR_FILENO));
   p->start_ns = progress_now_ns();
   if (!p->enabled) return 0;
   if (pthread_create(&p->thread, NULL, progress_worker, p) != 0)
   { p->enabled = 0; return -1; }
   return 0;
}

void msc_progress_finish(struct msc_progress *p, int success, const char *bottleneck)
{
   if (!p->enabled) return;
   p->stop = 1; pthread_join(p->thread, NULL);
   progress_print(p, success ? "complete" : (msc_cancelled() ? "cancelled" : "failed"));
   fprintf(stderr, "msc summary: %s; bottleneck: %s\n",
           success ? "success" : (msc_cancelled() ? "cancelled" : "failure"),
           bottleneck != NULL ? bottleneck : "unable to determine (insufficient timing separation)");
}
