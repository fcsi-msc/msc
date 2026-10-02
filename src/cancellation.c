#define _POSIX_C_SOURCE 200809L
#include "msc.h"

#include <errno.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>

volatile sig_atomic_t msc_cancel_signal;
static volatile sig_atomic_t signal_count;

static void cancellation_handler(int signo)
{
   if (++signal_count > 1)
      _exit(signo == SIGINT ? MSC_EXIT_SIGINT : MSC_EXIT_SIGTERM);
   msc_cancel_signal = signo;
}

void msc_install_signal_handlers(void)
{
   struct sigaction action;
   sigemptyset(&action.sa_mask);
   action.sa_flags = 0;
   action.sa_handler = cancellation_handler;
   sigaction(SIGINT, &action, NULL);
   sigaction(SIGTERM, &action, NULL);

   /* A peer disappearing can otherwise terminate the entire supervised
    * attempt with SIGPIPE before the socket code converts EPIPE into the
    * reconnectable MSC_EXIT_NETWORK status. */
   action.sa_handler = SIG_IGN;
   sigaction(SIGPIPE, &action, NULL);
}

int msc_cancelled(void)
{
   return msc_cancel_signal != 0;
}

int msc_cancel_exit_code(void)
{
   return msc_cancel_signal == SIGTERM ? MSC_EXIT_SIGTERM : MSC_EXIT_SIGINT;
}

int msc_interruptible_delay(uint64_t milliseconds)
{
   struct timespec delay;
   delay.tv_sec = (time_t)(milliseconds / 1000U);
   delay.tv_nsec = (long)(milliseconds % 1000U) * 1000000L;
   while (!msc_cancelled() && nanosleep(&delay, &delay) != 0)
      if (errno != EINTR)
         return -1;
   return msc_cancelled() ? -1 : 0;
}
