#define _GNU_SOURCE
#include "msc.h"

#include <signal.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

int msc_debug;
FILE *msc_debugout;
extern void local_singlemachine_start(struct argdata *);

static int wait_exit(pid_t pid)
{
   int status;
   if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status)) return -1;
   return WEXITSTATUS(status);
}

int main(void)
{
   char marker[128], probe_marker[128];
   char *machine = "test-receiver";
   struct argdata ad;
   struct jobinfo job;
   pid_t child;
   snprintf(marker, sizeof(marker), "/tmp/msc-retry-test.%ld", (long)getpid());
   snprintf(probe_marker, sizeof(probe_marker), "/tmp/msc-probe-test.%ld", (long)getpid());
   unlink(marker);
   unlink(probe_marker);
   memset(&ad, 0, sizeof(ad)); memset(&job, 0, sizeof(job));
   ad.retries = 2; ad.retry_delay_ms = 1; ad.childinfo = &job;
   setenv("MSC_TEST_RETRY_MARKER", marker, 1);
   local_singlemachine_start(&ad);
   if (job.xfercount != 123 || job.reused != 456)
   { fprintf(stderr, "transient retry did not recover/account correctly\n"); return 1; }

   /* A checkpointed finite retry must already be --resume; it must not wait
    * until retries are exhausted and SSH probe mode is entered. */
   unlink(marker);
   memset(&ad, 0, sizeof(ad)); memset(&job, 0, sizeof(job));
   ad.retries = 1; ad.retry_delay_ms = 1; ad.childinfo = &job;
   ad.udp = 1; ad.checkpoint_path = "/tmp/test-checkpoint";
   setenv("MSC_TEST_REQUIRE_RESUME", "1", 1);
   local_singlemachine_start(&ad);
   unsetenv("MSC_TEST_REQUIRE_RESUME");
   if (!ad.resume || job.xfercount != 123 || job.reused != 456)
   { fprintf(stderr, "finite checkpoint retry did not switch to resume mode\n"); return 1; }

   child = fork();
   if (child == 0)
   {
      setenv("MSC_TEST_RETRY_PERMANENT", "1", 1);
      local_singlemachine_start(&ad);
      _exit(0);
   }
   if (wait_exit(child) != MSC_EXIT_CLI)
   { fprintf(stderr, "permanent failure was retried or misclassified\n"); return 1; }

   unlink(marker);
   child = fork();
   if (child == 0)
   {
      msc_install_signal_handlers();
      ad.retry_delay_ms = 5000;
      local_singlemachine_start(&ad);
      _exit(msc_cancelled() ? msc_cancel_exit_code() : 0);
   }
   while (access(marker, F_OK) != 0) usleep(1000);
   kill(child, SIGINT);
   if (wait_exit(child) != MSC_EXIT_SIGINT)
   { fprintf(stderr, "SIGINT did not interrupt retry backoff\n"); return 1; }
   unlink(marker);

   /* A checkpointed network failure waits for SSH reachability, flips the
    * next invocation to --resume, and succeeds without user intervention. */
   memset(&ad, 0, sizeof(ad)); memset(&job, 0, sizeof(job));
   ad.udp = 1; ad.childinfo = &job; ad.checkpoint_path = "/tmp/test-checkpoint";
   ad.reconnect_interval_ms = 1; ad.remote_machines = &machine;
   {
      int fd = open(probe_marker, O_WRONLY | O_CREAT | O_EXCL, 0600);
      if (fd < 0) { perror("probe marker"); return 1; }
      close(fd);
   }
   setenv("MSC_TEST_RETRY_MARKER", marker, 1);
   setenv("MSC_TEST_PROBE_MARKER", probe_marker, 1);
   local_singlemachine_start(&ad);
   if (!ad.resume || job.xfercount != 123 || job.reused != 456)
   { fprintf(stderr, "automatic SSH probe/resume did not recover correctly\n"); return 1; }

   /* Waiting for an unreachable receiver remains interruptible forever. */
   unlink(marker); unlink(probe_marker);
   child = fork();
   if (child == 0)
   {
      msc_install_signal_handlers();
      local_singlemachine_start(&ad);
      _exit(msc_cancelled() ? msc_cancel_exit_code() : 0);
   }
   while (access(marker, F_OK) != 0) usleep(1000);
   usleep(10000);
   kill(child, SIGINT);
   if (wait_exit(child) != MSC_EXIT_SIGINT)
   { fprintf(stderr, "SIGINT did not interrupt SSH reconnect probing\n"); return 1; }
   unlink(marker); unlink(probe_marker);
   unsetenv("MSC_TEST_PROBE_MARKER");
   printf("retry policy tests passed\n");
   return 0;
}
