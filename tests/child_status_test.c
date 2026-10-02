#include "msc.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

int msc_debug = 0;
FILE *msc_debugout;

/* Stub for the netutil.c original: this harness links fork.o standalone (see
 * the Makefile rule), so every netutil symbol fork.c reaches for has to be
 * provided here. fork.c grew a msc_ssh_program() call without one, which broke
 * the link -- and with it `make valgrind`, since child_status_test is one of
 * its harnesses. Same semantics as the real one; nothing here ever execs. */
const char *msc_ssh_program(void)
{
   const char *program = getenv("MSC_SSH");
   return program != NULL && program[0] != '\0' ? program : "/usr/bin/ssh";
}

void msc_ssh_liveness(struct msc_ssh_liveness *out, enum msc_ssh_purpose purpose)
{
   (void)purpose;
   out->connect_timeout_s = 10;
   out->alive_interval_s = 15;
   out->alive_count = 4;
}

void *checkmalloc(size_t size, const char *message)
{
   void *result = malloc(size);
   if (result == NULL)
   {
      fprintf(stderr, "malloc failed for %s\n", message);
      exit(1);
   }
   return result;
}

static void parent_process(struct argdata *AD)
{
   (void)AD;
}

static void successful_child(struct argdata *AD)
{
   (void)AD;
   exit(0);
}

static void failing_child(struct argdata *AD)
{
   (void)AD;
   exit(7);
}

static int wait_succeeded(pid_t child)
{
   int status;
   return waitpid(child, &status, 0) == child &&
          WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

int main(void)
{
   struct argdata AD;
   pid_t test_process;

   makechild(parent_process, successful_child, &AD, "successful child");

   test_process = fork();
   if (test_process < 0)
      return 1;
   if (test_process == 0)
   {
      makechild(parent_process, failing_child, &AD, "failing child");
      return 0;
   }
   if (wait_succeeded(test_process))
   {
      fprintf(stderr, "makechild did not propagate child failure\n");
      return 1;
   }
   return 0;
}
