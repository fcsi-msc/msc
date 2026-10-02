#define _GNU_SOURCE
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

#include "msc.h"

int datalenpipes[4];
extern long long int *xfcounts;

extern void run_remote_command(int, int, char *, char *, char *);
extern void run_local_command(int, int, char *);
extern void local_singlemachine_start(struct argdata *);
extern void local_multimachine_start(struct argdata *);



long long int parse_xfercount(char *);

int *multipipes;


void local_msc(struct argdata *AD)
{
   if (AD->numsets > 1)
   {
      DEBUG0("start multi-machine\n");
      DEBUGSYNC;

      local_multimachine_start(AD);
      DEBUG0("end multi-machine\n");
   }
   else
   {
         /* start transfer, single machine case */
      DEBUG0("start single machine\n");
      DEBUGSYNC;
      local_singlemachine_start(AD);
      DEBUG0("end single machine\n");
   }
}
