#include "msc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

/*external variables*/

/*external routines*/
extern void parseargs(int, char**, struct argdata *);
/*extern void makechild(void (*p), void (*c), struct argdata *, char *);*/
extern void local_msc(struct argdata *);

/* msc: multi stream copy
 * open multiple sockets to transfer data over a network that
 * doesn't have a large enough TCP window for the bandwidth/latency.
 *
 *  files:
 *    msc.h:   common include file
 *    fork.c:  support to fork processes.
 *    parse.c: parse command line arguments.  For remote, send control
 *               to remote code.
 *    msc.c:  program main (this file)
 *    local_sockets.c:  utilities to handle sockets
 *    remote.c: program flow when remote.
 *    local.c: main program flow for client.
 */

FILE *locallog, *remotelog;

/* global variables for debug */
int msc_debug;
FILE *msc_debugout;

/* The version is informational only: local/remote compatibility is checked
 * by protocol negotiation, never by comparing this string. */
static char *getversion(void)
{
   return MSC_VERSION;
}

int main(int argc, char **argv)
{
   struct argdata Argdata;
   struct timeval starttime, endtime;
   long double etime;
   long double bytespersec;
   long double mbytesxfer;
   long double Mi = 1024.0*1024.0;
   long long int totalxfer;
   unsigned int set;

     /* setup metrics variables */
   gettimeofday(&starttime, NULL);
   bzero(&Argdata, sizeof(struct argdata));
   Argdata.version = getversion();
   Argdata.udp_ctl_rfd = -1;   /* 0 is a real fd; bzero must not read as "set" */
   Argdata.udp_ctl_wfd = -1;
   msc_install_signal_handlers();

   /* parse arguments */
   /* may print usage and exit */
   parseargs(argc, argv, &Argdata);

   local_msc(&Argdata);

   if (msc_cancelled())
   {
      fprintf(stderr, "msc cancelled by %s; resumable state was retained\n",
              msc_cancel_signal == SIGTERM ? "SIGTERM" : "SIGINT");
      return msc_cancel_exit_code();
   }

   gettimeofday(&endtime, NULL);
   etime = endtime.tv_sec + ((long double)endtime.tv_usec / 1000000.0) - 
             starttime.tv_sec - ((long double) starttime.tv_usec / 1000000.0);

   if (Argdata.numsets == 1)
   {
      bytespersec = ((long double)Argdata.childinfo->xfercount / etime);
      mbytesxfer = Argdata.childinfo->xfercount / (long double) 1048576.0;
      printf("msc transfer %lld bytes %.2Lf seconds %.1Lf bytes/second\n",
            Argdata.childinfo->xfercount, etime, bytespersec );
      printf("%.2Lf MiB %.1Lf MiB/s %.1Lf Mib/s\n", mbytesxfer , 
                   bytespersec / Mi, bytespersec / Mi * (long double) 8.0);
      
   }
   else
   {
      totalxfer = 0;
      for (set = 0; set < Argdata.numsets; set++)
      {
         totalxfer += Argdata.childinfo[set].xfercount;
         printf("msc job %u machine %s to %s size %lld\n", set,
                   Argdata.childinfo[set].localmachine, 
                   Argdata.childinfo[set].remotemachine, 
                   Argdata.childinfo[set].xfercount);
      }
      bytespersec = ((long double)totalxfer / etime);
      mbytesxfer = totalxfer / (long double) 1048576.0;
      printf("msc Total transfer %lld bytes %.2Lf seconds %.1Lf bytes/second\n",
            totalxfer, etime, bytespersec );
      printf("%.2Lf MiB %.1Lf MiB/s %.1Lf Mib/s\n", mbytesxfer ,
                   bytespersec / Mi, bytespersec / Mi * (long double) 8.0);
   }
   return 0;           
}

   /* parse first line printed from program (above: msc transfer bytes...) */
   /* return number of bytes transferred */
long long int parse_xfercount(char *buf)
{
   char *sp1, *sp2, *end;
   int len;

   len = strlen(buf);
   end = buf + len;
   sp1 = strchr(buf, ' ');
   if (sp1 == NULL)
      return 0;
   sp1++;
   if (sp1 >= end)
      return 0;
   sp2 = strchr(sp1, ' ');
   if (sp2 == NULL)
      return 0;
   sp2++;
   if (sp2 >= end)
      return 0;
   return atoll(sp2);
}
