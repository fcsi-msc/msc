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

extern void run_remote_command(int, int, char *, char *, char *);
extern void run_local_command(int, int, char *);

long long int parse_xfercount(char *);

extern char **environ;

/* single-quote one word for the local/remote shell (paths, env values) */
static char *quote_env_value(const char *s)
{
   size_t i, n = 3;
   char *q, *p;
   for (i = 0; s[i]; i++) n += s[i] == '\'' ? 4 : 1;
   q = checkmalloc(n, "environment quote"); p = q; *p++ = '\'';
   for (i = 0; s[i]; i++)
      if (s[i] == '\'') { memcpy(p, "'\\''", 4); p += 4; }
      else *p++ = s[i];
   *p++ = '\''; *p = '\0';
   return q;
}

static char *msc_udp_env_prefix(void)
{
   char **e, *out = strdup("");
   for (e = environ; *e; e++)
   {
      const char *eq;
      char *name, *value, *next;
      size_t len;
      if ((eq = strchr(*e, '=')) == NULL) continue;
      len = (size_t)(eq - *e);
      if (!msc_udp_env_forwardable(*e, len)) continue;
      name = checkmalloc(len + 1, "environment name");
      memcpy(name, *e, len); name[len] = '\0';
      value = quote_env_value(eq + 1);
      if (asprintf(&next, "%s%s=%s ", out, name, value) < 0) exit(MSC_EXIT_INTERNAL);
      free(out); free(name); free(value); out = next;
   }
   return out;
}

void get_info_from_datalen(ino_t *inode, off_t *size, char *lsout)
{
   char *ipoint, *spoint;
   char *comma;

   ipoint = lsout;
   /* findzero refusing to prepare the shared destination -- currently only a
    * --dest-stripe-count layout it could not apply or verify. It already
    * explained itself on stderr; carry its exit code rather than reducing a
    * destination problem to a malformed-probe report. */
   if (strncmp(lsout, "ERROR,", 6) == 0)
      exit(atoi(lsout + 6));
   comma = strchr(lsout, ',');
   if (comma == NULL)
   {
      /* The probe reply crosses SSH; garbage here means a failed or
       * version-skewed remote probe, not a local bug. */
      fprintf(stderr, "MSC multimachine probe returned malformed file state: %s\n",
              lsout);
      exit(MSC_EXIT_INCOMPATIBLE);
   }
   *comma = '\0';
   spoint = ++comma;
   *inode = (ino_t) atoll(ipoint);
   *size = (off_t) atoll(spoint);
}

void get_remote_datalen(struct argdata *AD, int set)
{
   char *rcommand, *debug;
   
   char *qbin = quote_env_value(AD->remote_binary);
   char *qdest = quote_env_value(AD->destfile);
   rcommand = checkmalloc(strlen(qdest) + strlen(qbin) + 512,
                          "remote datalen");

   if (msc_debug != 0)
      debug = " -d";
   else
      debug = "";

   /* findzero creates the shared destination, so it is the only process that can
    * still choose its Lustre layout. The -x argument list is a positional ABI an
    * older remote would misread, so the request rides an environment prefix
    * instead -- the one command built here that had none. */
   if (AD->dest_stripe_count != 0)
      sprintf(rcommand, "MSC_DEST_STRIPE_COUNT=%ld %s -x%s %d %s %lld",
              AD->dest_stripe_count, qbin, debug, AD->numsets,
              qdest, AD->sourcesize);
   else
      sprintf(rcommand, "%s -x%s %d %s %lld", qbin, debug, AD->numsets,
                           qdest, AD->sourcesize);
   free(qbin); free(qdest);

   run_remote_command(AD->childinfo[set].fd_child_read,
                      AD->childinfo[set].fd_child_write, AD->remote_user, 
                      (AD->remote_machines)[set], rcommand);

    /*does not return */
}

void local_multimachine_child(struct argdata *AD, int childnum)
{
   char *command;
   char options[1024];
   char sopt[32];
   char dsopt[48];
   char *envpfx;
   char *debug;
   char *qbin, *qsource, *qdest;

   if (msc_debug != 0)
      debug = " -d";
   else
      debug = "";

   /* Name the transport in every child command -- -U or -T, never neither. The
    * child is single-pair (one -r => numsets 1) and its -y receiver-launch
    * already carries the udp flag, so one flag is all multi-machine needs; but
    * it must be spelled out, because a child launched on another host runs
    * whatever msc is installed there, and inheriting "the default" from a
    * different binary is how a UDP parent ends up with a TCP child.
    * propagate -B too: the child spawns its own -y receiver over ssh, and
    * without it that falls back to "msc" from the remote PATH — a stale
    * installed copy with a different -y arity exits with "invalid argument
    * count" and the child blocks forever reading the dead ssh's pipe. */
   /* Spell --dest-stripe-count out here for the same reason as the transport: a
    * child runs whatever msc is installed on its host, and its own -y receiver
    * must be able to verify the layout of the shared destination findzero
    * already created. */
   if (AD->dest_stripe_count != 0)
      snprintf(dsopt, sizeof(dsopt), " --dest-stripe-count %ld",
               AD->dest_stripe_count);
   else
      dsopt[0] = '\0';
   snprintf(options, sizeof(options), "%s%s%s%s%s%s%s%s%s",
     AD->udp ? " -U" : " -T", AD->force ? " --force" : "",
     AD->gso ? " --gso" : " --no-gso", AD->gro ? " --gro" : " --no-gro",
     AD->stats ? " --stats" : "",
     AD->no_internal_checksum ? " --checksum=false" : " --checksum=true",
     dsopt,
     AD->interface_host ? " -I " : "", AD->interface_host ? AD->interface_host : "");
   /* An implicit UDP -s must stay implicit in the per-pair child so each
    * pair's PMTUD auto-raise can run; an explicit -s (and TCP's segment size)
    * is forwarded so both ends of every pair keep matching values. */
   if (AD->udp && !AD->packetsize_explicit)
      sopt[0] = '\0';
   else
      snprintf(sopt, sizeof(sopt), " -s %u", AD->packetsize);
   envpfx = msc_udp_env_prefix();
   /* Paths and the binary name pass through the local/remote shell, so they
    * must be quoted: an unquoted path with spaces or metacharacters used to
    * split or execute.  Machine names come from -l/-r and stay bare. */
   qbin = quote_env_value(AD->remote_binary);
   qsource = quote_env_value(AD->sourcefile);
   qdest = quote_env_value(AD->destfile);
   if (asprintf(&command,
     "%s%s%s%s -B %s -l %s -r %s -q %d -n %d -p %d --port-tries %u%s -i %s -o %s -t %ld -a %ld -b %ld -z %ld",
     envpfx, qbin, debug, options,
     qbin, AD->childinfo[childnum].localmachine,
     AD->childinfo[childnum].remotemachine,
     AD->numsets,
     AD->numstreams, AD->portnum, AD->port_tries, sopt, qsource, qdest,
     AD->childinfo[childnum].xferlen, AD->childinfo[childnum].startloc,
     AD->childinfo[childnum].startloc, AD->inode) < 0)
      exit(MSC_EXIT_INTERNAL);
   free(envpfx); free(qbin); free(qsource); free(qdest);

   close(AD->childinfo[childnum].fd_parent_read);
   close(AD->childinfo[childnum].fd_parent_write);

   DEBUG1("multimachine: start single msc. Command: %s\n", command);
   DEBUGSYNC;
   if (strcmp(AD->childinfo[childnum].localmachine, "localhost") == 0)
      run_local_command(AD->childinfo[childnum].fd_parent_read, 1,command);
   else
      run_remote_command(AD->childinfo[childnum].fd_child_read, 
                         AD->childinfo[childnum].fd_child_write, 
                         AD->remote_user, AD->childinfo[childnum].localmachine, 
                         command);
}

off_t local_multimachine_filecreate(struct argdata *AD)
{
   int child;
   char buf[1024];
   ssize_t bytes_read;
   off_t gooddata; /* offset to end of valid data at destination */
   int tmppipe[2];

   /* create pipes to communicate with children */
   if (pipe(tmppipe) == -1)
   {
      fprintf(stderr,"couldn't create read pipe for remote command\n");
      exit(MSC_EXIT_INTERNAL);
   }
   AD->childinfo[0].fd_parent_write = tmppipe[1];
   AD->childinfo[0].fd_child_read = tmppipe[0];
   if (pipe(tmppipe) == -1)
   {
      fprintf(stderr,"couldn't create write pipe for remote command\n");
      exit(MSC_EXIT_INTERNAL);
   }
   AD->childinfo[0].fd_child_write = tmppipe[1];
   AD->childinfo[0].fd_parent_read = tmppipe[0];

   /* Probe/create the shared destination before splitting work.  childinfo[0]
    * temporarily holds this probe's pipes; the complete array is cleared once
    * the returned inode has been captured. */
   child = makechild_inline(get_remote_datalen, AD, 0, "get filesize");

   /* Drop the parent's copies of the child's pipe ends. Without this the parent
    * itself holds the write end open, so a probe that dies before printing its
    * reply -- which findzero can now do, refusing a destination layout -- would
    * leave the read() below blocked forever instead of seeing EOF. */
   close(AD->childinfo[0].fd_child_write);
   close(AD->childinfo[0].fd_child_read);

      DEBUG1("local_msc prior to read, child %d\n", child);
      DEBUGSYNC;

   waitpid(child, NULL, WNOHANG);

      DEBUG0("local_msc prior to read\n");
      DEBUGSYNC;

   bytes_read = read(AD->childinfo[0].fd_parent_read, buf, sizeof(buf) - 1);
   if (bytes_read <= 0)
   {
      fprintf(stderr, "couldn't read remote file state\n");
      exit(MSC_EXIT_NETWORK);
   }
   buf[bytes_read] = '\0';

      DEBUG1("info from get remote filesize: %s\n", buf);
      DEBUGSYNC;

   get_info_from_datalen(&(AD->inode), &gooddata, buf);

      DEBUG2("inode: %ld gooddata %ld\n", AD->inode, gooddata);
      DEBUGSYNC;

   return gooddata;
}

   /* called when we read back the output from a single job ending */
void process_multimachine_result(char *inbuf, struct jobinfo *j, int set)
{
   j[set].xfercount = parse_xfercount(inbuf);
   /* Print completion as each parallel section finishes. */
   printf("Transferred section from %s to %s start %lld size %lld\n",
          j[set].localmachine, j[set].remotemachine, 
          (long long int)j[set].startloc, 
          (long long int)j[set].xferlen);
}

void gather_multimachine_results(struct argdata *AD)
{
   int cont = 1;
   struct timeval tv;
   fd_set accgroup;
   int retval;
   unsigned int lc;
   char buf[512];
   int highest;

   DEBUG0("in gather_multimachine_results\n");
   while (cont != 0)
   {
      cont = 0;
      tv.tv_sec = 10;
      tv.tv_usec = 0;
      FD_ZERO(&accgroup);
      highest = 0;
         /* prepare accept group and find highest file descriptor */
      for (lc = 0; lc < AD->numsets; lc++)
      {
         if (AD->childinfo[lc].doneread == 0)
         {
            DEBUG2("Added child %d, fd %d to select queue\n", lc, AD->childinfo[lc].fd_parent_read);
            FD_SET(AD->childinfo[lc].fd_parent_read, &accgroup);
            if (AD->childinfo[lc].fd_parent_read > highest)
               highest = AD->childinfo[lc].fd_parent_read;
            cont++;
         }
      }
      if (cont > 0)
      {
         highest++;
         DEBUG1("select highest = %d\n", highest);
         retval = select(highest, &accgroup, NULL, NULL, &tv);
      }
      else
         retval = 0;
      if (retval > 0)
      {
         for (lc = 0; lc < AD->numsets; lc++)
         {
            if (AD->childinfo[lc].doneread == 0)
            {
               if (FD_ISSET(AD->childinfo[lc].fd_parent_read, &accgroup))
               {
                  if (fgets(buf, 512, AD->childinfo[lc].instr) != NULL)
                     process_multimachine_result(buf, AD->childinfo, lc);
                  AD->childinfo[lc].doneread = 1;
               }
            }
         }
      }
      /* A section child that died without reporting used to leave this loop
       * selecting forever.  Reap opportunistically; a dead child's pipe holds
       * either a buffered result line (read it now -- its EOF is already
       * visible since the child was the only writer) or nothing. */
      for (lc = 0; lc < AD->numsets; lc++)
      {
         int status;
         struct jobinfo *j = &AD->childinfo[lc];
         if (j->doneread != 0 || j->reaped != 0)
            continue;
         if (waitpid(j->pid, &status, WNOHANG) == j->pid)
         {
            j->reaped = 1;
            j->exit_status = status;
            if (fgets(buf, 512, j->instr) != NULL)
               process_multimachine_result(buf, AD->childinfo, lc);
            else
               fprintf(stderr, "MSC multimachine section %u (%s -> %s) exited "
                       "without reporting a result\n", lc,
                       j->localmachine, j->remotemachine);
            j->doneread = 1;
         }
      }
   }
}
   
void get_source_filesize(struct argdata *AD)
{
   int err;
   struct stat statbuf;

   DEBUG0("in multimachine_setup_boundaries\n");
   if (AD->sourcefile != NULL)
   {
      err = stat(AD->sourcefile, &statbuf);
      if (err != 0)
      {
         fprintf(stderr,"error getting stat on source file %s\n",
                        AD->sourcefile);
         exit(MSC_EXIT_SOURCE);
      }
   }
   AD->sourcesize = statbuf.st_size;
}

void multimachine_setup_boundaries(struct argdata *AD)
{
   unsigned int set;
   off_t chunksize, startloc, xferlen, size;

   size = AD->sourcesize;
   startloc = 0;
   /* Every pair owns a disjoint contiguous range.  Integer division gives any
    * remainder to the last pair, which is why startloc advances by chunksize
    * rather than by the preceding xferlen. */
   chunksize = size / AD->numsets;

   for(set = 0; set < AD->numsets; set++)
   {
      if (set == (AD->numsets - 1))
         xferlen = size - startloc; /* last one gets leftovers */
      else
         xferlen = chunksize;
      AD->childinfo[set].startloc = startloc;
      AD->childinfo[set].xferlen = xferlen;
      AD->childinfo[set].remotemachine = (AD->remote_machines)[set];
      AD->childinfo[set].localmachine = (AD->local_machines)[set];
      startloc += chunksize;
   }
}

/* also create startlen, xferlen, chunksize in subroutine, make it 
 * available to parse routine to dump for manual restarts 
 */
void local_multimachine_start(struct argdata *AD)
{
   unsigned int set;
   int flags;
   int tmppipe[2];

   get_source_filesize(AD);
   /* Create the remote file and populate the destination inode. */
   local_multimachine_filecreate(AD);
   /* The initial file-probe state is no longer needed. */
   bzero(AD->childinfo, sizeof(struct jobinfo) * AD->numsets);

   multimachine_setup_boundaries(AD);

   /* start msc again with single destination on each source machine */
   for(set = 0; set < AD->numsets; set++)
   {
      if (pipe(tmppipe) == -1)      /* initialize from child pipe */
      {
         fprintf(stderr,"couldn't create child output pipe\n");
         exit(MSC_EXIT_INTERNAL);
      }
      AD->childinfo[set].fd_child_write = tmppipe[1];
      AD->childinfo[set].fd_parent_read = tmppipe[0];
      DEBUG2("set %d parent read fd %d\n",set, AD->childinfo[set].fd_parent_read);
      DEBUG2("set %d child write fd %d\n",set, AD->childinfo[set].fd_child_write);
      if (pipe(tmppipe) == -1)      /* initialize from child pipe */
      {
         fprintf(stderr,"couldn't create child input pipe\n");
         exit(MSC_EXIT_INTERNAL);
      }
      AD->childinfo[set].fd_parent_write = tmppipe[1];
      AD->childinfo[set].fd_child_read = tmppipe[0];
      DEBUG2("set %d parent write fd %d\n",set, AD->childinfo[set].fd_parent_write);
      DEBUG2("set %d child read fd %d\n",set, AD->childinfo[set].fd_child_read);
      flags = fcntl(AD->childinfo[set].fd_parent_read, F_GETFD, 0);
      if (flags != -1)
         fcntl(AD->childinfo[set].fd_parent_read, F_SETFD,
               flags | O_CLOEXEC);
      AD->childinfo[set].instr = fdopen(AD->childinfo[set].fd_parent_read, "r");
      if (AD->childinfo[set].instr == NULL)
      {
          fprintf(stderr,"couldn't fdopen set %d\n", set);
          exit(MSC_EXIT_INTERNAL);
      }

      AD->childinfo[set].pid = makechild_inline(local_multimachine_child, AD, 
                                     set, "multimachine child");
      close(AD->childinfo[set].fd_child_write);
      close(AD->childinfo[set].fd_child_read);
   }
   /* Gather results until every child reports completion. */
   gather_multimachine_results(AD);

   /* Collect every section's exit status and propagate the worst failure.
    * Before this, a crashed section still produced overall exit 0 with a
    * partial destination file. */
   {
      int worst = 0;
      for (set = 0; set < AD->numsets; set++)
      {
         struct jobinfo *j = &AD->childinfo[set];
         int status = j->exit_status;
         int code;
         if (!j->reaped)
         {
            while (waitpid(j->pid, &status, 0) < 0)
            {
               if (errno != EINTR)
               { perror("MSC multimachine waitpid"); status = -1; break; }
            }
            j->reaped = 1;
            j->exit_status = status;
         }
         if (status == -1 || !WIFEXITED(status))
            code = MSC_EXIT_INTERNAL;
         else
            code = WEXITSTATUS(status);
         if (code == 0)
            continue;
         fprintf(stderr, "MSC multimachine section %u (%s -> %s) failed with "
                 "exit %d\n", set, j->localmachine, j->remotemachine, code);
         /* Prefer contract classes the caller can act on; anything else
          * (raw shell 127, signals) reports as internal. */
         if (!((code >= MSC_EXIT_CLI && code <= MSC_EXIT_INCOMPATIBLE) ||
               code == MSC_EXIT_SIGINT || code == MSC_EXIT_SIGTERM))
            code = MSC_EXIT_INTERNAL;
         if (worst == 0)
            worst = code;
      }
      if (worst != 0)
         exit(worst);
   }
}
