#define _GNU_SOURCE
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <sys/wait.h>
#include <signal.h>
#include <time.h>

#include "msc.h"

int readpiper[2], writepiper[2];
#define PARENT_READ_REMOTE  readpiper[0]
#define CHILD_WRITE_REMOTE  readpiper[1]
#define CHILD_READ_REMOTE   writepiper[0]
#define PARENT_WRITE_REMOTE writepiper[1]

extern void opensockets(struct argdata *);
extern void xferdata(FILE *, struct argdata *);
extern void xferdata_segmented(FILE *, struct argdata *);
extern void closesockets(struct argdata *);
extern void run_remote_command(int, int, char *, char *, char *);
extern int *sockfd;
/*extern void makechild(void (*p), void (*c), struct argdata *, char *);*/

extern char **environ;

static char *shell_quote(const char *s)
{
   size_t i, n = 3;
   char *q, *p;
   for (i = 0; s[i] != '\0'; i++) n += s[i] == '\'' ? 4 : 1;
   q = checkmalloc(n, "shell quote");
   p = q; *p++ = '\'';
   for (i = 0; s[i] != '\0'; i++)
      if (s[i] == '\'') { memcpy(p, "'\\''", 4); p += 4; }
      else *p++ = s[i];
   *p++ = '\''; *p = '\0';
   return q;
}

/* Forward only documented MSC UDP knobs. Values are single-quoted so an
 * environment value cannot alter the SSH-launched receiver command. */
static char *msc_udp_remote_env(void)
{
   char **e;
   char *out = strdup("");
   for (e = environ; *e != NULL; e++)
   {
      const char *eq;
      char *name, *value, *next;
      size_t len;
      eq = strchr(*e, '=');
      if (eq == NULL) continue;
      len = (size_t)(eq - *e);
      if (!msc_udp_env_forwardable(*e, len)) continue;
      name = checkmalloc(len + 1, "environment name");
      memcpy(name, *e, len); name[len] = '\0';
      value = shell_quote(eq + 1);
      if (asprintf(&next, "%s%s=%s ", out, name, value) < 0) exit(MSC_EXIT_INTERNAL);
      free(out); free(name); free(value); out = next;
   }
   return out;
}



void parseportnum(char *buf, struct argdata *AD)
{
   char *ppoint, *lpoint;
   char *comma;

   /* The rendezvous line comes from the remote peer over SSH; a truncated or
    * version-skewed line must fail cleanly, not crash the sender. */
   ppoint = strchr(buf,':');
   comma = ppoint != NULL ? strchr(ppoint, ',') : NULL;
   if (comma == NULL)
   {
      fprintf(stderr, "MSC received a malformed rendezvous line from the "
                      "remote peer; local and remote msc versions may be "
                      "incompatible\n");
      exit(MSC_EXIT_INCOMPATIBLE);
   }
   ppoint++;
   while (*ppoint == ' ')
      ppoint++;
   *comma = '\0';
   lpoint = ++comma;
   AD->finalport = atoi(ppoint);
   AD->lastgood = (off_t) atoll(lpoint);
   DEBUG2("parseportnum strings: port %s, lastgood %s\n", ppoint, lpoint);
   DEBUG2("parseportnum: port %d, lastgood %lld\n",
            AD->finalport, AD->lastgood);
   DEBUGSYNC;
}

/* continue from dolocal after either fork or call because no local command*/
/* parent from spawn of child that runs msc on remote machine */
void dolocal(struct argdata *AD)
{
   FILE * reader;
   FILE * rem;
   int ret;
   char buf[1024] = "";

   DEBUG0("in dolocal\n");
   DEBUGSYNC;
   /* Drop our copies of the child's pipe ends.  While the parent still holds
    * the write end of the read pipe, a read on it can never see EOF -- so a
    * remote that dies during launch (bad -B path, missing binary, failed ssh)
    * left us blocked in read() forever instead of reporting the failure.  The
    * stdio control modes made that worse: the same pipe IS the control
    * channel, and it carries no socket timeout to break the wait. */
   close(CHILD_READ_REMOTE);
   close(CHILD_WRITE_REMOTE);
   /* read portnumber from remote (indicates it is setup) */
   rem = fdopen(PARENT_READ_REMOTE, "r");
   if (rem == NULL)
   {
      DEBUG1("dolocal: fdopen error %d\n", errno);
   }
   else
   {
      DEBUG0("dolocal: successful fdopen\n");
      /* Unbuffered: in the stdio control modes this same pipe carries framed
       * control traffic the moment the rendezvous line is consumed, and a
       * stdio readahead buffer would swallow the first control bytes.  The
       * handshake is one short line, so byte-at-a-time costs nothing. */
      setvbuf(rem, NULL, _IONBF, 0);
   }
   /* stdout is also the SSH command's status/result stream, so ignore any
    * preceding remote-shell output until the receiver's rendezvous marker. */
   while (strncmp(buf, "MSC-CONNECT:", 12) != 0)
   {
      if (fgets(buf, 1024, rem) == NULL)
      {
         /* SSH launch succeeded but the receiver died during setup (bad
          * destination, port exhaustion, remote crash).  Classify as network
          * so the retry/reconnect machinery can decide; the child's own exit
          * code reaches the launcher through makechild(). */
         fprintf(stderr, "MSC remote process exited before opening data sockets\n");
         exit(MSC_EXIT_NETWORK);
      }
      DEBUG1("read from link: %s\n",buf);
   }
   DEBUG0("dolocal: got header from link\n");
       

    /* sets finalport and lastgood */
   parseportnum(&(buf[11]), AD);

   if (AD->udp)
   {
      /* UDP path opens its own TCP control connection (to finalport) plus its
       * own udp data sockets; it does not use the N TCP streams.  In the stdio
       * control modes there is no port to dial: the pipes we spawned ssh with
       * are themselves the control channel, so hand them to the transport. */
      AD->udp_ctl_rfd = PARENT_READ_REMOTE;
      AD->udp_ctl_wfd = PARENT_WRITE_REMOTE;
      udp_send(AD);
      return;
   }

   /* open sockets to remote */
   opensockets(AD);

   DEBUG0("dolocal: local sockets opened\n");
   DEBUGSYNC;

   if (AD->recursive)
   {
      xferdirectory(sockfd, AD);
      closesockets(AD);
      return;
   }

   /* while more data read from input or local command, write to sockets */
   /* set up stdin and seek to src_offset if not zero */

   if (AD->sourcefile != NULL) /* read from file instead of stdin */
   {
      DEBUG1("dolocal: open input file %s\n",AD->sourcefile);
      DEBUGSYNC;
      reader = fopen(AD->sourcefile, "r");
      if (reader == NULL)
      {
         fprintf(stderr,"Couldn't open %s for input.  errno %d\n",
                 AD->sourcefile, errno);
         exit(MSC_EXIT_SOURCE);
      }
      if (AD->src_offset != 0)
      {
/* NOTE: the difference between 32 and 64 bit seek values might
 * require this to all change to lseek64
 */
         ret = fseek(reader, AD->src_offset, SEEK_SET);
         if (ret == -1)
         {
            fprintf(stderr, "couldn't lseek to %ld in input file\n", AD->src_offset);
            exit(MSC_EXIT_SOURCE);
         }
      }
      else
      {   /* only do this if a source offset wasn't specified */
         if (AD->lastgood > 0)   /* check to see if partial transfer */
         {
            ret = fseek(reader, AD->lastgood, SEEK_SET);
            if (ret == -1)
            {
               fprintf(stderr, "couldn't lseek to %lld in input file\n", AD->lastgood);
               exit(MSC_EXIT_SOURCE);
            }
         }
      }
   }
   else
   {
      DEBUG0("dolocal: input from stdin\n");
      DEBUGSYNC;
      reader = stdin;
   }

      /* go transfer data */
   if (AD->sourcefile != NULL && AD->destfile != NULL)
      xferdata_segmented(reader, AD);
   else
      xferdata(reader, AD);
   DEBUG0("dolocal: done with data transfer\n");

   /* close sockets */
   closesockets(AD);
   /* return to wait for children */
}



/* Child process from local: execute ssh and run the private positional -y
 * receiver ABI.  Paths and the command argument are shell-quoted because ssh
 * passes its command through the remote user's shell. */
void run_remote(struct argdata *AD)
{
   char *cmd;
   char *debug;
   char *envpfx, *qbin, *qdest, *qprogram, *qinterface, *qcheckpoint;
   unsigned int wire_packetsize;

   DEBUG0("in run_remote\n");
   DEBUGSYNC;
   /* connect to remote machine*/
      /* set up stdio for exec */
   dup2(CHILD_READ_REMOTE, STDIN_FILENO);
   dup2(CHILD_WRITE_REMOTE, STDOUT_FILENO);
   /* Symmetrically, drop the parent's ends here: they are dup'ed onto stdin
    * and stdout now, and holding the parent's side open would keep the remote
    * from ever seeing EOF if the local end goes away. */
   close(PARENT_READ_REMOTE);
   close(PARENT_WRITE_REMOTE);
   close(CHILD_READ_REMOTE);
   close(CHILD_WRITE_REMOTE);
      /* build remote command string */
   if (msc_debug != 0)
      debug = " -d";
   else
      debug = "";
   envpfx = msc_udp_remote_env();
   qbin = shell_quote(AD->remote_binary);
   qinterface = shell_quote(AD->interface_host ? AD->interface_host : "-");
   qcheckpoint = shell_quote(AD->checkpoint_path ? AD->checkpoint_path : "-");
   /* An implicit UDP -s crosses the wire as 0 so the receiver also hands MSC UDP
    * payload 0 and both ends adopt the same PMTUD-probed size; see udp_payload().
    * TCP and explicit -s forward the real (nonzero) value unchanged. */
   wire_packetsize = AD->udp && !AD->packetsize_explicit ? 0 : AD->packetsize;
   if (AD->destfile == NULL)
   {
      qprogram = shell_quote(AD->remote_program);
      if (asprintf(&cmd, "%s%s -y%s %d %d %d %d %d %d %d %d %u %d %d %d %d %d %d %d %s %llu %d %d %s - 0 %lld %lld %s\n", envpfx,
              qbin, debug,
              AD->parentsets, AD->obeylastgood,
              AD->numstreams, AD->portnum, wire_packetsize, AD->numsets, AD->recursive,
              AD->udp, AD->port_tries, AD->force, AD->gso, AD->gro, AD->stats,
              AD->no_internal_checksum, AD->resume, AD->keep_checkpoint, qcheckpoint,
              (unsigned long long)AD->stall_timeout_ms,
              AD->my_ost_start, AD->my_ost_count, qinterface,
              (long long int)AD->xferlen, (long long int)AD->inode,
              qprogram) < 0) exit(MSC_EXIT_INTERNAL);
      free(qprogram);
   }
   else
   {
      qdest = shell_quote(AD->destfile);
      if (asprintf(&cmd, "%s%s -y%s %d %d %d %d %d %d %d %d %u %d %d %d %d %d %d %d %s %llu %d %d %s %s %lld %lld %lld -\n", envpfx,
              qbin, debug,
              AD->parentsets, AD->obeylastgood,
              AD->numstreams, AD->portnum, wire_packetsize, AD->numsets, AD->recursive,
              AD->udp, AD->port_tries, AD->force, AD->gso, AD->gro, AD->stats,
              AD->no_internal_checksum, AD->resume, AD->keep_checkpoint, qcheckpoint,
              (unsigned long long)AD->stall_timeout_ms,
              AD->my_ost_start, AD->my_ost_count, qinterface, qdest,
              (long long int)AD->dst_offset, (long long int)AD->xferlen,
              (long long int)AD->inode) < 0) exit(MSC_EXIT_INTERNAL);
      free(qdest);
   }
   free(envpfx); free(qbin); free(qinterface); free(qcheckpoint);
      /* run ssh command */
   DEBUG1("run_remote: command %s\n", cmd);
   DEBUGSYNC;
   run_remote_command(CHILD_READ_REMOTE, CHILD_WRITE_REMOTE, AD->remote_user, 
                      *(AD->remote_machines), cmd);
       /* does not return */
} 

/* continuation of main for client process */
static void local_singlemachine_attempt(struct argdata *AD)
{
      /* set up pipes */
   if (pipe(readpiper) == -1)
   {
      fprintf(stderr,"couldn't create read pipe for remote command\n");
      exit(MSC_EXIT_INTERNAL);
   }
   if (pipe(writepiper) == -1)
   {
      fprintf(stderr,"couldn't create write pipe for remote command\n");
      exit(MSC_EXIT_INTERNAL);
   }
      /* remote starts as a child process */
      /* rest of program runs in dolocal */
   DEBUG0("in local_singlemachine_start\n");
   DEBUGSYNC;
   makechild(dolocal, run_remote, AD, "remote process");
}

static uint64_t reconnect_now_ms(void)
{
   struct timespec now;
   clock_gettime(CLOCK_MONOTONIC, &now);
   return (uint64_t)now.tv_sec * 1000U + (uint64_t)now.tv_nsec / 1000000U;
}

/* One application-level request/response over a fresh SSH connection. Using
 * the data-plane host here verifies the same route the transfer needs, while
 * keeping the probe independent of the timed-out data sockets. */
static int probe_remote(struct argdata *AD)
{
   const char *test_marker = getenv("MSC_TEST_PROBE_MARKER");
   char *userhost, *qbin, *command;
   char response[64] = "";
   int output[2], status;
   ssize_t used = 0;
   pid_t child;
   size_t userlen = AD->remote_user ? strlen(AD->remote_user) : 0;
   size_t hostlen = strlen(AD->remote_machines[0]);

   if (test_marker != NULL)
      return access(test_marker, F_OK) == 0 ? 0 : -1;
   if (pipe(output) != 0) return -1;
   userhost = checkmalloc(userlen + hostlen + 2, "probe user@host");
   if (AD->remote_user != NULL)
      sprintf(userhost, "%s@%s", AD->remote_user, AD->remote_machines[0]);
   else
      strcpy(userhost, AD->remote_machines[0]);
   qbin = shell_quote(AD->remote_binary);
   if (asprintf(&command, "%s --probe", qbin) < 0) exit(MSC_EXIT_INTERNAL);
   free(qbin);

   child = fork();
   if (child == 0)
   {
      /* Budgets follow the declared path class: the historic constants here
       * were a LAN's, and on a long path they report a live receiver as dead
       * (see msc_ssh_liveness()). */
      struct msc_ssh_liveness ssh;
      char connect_opt[48], interval_opt[48], count_opt[48];
      int nullfd = open("/dev/null", O_WRONLY);

      msc_ssh_liveness(&ssh, MSC_SSH_PROBE);
      snprintf(connect_opt, sizeof(connect_opt), "ConnectTimeout=%d", ssh.connect_timeout_s);
      snprintf(interval_opt, sizeof(interval_opt), "ServerAliveInterval=%d", ssh.alive_interval_s);
      snprintf(count_opt, sizeof(count_opt), "ServerAliveCountMax=%d", ssh.alive_count);
      close(output[0]);
      dup2(output[1], STDOUT_FILENO);
      if (nullfd >= 0) dup2(nullfd, STDERR_FILENO);
      close(output[1]);
      execlp(msc_ssh_program(), msc_ssh_program(),
            "-o", "BatchMode=yes", "-o", connect_opt,
            "-o", "ConnectionAttempts=1", "-o", interval_opt,
            "-o", count_opt, userhost, command, NULL);
      _exit(255);
   }
   close(output[1]);
   if (child < 0)
   {
      close(output[0]); free(userhost); free(command); return -1;
   }
   while (waitpid(child, &status, 0) < 0)
   {
      if (errno == EINTR && msc_cancelled())
      { kill(child, SIGTERM); continue; }
      if (errno != EINTR)
      { status = -1; break; }
   }
   while (used < (ssize_t)sizeof(response) - 1)
   {
      ssize_t n = read(output[0], response + used,
                       sizeof(response) - 1 - (size_t)used);
      if (n < 0 && errno == EINTR) continue;
      if (n <= 0) break;
      used += n;
   }
   response[used] = '\0';
   close(output[0]); free(userhost); free(command);
   return status != -1 && WIFEXITED(status) && WEXITSTATUS(status) == 0 &&
          strcmp(response, "MSC-PROBE-OK\n") == 0 ? 0 : -1;
}

static int wait_for_receiver(struct argdata *AD)
{
   uint64_t interval = AD->reconnect_interval_ms;
   fprintf(stderr,
           "msc reconnect: transfer is stalled; probing %s over SSH every %.1f seconds (Ctrl-C to stop)\n",
           AD->remote_machines[0], (double)interval / 1000.0);
   while (!msc_cancelled())
   {
      uint64_t started = reconnect_now_ms();
      if (probe_remote(AD) == 0)
      {
         fprintf(stderr,
                 "msc reconnect: receiver responded; resuming from the durable checkpoint\n");
         return 0;
      }
      if (!msc_cancelled())
      {
         uint64_t elapsed = reconnect_now_ms() - started;
         uint64_t delay = elapsed < interval ? interval - elapsed : 1;
         fprintf(stderr, "msc reconnect: no response; next SSH probe in %.1f seconds\n",
                 (double)delay / 1000.0);
         if (msc_interruptible_delay(delay) != 0) break;
      }
   }
   return -1;
}

static void reap_attempt_group(pid_t pgid)
{
   struct timespec pause = { 0, 10000000L };
   unsigned int tries;

   /* The direct attempt child has already been waited for, but an SSH child
    * may still be unwinding.  Do not let a retained control socket or worker
    * overlap the next, completely fresh session. */
   if (pgid <= 0) return;
   if (kill(-pgid, SIGTERM) != 0 && errno == ESRCH) return;
   for (tries = 0; tries < 200; tries++)
   {
      if (kill(-pgid, 0) != 0 && errno == ESRCH) return;
      nanosleep(&pause, NULL);
   }
   kill(-pgid, SIGKILL);
   for (tries = 0; tries < 200; tries++)
   {
      if (kill(-pgid, 0) != 0 && errno == ESRCH) return;
      nanosleep(&pause, NULL);
   }
   fprintf(stderr, "MSC could not fully reap failed attempt process group %ld\n",
           (long)pgid);
   exit(MSC_EXIT_INTERNAL);
}

void local_singlemachine_start(struct argdata *AD)
{
   unsigned int attempt = 0;
   unsigned int retries_used = 0;
   uint64_t delay = AD->retry_delay_ms ? AD->retry_delay_ms : 1000;
   int auto_reconnect = AD->checkpoint_path != NULL &&
                        AD->reconnect_interval_ms != 0;
   if (AD->retries == 0 && !auto_reconnect)
   {
      local_singlemachine_attempt(AD);
      return;
   }
   for (;;)
   {
      int result[2], status;
      pid_t child;
      struct { long long sent, reused; } counts;
      if (pipe(result) != 0) { perror("MSC retry result pipe"); exit(MSC_EXIT_INTERNAL); }
      AD->retry_current = attempt;
      child = fork();
      if (child == 0)
      {
         setpgid(0, 0);
         const char *marker = getenv("MSC_TEST_RETRY_MARKER");
         close(result[0]);
         if (getenv("MSC_TEST_RETRY_PERMANENT") != NULL)
            _exit(MSC_EXIT_CLI);
         if (marker != NULL)
         {
            if (access(marker, F_OK) != 0)
            {
               int markerfd = open(marker, O_WRONLY | O_CREAT | O_EXCL, 0600);
               if (markerfd >= 0) close(markerfd);
               _exit(MSC_EXIT_NETWORK);
            }
            if (getenv("MSC_TEST_REQUIRE_RESUME") != NULL && !AD->resume)
               _exit(MSC_EXIT_CLI);
            counts.sent = 123; counts.reused = 456;
            if (write(result[1], &counts, sizeof(counts)) != (ssize_t)sizeof(counts))
               _exit(MSC_EXIT_INTERNAL);
            close(result[1]); _exit(0);
         }
         local_singlemachine_attempt(AD);
         counts.sent = AD->childinfo->xfercount; counts.reused = AD->childinfo->reused;
         if (write(result[1], &counts, sizeof(counts)) != (ssize_t)sizeof(counts))
            _exit(MSC_EXIT_INTERNAL);
         close(result[1]); _exit(0);
      }
      if (child < 0)
      { close(result[0]); close(result[1]); perror("MSC attempt fork"); exit(MSC_EXIT_INTERNAL); }
      setpgid(child, child);
      close(result[1]);
      while (waitpid(child, &status, 0) < 0)
      {
         if (errno == EINTR && msc_cancelled())
         { kill(-child, SIGTERM); continue; }
         if (errno != EINTR) { perror("MSC retry waitpid"); exit(MSC_EXIT_INTERNAL); }
      }
      if (WIFEXITED(status) && WEXITSTATUS(status) == 0)
      {
         if (read(result[0], &counts, sizeof(counts)) == (ssize_t)sizeof(counts))
         { AD->childinfo->xfercount = counts.sent; AD->childinfo->reused = counts.reused; }
         close(result[0]); return;
      }
      close(result[0]);
      reap_attempt_group(child);
      if (msc_cancelled()) return;
      if (!((WIFEXITED(status) && WEXITSTATUS(status) == MSC_EXIT_NETWORK) ||
            (WIFSIGNALED(status) && WTERMSIG(status) == SIGPIPE)))
      {
         int code = WIFEXITED(status) ? WEXITSTATUS(status) : MSC_EXIT_INTERNAL;
         fprintf(stderr, "MSC session failed permanently after %u retries (exit %d)\n",
                 retries_used, code);
         exit(code);
      }
      /* Every new attempt at retained state is explicit resume, including
       * ordinary finite retries (not only retries after SSH probe mode). */
      if (AD->checkpoint_path != NULL)
         AD->resume = 1;
      if (retries_used >= AD->retries && auto_reconnect)
      {
         if (wait_for_receiver(AD) != 0) return;
         retries_used = 0;
         attempt++;
         delay = AD->retry_delay_ms ? AD->retry_delay_ms : 1000;
         continue;
      }
      if (retries_used >= AD->retries)
      {
         fprintf(stderr, "MSC session failed permanently after %u retries (exit %d)\n",
                 retries_used, MSC_EXIT_NETWORK);
         exit(MSC_EXIT_NETWORK);
      }
      {
         uint64_t bounded = delay > 30000 ? 30000 : delay;
         uint64_t jitter = bounded / 4 ? ((uint64_t)getpid() + attempt * 1103515245U) % (bounded / 4) : 0;
         fprintf(stderr, "msc retry: transient session failure, retry %u/%u in %.3fs; durable bytes will not be resent\n",
                 retries_used + 1, AD->retries, (double)(bounded + jitter) / 1000.0);
         if (msc_interruptible_delay(bounded + jitter) != 0) return;
         if (delay <= 15000) delay *= 2;
         retries_used++;
         attempt++;
      }
   }
}
