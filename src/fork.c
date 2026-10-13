#include <unistd.h>
#include <sys/types.h>
#include <errno.h>
#include <sys/wait.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>

#include "msc.h"

#define MSC_FINISHED_GRACE_MS 5000

/* Once the receiver has sent its final record, nothing its process does can
 * change the outcome. Wait a few seconds for it to exit, then stop waiting, so
 * a receiver stuck afterwards (closing a file on slow storage, say) cannot hold
 * msc. Returns 1 once the child has been reaped into *status, 0 if it was
 * abandoned. */
static int reap_finished_child(pid_t child, int *status)
{
   int waited;
   for (waited = 0; waited < MSC_FINISHED_GRACE_MS; waited += 50)
   {
      pid_t r = waitpid(child, status, WNOHANG);
      if (r == child)
         return 1;
      if (r < 0 && errno != EINTR)
         return 0;
      usleep(50000);
   }
   fprintf(stderr, "MSC receiver had not exited %d s after confirming the transfer; "
                   "not waiting for it\n", MSC_FINISHED_GRACE_MS / 1000);
   kill(child, SIGTERM);
   while (waitpid(child, status, 0) < 0 && errno == EINTR)
      ;
   return 0;
}

/* create child process and run a routine in the parent.  When
 * parent routine is done, wait for child.
 */
void makechild(msc_child_fn pprocess, msc_child_fn cprocess,
               struct argdata *arg,
               char *msg)
{

   pid_t childpid;
   int status;
   
   childpid = fork();

   if (childpid >= 0)
   {
      if (childpid == 0) /* 0 is the child */
      {
         cprocess(arg);
         /* Child callbacks normally exec or exit.  Never let an accidental
          * return continue through the caller's parent-side control flow. */
         _exit(MSC_EXIT_INTERNAL);
      }
      else
      {
         pprocess(arg);
         if (msc_cancelled())
            kill(childpid, SIGTERM);
         if (arg->finished && !msc_cancelled())
         {
            if (!reap_finished_child(childpid, &status))
               return;   /* the transfer is confirmed; its receiver is not needed */
         }
         else while (waitpid(childpid, &status, 0) < 0)
         {
            if (errno == EINTR)
               continue;
            perror("waitpid");
            exit(MSC_EXIT_INTERNAL);
         }
         if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
         {
            if (msc_cancelled()) return;
            if (WIFEXITED(status) && WEXITSTATUS(status) == 255)
            {
               fprintf(stderr, "MSC SSH/peer launch failed for %s (exit 255); verify host, key authentication, and remote msc path\n", msg);
               exit(MSC_EXIT_AUTH);
            }
            fprintf(stderr, "child process failed for %s\n", msg);
            if (WIFEXITED(status) && WEXITSTATUS(status) >= MSC_EXIT_CLI &&
                WEXITSTATUS(status) <= MSC_EXIT_INCOMPATIBLE)
               exit(WEXITSTATUS(status));
            exit(MSC_EXIT_INTERNAL);
         }
      }
   }
   else
   {
      fprintf(stderr,"problem in fork for %s\n", msg);
      perror("fork");
      exit(MSC_EXIT_INTERNAL);
   }
}

/* return child pid */
pid_t makechild_inline(msc_indexed_child_fn cprocess, struct argdata *arg,
                       int childnum,
                       char *msg)
{

   int childpid;
   
   childpid = fork();

   if (childpid >= 0)
   {
      if (childpid == 0) /* 0 is the child */
      {
         cprocess(arg, childnum);
         _exit(MSC_EXIT_INTERNAL);
      }
      else
      {
         return childpid;
      }
   }
   else
   {
      fprintf(stderr,"problem in fork for %s\n", msg);
      perror("fork");
   }
   return -1;
}

/* run remote command as child */
void run_remote_command(int readfd, int writefd, 
                        char *user, char *host, char *command)
{
   char *s;
   int ulen, hlen;

         /* connect up stdin and stdout */
   dup2 (readfd, STDIN_FILENO);
   dup2 (writefd, STDOUT_FILENO);

         /* create user@host string */

   if (user != NULL)
      ulen = strlen(user);
   else
      ulen = 0;
   if (host != NULL)
      hlen = strlen(host);
   else
      hlen = 0;

   s = checkmalloc(ulen + hlen + 2, "run_remote_command");

   if (user == NULL)
      sprintf(s,"%s",host);
   else
      sprintf(s,"%s@%s",user,host);

   DEBUG2("run_remote_command: readfd %d writefd %d\n",readfd, writefd);
   DEBUG2("run_remote_command userhost: %s command: %s\n",s,command);

   /* Keepalive, so a path that goes away mid-transfer is REPORTED rather than
    * leaving this session parked forever on a connection the peer has already
    * forgotten -- on the stdio control modes that session IS the control
    * channel.  Interval and count follow the declared path class
    * (msc_ssh_liveness()); a satellite hop needs a budget a LAN does not.
    *
    * Deliberately NOT BatchMode: msc documents public-key auth, but a user
    * with an encrypted key and no agent still gets a passphrase prompt today,
    * and BatchMode would turn that prompt into a hard failure. */
   {
      struct msc_ssh_liveness ssh;
      char interval_opt[48], count_opt[48];

      msc_ssh_liveness(&ssh, MSC_SSH_LAUNCH);
      snprintf(interval_opt, sizeof(interval_opt), "ServerAliveInterval=%d", ssh.alive_interval_s);
      snprintf(count_opt, sizeof(count_opt), "ServerAliveCountMax=%d", ssh.alive_count);
      execlp(msc_ssh_program(), msc_ssh_program(),
             "-o", interval_opt, "-o", count_opt, s, command, NULL);
   }

   fprintf(stderr,"Error exec ssh\n");
   free(s);
   exit(MSC_EXIT_AUTH);
} 

/* run command on local machine */
void run_local_command(int readfd, int writefd, char *command)
{
         /* connect up stdin and stdout */
   dup2 (readfd, STDIN_FILENO);
   dup2 (writefd, STDOUT_FILENO);

   /* Commands are currently supplied as shell strings by multimachine mode. */
   DEBUG1("run_local_command command: %s\n",command);
   DEBUGSYNC;
   execl("/bin/sh", "/bin/sh", "-c", command, NULL);

   fprintf(stderr,"Error exec sh\n");
   exit(MSC_EXIT_INTERNAL);
} 
