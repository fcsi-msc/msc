#include "msc.h"
#include "udp_session.h"   /* msc_lustre_*: shared Lustre destination helpers */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

   /* do research on destination file. 
    *  If numsets > 1  (multimachine)
    *     create file if it doesn't exist
    *  If file exists
    *     return inode number
    *  return end of valid data
    *
    *  format:  'inode,endofdata'
    */
/* Report a failure the initiator can act on.  The -x reply is a single line on
 * stdout, and until now findzero always had one to print, so exiting silently
 * would just leave the initiator blocked reading the pipe.  An 'ERROR,<code>'
 * line gives it the real exit code instead; the human-readable reason has
 * already gone to stderr, which rides the same ssh session.  Only ever emitted
 * for a destination-layout request, which an older initiator never makes. */
static void findzero_fail(int code)
{
   printf("ERROR,%d\n", code);
   fflush(stdout);
   exit(code);
}

   /* print out the end of valid data */
void findzero_process(struct argdata *AD)
{
   int e;
   struct stat statbuf;
   ino_t inode;
   off_t size;

   /* Despite the historical name, this no longer scans for zero-filled holes.
    * Resume treats the existing file size as the end of valid data. */
   e = stat(AD->destfile, &statbuf);

   DEBUG1("findzero_process: file %s\n",AD->destfile);
   DEBUGSYNC;
   if (AD->numsets > 1)
   {
      if (e < 0)
      {
         /* This is the shared multimachine destination, and this is the only
          * moment its Lustre layout can be set: every worker afterwards opens
          * this same inode in place (its number is handed to them via -z), so
          * nothing downstream may unlink and recreate it. */
         if (AD->dest_stripe_count != 0)
         {
            e = msc_lustre_create_striped(AD->destfile, O_CREAT | O_EXCL | O_RDWR,
                                          0666, 0, AD->dest_stripe_count);
            if (e < 0)
            {
               fprintf(stderr, "msc -x: cannot create %s with stripe count "
                               "%ld: %s\n",
                       AD->destfile, AD->dest_stripe_count, strerror(errno));
               findzero_fail(MSC_EXIT_DESTINATION);
            }
            close(e);
         }
         else
            e = creat(AD->destfile, 0666);
         DEBUG3("findzero_process: create file %s return %d errno %d\n",
                  AD->destfile, e, errno);
         DEBUGSYNC;
      }
      else if (AD->dest_stripe_count != 0)
      {
         /* Already there, so its layout is already fixed. Report the mismatch
          * rather than transferring onto a file whose OST width is not the one
          * that was asked for. */
         uint32_t actual = 0;
         if (msc_lustre_check_stripe_count(AD->destfile, AD->dest_stripe_count,
                                           &actual) != 0)
         {
            fprintf(stderr, "msc -x: destination %s has stripe count %u, not "
                            "the requested %ld; its layout was fixed when it "
                            "was created\n",
                    AD->destfile, actual, AD->dest_stripe_count);
            findzero_fail(MSC_EXIT_DESTINATION);
         }
      }
      e = stat(AD->destfile, &statbuf);
   }
   if (e < 0)
   {
      inode = 0; 
      size = 0;
      DEBUG2("findzero_process: error from stat %d, errno %d\n",e,errno);
      DEBUGSYNC;
   }
   else
   {
      inode = statbuf.st_ino;
      size = statbuf.st_size;
      DEBUG2("findzero_process: inode %lu size %ld\n", inode,size);
      DEBUGSYNC;
   }
   printf("%lld,%lld\n", (long long int) inode, (long long int) size);
   exit(0);
}
