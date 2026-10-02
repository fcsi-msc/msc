#include "msc.h"
#include "udp_session.h"   /* msc_udp_query_stripe(): Lustre source-layout probe */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <getopt.h>
#include <errno.h>
#include <dirent.h>


extern void remote_process(struct argdata *);
extern void findzero_process(struct argdata *AD);

static __attribute__((noreturn)) void parse_exit(int status)
{
   /* Legacy exit(1) calls in this file are command-line errors.  A status
    * already inside the stable contract (e.g. INCOMPATIBLE for a remote ABI
    * mismatch) passes through so the launcher can classify it. */
   if (status == 0)
      exit(MSC_EXIT_OK);
   if (status >= MSC_EXIT_CLI && status <= MSC_EXIT_INCOMPATIBLE)
      exit(status);
   exit(MSC_EXIT_CLI);
}
#define exit parse_exit

/* Printed partition commands are intended to be pasted into a POSIX shell.
 * Quote each dynamic argument as one word, including embedded apostrophes. */
static void print_shell_word(const char *value)
{
   const char *p;
   putchar('\'');
   for (p = value; *p != '\0'; p++)
   {
      if (*p == '\'') fputs("'\\''", stdout);
      else putchar((unsigned char)*p);
   }
   putchar('\'');
}


void usage(struct argdata *AD)
{
   fprintf(stderr,"msc version %s usage: \n", AD->version);
   fprintf(stderr, "(run 'msc man' for the full manual, 'msc --version' for the version)\n");
   fprintf(stderr, "%s %s %s %s",
 "msc [-h] [-l localmachine,localmachine...]",
 "-r remote[,remote...] [-i inputfile] [-o outputfile]|[-c remote command]",
 "[-t xferlen] [-a offset] [-b offset]",
 "[-u username] [-B remote-msc] [-n numstreams] [-p port] [-s chunksize] "
 "[-R] [-T|-U] [-I data-host] [--env NAME] [--force] [--gso|--no-gso] [--gro|--no-gro] [--stats] "
 "[--checksum=true|false] [--port-tries N] [--udp-data-ports K] "
 "[--udp-port-base PORT] [--udp-port-span S] [--udp-ports LIST] "
 "[--resume] [--checkpoint PATH] "
 "[--keep-checkpoint] [--retries N] [--retry-delay DURATION] "
 "[--progress=auto|always|never] [--progress-interval DURATION] "
 "[--stall-timeout DURATION] [--reconnect-interval DURATION]\n");
}

/* printed when -h specified */
void help(struct argdata *AD)
{
   usage(AD);
   fprintf(stderr," -h: display this help text\n");
   fprintf(stderr," -l: source machine list; count must match -r.\n");
   fprintf(stderr," -i: input file.  If omitted, use stdin.\n");
   fprintf(stderr," -o: output file. Must be specified if -c is not.\n");
   fprintf(stderr," -t: number of bytes to transfer (usually not specified)\n");
   fprintf(stderr," -a: offset to begin transfer. seek to this location in source file\n");
   fprintf(stderr," -b: offset to begin transfer. seek to this location in destination file\n");
   fprintf(stderr," -c: command to send output to. Must be quoted.  Must be specified if -o is not.\n");
   fprintf(stderr," -u: username to use at remote.  Must have public key for ssh from current user.\n");
   fprintf(stderr," -B: path to the msc binary to launch on the remote side, default \"msc\" (found on the remote PATH).  Set an absolute path to avoid needing msc on the remote's non-interactive PATH.\n");
   fprintf(stderr," -n: TCP streams or MSC UDP flows; defaults: %d recursive TCP, %d recursive UDP, %d single-file TCP, %d single-file UDP, %d pipe (pipe and -c transfers are always TCP)\n",
         DEFAULT_TREE_STREAMS, DEFAULT_UDP_TREE_FLOWS, DEFAULT_SINGLE_STREAMS,
         DEFAULT_UDP_SINGLE_FLOWS, DEFAULT_STREAMS);
   fprintf(stderr," -p: port number to use at remote, default %d\n",
         DEFAULT_PORT);
   fprintf(stderr," -s: TCP segment/pipe chunk bytes (default %d) or MSC UDP payload bytes (default: auto-sized by path MTU discovery, up to jumbo)\n",
         DEFAULT_PACKETSIZE);
   fprintf(stderr," -R: recursively transfer a directory of regular files\n");
   fprintf(stderr," -T: use TCP instead of the default reliable-UDP data path\n");
   fprintf(stderr," -U: use the reliable-UDP data path (the default; -U spells it out)\n");
   fprintf(stderr," -I: host/IP used by the UDP sender for the MSC UDP connection\n");
   fprintf(stderr," --env NAME: transport preset for the path being crossed: auto (default, picks\n");
   fprintf(stderr,"      from the measured RTT) | lan | wan | fiber | fiber-shared |\n");
   fprintf(stderr,"      wan-long | fiber-long | longhaul (long-haul/intercontinental) | geo | satellite\n");
   fprintf(stderr," --force: atomically replace an existing single-file UDP destination\n");
   fprintf(stderr," --gso / --no-gso: enable or disable MSC UDP_SEGMENT (default enabled)\n");
   fprintf(stderr," --gro / --no-gro: enable or disable the MSC UDP_GRO receive path (default enabled; silent no-op on kernels without UDP_GRO)\n");
   fprintf(stderr," --stats: print MSC UDP transport telemetry\n");
   fprintf(stderr," --checksum=true|false: enable or disable the UDP checksum/publish handshake (default false)\n");
   fprintf(stderr," --port-tries N: control ports to try from -p (default 100)\n");
   fprintf(stderr," --udp-data-ports K: UDP only; carry the -n flows on K UDP data sockets (flow f uses socket f mod K; default min(-n,8))\n");
   fprintf(stderr," --udp-port-base PORT: UDP only; first port scanned for the K data ports (default 17400)\n");
   fprintf(stderr," --udp-port-span S: UDP only; how many ports the scan may try (default 4*K; 0 = bind the exact block PORT..PORT+K-1)\n");
   fprintf(stderr," --udp-ports LIST: UDP only; use exactly these ports, e.g. 20000,20004,20100-20103 (fails if any is busy)\n");
   fprintf(stderr," --resume: require a valid persisted checkpoint and continue missing durable ranges\n");
   fprintf(stderr," --checkpoint PATH: receiver-side checkpoint path; enables a resumable fresh transfer\n");
   fprintf(stderr," --keep-checkpoint: retain a completed checkpoint\n");
   fprintf(stderr," --retries N: retry transient session failures up to N times\n");
   fprintf(stderr," --retry-delay DURATION: initial retry delay in ms, s, or m (default 1s)\n");
   fprintf(stderr," --progress=auto|always|never: rate-limited progress on stderr\n");
   fprintf(stderr," --progress-interval DURATION: progress refresh interval (default 1s)\n");
   fprintf(stderr," --stall-timeout DURATION: stop after no network progress (default 30s; 0 disables)\n");
   fprintf(stderr," --reconnect-interval DURATION: SSH probe interval after a resumable stall (default 10s; 0 disables)\n");
   fprintf(stderr," -x: don't transfer, just print out msc commands that would be run for a multimachine transfer.  Useful for restarting sections that may have failed.\n");
   fprintf(stderr,"\n");
   fprintf(stderr," Transport: reliable UDP by default, TCP with -T.  The UDP engine moves a\n");
   fprintf(stderr,"      file to a file, so a transfer that reads stdin or writes to a -c\n");
   fprintf(stderr,"      program runs on TCP; passing -U to one of those is an error.\n");
   fprintf(stderr,"\n");
   fprintf(stderr," Set --env to the kind of path you are crossing whenever you know it.  The\n");
   fprintf(stderr,"      defaults were tuned on a local fabric and are slow over latency; auto\n");
   fprintf(stderr,"      only guesses from RTT, and not until the transfer is already running:\n");
   fprintf(stderr,"        same fabric/rack (<5ms)     --env lan   (or nothing)\n");
   fprintf(stderr,"        another city (5-120ms)      --env wan\n");
   fprintf(stderr,"        another continent (120ms+)  --env wan-long\n");
   fprintf(stderr,"        satellite hop (450ms+)      --env geo\n");
   fprintf(stderr,"      MSC_UDP_STATS=1 prints which one is in force and the measured RTT.\n");
   fprintf(stderr,"\n");
   fprintf(stderr," example: msc -l localhost -i localfile -r remotehost -o remotefile\n");
   fprintf(stderr," example: msc --env wan -l localhost -i localfile -r remotehost -o remotefile\n");
   fprintf(stderr," example: msc -R --env wan-long -l localhost -i localdir -r remotehost -o remotedir\n");
   fprintf(stderr," example: msc -B /opt/msc/msc -u datamover -l localhost -i localfile -r remotehost -o remotefile\n");
   fprintf(stderr," example: msc --checkpoint f.cp -l localhost -i localfile -r remotehost -o remotefile\n");
   fprintf(stderr,"      then: msc --resume --checkpoint f.cp ... (continues where it stopped)\n");
   fprintf(stderr," example: msc --udp-data-ports 4 --udp-port-base 20000 --udp-port-span 16 ...  (pin the UDP data ports for a firewall)\n");
   fprintf(stderr," example: msc -T -l localhost -i localfile -r remotehost -o remotefile\n");
   fprintf(stderr," example: tar cf - . | msc -l localhost -r remotehost -c 'cd dir;tar xf -'   (TCP)\n");
   fprintf(stderr," example: msc -l ihost1,ihost2 -r ohost1,ohost2 -i infile -o outfile\n");
   fprintf(stderr,"\n run 'msc man' for the full manual (transports, install, examples)\n");
   exit(0);
}

/* if source isn't a full path, add the working directory to the front.
 * Copy to dest */
void full_pathify(char **dest, char *source)
{
   char wd[4096];

   if (*source == '/')  /* for now assume full path starts with a / */
   {
      *dest = checkmalloc(strlen(source)+1,"source file");
      strcpy(*dest,source);
   }
   else
   {
      /* getcwd, not getenv("PWD"): PWD is unset under cron/systemd and can
       * be stale after chdir; a NULL here used to crash. */
      if (getcwd(wd, sizeof(wd)) == NULL)
      {
         fprintf(stderr, "MSC cannot resolve the current directory for %s: %s\n",
                 source, strerror(errno));
         exit(MSC_EXIT_CLI);
      }
      *dest = checkmalloc(strlen(wd)+strlen(source)+4,"source file");
      sprintf(*dest,"%s/%s", wd, source);
   }
}

void setup_debug()
{
   char *home;
   pid_t pid;
   char host[256 + 2];
   char *fname;

   home = getenv("HOME");
   pid = getpid();
      /* actually max hostname len on linux is 64, but posix is 256 */
   gethostname(host, 256 + 2);
   fname = malloc(strlen(host) + strlen(home) + 50);
   if (fname == NULL)
   {
      fprintf(stderr,"Couldn't malloc fname in setup_debug\n");
      exit(1);
   }
   sprintf(fname,"%s/MSC-DEBUG-%s-%d",home, host, pid);
   msc_debugout = fopen(fname, "w");
   if (msc_debugout == NULL)
      msc_debug = 0; /* pretend nothing happened if we can't open file */
   free(fname);
}

/* Adaptive default tuning: when -n is implicit, gather
 * the workload-shape facts the tuned defaults will branch on -- single-file
 * size from stat(), and a shallow (one-level, deliberately non-recursive)
 * top-level entry count for -R. Fact-gathering only: a per-shape stream count
 * should land here only once a measured sweep supports it. The shallow count is a knowingly imprecise proxy (a small top level over a
 * deep nested tree misclassifies); an accurate version would need the
 * manifest, i.e. a protocol change. */
static void probe_workload_shape(struct argdata *AD)
{
   AD->shape_single_bytes = -1;
   AD->shape_toplevel_entries = -1;
   AD->shape_stripe_count = -1;
   if (AD->numstreams_explicit || AD->sourcefile == NULL)
      return;
   /* Lustre stripe count of the source (a file's layout, or a
    * directory's default layout for -R). msc_udp_query_stripe() stubs to -1 in
    * a non-LUSTRE build, off Lustre, and for layoutless files, so those all
    * take the shape defaults unchanged. MSC_TEST_STRIPE_COUNT
    * injects a value so the plumbing is testable on non-Lustre hosts. */
   {
      const char *fake = getenv("MSC_TEST_STRIPE_COUNT");
      uint32_t stripes = 0;
      if (fake != NULL)
         AD->shape_stripe_count = atol(fake);
      else if (msc_udp_query_stripe(AD->sourcefile, NULL, &stripes) == 0)
         AD->shape_stripe_count = (long)stripes;
   }
   if (!AD->recursive)
   {
      struct stat st;
      if (stat(AD->sourcefile, &st) == 0 && S_ISREG(st.st_mode))
         AD->shape_single_bytes = (long long)st.st_size;
      DEBUG1("workload shape: single file, %lld bytes\n",
             AD->shape_single_bytes);
      DEBUG1("workload shape: source stripe count %ld\n",
             AD->shape_stripe_count);
   }
   else
   {
      DIR *dir = opendir(AD->sourcefile);
      struct dirent *ent;
      long entries = 0;
      if (dir == NULL)
         return;
      while ((ent = readdir(dir)) != NULL)
      {
         if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
            continue;
         entries++;
      }
      closedir(dir);
      AD->shape_toplevel_entries = entries;
      DEBUG1("workload shape: recursive, %ld top-level entries\n", entries);
      DEBUG1("workload shape: source stripe count %ld\n",
             AD->shape_stripe_count);
   }
}

/* MSC_DEST_STRIPE_COUNT as the ssh-launched -y receiver, the -x findzero probe
 * and per-pair children see it: the one carrier that reaches the setup paths
 * running before (or entirely without) a UDP greeting. The initiating side
 * validated and re-exported the real request, so a value that fails to parse
 * here was hand-set; refuse it loudly rather than transferring to a
 * default-striped destination the caller thinks is striped. */
static long dest_stripe_count_from_env(void)
{
   const char *text = getenv("MSC_DEST_STRIPE_COUNT");
   char *end;
   long value;

   if (text == NULL || *text == '\0')
      return 0;
   errno = 0; value = strtol(text, &end, 10);
   if (errno != 0 || *end != '\0' || value == 0 || value < -1 ||
       value > MAX_DEST_STRIPE_COUNT)
   {
      fprintf(stderr, "MSC_DEST_STRIPE_COUNT must be an OST count from 1 to %d, "
                      "or -1 for every OST (got '%s')\n",
              MAX_DEST_STRIPE_COUNT, text);
      exit(MSC_EXIT_CLI);
   }
   return value;
}

void parse_findzero(int argc, char *argv[], struct argdata *AD)
{
   int argnum = 2;
    /* findzero: msc -x [-d] numsets destfile finalsize*/

   if ((argc != 5) && (argc != 6))
   {
      fprintf(stderr,"Fatal error running -x option, %d args\n",argc);
      exit(1);
   }
   if (strcmp(argv[argnum], "-d") == 0)
   {
      argnum++;
      msc_debug = 1;
   }
   AD->numsets = atoi(argv[argnum++]);
   AD->destfile = argv[argnum++];
   AD->xferlen = atoll(argv[argnum++]);
   /* findzero creates the shared multimachine destination, so it is the one
    * place its Lustre layout can still be chosen; the -x argument list stays
    * unchanged and the request rides the environment instead. */
   AD->dest_stripe_count = dest_stripe_count_from_env();
   if (msc_debug != 0)
      setup_debug();
   DEBUG1("msc version %s\n", AD->version);
   
}

void parse_remote(int argc, char *argv[], struct argdata *AD)
{
   int argnum = 2;
   int remote_len;

   /* Internal receiver ABI, not a user-facing CLI.  Keep this positional form
    * synchronized with both run_remote() format strings in local_single.c.
    * argc is 28 normally and 29 when the optional -d occupies argv[2]. */
   /* one of outfile or command must be '-' */
   if (argc != 28 && argc != 29)
   {
      /* Almost always a version-skewed local/remote pair (the positional -y
       * ABI changed size); classify as incompatible so the launcher reports
       * it as such instead of retrying. */
      fprintf(stderr, "MSC remote mode received an invalid argument count; "
                      "local and remote msc versions are likely incompatible\n");
      exit(MSC_EXIT_INCOMPATIBLE);
   }

   if (strcmp(argv[argnum], "-d") == 0)
   {
      argnum++;
      msc_debug = 1;
      setup_debug();
   }
   DEBUG1("msc version %s\n", AD->version);
                           DEBUG1("msc -y arg: %s\n",argv[argnum]);
   AD->parentsets = atoi(argv[argnum++]);
                           DEBUG1("msc -y arg: %s\n",argv[argnum]);
   AD->obeylastgood = atoi(argv[argnum++]);
                           DEBUG1("msc -y arg: %s\n",argv[argnum]);
   AD->numstreams = atoi(argv[argnum++]);
                           DEBUG1("msc -y arg: %s\n",argv[argnum]);
   AD->portnum = atoi(argv[argnum++]);
                           DEBUG1("msc -y arg: %s\n",argv[argnum]);
   AD->packetsize = atoi(argv[argnum++]);
   /* run_remote forwards 0 when a UDP -s was left implicit: both ends must
    * then hand MSC UDP payload 0 so the shared PMTUD probe picks the same size.
    * Nonzero means an explicit -s honored verbatim on both ends (TCP always
    * forwards its nonzero segment size). */
   AD->packetsize_explicit = AD->packetsize != 0;
                           DEBUG1("msc -y arg: %s\n",argv[argnum]);
   AD->numsets = atoi(argv[argnum++]);
                           DEBUG1("msc -y arg: %s\n",argv[argnum]);
   AD->recursive = atoi(argv[argnum++]);
                           DEBUG1("msc -y arg: %s\n",argv[argnum]);
   AD->udp = atoi(argv[argnum++]);
   AD->port_tries = (unsigned int)atoi(argv[argnum++]);
   AD->force = atoi(argv[argnum++]);
   AD->gso = atoi(argv[argnum++]);
   AD->gro = atoi(argv[argnum++]);
   AD->stats = atoi(argv[argnum++]);
   AD->no_internal_checksum = atoi(argv[argnum++]);
   AD->resume = atoi(argv[argnum++]);
   AD->keep_checkpoint = atoi(argv[argnum++]);
   AD->checkpoint_path = argv[argnum++];
   if (strcmp(AD->checkpoint_path, "-") == 0)
      AD->checkpoint_path = NULL;
   AD->stall_timeout_ms = strtoull(argv[argnum++], NULL, 10);
   AD->my_ost_start = atoi(argv[argnum++]);
   AD->my_ost_count = atoi(argv[argnum++]);
   AD->interface_host = argv[argnum++];
   if (strcmp(AD->interface_host, "-") == 0)
      AD->interface_host = NULL;
                           DEBUG1("msc -y arg: %s\n",argv[argnum]);
   AD->destfile = argv[argnum++];
                           DEBUG1("msc -y arg: %s\n",argv[argnum]);
   AD->dst_offset = atoll(argv[argnum++]);
                           DEBUG1("msc -y arg: %s\n",argv[argnum]);
   AD->xferlen = atoll(argv[argnum++]);
                           DEBUG1("msc -y arg: %s\n",argv[argnum]);
   AD->inode = atoi(argv[argnum++]);
                           DEBUG1("msc -y arg: %s\n",argv[argnum]);
   remote_len = strlen(argv[argnum]);
                           DEBUG1("msc -y arglen %d\n", remote_len);
   AD->remote_program = checkmalloc(remote_len+20,"remote_program");
   strcpy(AD->remote_program,argv[argnum++]);
   /* Not a positional: appending one would make the no-'-d' form 29 arguments,
    * which an older receiver's arity check accepts while silently ignoring the
    * value. The environment carries it to the TCP and resume setup paths, and
    * the UDP greeting (since protocol v5) carries it authoritatively. */
   AD->dest_stripe_count = dest_stripe_count_from_env();
   if ((strlen(AD->destfile) == 1) && (*(AD->destfile) == '-'))
      AD->destfile = NULL;
   if ((strlen(AD->remote_program) == 1) && 
               ((*AD->remote_program) == '-'))
      AD->remote_program = NULL;
}

 /* allocate space to store list of machines for -l and -r */
 /* returns number of machines */
int process_machine_list(char ***space, char *list)
{
    char *p, *newl, **point;
    int listlen;
    int nummach = 1;

      /* count commas to determine how many machines in list */
    listlen = strlen(list);
    p = list;
    while (p != NULL)
    {
       p = strchr(p, ','); /* return next comma or end of list */
       if (p != NULL)
       {
          p++;
          nummach++;
       }
    }

    *space = checkmalloc((nummach)*sizeof(char *) + listlen + 1,
                        "process_machine_list");

             /* now parse it all out */
             /* memory will look like:
              *   point
              *   point
              *   point 
              *   machine\0machine\0machine\0
              *
              *  So put in the pointers, copy the machine list, change
              *     commas to null
              */
        /* skip over the pointers */
   newl = ((char *)(*space)) + (sizeof(char *) * nummach);

   strcpy(newl, list);
   p = newl;
   point = *space;
   while (p != NULL)
   {
       *point = p;
       point++;  /* next pointer location */
       p = strchr(p, ','); /* return next comma or end of list */
       if (p != NULL)
       {
          *p = '\0';  /* change comma to null */
          p++;  /*next character, so it points at first character of next name*/
       }
   }
   return nummach;
}

void parseargs(int argc, char *argv[], struct argdata *AD)
{
   static const struct option longopts[] = {
      { "force", no_argument, NULL, 1000 },
      { "gso", no_argument, NULL, 1001 },
      { "gro", no_argument, NULL, 1002 },
      { "stats", no_argument, NULL, 1003 },
      { "no-internal-checksum", no_argument, NULL, 1004 },
      { "port-tries", required_argument, NULL, 1005 },
      { "no-gso", no_argument, NULL, 1006 },
      { "checksum", required_argument, NULL, 1007 },
      { "resume", no_argument, NULL, 1008 },
      { "checkpoint", required_argument, NULL, 1009 },
      { "keep-checkpoint", no_argument, NULL, 1010 },
      { "retries", required_argument, NULL, 1011 },
      { "retry-delay", required_argument, NULL, 1012 },
      { "progress", required_argument, NULL, 1013 },
      { "progress-interval", required_argument, NULL, 1014 },
      { "stall-timeout", required_argument, NULL, 1015 },
      { "reconnect-interval", required_argument, NULL, 1016 },
      { "no-gro", no_argument, NULL, 1017 },
      { "udp-data-ports", required_argument, NULL, 1018 },
      { "udp-port-base", required_argument, NULL, 1019 },
      { "udp-port-span", required_argument, NULL, 1020 },
      { "udp-ports", required_argument, NULL, 1021 },
      { "env", required_argument, NULL, 1022 },
      { "dest-stripe-count", required_argument, NULL, 1023 },
      { NULL, 0, NULL, 0 }
   };
   int needhelp=0;
   int opt;
   int numlocal = 0, numremote = 0;
   int lc;
   int partition_display_only = 0;

   /* program name used when launching msc on the remote side. Default "msc"
    * (resolved via the remote PATH, like rsync's default); -B overrides it with
    * an explicit path so msc need not be on the remote's non-interactive PATH. */
   AD->remote_binary = "msc";

   if (argc == 2 && strcmp(argv[1], "man") == 0)
   {
      print_manual();
      exit(0);
   }

   if (argc == 2 && strcmp(argv[1], "--version") == 0)
   {
      printf("msc %s\n", AD->version);
      exit(0);
   }

   /* Minimal receiver reachability probe used by automatic checkpoint resume.
    * Keep stdout to one stable token: the probing sender validates it. */
   if (argc == 2 && strcmp(argv[1], "--probe") == 0)
   {
      puts("MSC-PROBE-OK");
      exit(0);
   }

   if (argc < 3)
   {
      if (argc >=2 )
      for (lc = 1; lc < argc; lc++)
      {
         if (strcmp(argv[lc], "-h") == 0)
         {
            help(AD);
            exit(1);
         }
      }
      usage(AD);
      exit(1);
   }

   if (strncmp(argv[1],"-y", 2) == 0) /* this is remote*/
   {
      parse_remote(argc, argv, AD);
      remote_process(AD);  /*this does not return */
   }

   /* The private findzero ABI is exactly `-x [-d] N DEST SIZE`. A user's
    * `msc -x -l ... -r ...` must reach the ordinary partition printer. */
   if (strcmp(argv[1], "-x") == 0 && (argc == 5 || argc == 6))
   {
      parse_findzero(argc, argv, AD);
      findzero_process(AD);  /*this does not return */
   }


   if (argc < 5) /* must have at least 4 arguments -r and -c */
   {
      if (argc == 1)
      {
         usage(AD); 
         exit (1);
      }
      for (lc = 1; lc < argc; lc++)
      {
         if (strcmp(argv[lc], "-h") == 0)
         {
            help(AD); /* doesn't return*/
         }
      }
      usage(AD); 
   }

   AD->numstreams = DEFAULT_STREAMS;
   AD->portnum = DEFAULT_PORT;
   AD->packetsize = DEFAULT_PACKETSIZE;
   /* Reliable UDP is the transport unless -T asks for TCP. Set here rather than
    * with the other bzero'd fields in msc.c so it applies only to the initiating
    * side: the -y receiver and -x findzero dispatches above take their transport
    * from argv, never from this default. */
   AD->udp = 1;
   AD->port_tries = 100;
   AD->gso = 1;
   AD->gro = 1;
   AD->no_internal_checksum = 1;
   AD->retry_delay_ms = 1000;
   AD->progress_interval_ms = 1000;
   AD->stall_timeout_ms = DEFAULT_STALL_TIMEOUT_MS;
   AD->reconnect_interval_ms = DEFAULT_RECONNECT_INTERVAL_MS;

       /* already parsed last 2 arguments */
   while ((opt = getopt_long(argc, argv,
          "dhxRUTl:r:i:o:c:u:n:t:a:b:p:s:z:q:B:I:", longopts, NULL)) != -1)
   {
      switch(opt) 
      {
         case 'd':
            msc_debug = 1;
            setup_debug();
            break;
         case 'x':
            partition_display_only = 1;
            break;
         case 'R':
            AD->recursive = 1;
            break;
         /* -U and -T are the two spellings of one setting, so the last one on
          * the command line wins, as with every other option here. -U also
          * records that the user asked for UDP by name: that is what separates
          * "UDP because it is the default" (which may fall back to TCP for a
          * transfer shape the UDP engine cannot carry) from "UDP because I said
          * so" (which must fail loudly instead of quietly changing transport). */
         case 'U':
            AD->udp = 1;
            AD->udp_explicit = 1;
            break;
         case 'T':
            AD->udp = 0;
            AD->udp_explicit = 0;
            break;
         case 1000: AD->force = 1; break;
         case 1001: AD->gso = 1; break;
         case 1002: AD->gro = 1; break;
         case 1003: AD->stats = 1; break;
         case 1004: AD->no_internal_checksum = 1; break;
         case 1005: AD->port_tries = (unsigned int)strtoul(optarg, NULL, 10); break;
         case 1006: AD->gso = 0; break;
         case 1007:
            if (strcmp(optarg, "true") == 0)
               AD->no_internal_checksum = 0;
            else if (strcmp(optarg, "false") == 0)
               AD->no_internal_checksum = 1;
            else
            {
               fprintf(stderr, "--checksum must be true or false\n");
               exit(1);
            }
            break;
         case 1008: AD->resume = 1; break;
         case 1009:
            AD->checkpoint_path = checkmalloc(strlen(optarg) + 1, "checkpoint path");
            strcpy(AD->checkpoint_path, optarg);
            break;
         case 1010: AD->keep_checkpoint = 1; break;
         case 1011:
         {
            char *end;
            unsigned long value;
            errno = 0; value = strtoul(optarg, &end, 10);
            if (errno || *end || value > 1000)
            { fprintf(stderr, "--retries must be an integer from 0 to 1000\n"); exit(MSC_EXIT_CLI); }
            AD->retries = (unsigned int)value;
            break;
         }
         case 1012:
         case 1014:
         case 1015:
         case 1016:
         {
            char *end;
            unsigned long long value, multiplier;
            uint64_t *target = opt == 1012 ? &AD->retry_delay_ms :
                               opt == 1014 ? &AD->progress_interval_ms :
                               opt == 1015 ? &AD->stall_timeout_ms :
                                             &AD->reconnect_interval_ms;
            errno = 0; value = strtoull(optarg, &end, 10);
            if (strcmp(end, "ms") == 0 || *end == '\0') multiplier = 1;
            else if (strcmp(end, "s") == 0) multiplier = 1000;
            else if (strcmp(end, "m") == 0) multiplier = 60000;
            else { fprintf(stderr, "duration must use ms, s, or m\n"); exit(MSC_EXIT_CLI); }
            if (errno || (value == 0 && opt != 1015 && opt != 1016) ||
                value > UINT64_MAX / multiplier)
            { fprintf(stderr, "duration is out of range\n"); exit(MSC_EXIT_CLI); }
            *target = (uint64_t)value * multiplier;
            break;
         }
         case 1017: AD->gro = 0; break;
         case 1013:
            if (strcmp(optarg, "auto") == 0) AD->progress_mode = MSC_PROGRESS_AUTO;
            else if (strcmp(optarg, "always") == 0) AD->progress_mode = MSC_PROGRESS_ALWAYS;
            else if (strcmp(optarg, "never") == 0) AD->progress_mode = MSC_PROGRESS_NEVER;
            else { fprintf(stderr, "--progress must be auto, always, or never\n"); exit(MSC_EXIT_CLI); }
            break;
         case 1022:
            /* --env NAME selects the transport's path preset. It sets
             * MSC_UDP_PROFILE, which the UDP engine already reads and which is
             * already in msc_udp_env_forwardable(), so the choice crosses the
             * SSH launch to the receiver with no extra plumbing. An explicit
             * MSC_UDP_PROFILE in the environment still wins (it is applied after
             * argv), which keeps the A/B harnesses working. Validated here so a
             * typo fails at the CLI, not silently as "unknown ... using auto"
             * from deep in the engine. */
            if (strcmp(optarg, "auto") && strcmp(optarg, "lan") &&
                strcmp(optarg, "wan") && strcmp(optarg, "fiber") &&
                strcmp(optarg, "fiber-shared") && strcmp(optarg, "wan-long") &&
                strcmp(optarg, "fiber-long") && strcmp(optarg, "longhaul") &&
                strcmp(optarg, "geo") && strcmp(optarg, "satellite"))
            {
               fprintf(stderr, "--env must be one of: auto lan wan fiber "
                       "fiber-shared wan-long fiber-long longhaul geo "
                       "satellite (got '%s')\n", optarg);
               exit(MSC_EXIT_CLI);
            }
            if (getenv("MSC_UDP_PROFILE") == NULL)
               setenv("MSC_UDP_PROFILE", optarg, 1);
            break;
         case 1023:
         {
            char *end;
            long value;
            errno = 0; value = strtol(optarg, &end, 10);
            if (errno != 0 || *end != '\0' || value == 0 || value < -1 ||
                value > MAX_DEST_STRIPE_COUNT)
            {
               fprintf(stderr, "--dest-stripe-count must be an OST count from 1 "
                               "to %d, or -1 for every OST (got '%s')\n",
                       MAX_DEST_STRIPE_COUNT, optarg);
               exit(MSC_EXIT_CLI);
            }
            AD->dest_stripe_count = value;
            break;
         }
         case 'h':
            needhelp = 1;
            break;
         case 'l':
            numlocal = process_machine_list(&(AD->local_machines), optarg);
            break;
         case 'r':
            numremote = process_machine_list(&(AD->remote_machines), optarg);
            break;
         case 'i':
            full_pathify(&AD->sourcefile, optarg); 
            break;
         case 'o':
            full_pathify(&AD->destfile, optarg); 
            break;
         case 'c':
            AD->remote_program = checkmalloc(strlen(optarg)+1,"remote program");
            strcpy(AD->remote_program,optarg);
            break;
         case 'u':
            AD->remote_user = checkmalloc(strlen(optarg)+1,"remote_user");
            strcpy(AD->remote_user,optarg);
            break;
         case 'B':
            AD->remote_binary = checkmalloc(strlen(optarg)+1,"remote_binary");
            strcpy(AD->remote_binary,optarg);
            break;
         case 'I':
            AD->interface_host = checkmalloc(strlen(optarg)+1,"interface_host");
            strcpy(AD->interface_host,optarg);
            break;
         case 'n':
            AD->numstreams_explicit = 1;
            AD->numstreams = atoi(optarg);
            if ((AD->numstreams) > MAX_STREAMS)
            {
               fprintf(stderr,"-n set to %u, max %d\n",
                              AD->numstreams, MAX_STREAMS);
            }
            break;
         case 't':
            AD->xferlen = atoll(optarg);
            break;
         case 'a':
            AD->src_offset = atoll(optarg);
            break;
         case 'b':
            AD->dst_offset = atoll(optarg);
            break;
         case 'p':
            AD->portnum = atoi(optarg);
            break;
         case 'q':     /* number of parallel source/destination sets */
            AD->parentsets = atoi(optarg);
            break;
         case 's':
            AD->packetsize_explicit = 1;
            AD->packetsize = atoi(optarg);
            break;
         case 'z':     /* internal: destination inode passed to pair children */
            AD->inode = (ino_t) atoll(optarg);
            break;
         case '1':
            AD->parentsets = atoi(optarg);
            break;
         case 1018: AD->udp_data_ports = (unsigned int)strtoul(optarg, NULL, 10); break;
         case 1019: AD->udp_port_base = (unsigned int)strtoul(optarg, NULL, 10); break;
         case 1020:
            AD->udp_port_span = (unsigned int)strtoul(optarg, NULL, 10);
            AD->udp_port_span_explicit = 1;   /* 0 is meaningful: exact block */
            break;
         case 1021: AD->udp_port_list = optarg; break;
      }
   }

   if (needhelp == 1)
      help(AD);   /* this doesn't return*/

   /* Resolve the destination-layout request to one value every process in this
    * transfer sees. An explicit flag overwrites MSC_DEST_STRIPE_COUNT before any
    * remote command is built, so the UDP greeting and the forwarded environment
    * cannot disagree; with no flag, an inherited value (a benchmark harness
    * export) still applies. Both forms are a request, so both are validated
    * below. */
   if (AD->dest_stripe_count != 0)
   {
      char text[32];
      snprintf(text, sizeof(text), "%ld", AD->dest_stripe_count);
      setenv("MSC_DEST_STRIPE_COUNT", text, 1);
   }
   else
      AD->dest_stripe_count = dest_stripe_count_from_env();

   /* The request names the layout of one destination file, so the shapes that
    * create no destination file, or many, are refused rather than quietly
    * ignoring it. Recursive transfers create every file in the tree
    * (recursive.c), and one OST width for all of them is a policy this option
    * does not define. */
   if (AD->dest_stripe_count != 0)
   {
      if (AD->destfile == NULL)
      {
         fprintf(stderr, "--dest-stripe-count sets the Lustre layout of a "
                         "destination file, but the output goes to a -c "
                         "program\n");
         exit(MSC_EXIT_CLI);
      }
      if (AD->recursive)
      {
         fprintf(stderr, "--dest-stripe-count applies to a single destination "
                         "file; -R creates a whole tree, so set the layout on "
                         "the destination directory with lfs setstripe "
                         "instead\n");
         exit(MSC_EXIT_CLI);
      }
   }

   /* The UDP engine moves files between paths on disk: it opens the source by
    * name (udp_transport.c udp_send) and publishes the destination atomically
    * from a temp file (udp_receive). It has no stdin source and no way to feed
    * a -c program, both of which the TCP path does. So a transfer missing
    * either path -- "tar cf - . | msc ... -c 'tar xf -'", the first example in
    * the help text -- is TCP's to carry.
    *
    * Making UDP the default must not silently break those, so an implicit UDP
    * falls back here; but a typed -U must not silently become TCP either, so it
    * is refused instead. This runs BEFORE the implicit -n/-s block below, which
    * branches on AD->udp: falling back afterwards would hand a TCP transfer the
    * UDP flow count and the PMTUD-probed payload size. */
   if (AD->udp && (AD->sourcefile == NULL || AD->destfile == NULL))
   {
      if (AD->udp_explicit)
      {
         fprintf(stderr, "MSC -U transfers a file to a file; %s needs TCP, so "
                         "drop -U (or pass -T)\n",
                 AD->sourcefile == NULL ? "reading the source from stdin"
                                        : "sending the output to a -c program");
         exit(MSC_EXIT_CLI);
      }
      AD->udp = 0;
   }

   /* Workload-shaped implicit -n: the sweep winners keyed by facts known at parse time -- recursive flag,
    * transport, and source presence. See msc.h for the numbers' rationale
    * and the sweep summary. Pipe mode (no -i) keeps DEFAULT_STREAMS. */
   if (!AD->numstreams_explicit)
   {
      if (AD->recursive)
         AD->numstreams = AD->udp ? DEFAULT_UDP_TREE_FLOWS : DEFAULT_TREE_STREAMS;
      else if (AD->udp)
         AD->numstreams = DEFAULT_UDP_SINGLE_FLOWS;
      else if (AD->sourcefile != NULL)
         AD->numstreams = DEFAULT_SINGLE_STREAMS;
   }
   if (AD->udp && !AD->packetsize_explicit)
      AD->packetsize = DEFAULT_UDP_PAYLOAD;

   if (AD->numstreams == 0 || AD->numstreams > MAX_STREAMS)
   {
      fprintf(stderr, "Number of streams must be between 1 and %d\n", MAX_STREAMS);
      exit(1);
   }
   if (AD->udp && AD->numstreams > MAX_UDP_FLOWS)
   {
      fprintf(stderr, "UDP flows must be between 1 and %d\n", MAX_UDP_FLOWS);
      exit(1);
   }
   if (AD->packetsize == 0)
   {
      fprintf(stderr, "Chunk size must be greater than zero\n");
      exit(1);
   }
   /* The control-channel topology is a property of the UDP engine; on a TCP
    * transfer there is no engine to carry it and the setting would silently do
    * nothing (worse for the stdio arms, whose whole promise is "no listening
    * port"). Name the reason this transfer is on TCP: -T is the user's doing,
    * anything else is the stdin/-c fallback above. */
   {
      const char *ctl = getenv("MSC_UDP_CTL");
      if (ctl != NULL && ctl[0] != '\0' && strcmp(ctl, "tcp") != 0 && !AD->udp)
      {
         fprintf(stderr, "MSC_UDP_CTL=%s selects an MSC UDP control channel, "
                         "but this transfer runs on TCP (%s)\n", ctl,
                 AD->sourcefile == NULL ? "the source is stdin"
               : AD->destfile == NULL   ? "the output goes to a -c program"
                                        : "-T was given");
         exit(MSC_EXIT_CLI);
      }
   }
   /* Data-port footprint (UDP only). The knobs reach the UDP engine -- here and,
    * via msc_udp_remote_env(), on the receiver MSC launches over ssh -- as
    * MSC_UDP_PORTS / MSC_UDP_PORT_BASE / MSC_UDP_PORT_SPAN / MSC_UDP_PORT_LIST.
    * Resolved after the implicit -n above so K resolves against the flow count
    * actually in force, and validated through the same port_spec.c the engine
    * uses so a bad request is refused here, with identical wording, rather than
    * on the far side of an ssh launch. */
   if (AD->udp_data_ports != 0 || AD->udp_port_base != 0 ||
       AD->udp_port_span_explicit || AD->udp_port_list != NULL)
   {
      const char *ctl = getenv("MSC_UDP_CTL");
      struct msc_port_spec spec;
      char err[MSC_UDP_PORT_ERRLEN];
      char buf[32];
      int bad = 0;
      if (!AD->udp)
      {
         fprintf(stderr, "the --udp-ports/--udp-data-ports/--udp-port-base/"
                         "--udp-port-span flags describe the MSC UDP data "
                         "sockets, but this transfer runs on TCP (%s), which "
                         "already uses one port\n",
                 AD->sourcefile == NULL ? "the source is stdin"
               : AD->destfile == NULL   ? "the output goes to a -c program"
                                        : "-T was given");
         exit(MSC_EXIT_CLI);
      }
      /* K spreads N flows over K data sockets, so it only means anything in
       * the arms that open a per-flow data socket set: stdio (ssh control, the
       * default -- and the case an unset MSC_UDP_CTL lands in) and tcp (TCP
       * control).  many/one bind their control on UDP, which MSC's
       * orchestration does not supply, and stdio1 is by definition one shared
       * socket -- K is already 1 there. */
      if (ctl != NULL && ctl[0] != '\0' &&
          strcmp(ctl, "stdio") != 0 && strcmp(ctl, "tcp") != 0)
      {
         fprintf(stderr, "MSC UDP supports the default MSC_UDP_CTL=stdio, or "
                         "MSC_UDP_CTL=tcp, here; MSC_UDP_CTL=%s %s, so the "
                         "data-port flags cannot apply\n", ctl,
                 strcmp(ctl, "stdio1") == 0
                    ? "already carries every flow on one data socket"
                    : "is not supported");
         exit(MSC_EXIT_CLI);
      }
      msc_port_spec_init(&spec);
      if (AD->udp_port_list != NULL)
      {
         if (AD->udp_port_base != 0 || AD->udp_port_span_explicit)
         {
            fprintf(stderr, "MSC: --udp-ports names exact ports; drop "
                            "--udp-port-base/--udp-port-span\n");
            exit(MSC_EXIT_CLI);
         }
         bad = msc_port_spec_set_list(&spec, AD->udp_port_list, err, sizeof(err));
      }
      if (!bad && AD->udp_data_ports != 0)
      {
         snprintf(buf, sizeof(buf), "%u", AD->udp_data_ports);
         bad = msc_port_spec_set_count(&spec, buf, err, sizeof(err));
      }
      if (!bad && AD->udp_port_base != 0)
      {
         snprintf(buf, sizeof(buf), "%u", AD->udp_port_base);
         bad = msc_port_spec_set_base(&spec, buf, err, sizeof(err));
      }
      if (!bad && AD->udp_port_span_explicit)
      {
         snprintf(buf, sizeof(buf), "%u", AD->udp_port_span);
         bad = msc_port_spec_set_span(&spec, buf, err, sizeof(err));
         if (!bad && AD->udp_port_span == 0)
            spec.mode = MSC_PORT_MODE_BLOCK;
      }
      if (!bad)
         bad = msc_port_spec_resolve(&spec, AD->numstreams, err, sizeof(err));
      if (bad)
      {
         fprintf(stderr, "MSC: %s\n", err);
         msc_port_spec_clear(&spec);
         exit(MSC_EXIT_CLI);
      }
      /* A clamped K is not reported here: the engine warns about it on both
       * ends anyway (and it is the only warning path when a harness sets the
       * env directly), so saying it again adds a third copy, not clarity. */
      msc_port_spec_clear(&spec);

      /* Forward only what the user actually asked for: both ends must derive
       * the same mode and the same K from the same env, and an unset var is
       * what selects the default. */
      if (AD->udp_port_list != NULL)
         setenv("MSC_UDP_PORT_LIST", AD->udp_port_list, 1);
      if (AD->udp_data_ports != 0)
      {
         snprintf(buf, sizeof(buf), "%u", AD->udp_data_ports);
         setenv("MSC_UDP_PORTS", buf, 1);
      }
      if (AD->udp_port_base != 0)
      {
         snprintf(buf, sizeof(buf), "%u", AD->udp_port_base);
         setenv("MSC_UDP_PORT_BASE", buf, 1);
      }
      if (AD->udp_port_span_explicit)
      {
         snprintf(buf, sizeof(buf), "%u", AD->udp_port_span);
         setenv("MSC_UDP_PORT_SPAN", buf, 1);
      }
   }
   if (AD->recursive)
   {
      struct stat statbuf;
      if (AD->sourcefile == NULL || AD->destfile == NULL ||
          AD->remote_program != NULL)
      {
         fprintf(stderr, "MSC recursive mode requires -i and -o and cannot use -c\n");
         exit(1);
      }
      if (numremote != 1 || AD->src_offset != 0 || AD->dst_offset != 0 ||
          AD->xferlen != 0)
      {
         fprintf(stderr, "MSC recursive mode does not support multiple machine "
                         "sets, offsets, or transfer length limits\n");
         exit(1);
      }
      if (lstat(AD->sourcefile, &statbuf) != 0 || !S_ISDIR(statbuf.st_mode))
      {
         fprintf(stderr, "MSC recursive source must be a directory: %s\n",
                 AD->sourcefile);
         exit(1);
      }
   }

   probe_workload_shape(AD);

   if (AD->resume && AD->checkpoint_path == NULL && AD->destfile != NULL)
      AD->checkpoint_path = msc_default_checkpoint_path(AD->destfile);
   if ((AD->resume || AD->checkpoint_path != NULL) &&
       (AD->sourcefile == NULL || AD->destfile == NULL || numremote != 1))
   {
      fprintf(stderr, "checkpoint/resume requires one file-to-file machine pair\n");
      exit(MSC_EXIT_CLI);
   }
   if (AD->udp && AD->checkpoint_path != NULL &&
       (AD->src_offset != 0 || AD->dst_offset != 0 || AD->xferlen != 0))
   {
      fprintf(stderr, "checkpointed UDP requires a complete file transfer; -a, -b, and -t slices are not resumable\n");
      exit(MSC_EXIT_CLI);
   }

           /* debug the machine list */
   for(lc = 0; lc < numremote; lc++)
   {
      DEBUG1("remote_machine: %s\n", (AD->remote_machines)[lc]);
      DEBUGSYNC;
   }
   for(lc = 0; lc < numlocal; lc++)
   {
      DEBUG1("local_machine: %s\n", (AD->local_machines)[lc]);
      DEBUGSYNC;
   }
       /* check arguments */
   if (numremote == 0)
   {
      fprintf(stderr,"Must specify a remote machine with -r\n");
      exit(1);
   }
   if (numlocal != numremote)
   {
      fprintf(stderr,"Number of local machines %d must be the same as the number of remote machines %d\n", numlocal, numremote);
      exit(1);
   }
   if (numremote > 1) /* multimachine*/
   {
      if (AD->destfile == NULL)
      {
         fprintf(stderr, "If multiple machines are used for transfer, output file must be specified\n");
         usage(AD);
         exit(1);
      }
      if (AD->sourcefile == NULL)
      {
         fprintf(stderr, "If multiple machines are used for transfer, input file must be specified\n");
         usage(AD);
         exit(1);
      }
   }
   if (AD->remote_program == NULL)
   {
      if (AD->destfile == NULL)
      {
         fprintf(stderr,"msc: Either a remote program or remote file must be specified.\n");
         usage(AD);
         exit(1);
      }
   }
   else
   {
      if (AD->destfile != NULL)
      {
         fprintf(stderr,"msc: Either a remote program or remote file must be specified, but not both.\n");
         usage(AD);
         exit(1);
      }
   }
   AD->numsets = numremote;
   /* Every receiver in a multi-set transfer resolves the same port request, so
    * an EXACT one (a list, or a block via --udp-port-span 0) would have them
    * all fight over the same ports and all but the first fail to bind. A scan
    * window is precisely the form that works here: the exclusive binds hand
    * each receiver a disjoint slice of it. */
   if (AD->numsets > 1 && AD->udp &&
       (AD->udp_port_list != NULL ||
        (AD->udp_port_span_explicit && AD->udp_port_span == 0)))
   {
      fprintf(stderr, "msc: %s names exact ports, but this transfer starts %u "
                      "receivers that would all want them; use --udp-port-base "
                      "(with --udp-port-span) so each receiver takes a "
                      "different part of the window\n",
              AD->udp_port_list != NULL ? "--udp-ports" : "--udp-port-span 0",
              AD->numsets);
      exit(MSC_EXIT_CLI);
   }
   DEBUG1("msc version %s\n", AD->version);

      /* determine if we are going to try to restart */
   if (AD->dst_offset != 0 && AD->src_offset != 0 && AD->destfile != NULL)
      AD->obeylastgood = 1;

        /* allocate space for childinfo */
   AD->childinfo = checkmalloc(sizeof(struct jobinfo) * AD->numsets,
                               "childinfo");
   bzero(AD->childinfo, sizeof(struct jobinfo) * AD->numsets);

   if (partition_display_only != 0)
   {
      get_source_filesize(AD);
      multimachine_setup_boundaries(AD);
      for (lc = 0; lc < (int)AD->numsets; lc++)
      {
         /* Spell the transport out rather than leaning on the default: these
          * lines are meant to be copied back into a shell to restart a failed
          * section, possibly long after the default changes again, and possibly
          * against an msc on another host. */
         printf("msc %s -l ", AD->udp ? "-U" : "-T");
         print_shell_word(AD->childinfo[lc].localmachine);
         fputs(" -r ", stdout);
         print_shell_word(AD->childinfo[lc].remotemachine);
         printf(" -q %u ", AD->numsets);
         if (AD->remote_user != NULL)
         {
            fputs("-u ", stdout);
            print_shell_word(AD->remote_user);
            putchar(' ');
         }
         if (strcmp(AD->remote_binary, "msc") != 0)
         {
            fputs("-B ", stdout);
            print_shell_word(AD->remote_binary);
            putchar(' ');
         }
         if (AD->numstreams != DEFAULT_STREAMS)
            printf("-n %u ", AD->numstreams);
         if (AD->portnum != DEFAULT_PORT)
            printf("-p %u ", AD->portnum);
         if (AD->packetsize_explicit)
            printf("-s %u ", AD->packetsize);
         /* Spell the destination layout out for the same reason as the
          * transport: a restarted section must recreate the same file, and the
          * OST width cannot be changed after creation. */
         if (AD->dest_stripe_count != 0)
            printf("--dest-stripe-count %ld ", AD->dest_stripe_count);
         if (AD->sourcefile != NULL)
         {
            fputs("-i ", stdout);
            print_shell_word(AD->sourcefile);
            putchar(' ');
         }
         if (AD->destfile == NULL)
         {
            fputs("-c ", stdout);
            print_shell_word(AD->remote_program);
            putchar(' ');
         }
         else
         {
            fputs("-o ", stdout);
            print_shell_word(AD->destfile);
            putchar(' ');
         }
         printf("-t %lld -a %lld -b %lld\n", 
                 (long long int)AD->childinfo[lc].xferlen,
                 (long long int)AD->childinfo[lc].startloc, 
                 (long long int)AD->childinfo[lc].startloc);
      }

      exit(0);
   }


}
