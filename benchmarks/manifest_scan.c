/* Read-only manifest scaling benchmark. Does not open a transfer or peer. */
#include "msc.h"
#include <stdio.h>
#include <string.h>

int msc_debug;
FILE *msc_debugout;

int main(int argc, char **argv)
{
   struct argdata args;
   struct directory_manifest *manifest;
   if (argc != 2)
   { fprintf(stderr, "usage: %s SOURCE_DIRECTORY\n", argv[0]); return 2; }
   memset(&args, 0, sizeof(args));
   args.sourcefile = argv[1];
   manifest = manifest_build_source(&args);
   printf("%llu %llu %016llx\n",
          (unsigned long long)manifest_file_count(manifest),
          (unsigned long long)manifest_total_size(manifest),
          (unsigned long long)manifest_hash(manifest));
   manifest_destroy(manifest);
   return 0;
}
