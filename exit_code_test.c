#include "msc.h"

#include <stdio.h>

int main(void)
{
   if (MSC_EXIT_OK != 0 || MSC_EXIT_CLI != 2 || MSC_EXIT_SOURCE != 3 ||
       MSC_EXIT_DESTINATION != 4 || MSC_EXIT_AUTH != 5 ||
       MSC_EXIT_NETWORK != 6 || MSC_EXIT_INTEGRITY != 7 ||
       MSC_EXIT_INCOMPATIBLE != 8 || MSC_EXIT_INTERNAL != 70 ||
       MSC_EXIT_SIGINT != 130 || MSC_EXIT_SIGTERM != 143)
      return 1;
   puts("stable exit-code contract passed");
   return 0;
}
