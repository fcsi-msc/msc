/*
 * port_spec_test.c -- table tests for the UDP data-port request parser.
 *
 * port_spec.c is pure, so this needs no sockets and no transfer: it pins the
 * rules the CLI and the engine both depend on (K resolution, window sizing,
 * exact-mode rejection, the report formatting). Linked against port_spec.o
 * alone; run from `make udp_parity`.
 */
#include "udp_session.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;

#define CHECK(cond, ...)                                                      \
   do {                                                                       \
      if (!(cond)) {                                                          \
         fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__);                 \
         fprintf(stderr, __VA_ARGS__);                                        \
         fprintf(stderr, "\n");                                               \
         failures++;                                                          \
      }                                                                       \
   } while (0)

static void test_list_parse(void)
{
   struct msc_port_spec spec;
   char err[MSC_UDP_PORT_ERRLEN];

   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_set_list(&spec, "20000,20004,20100-20103", err, sizeof(err)) == 0,
         "plain list rejected: %s", err);
   CHECK(spec.mode == MSC_PORT_MODE_LIST, "mode not LIST");
   CHECK(spec.nlist == 6, "expected 6 ports, got %d", spec.nlist);
   CHECK(spec.list[0] == 20000 && spec.list[1] == 20004 && spec.list[2] == 20100 &&
         spec.list[5] == 20103, "list order not preserved");
   msc_port_spec_clear(&spec);

   /* K comes from the list, and the list may be unsorted -- flow f uses socket
    * f % K, so the order the user wrote is the order flows are assigned. */
   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_set_list(&spec, "30000,20000", err, sizeof(err)) == 0,
         "unsorted list rejected: %s", err);
   CHECK(spec.list[0] == 30000 && spec.list[1] == 20000, "unsorted list reordered");
   CHECK(msc_port_spec_resolve(&spec, 8, err, sizeof(err)) == 0, "resolve: %s", err);
   CHECK(spec.count == 2, "list K should be 2, got %d", spec.count);
   msc_port_spec_clear(&spec);

   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_set_list(&spec, "20000,20000", err, sizeof(err)) != 0,
         "duplicate port accepted");
   CHECK(msc_port_spec_set_list(&spec, "20000-19999", err, sizeof(err)) != 0,
         "backwards range accepted");
   CHECK(msc_port_spec_set_list(&spec, "0", err, sizeof(err)) != 0, "port 0 accepted");
   CHECK(msc_port_spec_set_list(&spec, "65536", err, sizeof(err)) != 0, "port 65536 accepted");
   CHECK(msc_port_spec_set_list(&spec, "20000x", err, sizeof(err)) != 0, "trailing junk accepted");
   CHECK(msc_port_spec_set_list(&spec, "", err, sizeof(err)) != 0, "empty list accepted");
   CHECK(msc_port_spec_set_list(&spec, "20000-20999", err, sizeof(err)) != 0,
         "a range longer than the flow maximum was accepted");
   msc_port_spec_clear(&spec);
}

static void test_default_window(void)
{
   struct msc_port_spec spec;
   char err[MSC_UDP_PORT_ERRLEN];

   /* the shipped default: min(flows, 8) ports, window 4*K from 17400 */
   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_resolve(&spec, 30, err, sizeof(err)) == 0, "resolve: %s", err);
   CHECK(spec.mode == MSC_PORT_MODE_SCAN, "default mode is not SCAN");
   CHECK(spec.base == MSC_UDP_DEFAULT_PORT, "default base is %u", spec.base);
   CHECK(spec.count == 8, "default K at 30 flows should be 8, got %d", spec.count);
   CHECK(spec.span == 32, "default span at K=8 should be 32, got %u", spec.span);
   CHECK(spec.base_explicit == 0, "default base reported as explicit");

   /* fewer flows than the ceiling: K never exceeds the flow count */
   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_resolve(&spec, 4, err, sizeof(err)) == 0, "resolve: %s", err);
   CHECK(spec.count == 4, "K at 4 flows should be 4, got %d", spec.count);
   CHECK(spec.span == 16, "span at K=4 should be 16, got %u", spec.span);

   CHECK(msc_port_spec_default_count(1) == 1, "default K at 1 flow");
   CHECK(msc_port_spec_default_count(64) == 8, "default K at 64 flows");
}

static void test_count_and_span(void)
{
   struct msc_port_spec spec;
   char err[MSC_UDP_PORT_ERRLEN];

   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_set_count(&spec, "4", err, sizeof(err)) == 0, "count: %s", err);
   CHECK(msc_port_spec_resolve(&spec, 30, err, sizeof(err)) == 0, "resolve: %s", err);
   CHECK(spec.count == 4 && spec.span == 16, "K=4 -> span 16, got K=%d span=%u",
         spec.count, spec.span);

   /* an explicit K above the flow count is clamped, not refused */
   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_set_count(&spec, "16", err, sizeof(err)) == 0, "count: %s", err);
   CHECK(msc_port_spec_resolve(&spec, 4, err, sizeof(err)) == 0, "resolve: %s", err);
   CHECK(spec.count == 4 && spec.count_clamped, "K=16 at 4 flows should clamp to 4");

   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_set_count(&spec, "0", err, sizeof(err)) != 0, "K=0 accepted");
   CHECK(msc_port_spec_set_count(&spec, "257", err, sizeof(err)) != 0, "K=257 accepted");

   /* a window that cannot hold K is a request error, not a silent widening */
   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_set_count(&spec, "8", err, sizeof(err)) == 0, "count: %s", err);
   CHECK(msc_port_spec_set_span(&spec, "4", err, sizeof(err)) == 0, "span: %s", err);
   CHECK(msc_port_spec_resolve(&spec, 30, err, sizeof(err)) != 0,
         "an 4-port window accepted 8 data ports");

   /* span 0 is the exact-block escape hatch */
   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_set_span(&spec, "0", err, sizeof(err)) == 0, "span 0: %s", err);
   spec.mode = MSC_PORT_MODE_BLOCK;
   CHECK(msc_port_spec_set_base(&spec, "20000", err, sizeof(err)) == 0, "base: %s", err);
   CHECK(msc_port_spec_resolve(&spec, 4, err, sizeof(err)) == 0, "resolve: %s", err);
   CHECK(spec.count == 4 && spec.span == 0, "block K=4 span=0");
   CHECK(msc_port_spec_contains(&spec, 20003) && !msc_port_spec_contains(&spec, 20004),
         "block window is 20000..20003");

   /* the span cap keeps a big K from claiming an absurd window */
   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_set_count(&spec, "256", err, sizeof(err)) == 0, "count: %s", err);
   CHECK(msc_port_spec_resolve(&spec, 256, err, sizeof(err)) == 0, "resolve: %s", err);
   CHECK(spec.span == MSC_UDP_PORT_SPAN_MAX, "span cap not applied: %u", spec.span);
}

static void test_range_end(void)
{
   struct msc_port_spec spec;
   char err[MSC_UDP_PORT_ERRLEN];

   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_set_base(&spec, "65530", err, sizeof(err)) == 0, "base: %s", err);
   CHECK(msc_port_spec_resolve(&spec, 8, err, sizeof(err)) != 0,
         "a window running past 65535 was accepted");

   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_set_base(&spec, "65530", err, sizeof(err)) == 0, "base: %s", err);
   CHECK(msc_port_spec_set_span(&spec, "0", err, sizeof(err)) == 0, "span: %s", err);
   spec.mode = MSC_PORT_MODE_BLOCK;
   CHECK(msc_port_spec_set_count(&spec, "8", err, sizeof(err)) == 0, "count: %s", err);
   CHECK(msc_port_spec_resolve(&spec, 8, err, sizeof(err)) != 0,
         "a block running past 65535 was accepted");
}

static void test_contains(void)
{
   struct msc_port_spec spec;
   char err[MSC_UDP_PORT_ERRLEN];

   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_resolve(&spec, 8, err, sizeof(err)) == 0, "resolve: %s", err);
   CHECK(msc_port_spec_contains(&spec, 17400), "default window excludes its base");
   CHECK(msc_port_spec_contains(&spec, 17431), "default window excludes its last port");
   CHECK(!msc_port_spec_contains(&spec, 17432), "default window includes base+span");
   CHECK(!msc_port_spec_contains(&spec, 40000),
         "default window includes an ephemeral port -- the old-receiver check would miss");

   msc_port_spec_init(&spec);
   CHECK(msc_port_spec_set_list(&spec, "20000,30000", err, sizeof(err)) == 0, "list: %s", err);
   CHECK(msc_port_spec_contains(&spec, 30000) && !msc_port_spec_contains(&spec, 20001),
         "list membership");
   msc_port_spec_clear(&spec);
}

static void test_format(void)
{
   char buf[MSC_UDP_PORT_TEXTLEN];
   unsigned int a[] = { 17400, 17401, 17402, 17403, 17405, 17407, 17408, 17409 };
   unsigned int b[] = { 20000 };
   unsigned int c[] = { 30000, 20000, 20001 };

   msc_port_spec_format(a, 8, buf, sizeof(buf));
   CHECK(strcmp(buf, "17400-17403,17405,17407-17409") == 0, "collapsed form: %s", buf);
   msc_port_spec_format(b, 1, buf, sizeof(buf));
   CHECK(strcmp(buf, "20000") == 0, "single port: %s", buf);
   msc_port_spec_format(c, 3, buf, sizeof(buf));
   CHECK(strcmp(buf, "30000,20000-20001") == 0, "unsorted form: %s", buf);
   msc_port_spec_format(a, 0, buf, sizeof(buf));
   CHECK(buf[0] == '\0', "empty list formatted as \"%s\"", buf);
}

static void test_env(void)
{
   struct msc_port_spec spec;
   char err[MSC_UDP_PORT_ERRLEN];

   unsetenv("MSC_UDP_PORTS"); unsetenv("MSC_UDP_PORT_BASE");
   unsetenv("MSC_UDP_PORT_SPAN"); unsetenv("MSC_UDP_PORT_LIST");
   CHECK(msc_port_spec_from_env(&spec, err, sizeof(err)) == 0, "empty env: %s", err);
   CHECK(spec.mode == MSC_PORT_MODE_SCAN && spec.base == MSC_UDP_DEFAULT_PORT,
         "empty env is not the default window");

   setenv("MSC_UDP_PORT_SPAN", "0", 1);
   CHECK(msc_port_spec_from_env(&spec, err, sizeof(err)) == 0, "span 0: %s", err);
   CHECK(spec.mode == MSC_PORT_MODE_BLOCK, "MSC_UDP_PORT_SPAN=0 did not select BLOCK");

   /* exact ports and a window are contradictory requests, not a precedence
    * puzzle: refuse rather than silently pick one */
   setenv("MSC_UDP_PORT_LIST", "20000,20001", 1);
   CHECK(msc_port_spec_from_env(&spec, err, sizeof(err)) != 0,
         "PORT_LIST alongside PORT_SPAN accepted");
   unsetenv("MSC_UDP_PORT_SPAN");
   CHECK(msc_port_spec_from_env(&spec, err, sizeof(err)) == 0, "list env: %s", err);
   CHECK(spec.mode == MSC_PORT_MODE_LIST && spec.nlist == 2, "list env not applied");
   msc_port_spec_clear(&spec);
   unsetenv("MSC_UDP_PORT_LIST");

   setenv("MSC_UDP_PORTS", "notanumber", 1);
   CHECK(msc_port_spec_from_env(&spec, err, sizeof(err)) != 0, "bad MSC_UDP_PORTS accepted");
   unsetenv("MSC_UDP_PORTS");
}

int main(void)
{
   test_list_parse();
   test_default_window();
   test_count_and_span();
   test_range_end();
   test_contains();
   test_format();
   test_env();
   if (failures != 0)
   {
      fprintf(stderr, "port_spec_test: %d check(s) failed\n", failures);
      return 1;
   }
   printf("port_spec_test: all checks passed\n");
   return 0;
}
