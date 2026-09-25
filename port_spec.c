/*
 * port_spec.c -- how msc chooses its UDP data ports.
 *
 * Pure logic, deliberately: no sockets, no env writes, no printing. The CLI
 * (parse.c) and the engine (udp_session.c) both resolve a request through this
 * one implementation, so `msc --udp-ports 1-2` and MSC_UDP_PORT_LIST=1-2 fail
 * with the same words, and the sender can verify what the receiver bound
 * without duplicating the rules. Binding lives in udp_session.c; the contract
 * this file encodes is documented in udp_session.h.
 */
#include "udp_session.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __GNUC__
static void seterr(char *err, size_t errlen, const char *fmt, ...)
   __attribute__((format(printf, 3, 4)));
#endif

static void seterr(char *err, size_t errlen, const char *fmt, ...)
{
   va_list ap;
   if (err == NULL || errlen == 0)
      return;
   va_start(ap, fmt);
   vsnprintf(err, errlen, fmt, ap);
   va_end(ap);
}

/* strtoul with the whole-token strictness the CLI needs: "17400x" and "" are
 * errors, not 17400 and 0. */
static int parse_port_number(const char *text, unsigned int *out)
{
   char *end;
   unsigned long v;
   if (text == NULL || *text == '\0')
      return -1;
   errno = 0;
   v = strtoul(text, &end, 10);
   if (errno != 0 || end == text || *end != '\0' || v < 1 || v > 65535)
      return -1;
   *out = (unsigned int)v;
   return 0;
}

void msc_port_spec_init(struct msc_port_spec *spec)
{
   memset(spec, 0, sizeof(*spec));
   spec->mode = MSC_PORT_MODE_SCAN;
   spec->base = MSC_UDP_DEFAULT_PORT;
}

void msc_port_spec_clear(struct msc_port_spec *spec)
{
   free(spec->list);
   spec->list = NULL;
   spec->nlist = 0;
}

int msc_port_spec_default_count(int flow_count)
{
   if (flow_count < 1)
      return 1;
   return flow_count < MSC_UDP_DEFAULT_DATA_PORTS
             ? flow_count : MSC_UDP_DEFAULT_DATA_PORTS;
}

/* "20000,20004,20100-20103" -> an ordered, duplicate-free port array. Order is
 * preserved because flow f uses socket f % K: the user's first port carries
 * flow 0, and a run that reports its ports must be reproducible from the
 * spec. */
int msc_port_spec_set_list(struct msc_port_spec *spec, const char *text,
                           char *err, size_t errlen)
{
   char buf[MSC_UDP_PORT_TEXTLEN];
   unsigned int *list;
   int n = 0;
   char *tok, *save = NULL;

   if (text == NULL || *text == '\0')
   {
      seterr(err, errlen, "port list is empty");
      return -1;
   }
   if (strlen(text) >= sizeof(buf))
   {
      seterr(err, errlen, "port list is too long (limit %d characters)",
             (int)sizeof(buf) - 1);
      return -1;
   }
   strcpy(buf, text);
   list = calloc((size_t)MSC_UDP_MAX_FLOWS, sizeof(*list));
   if (list == NULL)
   {
      seterr(err, errlen, "out of memory parsing the port list");
      return -1;
   }

   for (tok = strtok_r(buf, ",", &save); tok != NULL; tok = strtok_r(NULL, ",", &save))
   {
      unsigned int lo, hi, p;
      char *dash = strchr(tok, '-');
      if (dash != NULL)
      {
         *dash = '\0';
         if (parse_port_number(tok, &lo) != 0 || parse_port_number(dash + 1, &hi) != 0)
         {
            seterr(err, errlen, "\"%s-%s\" is not a port range in 1-65535", tok, dash + 1);
            free(list);
            return -1;
         }
         if (lo > hi)
         {
            seterr(err, errlen, "port range %u-%u runs backwards", lo, hi);
            free(list);
            return -1;
         }
      }
      else if (parse_port_number(tok, &lo) != 0)
      {
         seterr(err, errlen, "\"%s\" is not a port in 1-65535", tok);
         free(list);
         return -1;
      }
      else
         hi = lo;

      for (p = lo; p <= hi; p++)
      {
         int i;
         for (i = 0; i < n; i++)
            if (list[i] == p)
            {
               seterr(err, errlen, "port %u appears twice in the port list", p);
               free(list);
               return -1;
            }
         if (n == MSC_UDP_MAX_FLOWS)
         {
            seterr(err, errlen, "port list is longer than the %d-flow maximum",
                   MSC_UDP_MAX_FLOWS);
            free(list);
            return -1;
         }
         list[n++] = p;
      }
   }
   if (n == 0)
   {
      seterr(err, errlen, "port list is empty");
      free(list);
      return -1;
   }
   msc_port_spec_clear(spec);
   spec->mode = MSC_PORT_MODE_LIST;
   spec->list = list;
   spec->nlist = n;
   return 0;
}

int msc_port_spec_set_base(struct msc_port_spec *spec, const char *text,
                           char *err, size_t errlen)
{
   unsigned int b;
   if (parse_port_number(text, &b) != 0)
   {
      seterr(err, errlen, "\"%s\" is not a port in 1-65535", text == NULL ? "" : text);
      return -1;
   }
   spec->base = b;
   spec->base_explicit = 1;
   return 0;
}

int msc_port_spec_set_span(struct msc_port_spec *spec, const char *text,
                           char *err, size_t errlen)
{
   char *end;
   unsigned long v;
   if (text == NULL || *text == '\0')
   {
      seterr(err, errlen, "port span is empty");
      return -1;
   }
   errno = 0;
   v = strtoul(text, &end, 10);
   if (errno != 0 || end == text || *end != '\0' || v > 65535)
   {
      seterr(err, errlen, "\"%s\" is not a port-window width in 0-65535", text);
      return -1;
   }
   spec->span_requested = (unsigned int)v;
   spec->span_explicit = 1;
   return 0;
}

int msc_port_spec_set_count(struct msc_port_spec *spec, const char *text,
                            char *err, size_t errlen)
{
   char *end;
   unsigned long v;
   if (text == NULL || *text == '\0')
   {
      seterr(err, errlen, "data-port count is empty");
      return -1;
   }
   errno = 0;
   v = strtoul(text, &end, 10);
   if (errno != 0 || end == text || *end != '\0' || v < 1 || v > MSC_UDP_MAX_FLOWS)
   {
      seterr(err, errlen, "\"%s\" is not a data-socket count in 1-%d", text,
             MSC_UDP_MAX_FLOWS);
      return -1;
   }
   spec->count_requested = (int)v;
   return 0;
}

int msc_port_spec_from_env(struct msc_port_spec *spec, char *err, size_t errlen)
{
   const char *v;
   msc_port_spec_init(spec);
   if ((v = getenv("MSC_UDP_PORTS")) != NULL && *v != '\0' &&
       msc_port_spec_set_count(spec, v, err, errlen) != 0)
      return -1;
   if ((v = getenv("MSC_UDP_PORT_BASE")) != NULL && *v != '\0' &&
       msc_port_spec_set_base(spec, v, err, errlen) != 0)
      return -1;
   if ((v = getenv("MSC_UDP_PORT_SPAN")) != NULL && *v != '\0' &&
       msc_port_spec_set_span(spec, v, err, errlen) != 0)
      return -1;
   if ((v = getenv("MSC_UDP_PORT_LIST")) != NULL && *v != '\0')
   {
      if (spec->base_explicit || spec->span_explicit)
      {
         seterr(err, errlen, "MSC_UDP_PORT_LIST names exact ports; drop "
                             "MSC_UDP_PORT_BASE/MSC_UDP_PORT_SPAN");
         return -1;
      }
      if (msc_port_spec_set_list(spec, v, err, errlen) != 0)
         return -1;
   }
   else if (spec->span_explicit && spec->span_requested == 0)
      spec->mode = MSC_PORT_MODE_BLOCK;
   return 0;
}

int msc_port_spec_resolve(struct msc_port_spec *spec, int flow_count,
                          char *err, size_t errlen)
{
   if (flow_count < 1)
      flow_count = 1;
   spec->count_clamped = 0;

   if (spec->mode == MSC_PORT_MODE_LIST)
   {
      if (spec->count_requested != 0 && spec->count_requested != spec->nlist)
      {
         seterr(err, errlen, "%d data ports were requested but the port list "
                             "names %d", spec->count_requested, spec->nlist);
         return -1;
      }
      if (spec->nlist > flow_count)
      {
         seterr(err, errlen, "the port list names %d ports but there are only "
                             "%d flows to carry on them", spec->nlist, flow_count);
         return -1;
      }
      spec->count = spec->nlist;
      spec->span = 0;
      return 0;
   }

   if (spec->count_requested != 0)
   {
      spec->count = spec->count_requested;
      if (spec->count > flow_count)
      {
         spec->count = flow_count;
         spec->count_clamped = 1;
      }
   }
   else
      spec->count = msc_port_spec_default_count(flow_count);

   if (spec->mode == MSC_PORT_MODE_BLOCK)
   {
      spec->span = 0;
      if (spec->base + (unsigned)spec->count - 1 > 65535)
      {
         seterr(err, errlen, "the data-port block %u..%u runs past 65535",
                spec->base, spec->base + (unsigned)spec->count - 1);
         return -1;
      }
      return 0;
   }

   if (spec->span_explicit)
   {
      spec->span = spec->span_requested;
      if (spec->span < (unsigned)spec->count)
      {
         seterr(err, errlen, "a %u-port window cannot hold %d data ports",
                spec->span, spec->count);
         return -1;
      }
   }
   else
   {
      unsigned long s = (unsigned long)spec->count * MSC_UDP_PORT_SPAN_MULT;
      if (s > MSC_UDP_PORT_SPAN_MAX)
         s = MSC_UDP_PORT_SPAN_MAX;
      spec->span = (unsigned int)s;
   }
   if (spec->base + spec->span - 1 > 65535)
   {
      seterr(err, errlen, "the data-port window %u..%u runs past 65535",
             spec->base, spec->base + spec->span - 1);
      return -1;
   }
   return 0;
}

int msc_port_spec_contains(const struct msc_port_spec *spec, unsigned int port)
{
   int i;
   switch (spec->mode)
   {
   case MSC_PORT_MODE_LIST:
      for (i = 0; i < spec->nlist; i++)
         if (spec->list[i] == port)
            return 1;
      return 0;
   case MSC_PORT_MODE_BLOCK:
      return port >= spec->base && port < spec->base + (unsigned)spec->count;
   default:
      return port >= spec->base && port < spec->base + spec->span;
   }
}

void msc_port_spec_format(const unsigned int *ports, int n, char *buf, size_t buflen)
{
   size_t used = 0;
   int i = 0;
   if (buflen == 0)
      return;
   buf[0] = '\0';
   while (i < n)
   {
      int j = i;
      int len;
      char item[16];
      /* collapse only ASCENDING runs: the list is printed in flow order, and a
       * user-supplied list need not be sorted. */
      while (j + 1 < n && ports[j + 1] == ports[j] + 1)
         j++;
      if (j > i)
         len = snprintf(item, sizeof(item), "%s%u-%u", used ? "," : "", ports[i], ports[j]);
      else
         len = snprintf(item, sizeof(item), "%s%u", used ? "," : "", ports[i]);
      if (len < 0)
         return;
      if (used + (size_t)len + 1 > buflen)
      {
         /* Truncation is cosmetic (the report line), never a decision input. */
         if (used + 4 <= buflen)
            memcpy(buf + used, ",...", 5);
         return;
      }
      memcpy(buf + used, item, (size_t)len + 1);
      used += (size_t)len;
      i = j + 1;
   }
}
