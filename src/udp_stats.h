#ifndef MSC_UDP_STATS_STREAM_H
#define MSC_UDP_STATS_STREAM_H
/*
 * udp_stats.h - machine-readable telemetry stream for the MSC UDP engine, for
 * external tools such as live visualizers or dashboards. Off by default;
 * MSC_UDP_STATS_STREAM=1 turns it on.
 *
 * Every emission is one NDJSON line prefixed "MSC-UDP-STATS " so it can share a
 * pipe with the human MSC_UDP_STATS logs and the MSC-CONNECT/MSC-RESULT markers
 * without ambiguity. The stream is SAMPLED, not per-packet: continuous state
 * (flow_sample, acknowledgment, write_sample) is rate-gated per flow per event kind
 * at MSC_UDP_STATS_STREAM_RATE_HZ (default 20); discrete events (session_start,
 * session_done, retransmit) emit immediately, with a token-bucket cap on retransmissions
 * so injected-loss runs cannot flood the pipe.
 *
 * Env knobs (read once by msc_udp_stats_init):
 *   MSC_UDP_STATS_STREAM=1              enable the stream
 *   MSC_UDP_STATS_STREAM_RATE_HZ=N      max snapshots per flow per event kind (default 20)
 *   MSC_UDP_STATS_STREAM_OUTPUT=stdout|stderr|<path>   output target (default stderr;
 *                             stdout stays clean for MSC-CONNECT parsing)
 */
#include <stdint.h>

/* rate-gated event kinds (one last-emit slot per flow per kind) */
#define MSC_UDP_STATS_FLOW    0
#define MSC_UDP_STATS_ACK 1
#define MSC_UDP_STATS_WRITE   2
#define MSC_UDP_STATS_NKINDS  3

/* fast-path guard: nonzero once msc_udp_stats_init saw MSC_UDP_STATS_STREAM=1 */
extern int msc_udp_stats_on;

/* Read the env knobs and open the stream. Safe to call more than once (the
 * session-open paths of both roles call it); only the first call binds. `side`
 * is stamped into every line ("sender" or "receiver"). */
void msc_udp_stats_init(const char *side);

/* nonzero when flow `flow_id` is due a `kind` sample (>= one visual tick since
 * its last one); claims the tick when due. Call only when msc_udp_stats_on. */
int msc_udp_stats_due(int flow_id, int kind);

/* nonzero when a retransmit event for this flow fits the flood-control budget
 * (token bucket; the flow_sample `retransmitted` receiver stays exact regardless). */
int msc_udp_stats_retransmit_ok(int flow_id);

/* Emit one line: MSC-UDP-STATS {"v":1,"ts_ns":<now>,"side":"...","event":"<event>"
 * <, fmt...>}. `fmt` is the printf-style tail of the JSON object -- start it
 * with a comma, or pass "" for no extra fields. One fwrite under a mutex, so
 * concurrent flow_worker threads never interleave partial lines. */
void msc_udp_stats_emit(const char *event, const char *fmt, ...)
   __attribute__((format(printf, 2, 3)));

#endif /* MSC_UDP_STATS_STREAM_H */
