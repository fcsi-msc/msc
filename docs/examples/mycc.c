/* Paste before the cc_ops_for() definition in udp_session.c.
 * See ../adding-a-controller.md for enum and parser registration.
 * Compiled as part of udp_session.c, not as a standalone source file.
 */
static void mycc_on_ack(struct sender_flow *g, uint64_t newest_seq,
                        uint64_t acked, uint64_t now)
{
   (void)newest_seq;
   (void)now;
   if (g->cwnd < 1.0)
      g->cwnd = 1.0;
   /* Approximately two units of additive increase per window acknowledged. */
   g->cwnd += 2.0 * (double)acked / g->cwnd;
   if (g->cwnd > (double)(MSC_UDP_WIN_RING - 1))
      g->cwnd = (double)(MSC_UDP_WIN_RING - 1);
}

static const struct msc_udp_cc_ops cc_mycc_ops = {
   .name = "mycc",
   .uses_rate_samples = 0,
   .init = cc_noop_init,
   .destroy = cc_noop_destroy,
   .on_sent = cc_noop_sent,
   .on_delivered = cc_noop_delivered,
   .on_rtt = cc_noop_rtt,
   .on_ack = mycc_on_ack,
   .on_sack = cc_noop_sack,
   .on_loss = reno_cc_on_loss,
   .on_ecn = reno_cc_on_ecn,
   .on_rto = reno_cc_on_rto
};
