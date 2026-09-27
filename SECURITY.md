# Security policy

MSC is intended for transfers between trusted Linux hosts over protected data
networks. SSH authenticates launch and protects the default control channel.
MSC's separate UDP and TCP file-data connections are neither encrypted nor
cryptographically authenticated. Random transfer IDs reject stale packets;
they do not authenticate a sender. The optional FNV-style checksum is not a
security primitive. SHA-256 resume verification detects mismatched reused data
but does not replace an authenticated transport.

Use network isolation or an authenticated VPN for data that crosses an untrusted
network. Restrict inbound transfer ports to the intended peers. Run MSC with the
permissions needed for its source and destination, and keep sources stable
during a copy. Recursive reception writes into its destination tree in place.

## Reporting a vulnerability

Use the repository's [private vulnerability reporting page](https://github.com/ojaroker/msc/security/advisories/new)
when enabled. If it is unavailable, open an issue requesting a private contact
route, without including exploit details or sensitive data. Include the commit
or version, kernel, reproduction steps, impact, and a minimized example in the
private report. Do not attach credentials, private file contents, or real peer
addresses unless necessary and agreed with the maintainers.

Fixes target the current development branch and the latest published release;
no older-version backport commitment is established. Install matching protocol
versions on both peers. Maintainers should enable the private reporting feature
before publishing the first release governed by this policy.
