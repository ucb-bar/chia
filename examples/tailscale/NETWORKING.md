# How CHIA tailnet networking works

A complete, one-place description of the networking behind CHIA's
tailnet (tailscale) clusters. For the user-facing config and quickstart,
see [README.md](README.md); this document explains the mechanism.

## The setting

Machines that can only reach each other through a tailscale network,
with **tailscaled in userspace-networking mode** — no root, no TUN
device, no sudo. The cluster can mix: a head, on-prem workers (you run
tailscaled yourself), and cloud workers (CHIA installs and runs
tailscaled for them). Their real networks may firewall each other
completely; the only assumed connectivity is the tailnet plus outbound
internet.

Two primitives are all userspace tailscaled gives you, and everything is
built on them:

- **Inbound**: a connection arriving over the tailnet addressed to this
  machine's tailnet IP `:port` is handed by tailscaled to
  `127.0.0.1:port` locally.
- **Outbound**: a process *cannot* open a socket to a `100.x` tailnet IP
  (there's no interface). It must dial through tailscaled's local
  **SOCKS5 proxy** (e.g. `127.0.0.1:1055`) or HTTP proxy (`:1056`).

## Addressing: loopback advertise IPs

Each Ray participant — the head, and every logical worker — is assigned
a unique **loopback** address that it advertises to Ray as its identity:

- head → `127.200.0.1`
- workers → `127.0.0.2`, `127.0.0.3`, …

Two facts force this choice:

1. **Loopback is always bindable**, with no config or root — unlike the
   real `100.x` tailnet IP, which has no local interface in userspace
   mode. Ray components must bind *something*, and some of them (the
   runtime-env and dashboard agents) bind their advertised node IP
   *specifically* — those fail on an unbindable address. Loopback is the
   only address that's both a usable identity and locally bindable
   everywhere.
2. **The advertise IP is the routing key.** It's globally unique across
   the cluster and answers "which participant is this?" Ray registers
   each participant under it, and GCS hands it out to everyone who needs
   to dial that participant.

One critical Ray behavior underpins the whole design: **Ray's gRPC
services bind the wildcard address (`*:port`), not their advertised IP.**
`--node-ip-address=127.0.0.2` changes what Ray *advertises* but services
still bind `0.0.0.0`. The lone exception is ChiaTool's uvicorn servers,
which bind their advertise IP specifically — and that exception is
exactly why tools need extra plumbing (below).

## The relay (one per machine)

A small stdlib-Python process CHIA deploys to every machine
(`chia/cluster/tailnet.py`). It has exactly three kinds of listener plus
a routing table:

1. **One HTTP CONNECT proxy** on `127.0.0.1:13129` — carries *all* of
   Ray's gRPC.
2. **Per-port SOCKS-forwarding listeners** for each *peer's* tool ports,
   bound at `<peer_advertise_ip>:<tool_port>`.
3. **Local "direct" bridges** for this machine's *own* tool ports:
   `127.0.0.1:<tool_port>` → `<own_advertise_ip>:<tool_port>`.

The **routes table** maps every advertise IP → the owning machine's
tailnet IP (or `null` meaning "local, dial 127.0.0.1 directly").

## Path 1 — Ray gRPC, cross-machine (the core data plane)

CHIA injects into every `ray start`:

```
RAY_grpc_enable_http_proxy=1
grpc_proxy=http://127.0.0.1:13129
no_grpc_proxy=<own advertise IP>
```

So when Ray on machine A dials a peer B at `127.0.0.3:24000`:

```
Ray on A  ──dials 127.0.0.3:24000
  └─ gRPC (grpc_proxy set) sends "CONNECT 127.0.0.3:24000" to A's relay :13129
       └─ relay looks up 127.0.0.3 in routes → B's tailnet IP 100.x.B
            └─ dials 100.x.B:24000 through A's SOCKS5 proxy (127.0.0.1:1055)
                 └─ WireGuard → B's tailscaled → delivers to 127.0.0.1:24000
                      └─ B's raylet, bound *:24000, receives it
```

The receiving side needs no relay involvement — tailscaled drops the
connection on `127.0.0.1` and Ray's wildcard bind catches it. (gRPC
proxies loopback destinations by default, where many HTTP clients would
bypass them — that is what makes this work at all.)

**Self-dials** are the subtlety: a node constantly dials *its own*
services (raylet → its GCS, etc.). `no_grpc_proxy=<own advertise IP>`
makes those bypass the proxy and connect straight to the local wildcard
bind. This also solves a bring-up ordering problem — the head's Ray
starts *before* the head's relay exists, which is safe precisely because
the head only self-dials until workers join (and by then its relay is
up).

## Path 2 — ChiaTool / MCP calls, cross-machine

Tool traffic is plain HTTP over httpx, which does **not** honor
`grpc_proxy` (that's gRPC-only). The alternatives both dead-end: httpx
*does* honor `HTTP_PROXY`/`ALL_PROXY`, but for `http://` URLs it uses
forward-proxy style (`GET http://host/path`), and uvicorn 404s on that;
the SOCKS route needs `socksio`, which isn't installed. So tools keep
per-port listeners. A call from A to B's tool at `127.0.0.3:24016`:

```
MCP client on A ──dials 127.0.0.3:24016 (no proxy — direct)
  └─ A's relay has a listener bound at 127.0.0.3:24016
       └─ forwards through SOCKS5 → 100.x.B:24016
            └─ B's tailscaled → 127.0.0.1:24016
                 └─ B's relay "direct" bridge 127.0.0.1:24016 → 127.0.0.3:24016
                      └─ uvicorn, bound 127.0.0.3:24016, receives it
```

The extra receiving-side bridge (last two hops) exists *only* for tools,
because uvicorn binds the advertise IP specifically — so the inbound
delivery to `127.0.0.1` would otherwise miss it. Ray services skip that
hop because they bind wildcard.

## Port allocation: per-machine, not global

- **Advertise IPs**: globally unique, counting up from `127.0.0.2` (head
  fixed at `127.200.0.1`).
- **Port blocks**: indexed **per machine**. Worker 0 on machine A and
  worker 0 on machine B *both* get block 0 (ports from 24000, tools at
  24016, Ray worker ports 24064–24191). Only two workers sharing *one
  physical machine* get distinct blocks (24000, then 24256, …).

Per-machine reuse is sound because there are **no per-port outbound
listeners** — the CONNECT proxy reads the destination from each request
rather than pre-binding it, so machine A never binds machine B's port
numbers. The only way two things collide on a port is if they're on the
same machine. This is the payoff of the design: **port consumption does
not grow with cluster size.**

Layout details: the head owns a block *just below* `worker_block_base`
(23744–23935), so worker blocks growing upward from 24000 never overlap
head ports; a single machine can host up to 162 workers before the 65535
ceiling. Each Ray worker-port range must exceed the machine's CPU count
(Ray prestarts one worker process per core).

## The SSH control plane (separate from the data plane)

`chia up`/`down` and rsync run over SSH — this is orchestration, not
tailnet data-plane, and it's how CHIA reaches machines to install things
and start Ray:

- Workers **addressed by a tailnet IP** (on-prem, unmanaged): SSH dials
  through the SOCKS5 proxy via `ssh_proxy_command`
  (`nc -X 5 -x 127.0.0.1:1055 %h %p` — head needs OpenBSD netcat).
- Workers **addressed by an ordinary IP** (cloud, or managed on-prem):
  SSH goes direct to the public/LAN IP, no proxy.

## Managed tailscale

For machines CHIA manages (cloud by default; on-prem via
`manage_tailscale`; the whole cluster via `manage_all`), during setup it
installs userspace tailscale (static tarball, no root) into
`/tmp/<cluster>/tailscale`, starts tailscaled with the SOCKS/HTTP proxy,
runs `tailscale up --auth-key=<tailnet.auth_key>`, and discovers the
machine's tailnet IP with `tailscale ip -4`. That discovered IP is
written into every relay's routes table so peers can reach it. Because
you can't bootstrap tailscale over tailscale, managed machines **must**
be addressed by an ordinary reachable IP (enforced at config load).
`chia down` stops these daemons (head last, since SSH to unmanaged peers
rides the head's proxy).

## Bring-up order (why it's safe)

1. Provision cloud instances + run setup (conda, chia, tailscale install).
2. Join managed machines; discover their tailnet IPs.
3. Allocate advertise IPs + per-machine port blocks.
4. Start the head's Ray (self-dials only — no relay needed yet).
5. Start every machine's relay.
6. Start each worker's Ray, which dials the head's GCS through the
   worker's CONNECT proxy (relay is up by now).

A **flow driver** on the head reaches the cluster the same way the
head's own Ray does, so it needs `RAY_ADDRESS=127.200.0.1:6379` plus the
same three `grpc_proxy` env vars.
