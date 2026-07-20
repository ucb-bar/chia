# CHIA over a tailscale network (userspace, no root)

This example runs a CHIA cluster whose machines can only reach each
other through a [tailscale](https://tailscale.com) network, with
tailscaled in **userspace-networking mode** — no root, no `/dev/net/tun`,
no sudo anywhere. It works even when the machines' real networks
firewall each other off (only outbound internet access is needed for
tailscale itself).

Unlike CHIA's SSH-tunnel path for cloud workers, tailnet mode needs
**no SSH tunnels, no reverse port forwards, no sshd configuration, and
no iptables**. Worker↔worker traffic between different hosts works too
(full mesh).

## How it works

Userspace tailscaled gives each machine two half-primitives:

- **Inbound**: TCP connections arriving at the machine's tailscale IP
  are delivered to `127.0.0.1:<same port>`.
- **Outbound**: connections to other tailnet IPs are only possible
  through the local SOCKS5 proxy (`--socks5-server`).

Ray can't speak SOCKS5, so CHIA bridges the gap with one small
stdlib-Python **relay** per machine (`chia/cluster/tailnet.py`):

```
HEAD  (advertises 127.200.0.1)                WORKER 0  (advertises 127.0.0.2)
ray head --node-ip-address=127.200.0.1        ray --node-ip-address=127.0.0.2
services bind *:<head ports>                  services bind *:<worker-0 ports>

relay listens on peers' addresses:            relay listens on peers' addresses:
  127.0.0.2:<w0 ports> ─► SOCKS5 ─► 100.x.w0    127.200.0.1:<head ports> ─► SOCKS5 ─► 100.x.head
```

The head and every logical worker register in Ray under a unique
loopback address. Dialing a
*peer's* address hits the local relay, which forwards through the SOCKS5
proxy to the peer's tailnet IP; the peer's tailscaled delivers to
`127.0.0.1`, where the wildcard-bound Ray service accepts it. Dialing a
*local* address hits the wildcard bind directly — no relay hop.

Because Ray services bind the wildcard address, each participant's pinned port
block must be **globally unique** — that's what the `tailnet:` port
fields manage. SSH (for `chia up` orchestration and rsync only) reaches
tailnet workers through the same SOCKS5 proxy via `ssh_proxy_command`.

## Prerequisites

On **every** machine (head and workers):

1. tailscaled running in userspace mode with a SOCKS5 proxy:

   ```bash
   ./tailscaled --tun=userspace-networking --statedir=./data \
       --socket=./run/tailscaled.sock --socks5-server=localhost:1055 \
       --outbound-http-proxy-listen=localhost:1056 > tailscaled.log 2>&1 &
   ./tailscale --socket=./run/tailscaled.sock up
   ```

2. A conda env with matching `ray` and `chia` versions on every machine
   (the example assumes it is named `chia_env` — adjust the
   `*_env_commands` in `cluster.yaml` if yours differs per machine).
3. SSH from the head to each worker's **tailscale IP** must authenticate
   non-interactively (ssh-agent or key file). Test it:

   ```bash
   ssh -o "ProxyCommand=nc -X 5 -x 127.0.0.1:1055 %h %p" user@100.x.y.z hostname
   ```

   (`nc` must be OpenBSD netcat on the *head*; only the head dials.)

## Running

```bash
conda activate chia_env
export HEAD_IP=<head real IP or hostname>      # how CHIA SSHes to the head
export HEAD_TAILNET_IP=<head 100.x address>    # `tailscale ip -4` on the head
export WORKER_TAILNET_IP=<worker 100.x address>

chia up examples/tailscale/cluster.yaml        # add --dry-run to inspect first
export RAY_ADDRESS=127.200.0.1:6379            # head_advertise_ip:gcs_port —
                                               # disambiguates on shared machines
                                               # that host other Ray clusters
python examples/tailscale/loop.py              # smoke test
python examples/tailscale/connectivity-matrix.py   # full NxN sweep: ChiaFunctions
                                               # dispatched from every machine to
                                               # every machine, plus a BashTool
                                               # hosted on each machine and called
                                               # from each machine over MCP
chia down examples/tailscale/cluster.yaml
```

## Adapting to your tailnet

The example config takes the machine addresses from the environment
(`HEAD_IP`, `HEAD_TAILNET_IP`, `WORKER_TAILNET_IP`); for more workers,
add their tailscale IPs to `compatible_ips` and raise `num_workers`.
Adjust the conda env names in `*_env_commands` if yours differ.

That's it — the presence of the `tailnet:` block opts the cluster in.
Every worker IP that isn't the head machine is automatically treated as
a tailnet machine, and SSH to it automatically goes through the SOCKS5
proxy (`nc -X 5 -x <socks_proxy> %h %p` — the head needs OpenBSD
netcat). Use `auth.overrides.<ip>` only for special cases: a different
ssh user/key for one host, or a custom `ssh_proxy_command`.

Port knobs (all optional, defaults in `TailnetConfig` in
`chia/cluster/config.py`): the head uses `gcs_port` (must match `--port`
in `head_start_ray_commands`), `head_node_manager_port`,
`head_object_manager_port`, `head_worker_port_min/max`, and
`head_tool_port_min/max`; each worker gets a 256-port block starting at
`worker_block_base`. **A machine's Ray worker-port range must exceed its
CPU count** (Ray prestarts one worker process per CPU) — the defaults
allow 128.

Docker workers work unchanged (CHIA containers run with `--net=host`,
so the loopback addressing carries into the container).

## Cloud workers (EC2 / GCP)

With a `tailnet:` section present, `aws_nodes` and `gcp_nodes` workers
join over the tailnet **by default** — no reverse SSH tunnels, no
`GatewayPorts`, no iptables. CHIA installs the userspace tailscale
binaries during instance setup, starts `tailscaled`, joins with
`tailnet.auth_key`, discovers each instance's tailnet IP, and wires it
into the relay mesh. Just add your cloud section and an auth key:

```yaml
tailnet:
    head_tailnet_ip: ${HEAD_TAILNET_IP}
    auth_key: ${TS_AUTHKEY}        # reusable (ideally ephemeral) tskey-auth-...

aws_nodes:
    region: us-west-2
    ec2_worker:
        KeyName: my-keypair
        InstanceType: c5.4xlarge
        count: 2
        ssh_user: ubuntu
        ssh_private_key: ~/keys/my-keypair.pem

available_node_types:
    ec2_worker:
        resources: {"verilator_run": 16}
        num_workers: 2
        compatible_ips: ["@ec2_worker:0", "@ec2_worker:1"]
```

Generate the key in the tailscale admin console (Settings → Keys):
make it **reusable** and pre-authorized (ephemeral keys keep the
tailnet tidy when instances terminate). Set `join_tailnet: false` on a
machine type to opt back into SSH tunnels — but tunnel and tailnet workers
cannot mix in one cluster.

## Fully managed: `manage_all`

Add `manage_all: true` to the `tailnet:` section and CHIA runs
tailscale on **every** machine including the head — no manual `tailscaled`
anywhere, and `head_tailnet_ip` may be omitted (discovered at
bring-up). Binaries/state live in `/tmp/<cluster_name>/tailscale` per
machine (override with `tailscale_dir`), so cluster daemons are isolated
from any tailscaled you run yourself — give the cluster its own
`socks_proxy` port (e.g. `127.0.0.1:1155`) to avoid clashing with a
personal daemon's proxy. The one constraint: tailscale can't be
bootstrapped over tailscale, so every managed worker must be listed by
an ordinary SSH-reachable IP (not its `100.x` address — rejected at
config load). Opt a machine out with `manage_tailscale: false` and run
its daemon yourself. `chia down` stops the managed daemons.

## Limitations

- All workers must be tailnet workers (or live on the head machine):
  the head advertises a loopback IP that ordinary LAN workers can't
  route to. Mixing with SSH-tunneled/cloud workers is rejected at
  config load.
- `chia up --add` is not yet supported for tailnet clusters; re-run
  `chia up` (existing workers are detected and skipped).
- Throughput is bounded by userspace wireguard-go (fine for control
  traffic and moderate object transfer; don't expect LAN speeds).
- ChiaTool HTTP servers on cluster machines advertise loopback URLs that
  resolve only inside the cluster. Flows that run tools on the *head
  driver* should export `CHIA_TOOL_ADVERTISE_HOST=<head_advertise_ip>`
  and `CHIA_TOOL_BASE_PORT`/`CHIA_TOOL_MAX_PORT` matching the
  `head_tool_port_*` range before launching.
