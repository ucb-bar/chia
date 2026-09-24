# ECAD bitstream smoke test

Proves the path from *an LLM edited chipyard* to *a bitstream in hand*, with the
build machine launched and terminated by the loop.

```
chisel_build worker (your cluster)      ECAD worker (EC2, launched by the loop)
──────────────────────────────────      ───────────────────────────────────────
LLM makes a one-line RTL change
git diff  ──────────── diff ─────────>  git apply
                                        FireSim's build code
                                          Chisel + driver  (in the container)
                                          Vivado           (host, ssh localhost)
                                          AGFI             (create-fpga-image)
          <────────── FSBitstream ────  agfi + driver
```

## What it builds

`FireSimRocketConfig` at 75 MHz — the cheapest real f2 build there is. It is
still Vivado on a single-core Rocket, so budget **1-3 hours** and a `z1d.2xlarge`
for that whole time. Everything before the launch takes minutes.

## Run it

```bash
chia up examples/new_firesim/cluster.yaml -y

# Minutes. Checks the LLM edit and the diff, launches nothing.
chia job submit --working-dir examples/new_firesim -- python bitstream_build_loop.py --diff-only

# Hours. Launches the ECAD machine, builds, tears it down.
chia job submit --working-dir examples/new_firesim -- python bitstream_build_loop.py
```

Run `--diff-only` first. It exercises everything cheap, and a failure there
costs nothing.

## What comes back

```python
EcadBuildResult(recipe_name="rocket-smoke", success=True,
                bitstream=FSBitstream(quintuplet="f2-firesim-FireSim-...",
                                      agfi="agfi-0123...",      # flashable
                                      driver_bytes=b"..."))     # driver-bundle.tar.gz
```

That is exactly what `FireSimManagerNode.run_workload` takes, so the output of
this test is directly runnable on an F2.

## Why the node doesn't call the `firesim` CLI

`firesim buildbitstream` runs Chisel over ssh to `localhost`, hardcoded. Chia
containers run `--net=host`, so `localhost` is the instance, which has no
chipyard. The node runs the same steps through FireSim's own Python functions
instead: Chisel and the driver in the container, then Vivado and the AGFI on
the instance over ssh, exactly as the CLI does.

## Prerequisites

- An EC2 key pair whose private key is the cluster's `ssh_private_key`. If those
  disagree the ECAD machine launches and silently fails to join.
- Quota for one `z1d.2xlarge`.
- Credentials on the ECAD machine allowing `ec2:CreateFpgaImage` and S3 write.
  The bucket itself needs no setup: it defaults to `firesim-<account>-<region>`
  and is created if missing. `create-fpga-image` takes its input only from an
  S3 location, which is why one exists at all.
- Claude Code credentials mounted into the chipyard worker (see `cluster.yaml`).
