# ECAD bitstream smoke test

Proves the path from *an LLM edited chipyard* to *a bitstream in hand*, with the
build machine launched and terminated by the loop.

```
chisel_build worker (your cluster)      ECAD worker (EC2, launched by the loop)
──────────────────────────────────      ───────────────────────────────────────
LLM makes a one-line RTL change
git diff  ──────────── diff ─────────>  git apply
                                        make replace-rtl   (container: chipyard)
                                        make driver        (container: chipyard)
                                        Vivado             (host: FPGA Dev AMI)
          <────────── FSBitstream ────  bitstream + driver, by value
```

## What it builds

`FireSimRocketConfig` at 75 MHz — the cheapest real f2 build there is. It is
still Vivado on a single-core Rocket, so budget **1-3 hours** and a `z1d.2xlarge`
for that whole time. Everything before the launch takes minutes.

## Run it

```bash
chia up examples/new_firesim/cluster.yaml -y

# Minutes. Checks the LLM edit and the diff, launches nothing.
chia job submit --working-dir examples/new_firesim -- python ecad_build_loop.py --diff-only

# Hours. Launches the ECAD machine, builds, tears it down.
chia job submit --working-dir examples/new_firesim -- python ecad_build_loop.py
```

Run `--diff-only` first. It exercises everything cheap, and a failure there
costs nothing.

## What comes back

```python
EcadBuildResult(recipe_name="rocket-smoke", success=True,
                bitstream=FSBitstream(quintuplet="f2-firesim-FireSim-...",
                                      bitstream_bytes=b"...",   # the DCP tarball
                                      driver_bytes=b"..."))     # driver-bundle.tar.gz
```

Both halves travel by value over Ray — tens of MB, so no S3 and no credentials
are involved.

## What this does not do

**No AGFI.** On f2 the thing you flash is an AGFI, minted by
`aws ec2 create-fpga-image` from the DCP tarball, which needs the tarball in S3
and AWS credentials on the builder. This test stops at the tarball. Until that
step exists, the result cannot be handed to `FireSimManagerNode.run_workload`.

**The ECAD machine never runs `firesim buildbitstream`.** It runs the same steps
directly, because `deploy/firesim` pins Chisel elaboration to `hosts=['localhost']`
and, under `--net=host`, that is the instance rather than the container where
chipyard lives.

## Prerequisites

- An EC2 key pair whose private key is the cluster's `ssh_private_key`. If those
  disagree the ECAD machine launches and silently fails to join.
- Quota for one `z1d.2xlarge`.
- Claude Code credentials mounted into the chipyard worker (see `cluster.yaml`).
