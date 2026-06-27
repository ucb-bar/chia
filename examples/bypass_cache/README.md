# bypass_cache — caching + bypass on a 4×4 loop

A minimal, runnable demo of CHIA's two output-replay mechanisms (see the
**Caching and Bypass** user guide for the full reference):

- **cache** (`chia.base.cache`) — write a function's real output to an on-disk cache,
  keyed by the call's tag (`_chia_tag`).
- **bypass** (`chia.base.bypass`) — skip a function's real computation and serve
  pre-recorded data instead, gated by a tag match.

`bypass_cache_loop.py` runs a 4-iteration loop with 4 parallel calls per
iteration (16 calls total), each tagged `iter<N>_call<M>`. There is one loop;
**run the script twice** to see populate-then-replay. The difference comes
entirely from the cache being cold vs warm, gated by a `bypass_cond`:

1. **Run 1 — cold cache.** The bypass regex `iter[13]_.*` selects iterations 1
   and 3, but a `bypass_cond` (`cache_hit_cond`) gates the bypass on the value
   actually being cached. On the first run, with a cold cache those calls run for real and
   auto-write to the cache (`cache: true`, regex `iter.*`). Iterations 0 and 2
   don't match the bypass regex, so they also run for real. All 16 run for real.
2. **Run 2 — warm cache.** Iterations 1 and 3 now hit the cache, the cond
   returns `True`, and they are served instantly (no real work) while still
   dispatching through Ray. Iterations 0 and 2 still run for real.

The `bypass_cond` is what makes the same loop body safe both cold and warm:
without it, Run 1 would dispatch the cache-reading provider against a cold cache
and raise `KeyError`. Pass `--flush-cache` to reset to cold and re-observe Run 1.

## Files

| File | Purpose |
|------|---------|
| `bypass_cache_loop.py` | The driver: the loop, the `expensive_step` ChiaFunction, the cache-reading bypass provider, and the `cache_hit_cond` bypass condition. |
| `bypass_cache.yaml` | The `cache:` and `bypass:` config (the regexes live here). |
| `cluster.yaml` | A 1-head + 4-local-worker cluster (`${CHIA_HEAD}`), for the `chia up` path. |
| `out/cache/` | Generated cache pickles (created on first run). |

## Run it

Launch the CHIA cluster:

```bash
export CHIA_HEAD=$(hostname)
chia up examples/bypass_cache/cluster.yaml
```

Run the example: 

```bash
python examples/bypass_cache/bypass_cache_loop.py  # run 1: cold
python examples/bypass_cache/bypass_cache_loop.py  # run 2: warm
python examples/bypass_cache/bypass_cache_loop.py  --flush-cache # flush the cache and run the example
```

Or run the example using the job submission interface:

```bash
chia job submit --working-dir . -- python examples/bypass_cache/bypass_cache_loop.py  # run 1: cold
chia job submit --working-dir . -- python examples/bypass_cache/bypass_cache_loop.py  # run 2: warm
chia job submit --working-dir . -- python examples/bypass_cache/bypass_cache_loop.py  --flush-cache # flush the cache and run the example
```

Finally, bring down the cluster:

```bash
chia down examples/bypass_cache/cluster.yaml
```

You can also run it as a plain driver that connects to a running cluster
(`python examples/bypass_cache/bypass_cache_loop.py`).

## Expected output

On Run 1 (cold cache) all 16 print `[REAL RUN]` and take ~0.5s/iter. On Run 2
(warm), iterations 1 and 3 (matching `iter[13]_.*` and present in the cache)
come back in ~0.08s with no `[REAL RUN]` line; iterations 0 and 2 re-run with ~0.5s/iter.
