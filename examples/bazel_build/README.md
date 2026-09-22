# bazel_build

A Bazel build driven as CHIA nodes, with CHIA's cache keyed by a content digest
of each target's inputs, so a rerun skips the targets whose sources didn't
change.

Two caches are in play and they do different jobs. Bazel's action cache makes a
warm `bazel build` fast, but you still pay for the dispatch, the Bazel server
and the analysis phase, and it is local to whichever machine ran the build.
CHIA's cache skips the node: nothing is dispatched and the previous
`BazelResult`, outputs included, is replayed from disk. In an agentic loop that
re-evaluates the same graph after every edit, that is most of the saving.

The key is `BazelNode.source_digest(target)`, which asks Bazel for the source
and BUILD/`.bzl` files in the target's dependency closure and hashes their
contents. Edit `lib/core.txt` and `//lib:core` gets a new digest, and so does
`//app:bundle` because it depends on it, while `//lib:util` keeps its digest and
is replayed.

The workspace under `workspace/` (three `genrule` targets that `sleep 3`, plus
an `sh_test`) is copied into `out/` on first run, so all you need is `bazel` or
`bazelisk` on PATH.

## Run it

```bash
export CHIA_HEAD=$(hostname)
chia up examples/bazel_build/cluster.yaml

python examples/bazel_build/bazel_loop.py               # cold: all three build
python examples/bazel_build/bazel_loop.py               # warm: all three replayed
python examples/bazel_build/bazel_loop.py --edit lib    # //lib:core and //app:bundle miss
python examples/bazel_build/bazel_loop.py --flush-cache
python examples/bazel_build/bazel_loop.py --clean

chia down examples/bazel_build/cluster.yaml
```

Or `--local N` to run against a local Ray with N `bazel` workers and no
cluster. Through job submission:

```bash
chia job submit --working-dir . -- python examples/bazel_build/bazel_loop.py
```

## Output

Cold, the build phase takes about 3s per target, or ~9s if the workers serialize
on one Bazel output base. Warm, the same three digests come back marked
`CACHED (replay)` and the phase drops under a second with no Bazel server
involved. After `--edit lib`, exactly two digests change.

## Files

| File | |
|------|---|
| `bazel_loop.py` | Materializes the workspace, digests each target, builds in parallel, tests. |
| `workspace/` | The example Bazel workspace, as real files. |
| `bazel_cache.yaml` | `cache:`/`bypass:` for the `build` node. |
| `cluster.yaml` | 1 head + 3 `bazel` workers. |
| `out/workspace/` | A copy of `workspace/`, edited in place by `--edit`. |
| `out/cache/` | CHIA cache pickles. |

The cache read path is shared with the other examples in
`examples/common/result_cache.py`.

## Using an external build cluster (RBE)

`BazelNode` is a Bazel client, so remote execution is flags, and the CHIA driver
can be a laptop with no cluster:

```python
ray.init(resources={"bazel": 8})                     # 8 thin clients locally
node = BazelNode(WORKSPACE, config="remote-exec")    # or common_flags=[...]
```

The actions run on the farm while CHIA orchestrates the graph. The three cache
layers stack: the farm's action cache, Bazel's local one, and CHIA's tag cache
which skips the dispatch.

One interaction matters. Most RBE setups run Build without the Bytes
(`--remote_download_minimal`), where outputs stay in the CAS, so
`collect_outputs=True` adds `--remote_download_outputs=toplevel` to leave
something local to read. Your own `--remote_download_*` flag overrides it. Test
logs are fetched regardless.

Verified against a Teleport-fronted Buildbarn cluster: with the flag, 3/3
declared outputs collected; with `--remote_download_minimal` forced back on,
`output_paths` still lists 3 and `outputs` is empty.

Keep credentials off the command line. The node logs argv at INFO and argv is
visible in `ps`, so use a credential file or helper and pass secrets through the
node's `env=`.

## Caveats

- The digests are computed on the driver, one `bazel query` per target, so the
  driver needs the workspace and a `bazel` binary too. Move `source_digest` into
  a node if that isn't true for you.
- `source_digest` is not Bazel's action key. External-repository files are
  folded in by label only, their pinned versions coming from `MODULE.bazel`
  which is hashed by content, and the toolchain is covered only to the extent
  you pass it in `extra=[...]`.
- If the workspace's `.bazelrc` writes a fixed `--build_event_json_file`,
  concurrent CHIA dispatches over one repo clobber each other's BEP. Give each
  call its own path via `flags=`.
- Workers sharing one workspace share one Bazel output base, and Bazel locks it
  exclusively, so concurrent builds serialize. Give each worker its own
  `output_base` for real parallelism, at the cost of a JVM and a cold analysis
  cache apiece.
