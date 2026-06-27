# Chia Profiler

Execution timing, worker attribution, and dependency tracking for
`@ChiaFunction` calls. Implemented in [`profiler.py`](./profiler.py).

## How it works

Profiling is enabled when a `ChiaProfileCollector` Ray actor is running. Call
`start_collector()` from the driver **after** `ray.init()` and before any
profiled calls:

```python
ray.init(address=ray_address, runtime_env=RUNTIME_ENV)
start_collector(log_dir=os.path.join(LOG_DIR, job_id))
```

`start_collector` spawns a `ProfileCollectorActor` pinned to the head node
(`num_cpus=0`, so many can coexist). The actor collects events in memory and
appends them as JSONL to `{log_dir}/ChiaProfileCollector.log` (default
`/tmp/ray/{ray_job_id}/`).

Profiled `@ChiaFunction` calls emit events through a fire-and-forget
`actor.record.remote(event)`, so workers never block on the collector. When no
collector actor is running, every profiler method is a fast no-op.

### Event types

| Type          | Emitted when                                    |
| ------------- | ----------------------------------------------- |
| `dispatch`    | remote call starts on a worker (`chia_remote()`)|
| `complete`    | remote call finishes on a worker                |
| `local_start` | local `@ChiaFunction` call started              |
| `local_end`   | local `@ChiaFunction` call finished             |
| *(custom)*    | anything logged via `log_event()`               |

Each event carries `call_id`, `func`, `ts`, and worker attribution
(`worker_id`, `node_id`, `worker_ip`). Call IDs are globally unique:
`{worker_id[:12]}_{counter:06d}`.

### Concurrency on a single collector

A Ray actor is single-threaded, so all `record()` / `record_batch()` calls are
serialized by Ray regardless of how many workers fire concurrently. Combined
with POSIX append-mode writes, no explicit file locking is needed.

## Multiple jobs on one cluster

**This is the important and non-obvious part.** The behavior depends entirely
on **Ray namespaces**, because `ChiaProfileCollector` is a *named* actor and
named actors are scoped per-namespace.

### Parallel jobs as separate driver processes (the common case)

When you launch several jobs as independent processes
(e.g. `python merge.py`, `python opt_pipeline.py`, …) and each does a plain
`ray.init(address=...)` **without** passing `namespace=`, Ray assigns each
driver its own **anonymous (random UUID) namespace**.

As a result, inside `start_collector`:

```python
existing = _ray.get_actor(_COLLECTOR_ACTOR_NAME, **lookup_kwargs)  # namespace-scoped
```

each job's lookup happens in its *own* namespace and cannot see the other jobs'
collectors. So **each job creates its own collector actor**, writing to its own
`log_dir`. The isolation propagates to workers automatically: a worker spawned
by job B runs within job B's namespace, so its `get_collector()` resolves to
job B's collector. No cross-talk — each job's events land in the right log file.

This is why running jobs in parallel "just works" and the right logs get dumped
to each job's output directory.

### Multiple logical jobs inside one driver process

If a single driver / single `ray.init` runs several logical jobs 
and calls `start_collector(log_dir=...)`
again, the second call is a **no-op**: it finds the existing actor (same
namespace, plus the `_collector_override` module global short-circuit), so the
new `log_dir` is **ignored** and all events keep flowing to the first log file.

### Explicit shared namespace

If you deliberately pass the same `namespace=` to both `ray.init` and
`start_collector` across jobs, they collapse onto a **single shared collector**,
and only the first job's `log_dir` wins (later ones are ignored). Avoid this
unless you actually want one merged log.

### Summary

| Scenario                                              | Namespace                     | Result                                              |
| ----------------------------------------------------- | ----------------------------- | --------------------------------------------------- |
| Parallel **separate driver processes** (common)       | each gets its own anonymous   | one collector per job, separate logs ✓              |
| Multiple iterations **within one driver**             | same namespace                | shared collector; later `log_dir` args ignored      |
| Explicit shared `namespace=` on init + start_collector| shared named namespace        | single shared collector; only first `log_dir` wins  |

## `add_info` vs `log_event`

Both attach custom data to the trace, but in different ways.

`add_info(info: dict)` **enriches the current call's event**. It does not write
anything immediately — it stashes the dict in thread-local storage, which is
then merged into the `complete` event of the `@ChiaFunction` call you're
currently inside. The result is still *one event per call*, just with extra
fields. Call it from within a `@ChiaFunction` body; being thread-local, it's
safe under concurrent tasks. Use it to annotate a call, e.g.
`add_info({"tokens": 1423, "cache_hit": True})`.

`log_event(event_type, **kwargs)` **emits a new standalone event** immediately,
with its own `type` and timestamp, auto-tagged with the current `worker_id`. It
is not tied to any in-flight call and can be called from anywhere. Use it to
mark discrete moments, e.g. `log_event("checkpoint", step=5)`.

|                  | `add_info`                                  | `log_event`                       |
| ---------------- | ------------------------------------------- | --------------------------------- |
| Emits new event? | No — merges into the current `complete` event | Yes — a fresh standalone event  |
| Tied to a call?  | Yes, the enclosing `@ChiaFunction` call     | No                                |
| Timing           | flushed when the call completes             | written immediately               |
| Typical use      | annotate a call (`tokens`, `cache_hit`)     | mark a moment (`checkpoint`)      |

## API reference

| Function                                   | Purpose                                                        |
| ------------------------------------------ | -------------------------------------------------------------- |
| `start_collector(log_dir=, namespace=)`    | Create the collector actor on the head node. Idempotent.       |
| `stop_collector()`                         | Kill the collector actor. Idempotent.                          |
| `get_collector(namespace=)`                | Return a handle to the collector actor, or `None`.             |
| `get_profiler()`                           | Return the per-process `ChiaProfiler` singleton.               |
| `ChiaProfiler.add_info(dict)`              | Merge extra metadata into the current call's event (thread-local). |
| `log_event(...)`                           | Emit a custom event.                                           |
