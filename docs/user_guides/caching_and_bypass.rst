Caching and Bypass
==================

CHIA's **cache** mechanism persists a function's real output to disk, and
**bypass** replaces a function's computation with pre-recorded data while
still dispatching the call through Ray. These mechanisms enable redundant and costly work to be skipped on reruns, easy testing of cluster orchestration, and improved fault tolerance by storing in-progress results. Both are built around ``ChiaFunction``,
keyed by a per-call tag (``_chia_tag``), and configured in the same YAML
file passed to your loop.

At a high level, the cache is the write path (populate from a real run) and bypass is the read path (replay it). They share the tag.

.. contents::
   :local:
   :depth: 1

Tags
----

Every dispatch can carry an optional ``_chia_tag`` that names this specific call — e.g.
``f"iter{i}_opt{j}"``. Both caching and bypass use it as the key, and both can be
restricted to tags matching a regex:

.. code-block:: python

   # The tag identifies this call for both the cache and bypass.
   for i in range(N_ITERS):
       for j in range(N_PARALLEL):
         ref = run_verilator_test.chia_remote(design, _chia_tag=f"iter{i}_opt{j}")

Bypass
------

When a function is bypassed it is still dispatched according to its resource requirements, but
the real computation is replaced by a provider that returns pre-recorded data.
This lets you test cluster orchestration without paying for the underlying work
(builds, simulations, LLM calls, ...).

Configuration
~~~~~~~~~~~~~

List functions under a ``bypass:`` section. ``bypass: true`` opts a function in;
an optional ``tags:`` list (regex patterns) restricts it to matching calls; an
optional ``data:`` path supplies a file to serve.

.. code-block:: yaml

   bypass:
     # Bypass every call to this function.
     simple_add:
       bypass: true

     # Bypass only calls whose _chia_tag matches one of these patterns.
     run_verilator_test:
       bypass: true
       tags: ["iter0_.*"]

     # Shorthand: bypass: true with no extra options.
     simple_multiply: true

     summarize_perf:
       bypass: true
       data: /path/to/recorded_perf.md   # served to the worker as a string

A function not listed (or ``bypass: false``) runs normally. If no YAML is loaded
at all, bypass is a complete no-op.

Setup and providers
~~~~~~~~~~~~~~~~~~~~

Create one ``Bypass`` instance as part of loop setup and register a provider
for each bypassed function. The provider runs on the worker (with the same
scheduling as the real function) and returns the replacement value:

.. code-block:: python

   from chia.base.bypass import Bypass
   from chia.base.ChiaFunction import ChiaFunction, get

   # If yaml_path is None, bypass does nothing.
   bypass = Bypass(yaml_path=args.bypass_config)

   # Required function signature: 
   # tag: the function's _chia_tag
   # data_path: the YAML "data:" path or None
   # *args, **kwargs: the original call's
   def provider_42(tag, data_path, *args, **kwargs):
       return 42

   # Remote ChiaFunction
   @ChiaFunction(resources={"adder": 1})
   def simple_add(x, y):
       return x + y

   bypass.set_provider("simple_add", provider_42)

   # Dispatched to a node with "adder" resource,
   # but returns 42 from the provider instead of running.
   result = get(simple_add.chia_remote(1, 2))   # -> 42

A function is only bypassed when it is
``bypass: true`` and has either a registered provider or a ``data:`` path.

.. _serving-a-file:

Serving a file instead of a provider
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you give a function a ``data:`` path but no provider, CHIA serves that file's
contents as the result. The read is routed through a Ray actor pinned to the
node that constructed the ``Bypass`` (the head), so workers on other nodes can
read it **without a shared filesystem**:

.. code-block:: yaml

   bypass:
     summarize_perf:
       bypass: true
       data: /path/to/recorded_perf.md   # served to the worker as a string

Gating with a condition
~~~~~~~~~~~~~~~~~~~~~~~~~

Besides a provider, a function may register a *condition* — an extra gate that
decides, at dispatch time, whether the bypass actually happens. It runs on the
caller as the last step of the bypass decision (after the ``bypass: true``
flag, the provider/data check, and the tag patterns) and returns a bool:

.. code-block:: python

   # cond(tag, data_path, *args, **kwargs) -> bool  (same args as a provider)
   def cache_hit(tag, data_path, *args, **kwargs):
       return get(get_active_cache().has.chia_remote(tag))

   bypass.set_cond("run_verilator_test", cache_hit)

A falsy return means the call is **not** bypassed and runs for real; a truthy
return lets the bypass proceed. When no condition is registered the default is
``True`` (no extra gate). Use it to make the decision depend on runtime
state, most usefully to only replay from the cache when the value is actually
present (see :ref:`putting-it-together` below).

Cache
-----

The cache is an LRU key/value store of
pickled ``(tag, data)`` files on disk that warm-starts by scanning the cache directory, so cached values survive across loop runs. 
Since the cache is implemented as a remote Ray actor located on the head node, it must be accessed using ``chia_remote``.
Start it once on the
driver after ``ray.init()``:

.. code-block:: python

   from chia.base.cache import start_cache

   start_cache(size=4, units="GB",
               cache_dir_path="/data/chia_cache",
               yaml_path="path/to/yaml.yaml")

   # Access via chia_remote and get
   cache_hit = get(get_active_cache().has.chia_remote(tag))

Writing is automatic
~~~~~~~~~~~~~~~~~~~~~

In the yaml, mark a function ``cache: true`` in the ``cache:`` section (same file as
``bypass:``). Its output is then written to the cache automatically, keyed by the call's ``_chia_tag``. An optional ``tags:`` list
gates which calls are written.

.. code-block:: yaml

   cache:
     run_verilator_test:
       cache: true
       tags: ["iter.*"]      # only cache calls whose tag matches

     # shorthand (cache, no tag filter):
     build_megaboom: true

.. code-block:: python

   # With run_verilator_test cache:true, the real result is written under "iter0".
   result = get(run_verilator_test.chia_remote(design, _chia_tag="iter0"))

Reading is manual (via bypass)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To replay a cached value, register a bypass
provider that reads it back:

.. code-block:: python

   from chia.base.cache import get_active_cache
   from chia.base.ChiaFunction import get

   def cache_provider(tag, data_path, *args, **kwargs):
       hit, value = get(get_active_cache().read.chia_remote(tag))
       if not hit:
           raise KeyError(f"cache miss for tag {tag!r}")
       return value

   bypass.set_provider("run_verilator_test", cache_provider)

   # "iter0" was written on the real run above; this serves it without rerunning.
   served = get(run_verilator_test.chia_remote(design, _chia_tag="iter0"))

.. _putting-it-together:

Putting it together: populate, then replay
-------------------------------------------

The common workflow is one real run that *populates* the cache, then later runs
that *replay* it so you can iterate on the loop/orchestration without recomputing
the expensive steps.

See ``examples/bypass_cache/bypass_cache_loop.py`` for a runnable version (run
it twice: cold then warm).

.. Notes
.. ------

.. - **Bypass still dispatches through Ray.** That is the point: you test
..   scheduling/placement cheaply. It is not the same as simply not calling the
..   function.
.. - **Read is deliberately manual.** The cache never serves on its own — you
..   always opt in to replay by registering a bypass provider. This keeps you in
..   control of when a stale value is served.
.. - **Cross-run persistence is the on-disk pickles**, not a live actor. Stopping
..   the cache actor loses nothing on disk; a new actor warm-starts from the same
..   directory.
.. - **LRU byte budget.** Entries are evicted least-recently-used once the budget
..   is exceeded; a single item larger than the whole budget is never cached.
.. - **Conditions default to on.** A bypass condition (``set_cond``) is an opt-in
..   extra gate evaluated after tag matching; with none registered, a tag-matching
..   call is bypassed as before. A registered condition runs on the caller and may
..   do work (e.g. a cache lookup), so it is only consulted once the tag check
..   passes.
.. - **No YAML, no effect.** With no config loaded, both bypass and caching are
..   inert and every function runs normally.
