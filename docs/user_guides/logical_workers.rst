Setting up logical workers
==========================

A CHIA cluster starts from a set of **physical machines**, but the things that
actually execute your ``@ChiaFunction`` nodes are **logical workers**
that advertise a set of *resources* (virtualized hardware and software
capabilities) and are mapped onto compatible machines when the cluster comes up
(see :doc:`/concepts/overview`). This guide walks through defining a logical
worker in your cluster YAML, choosing between a **dockerized** and a
**non-dockerized (bare)** worker, and — most importantly — using CHIA's command
hooks to make sure the environment your remote functions execute in is the one
you intended.

For the full YAML schema this guide references, see
:doc:`/user_guides/cluster_config_reference`.

.. contents::
   :local:
   :depth: 2

What a logical worker is
------------------------

A logical worker is one Ray worker process, launched by ``chia up`` on some
physical machine, that advertises the resources you gave it. Its definition
lives under ``available_node_types`` in the cluster YAML and answers four
questions:

- **What can it do?** — ``resources``, e.g. ``{"verilator_run": 8}``. A node
  decorated ``@ChiaFunction(resources={"verilator_run": 1})`` waits until a
  worker advertising that resource has a free unit, then runs there. Quantities
  matter: a worker advertising 8 units can concurrently run 4 nodes requesting 2 units.
- **How many of them?** — ``num_workers``.
- **Where may it run?** — ``compatible_ips``, the machines this type can be
  placed on. The cluster's worker pool is the union of every type's
  ``compatible_ips``; multiple workers (of the same or different types) can
  share one machine.
- **What environment does it execute in?** — either a container image
  (``docker:`` block) or the host's own environment, plus the command hooks
  described below.

A worker type is *logical* because it says nothing about a specific machine:
the same ``verilator_run`` definition can put three workers on one big on-prem
box, or spread them across a mix of on-prem and cloud machines, without any
change to the loop code that dispatches onto it.

Anatomy of a worker definition
------------------------------

.. code-block:: yaml

   available_node_types:
       verilator_run:
           resources: {"verilator_run": 8}      # what this worker advertises
           num_workers: 4                        # how many workers of this type
           compatible_ips: [machine9, machine10] # where they may be placed
           worker_env_commands:                  # environment activation (up AND down)
               - "source ~/.bashrc && conda activate chia_env"
           worker_setup_commands: []             # one-time setup (up only)
           docker:                               # omit this block for a bare worker
               image: "ghcr.io/ucb-bar/chia-verilator-run:latest"
               container_name: "chia-verilator-run-${USER}"
               run_options:
                   - --ulimit nofile=65536:65536
                   - --shm-size=10.24gb

   worker_start_ray_commands:
       - ray stop
       - ray start --address=$RAY_HEAD_IP:6379 --dashboard-agent-listen-port=0

CHIA injects ``--resources`` (and, for tunneled cloud workers, pinned ports)
into the ``ray start`` line automatically — you never write the resources into
``worker_start_ray_commands`` yourself.

Dockerized vs. non-dockerized workers
-------------------------------------

Both dockerized and non-dockerized workers join the same Ray cluster and are indistinguishable
to the loop; they differ in *where the worker's environment comes from* and
*what CHIA has to do to create it*.

.. list-table::
   :header-rows: 1
   :widths: 22 39 39

   * -
     - Dockerized (``docker:`` block)
     - Bare (no ``docker:`` block)
   * - Environment source
     - The container image. Tools, Ray, Python, and the chia package are baked
       in; every machine runs the identical environment.
     - The host itself. Every compatible machine must already provide the
       tools, a matching Ray/Python, and the chia package (typically via a
       shared conda env).
   * - Where commands run
     - Host: ``initialization_commands``, rsync, container start. Everything
       else (env/setup hooks, ``ray start``, your functions) runs **inside**
       the container.
     - Everything runs directly on the host over SSH.
   * - Isolation
     - Own filesystem and PID namespace. An agent node cannot read files you
       did not mount, and ``ray stop`` in one worker cannot kill another.
     - None — workers share the host's filesystem and processes. CHIA skips
       ``ray stop`` for the second and later bare workers on a host so they
       don't kill each other, but nothing stops your functions from
       interfering through the filesystem.
   * - First-time setup cost
     - An image pull (seconds to minutes). E.g. CHIA's Chipyard image replaces
       an ~hour-long from-source setup.
     - Whatever it takes to install the toolchain on every machine, by hand or
       via ``worker_setup_commands``.
   * - Host access
     - Nothing from the host is visible unless you mount it with ``-v`` in
       ``run_options`` (CHIA adds ``--net=host`` and ``--shm-size=8g``, and
       **no** volume mounts, itself).
     - Full access to the host — needed for host-attached hardware (FPGAs),
       license servers, and site tool trees (e.g. ``/ecad``).

**Use a dockerized worker when you can.** It front-loads environment setup,
makes the cluster portable across heterogeneous machines, and isolates agentic
workers — an LLM worker that can only see its own container cannot copy your
reference implementation or read a proprietary PDK. The image must be
CHIA-compatible (Ray and Python matching the head, chia package installed, bash
login shell, ``openssh-client``/``rsync``); building one is covered in
:doc:`/user_guides/docker_images`.

**Use a bare worker when the environment can't be containerized**: FireSim
workers that drive physical FPGAs, vendor EDA tools tied to host-mounted
installs and license daemons, or machines where Docker isn't available. A bare
worker's YAML is the same minus the ``docker:`` block — but now *you* are
responsible for the host environment, usually by activating it in
``worker_env_commands``, which are run before starting the worker process, meaning
that all nodes execute in a process started from that environment (e.g. the $PATH from
that environment).
We recommend that bare workers have the same conda environment as the head, to ensure
that there are no Ray or CHIA version mismatches. To do this, export the head's
environment to a YAML file with ``conda env export --no-builds > env.yml``
(``--no-builds`` drops platform-specific build strings so the file ports across
machines) and copy it to the worker host. There, recreate it with
``conda env create -f env.yml`` — the new environment keeps the name
recorded in the file (``chia_env``), so the ``conda activate chia_env`` in
``worker_env_commands`` works unchanged.

The process is started as the SSH user from the ``auth`` block — the global
``ssh_user`` / ``ssh_private_key`` by default, overridable **per machine** under
``auth.overrides`` (keyed by hostname/IP, or an ``@node_type:index`` placeholder
for cloud nodes). Because a bare worker runs as its SSH login user, the override
decides which Unix account your functions execute under on that machine — and
therefore whose ``~/.bashrc`` is sourced and which files, groups, and tool
licenses they can reach:

.. code-block:: yaml

   available_node_types:
       vlsi:
           resources: {"VLSI": 1, "syn": 1}
           num_workers: 6
           compatible_ips: [machine1, machine2, machine3]
           worker_env_commands:
               - "source ~/.bashrc && source /usr/licensed/scripts/ecad.bashrc && conda activate chia_env"

   auth:
       ssh_user: ${USER}                    # default for every machine
       # ssh_private_key: ~/.ssh/key        # omit if your key is in ssh-agent
       overrides:
           machine3:                        # per-machine: different account here
               ssh_user: vlsi-svc
               ssh_private_key: ~/.ssh/vlsi_svc_key

Here workers landing on ``machine1`` / ``machine2`` run as ``${USER}``, while
the one on ``machine3`` runs as the ``vlsi-svc`` service account. CHIA fills in
``auth.overrides`` entries automatically for cloud-provisioned machines (SSH
user, key, and tunnel config), so you normally only write them by hand for
on-prem hosts.

A ``docker:`` block may also be given once at the top level as a cluster-wide
default that individual node types override. The head node is never
containerized.

Controlling the execution environment
-------------------------------------

The central fact to understand: **the shell session that runs your env/setup
hooks is the same session that runs** ``ray start``. The Ray worker processes
it spawns inherit that session's environment — and those processes are exactly
where your ``@ChiaFunction`` bodies execute. So "making the remote environment
correct" means putting the right commands in the right hooks, in this order
(full walk-through in
:doc:`/user_guides/cluster_config_reference`):

.. code-block:: text

   1. initialization_commands        ← HOST, each in its own SSH session
   2. file_mounts rsync              ← HOST
   3. docker setup                   ← HOST (dockerized only: pull, run,
                                        then run_setup_commands inside)
   4. ┌─ single session — on the host (bare) or INSIDE the container ─┐
      │ <type>.worker_env_commands      per-type env activation       │
      │ setup_commands                  global, all nodes             │
      │ <type>.worker_setup_commands    per-type one-time setup       │
      │ export RAY_HEAD_IP=...                                        │
      │ worker_start_ray_commands       (--resources injected)        │
      └───────────────────────────────────────────────────────────────┘

Which hook to use for what:

- ``worker_env_commands`` — **environment activation**: conda envs, tool
  ``env.sh`` scripts, ``export``\ s your functions need. These run on both
  ``chia up`` and ``chia down`` (teardown needs ``ray`` on the ``PATH`` too),
  so they must be idempotent and side-effect-free. This is the hook that makes
  ``source ~/chipyard/env.sh`` visible to every node that runs on the worker.
- ``worker_setup_commands`` — **one-time setup** during ``chia up`` only:
  cloning a repo, generating a config, warming a cache. Runs after activation,
  in the same session.
- ``setup_commands`` — like ``worker_setup_commands`` but global: runs on the
  head and every worker (inside the container for dockerized types).
- ``docker.run_setup_commands`` — dockerized only; runs inside the container
  right after it starts, *before* the main session (e.g. ``git pull`` a
  mounted checkout, fix up ``/etc/passwd`` for an arbitrary ``--user``).
- ``initialization_commands`` — host-side, outside any container, each command
  in its own SSH session (no shared environment). Use for host prerequisites
  like ``docker login``.

.. note::

   All of these run in a **non-interactive login shell** (``bash --login``):
   ``/etc/profile`` and ``~/.bash_profile`` are sourced but ``~/.bashrc`` is
   **not** — and many ``~/.bashrc`` files exit early for non-interactive
   shells anyway. If your conda setup lives in ``~/.bashrc``, source it
   explicitly: ``source ~/.bashrc && conda activate chia_env``. In a
   dockerized worker, prefer baking the ``PATH`` into the image with ``ENV``
   (see :doc:`/user_guides/docker_images`).

Verifying the environment
-------------------------

Before trusting a worker with real work, check it from the outside in.

**1. Preview the generated scripts.** ``--dry-run`` prints the node
assignments and the exact per-worker scripts CHIA would run, without touching
any machine:

.. code-block:: bash

   chia up my_cluster.yaml --dry-run

**2. Confirm the workers registered.** After ``chia up``, on the head node (in
the same env as ``head_env_commands``):

.. code-block:: bash

   ray status        # every worker and its advertised resources

or, for the raw resource totals:

.. code-block:: bash

   python -c "import ray; ray.init(address='auto'); print(ray.cluster_resources())"

Your custom resources (``verilator_run``, ``chipyard``, …) should appear with
the expected totals (``num_workers`` × the per-worker quantity). A worker whose
Ray or Python *minor* version doesn't match the head fails to join — recheck
image/conda pins if a resource is missing.

**3. Inspect a worker's shell directly.** For a dockerized worker, exec into
its container on the machine it landed on (CHIA appends the worker index to
``container_name``):

.. code-block:: bash

   ssh machine9
   docker exec -it chia-verilator-run-${USER}-0 bash --login
   which ray && ray --version     # matches the head?
   python --version
   which verilator                # your tool on the login-shell PATH?

For a bare worker, SSH to the host, run your ``worker_env_commands`` by hand,
and make the same checks. Remember to test with ``bash --login`` (not your
interactive shell) so you see what CHIA's scripts see.

**4. Smoke-test with a real dispatch.** The ground truth is a node that runs
on the worker and reports what it found. Request exactly one unit of the
worker's resource and return the environment:

.. code-block:: python

   import getpass, os, shutil, socket
   from chia.base.ChiaFunction import ChiaFunction, get

   @ChiaFunction(resources={"verilator_run": 1})
   def check_env() -> dict:
       return {
           "host": socket.gethostname(),
           "user": getpass.getuser(),
           "verilator": shutil.which("verilator"),
           "ray_head": os.environ.get("RAY_HEAD_IP"),
           "path": os.environ.get("PATH"),
       }

   print(get(check_env.chia_remote()))

If ``shutil.which`` comes back ``None`` for a tool you sourced in
``worker_env_commands``, the activation didn't take (most often the
``~/.bashrc`` pitfall above). Because this dispatches through the normal
scheduler, it also confirms placement: the reported ``host`` must be one of the
type's ``compatible_ips``.

Placement and scaling
---------------------

Workers are assigned to machines when the cluster comes up. Each type is
constrained to its ``compatible_ips``; within them, ``balance_level`` picks the
strategy: ``cluster`` (default) places each worker on the machine with the
fewest workers globally, ``worker`` spreads this type's own workers evenly
across its IP pool regardless of what other types placed. Listing a machine
twice in ``compatible_ips`` biases more workers onto it.

Cloud machines fold into the same mechanism: declare instances under
``aws_nodes`` / ``gcp_nodes`` and reference them in ``compatible_ips`` with
``@<node_type>:<index>`` placeholders. CHIA provisions the instances,
reverse-tunnels them to the head, and pins the worker's Ray ports
automatically — the logical-worker definition itself is unchanged. See
`Cloud nodes <cluster_config_reference.html#cloud-nodes>`_.

Lifecycle
---------

.. code-block:: bash

   chia up my_cluster.yaml          # provision cloud nodes, assign + start workers
   chia up my_cluster.yaml --add    # add newly-declared workers / restart dead ones
   chia down my_cluster.yaml        # stop workers (and containers), then the head

``chia up --add`` compares the YAML against the live cluster and only brings up
workers that aren't already registered, so you can grow a running cluster (or
reintegrate a machine that died) without downtime. On ``chia down``, dockerized
workers get ``ray stop`` inside the container followed by ``docker stop`` /
``docker rm``; bare workers get ``ray stop`` on the host (skipped when the
worker shares the head's machine, so it can't kill the head's Ray).

Common pitfalls
---------------

- **Missing tools at run time** — activation lives in ``~/.bashrc``, which
  login shells don't source. Move it to ``worker_env_commands`` or the image's
  ``ENV``.
- **Worker never joins the cluster** — Ray/Python minor-version mismatch with
  the head. Pin both to the head's versions (see
  :doc:`/user_guides/docker_images`).
- **Container name collisions on shared machines** — include ``${USER}`` in
  ``container_name``; CHIA only appends the worker index.
- **Locally-built image + ``pull_before_run: true``** (the default) — the pull
  fails. Set ``pull_before_run: false``.
- **Files missing inside a container** — CHIA adds no volume mounts; add
  ``-v`` entries to ``run_options``, or bake the files into the image (mounts
  can't work for cloud workers — the files don't exist on the remote host).
- **One bare worker kills another** — don't put ``ray stop`` in
  ``worker_setup_commands``; leave it in ``worker_start_ray_commands`` where
  CHIA knows when to skip it.

See also
--------

- :doc:`/concepts/overview` — how loops, nodes, workers, and clusters fit together.
- :doc:`/user_guides/cluster_config_reference` — every YAML key and the exact command execution order.
- :doc:`/user_guides/docker_images` — building a CHIA-compatible worker image.
- :doc:`/user_guides/chia_function` — how nodes request the resources workers advertise.
