Architecture Overview
=====================

A **CHIA project** is a composition of two pieces: a **loop** and a **cluster**.

- A **loop** is an orchestration script that defines a pipeline of tasks — the
  *what to do*.
- A **cluster** is the collection of computers those tasks are scheduled onto — the
  *where and how it runs*.

CHIA loops
----------

A loop can be described as a **graph**: the nodes are tasks (functions) and the
edges are the data and control flow between them. CHIA treats both **programmatic**
and **agentic** edges as first-class primitives, and any function can be driven
either way.

Nodes
~~~~~

A **node** is a Python function — *any* Python function can be a node. Each node is
tagged with the resources it needs from the worker it runs on. A node is held until
(a) all of its inputs are ready and (b) a logical worker is available whose
resources exceed the node's demands. Once it can be scheduled, its arguments are
serialized and sent to that worker to execute.

Programmatic edges
~~~~~~~~~~~~~~~~~~~

A node meant to run programmatically is annotated with
``@ChiaFunction(resources=...)`` and dispatched with ``fn.chia_remote(args)``. The call
returns control to the caller immediately (asynchronous), handing back a reference
you can later wait on and collect with ``get()``, or pass directly as an argument to
another node (an explicit edge). This enables asynchronous execution of many different nodes. A
``@ChiaFunction`` called directly — without ``chia_remote`` — just runs in the caller's
own process and isn't a node.

Agentic edges with tools
~~~~~~~~~~~~~~~~~~~~~~~~~~

A node can also be exposed to an agent as an **MCP tool**, registered on a
``ChiaTool`` object via ``ChiaTool.mcp.add_tool(...)``. The ``ChiaTool`` stands up an MCP
server that hosts its registered tools; the server itself runs on a worker chosen by
the ``task_options`` given when it is created. By default a registered function runs
on the worker hosting the tool server, but registering ``ChiaTool.mcp.add_tool(fn.chia_remote_blocking)``
lets the tool function execute remotely according to its *own* resources — so a tool server
can live on one worker while its tools run on another. Every LLM/agent node exposes
a query method that takes a list of ``ChiaTool``\ s to offer the model, and each tool's
docstring becomes the description the model sees.

CHIA clusters
-------------

A loop needs diverse infrastructure working in tandem: different nodes need
different *physical* hardware (FireSim needs FPGAs) and different *software*
environments (dependencies, credentials, isolation). The cluster provides this.

Physical machines and logical workers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You start with a set of **physical machines** — these can be heterogeneous, with different
operating systems, memory capacity, accelerators, etc. On top of
them you define **logical workers** that expose virtualized hardware resources
(CPU cores, GPUs, accelerators, FPGAs) and software resources (dependencies, credentials,
isolation). An LLM worker, for instance, carries the dependencies for an agentic
query (e.g. a Claude Code CLI and the provider's credentials); a FireSim-runner
worker would expose an FPGA resource and only map onto machines that have one. Workers
are mapped onto machines when the cluster comes online — multiple workers can share
a machine, and CHIA supports several allocation strategies for load balancing.

Containerization
~~~~~~~~~~~~~~~~~

Logical workers can run inside **containers**. This gives isolation between workers, front-loads environment setup, and makes clusters portable.

Cloud integration
~~~~~~~~~~~~~~~~~~

Public-cloud machines can be folded directly into a cluster to add compute on
demand. Spanning owned (on-prem) and borrowed (cloud) resources makes for
cost-effective, efficient clusters. In firewalled environments, CHIA uses SSH
reverse tunneling to connect local and cloud machines, with some small limitations
on orchestration.

The cluster config and CLI
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A cluster is specified in a YAML file: available physical machines under a
``provider`` section, logical-worker types (with their exposed resources and container
images) under ``available_node_types``, and any cloud instances under a provider
section such as ``aws_nodes``. Bring it up with ``chia up <config>.yaml`` — which spawns
any cloud nodes, assigns workers to machines, and launches the workers — and tear it
down with ``chia down <config>.yaml``. See the :doc:`CLI Reference </cli/reference>`.

Fault tolerance & reproducibility
---------------------------------

CHIA is built for long, expensive runs:

- **Worker / machine failure** is detected; no new work is scheduled there, and any
  tasks that were running are automatically re-queued onto another worker with the
  required resources. Clusters can grow at runtime — ``chia up --add`` adds resources
  to a live cluster (and reintegrates recovered nodes) with no downtime.
- **Process-leak prevention** — CHIA tracks the processes a node spawns and stops
  them when the node or loop is stopped or cancelled.
- **Caching & bypass** — a loop can reuse a node's cached result instead of
  recomputing it, so it can restart quickly from anywhere after a crash and bypass
  nondeterministic nodes.
- **Profiling & visualization** record what ran where and for how long.

Infrastructure
--------------

CHIA curates a set of existing tools into a single fabric. The
`Ray <https://www.ray.io/>`_ distributed-computing platform is the substrate beneath
CHIA's loops and clusters — providing scheduling, distributed execution, fault
tolerance, and data collection. Ray was chosen over alternatives like LangGraph, the
Microsoft Agent Framework, and Apache Airflow for its expressive control-flow
semantics, fine-grained flexible scheduling, and distributed execution and fault
tolerance. Around it, CHIA uses Docker for containerization, FastMCP for agent
tools, Boto3 and the Google Cloud client libraries for cloud integration, and
TensorBoard, Weights & Biases, and GraphViz for profiling and visualization.

A particular strength is how cleanly CHIA fits diverse components together. There
are already CHIA nodes for hardware-design tools — Chipyard, FireSim, Hammer, CIRCT,
gem5, ChampSim, and Verilator — for a wide range of LLM providers and local model
serving (AWS Bedrock, Google Vertex, OpenAI and Anthropic APIs, Fireworks, Groq,
OpenRouter, Ollama, vLLM) and agent CLIs (Claude Code, OpenAI Codex, GitHub Copilot,
Opencode, Google Antigravity), and for supporting tasks like maintaining relational
databases, compiling software, and collating GitHub issues.
