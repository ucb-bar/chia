CHIA Basics
===========

**CHIA** is a framework for **fast and flexible orchestration of agentic hardware design workflows**, letting humans, agents, and hardware-design tools work together smoothly.

A hardware-design workflow is any sequence of steps you need to execute in the process of accomplishing some hardware-design task. For example, a simple workflow may include writing some Verilog, simulating the Verilog with Icarus Verilog or Verilator on some unit tests, and if it passes, synthesizing the RTL to get an area and timing estimate. An agentic workflow may include having an LLM write the Verilog or the unit tests.

CHIA boils a workflow down into two pieces:

- **Flows/Loops** — a CHIA flow, or CHIA loop, is a Python script that describes the orchestration of a pipeline of tasks, and the data and control flow between them.
  These flows form a graph of sorts, so we often call tasks "*nodes*", and control/data
  flow between them "*edges*". Notably, any Python function can be a CHIA node.
  LLMs and agents are first-class participants — both as nodes in a flow and
  as orchestrators of the flow. Because flows are just code over a
  library of (often open-source) nodes, rearranging, scaling up, or completely
  reinventing a flow takes little more than changing some function calls and loop
  bounds.

- **Clusters** — a cluster is the collection of computers a flow executes on. You
  assemble a group of physical machines and, on top of them, define *logical workers* that
  expose virtualized hardware resources (CPU cores, GPUs, accelerators, FPGAs) and
  software resources (dependencies, environments, isolation, credentials). Nodes
  are scheduled onto workers that meet their requirements, and workers
  can be containerized for isolation, fast setup, and portability.

Beyond speed and flexibility, CHIA prioritizes **fault tolerance** — automatic
rescheduling of failed tasks, on-the-fly cluster growth with no downtime, bypassing
nodes with cached results for rapid restart, and process-leak prevention through
subprocess tracking — and **good science**: thorough profiling, result caching,
visualization, the ability to bypass nondeterministic nodes with cached results,
and parallel execution of multiple instances of the same experiment.

CHIA is built on the `Ray <https://www.ray.io/>`_ distributed-computing platform,
chosen for its expressive control flow, fine-grained scheduling, and distributed
execution and fault tolerance. It weaves in Docker for containerization, MCP (via
FastMCP) for agent tools, Boto3 / Google Cloud client libraries for cloud
integration, and TensorBoard, Weights & Biases, and GraphViz for profiling and
visualization.
