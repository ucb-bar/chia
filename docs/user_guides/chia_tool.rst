ChiaTool
========

``ChiaTool`` is the primitive CHIA uses to allow an **AI
agent** to orchestrate control of the loop. Where ``chia_remote(...)`` creates a node that your flow
dispatches programmatically — a programmatic edge in the task graph — a ``ChiaTool`` stands up an
`MCP <https://modelcontextprotocol.io/>`_ server on a worker that an *agent* can
call on its own initiative. That is an **agentic edge**: you give the agent a
set of tools and a prompt, and the agent decides which tools to call, when, and
with what arguments. This page explains the MCP concepts the tool layer leans
on, walks through a tool's lifecycle, shows how tools attach to an agent, and how
to define your own tools.

.. contents::
   :local:
   :depth: 2

A few MCP concepts
------------------

CHIA tools speak the `Model Context Protocol <https://modelcontextprotocol.io/>`_
(MCP), the open standard agents use to discover and call external tools over
HTTP.

- **Tool (method)** — a single callable the agent can invoke, with a name and a
  docstring/type signature. The agent reads those to decide *when* to call the
  tool and *how* to fill in its arguments, so a tool method's docstring is part
  of its interface, not just documentation.

- **Tool server** — a small HTTP server (FastAPI + uvicorn) that hosts one
  ``ChiaTool``'s methods and speaks MCP. CHIA deploys it onto a worker as a Ray
  **actor** so it persists across many agent calls (see
  :doc:`/user_guides/chia_function` for the actor/worker vocabulary). Its MCP
  endpoint is::

      http://{host}:{port}/{name}/mcp

- **Resources / placement** — like a node, a tool server is pinned onto a worker
  that advertises the resources it needs. You pass these as ``task_options`` (see
  :ref:`tool-placement`), so the agent's bash tool lands on, say, the worker that
  holds a Chipyard checkout.

Anatomy of a tool
-----------------

CHIA provides a library of ready-made tools and a base class,
:mod:`chia.base.tools.ChiaTool`, for writing your own. A tool is a plain Python
**class** that subclasses ``ChiaTool``, bundles the tool's configuration and
state, and registers one or more **methods** as the callable tools the agent
sees.

:mod:`chia.base.tools.BashTool` is a representative example: it gives an agent a
bash interface to the worker it is deployed on. The class holds the working
directory and registers a single ``run_command`` method:

.. code-block:: python

    class BashTool(ChiaTool):
       def setup(self, work_dir="/"):
           self.work_dir = work_dir
           self.mcp.add_tool(self.run_command, name=f"{self.name}_run_command")

       def run_command(self, command: str) -> str:
           """Run a bash command and return combined stdout/stderr."""
           ...                                                       # runs in the container
    
    bt = BashTool(name="bash", work_dir="/home/", task_options={"resources": {"chipyard": 1}})

To create a ``ChiaTool``, you create a subclass which defines a ``setup()`` method where you register all of the
tools with ``self.mcp.add_tool(...)``.

The base class runs its own initializer behind the scenes — creating the
``self.mcp`` server object your tools attach to — then calls your ``setup()`` method
to register the tools, then deploys the running server behind the scenes.

The constructor of the child class forwards arguments to the setup call (like ``work_dir``)
and takes two additional arguments: ``name`` and ``task_options``. The former is a human
readable string used by the agent to reference the tool, and the latter is used to
determine where to place the tool server, and includes things like resource specifications
and placement groups.

Alternatively, if you would rather write the constructor by hand, an
:ref:`explicit initialization idiom <explicit-init-idiom>` is supported and
can be used instead of ``setup()``.

The tool lifecycle
-------------------

Constructing a tool deploys it
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Unlike a node, which only runs when you call ``chia_remote``, **instantiating** a
``ChiaTool`` immediately stands up its server on a worker: once ``setup()`` has
registered the tools, the base deploys the server automatically. So construction
is the deploy step:

.. code-block:: python

   from chia.base.tools.BashTool import BashTool

   chipyard_bash = BashTool(
       name = "chipyard_bash", work_dir="/home/ray/chipyard",
       task_options={"resources": {"chipyard": 1}},   # land on a chipyard worker
   )
   # The MCP server is now live at
   # http://{chipyard_bash.hostname}:{chipyard_bash.port}/chipyard_bash/mcp

Attaching a tool to an agent
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pass the tool (or several) to any CHIA provided LLM or agent's ``prompt`` node
via the ``tools`` argument. The agent receives each tool's MCP endpoint, discovers
the methods, and calls them as needed to satisfy the prompt:

.. code-block:: python

   from chia.base.ChiaFunction import get

   prompt = (
       "Use the chipyard_bash tool to add a printf to the Rocket core's "
       "Chisel source that fires when instret reaches 1000."
   )
   result = get(llm.prompt.chia_remote(llm, prompt, tools=[chipyard_bash]))
   print(result.result)

Here the agent itself is a node — ``llm.prompt`` is dispatched with
``chia_remote`` — and the tool it was handed lets it reach out and act on the
cluster while it runs. The :doc:`/getting-started/quickstart` builds up exactly
this pattern, ending with an agent editing RTL through a ``BashTool``.

Stopping a tool
~~~~~~~~~~~~~~~

A tool server stays running until you stop it. Call ``stop()`` once the agent
work that needs it is done; it shuts uvicorn down and kills the actor, freeing
the port and the worker slot:

.. code-block:: python

   chipyard_bash.stop()

.. _tools-calling-nodes:

Bridging tools back to nodes
----------------------------

A tool method is ordinary Python, so it can dispatch ``@ChiaFunction`` nodes just
like the rest of your flow. Because the agent expects a concrete return value
(not an ``ObjectRef``), use :ref:`chia_remote_blocking <per-call-options>`, which
dispatches remotely and returns the unwrapped result.

This is a composition point between the two primitives: the agent makes one
tool call (an agentic edge), and that call fans out real, scheduled cluster work
(programmatic edges) before returning a value the agent can reason about.

Defining your own tool
-----------------------

Subclass ``ChiaTool`` and define a ``setup()`` method that registers your tools,
giving each method a clear docstring (which is given to the agent to describe the 
tool) and typed signature — the agent relies on both to call it correctly. 
:mod:`chia.base.tools.ChiaToolTemplate` is a copyable starting point.

Here's an example:

.. code-block:: python

   from chia.base.tools.ChiaTool import ChiaTool

   class GreetingTool(ChiaTool):
       def setup(self, greeting="Hello"):
           self.greeting = greeting
           self.mcp.add_tool(self.greet, name=f"{self.name}_greet")

       def greet(self, who: str) -> str:
           """Return a greeting for *who*.

           Args:
               who: The name to greet.
           """
           return f"{self.greeting}, {who}! This is {self.name}."

A tool method may return **any** type — MCP always serializes the result for the
agent, falling back to a JSON (then ``str``) rendering of arbitrary objects. Annotating the method's 
return type (as ``greet`` does with ``-> str``) can allow MCP to provide the agent a schema for the 
return value in some cases. The following return types are formatted more cleanly and are preferred:

- ``str`` — handed to the agent as plain text.
- ``dict``, a pydantic ``BaseModel``, a ``TypedDict``, or a dataclass — returned
  as structured, machine-readable JSON. 
- ``list`` / ``tuple``.
- ``None`` — an empty result.
- MCP ``Image`` / ``Audio`` content objects (from ``mcp.server.fastmcp``) — for
  image or audio payloads, instead of raw ``bytes``.

For tools that wrap long-running work, CHIA also provides asynchronous variants
(:mod:`chia.base.tools.AsyncBashTool`, :mod:`chia.base.tools.AsyncJobTool`) that
split a job into a *submit* method and a *status* method so the agent can poll
rather than block.

.. _tool-placement:

Tool placement and resources
----------------------------

``task_options`` is the dict of Ray scheduling constraints applied to the tool's
server actor — the tool analog of a node's per-call ``.options(...)``. The most
common entry is ``resources``, which pins the server onto a worker advertising
the right capability:

.. code-block:: python

   # Land the bash tool on a worker that has a chipyard checkout.
   BashTool("chipyard_bash", "/home/ray/chipyard",
            task_options={"resources": {"chipyard": 1}})

Because the server runs *on* that worker, anything the tool's methods do — run a
command, read a file, dispatch a node — happens with that worker's filesystem and
resources in reach. Omitting ``task_options`` lets the server land on any
available worker.

.. _explicit-init-idiom:

Advanced
--------

The explicit initialization idiom
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before the ``setup()`` hook existed, every tool was written with an explicit
``__init__`` that called the base class's construction steps by hand. This style
is **still fully supported**: define ``__init__`` instead of ``setup()`` and CHIA
leaves it untouched. You might prefer it when you want the tool's constructor
signature to be explicit (for introspection or IDE help), or to match tools
already written this way.

An ``__init__`` written this way follows a fixed three-step order, and the order
matters:

1. **``super().__init__(name, task_options=...)``** creates the ``FastMCP``
   instance (``self.mcp``) the tools attach to. It does not start a server yet.

2. **``self.mcp.add_tool(self.method, name=...)``** registers each method as an
   MCP tool. This must happen *after* step 1 (so ``self.mcp`` exists) and
   *before* step 3 (so the server has its tools when it starts serving). Call it
   once per method you want to expose.

3. **``super().__post_init__()``** deploys the server: it creates a Ray actor
   (constrained by ``task_options``), which probes for a free port, starts
   uvicorn in a background thread, and registers the running tool. After this
   returns, ``self.hostname`` and ``self.port`` point at the live endpoint.

The same ``BashTool`` written explicitly:

.. code-block:: python

   class BashTool(ChiaTool):
       def __init__(self, name, work_dir="/", task_options=None):
           super().__init__(name, task_options=task_options)               # 1. init base
           self.work_dir = work_dir
           self.mcp.add_tool(self.run_command, name=f"{name}_run_command")  # 2. register
           super().__post_init__()                                         # 3. deploy

       def run_command(self, command: str) -> str:
           """Run a bash command and return combined stdout/stderr."""
           ...

The two styles are interchangeable and can coexist in the same codebase: a
subclass that defines ``setup()`` gets the automatic bracketing, while one that
defines ``__init__`` keeps full manual control.

Intermediate base classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~

An *intermediate* base sits between ``ChiaTool`` and a concrete tool, bundling
functionality that several tools share — :mod:`chia.base.tools.AsyncJobTool` (the
base for the async variants above) is the canonical example, adding a
background-job runner its subclasses inherit.

Writing one correctly hinges on a single fact: the ``setup()`` idiom's generated
``__init__`` calls ``ChiaTool.__init__`` **directly**, skipping every intermediate
``__init__`` in between. So a ``setup()``-style subclass of your intermediate
never runs your intermediate's ``__init__``. Make the intermediate work whether
or not its own ``__init__`` runs:

Initialize state lazily, not in** ``__init__``. E.g. provide an idempotent
``_ensure_X()`` that creates the state on first use, and call it at the top of
every helper that needs it. This is the rule that makes the class work under
both idioms with no per-subclass boilerplate:

  .. code-block:: python

     def _ensure_job_state(self):
         if not hasattr(self, "_job_lock"):
             self._init_job_state()

     def _job_start(self, work):
         self._ensure_job_state()      # works whether or not __init__ ran
         ...

This covers state that can be created lazily with sensible defaults. It does not
transparently cover eager work that depends on constructor arguments (e.g.
opening a connection from a ``dsn=`` passed in). For that, you may requier the leaf's
``setup()`` pass the arguments into the super class explicitly.

See also
--------

- :doc:`/user_guides/chia_function` — the programmatic counterpart; tools call nodes via ``chia_remote_blocking``.
- :doc:`/getting-started/quickstart` — a hands-on example that ends with an agent using a ``BashTool`` to edit RTL.
- :doc:`/concepts/overview` — how nodes, tools, agents, workers, and clusters fit together.
- :doc:`/user_guides/profiling` — recording and visualizing a loop's execution, including agent calls.
