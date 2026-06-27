Building a CHIA-compatible Docker image
=======================================

CHIA can run worker nodes inside Docker containers (see
:doc:`/user_guides/cluster_config_reference`). When you bring a cluster up with
``chia up``, CHIA starts each worker's container, then runs your tool inside it.
This guide shows how to build an image that works as a CHIA worker — starting
from a CHIA base image, and (for the cases that need it) from scratch.

.. contents::
   :local:
   :depth: 2

What "CHIA-compatible" means
----------------------------

At cluster start, ``chia up`` (over SSH to the worker's host) does roughly this:

.. code-block:: text

   docker run -d --net=host --shm-size=8g <your run_options> <image> sleep infinity
   docker exec -i <container> bash --login   # <- your worker_setup_commands + `ray start ...`

The container is kept alive by ``sleep infinity``; **Ray is not started by the
image** — CHIA execs a login shell into the running container and runs the
``ray start --address=...`` command from your cluster YAML there. So a
compatible image only has to provide the right *environment* for that command to
succeed. Concretely it must:

- have **Ray ``2.54.0``** importable on the login-shell ``PATH``, on a **Python
  3.10.19** interpreter;
- have the **chia package installed** (so worker-side ``chia`` imports and MCP
  :doc:`tool servers </user_guides/chia_tool>` work);
- provide a **``bash`` login shell**;
- ship **``openssh-client``** and **``rsync``** for file transport.

The Ray and Python versions must **match the head** — Ray refuses to connect a
worker whose Ray or Python *minor* version differs from the cluster head's. The
quickest way to satisfy all of the above is to start from a CHIA base image,
which already does.

Extend a CHIA base image with your tool
---------------------------------------

``ghcr.io/ucb-bar/chia`` is the canonical base. It is
``rayproject/ray:2.54.0-cpu`` with the ``chia`` package pip-installed on top, so
it already provides the right Python, Ray ``2.54.0``, the chia package (and its
MCP/FastAPI dependencies), and a ``ray`` user with ``HOME=/home/ray``. You just
add your tool:

.. code-block:: dockerfile

   FROM ghcr.io/ucb-bar/chia:latest

   # Install your tool's system dependencies as root, then drop back to `ray`.
   USER root
   RUN apt-get update \
       && apt-get install -y --no-install-recommends my-tool openssh-client rsync \
       && rm -rf /var/lib/apt/lists/*
   USER ray

   # (Optional) make your tool discoverable on the login-shell PATH.
   ENV PATH="/opt/my-tool/bin:${PATH}"

That image is ready to be a CHIA worker — no ``EXPOSE``, ``ENTRYPOINT``, or
``ray start`` is needed in the Dockerfile; CHIA supplies those at run time.

.. note::

   A login shell (``bash --login``) sources ``/etc/profile`` and
   ``~/.bash_profile`` / ``~/.profile`` but **not** ``~/.bashrc``. Put anything
   your tool needs on the ``PATH`` via ``ENV`` in the Dockerfile, or activate it
   in ``worker_env_commands`` / ``worker_setup_commands`` in the cluster YAML —
   don't rely on ``~/.bashrc``.

Extend your tool image with CHIA
--------------------------------

When you already have a working image — a vendor EDA tool, your own build
environment, anything not based on ``rayproject/ray`` — you don't rebuild it from
scratch. You **layer a self-contained CHIA interpreter on top**, leaving the
image's own Python and tools untouched. CHIA only needs Ray, the chia package,
and Python 3.10.19; keeping those in their own venv means they can't disturb
whatever Python your tool already depends on.

The pattern (the same one CHIA's ``chia-circt`` image uses,
since its base ships a different Python): install a relocatable Python 3.10.19,
make a dedicated venv for Ray + chia, and put it first on the ``PATH``.

.. code-block:: dockerfile

   FROM your-registry/your-tool-image:latest

   USER root
   # Add transport + download tools if the base lacks them.
   RUN apt-get update \
       && apt-get install -y --no-install-recommends curl ca-certificates openssh-client rsync \
       && rm -rf /var/lib/apt/lists/*

   # A relocatable Python 3.10.19 (glibc 2.17), independent of the image's own
   # Python. Pin these to match every other worker on the cluster.
   ARG PYTHON_VERSION=3.10.19
   ARG PBS_TAG=20260203
   RUN curl -fsSL \
         "https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_TAG}/cpython-${PYTHON_VERSION}+${PBS_TAG}-x86_64-unknown-linux-gnu-install_only.tar.gz" \
         | tar -xzf - -C /opt \
    && mkdir -p /home/ray/anaconda3/envs \
    && /opt/python/bin/python3 -m venv /home/ray/anaconda3/envs/py_worker \
    && /home/ray/anaconda3/envs/py_worker/bin/pip install --no-cache-dir \
         "ray[default]==2.54.0" pyyaml

   # Install the chia package into that venv. chia isn't on PyPI, so install from
   # your chia checkout (here copied in from the build context).
   COPY chia /tmp/chia
   RUN /home/ray/anaconda3/envs/py_worker/bin/pip install --no-cache-dir /tmp/chia \
       && rm -rf /tmp/chia

   # Put CHIA's interpreter first so `ray`, `python`, and `chia` resolve to it.
   ENV PATH="/home/ray/anaconda3/envs/py_worker/bin:${PATH}"
   ENV HOME=/home/ray
   WORKDIR /home/ray
   # Allow an arbitrary UID — chia up may launch with --user $(id -u):$(id -g).
   RUN mkdir -p /home/ray && chmod -R a+rwX /home/ray && chmod a+rw /etc/passwd

.. note::

   Prepending the venv to ``PATH`` makes ``python`` / ``pip`` resolve to CHIA's
   interpreter for the whole container. If your image's tools rely on *their* own
   ``python`` being first on the ``PATH``, either invoke them by absolute path, or
   skip the global ``ENV PATH`` and instead prepend
   ``/home/ray/anaconda3/envs/py_worker/bin`` only in the node type's
   ``worker_env_commands`` — that is enough for CHIA's ``ray start`` to find Ray.

The same steps on a bare ``ubuntu`` (or ``python``) base build a worker entirely
from scratch. ``dockerfiles/ChiaCirctBaseDockerfile`` is a complete worked example
of this pattern (CHIA layered onto a CIRCT toolchain on ``ubuntu:24.04``), and
``dockerfiles/SimWorkerMinimalDockerfile`` shows the smallest viable image.

Wiring the image into a cluster
-------------------------------

Reference the image from a node type's ``docker:`` block, and start Ray with the
cluster's ``worker_start_ray_commands``:

.. code-block:: yaml

   available_node_types:
       my_tool_worker:
           resources: {"my_tool": 1}            # what this worker advertises
           num_workers: 1
           compatible_ips: [${THIS_MACHINE}]    # required when num_workers > 0
           worker_setup_commands: ["source /opt/my-tool/env.sh"]
           docker:
               image: "ghcr.io/you/chia-my-tool:latest"
               container_name: "chia-my-tool-${USER}"
               pull_before_run: true            # set false for a locally-built image
               run_options:
                   - --shm-size=10.24gb

   worker_start_ray_commands:
       - ray stop
       - ray start --address=$RAY_HEAD_IP:6379

A few practical notes on the ``docker:`` block (full reference:
:doc:`/user_guides/cluster_config_reference`):

- **``pull_before_run``** — defaults to ``true``. Set it to ``false`` when the
  image was built locally and isn't in a registry, or CHIA's ``docker pull``
  will fail.
- **``run_options``** — flags spliced verbatim into ``docker run``. CHIA adds
  **no** volume mounts itself; add ``-v`` here. The SSH-agent convention is
  ``-v $SSH_AUTH_SOCK:/ssh-agent -e SSH_AUTH_SOCK=/ssh-agent`` (CHIA chmods
  ``/ssh-agent`` for you). ``--net=host`` and ``--shm-size=8g`` are always added
  by CHIA.
- **``resources``** — the labels this worker advertises; ``@ChiaFunction`` nodes
  and tools land here by requesting them (see :doc:`/user_guides/chia_function`).
- **``compatible_ips``** — the machines this worker type may run on; required for
  any type with workers.

Pitfalls
--------

- **Version mismatch.** A worker whose Ray or Python *minor* version differs from
  the head silently fails to join the cluster. Match the head's ``2.54.0`` /
  ``3.10.x`` exactly.
- **``~/.bashrc`` is not sourced.** Use ``ENV``/profile files or
  ``worker_env_commands`` to put tools on the ``PATH`` (see the note above).
- **No automatic mounts.** Anything the container needs from the host must be a
  ``-v`` entry in ``run_options``.
- **Local image + ``pull_before_run: true``.** CHIA will try to pull and fail;
  set ``pull_before_run: false`` for images that only exist locally.
- **Tool-server ports.** Because containers run with ``--net=host``, an MCP tool
  server binds a port directly on the host (probing from ``8000`` by default).
  Make sure that range is free on machines that host tool-serving workers.

See also
--------

- :doc:`/user_guides/cluster_config_reference` — the full ``docker:`` / node-type config schema.
- :doc:`/getting-started/quickstart` — brings up a cluster with CHIA's Chipyard and Verilator images.
- :doc:`/user_guides/chia_tool` — MCP tool servers, which run inside these worker containers.
- :doc:`/user_guides/chia_function` — how nodes request the ``resources`` a worker advertises.
