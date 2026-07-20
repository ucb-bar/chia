Cluster Configuration Reference
===============================

A CHIA cluster is described by a single YAML file that you pass to ``chia up``
and ``chia down``. This page is a complete reference for every key CHIA reads,
a worked example that mixes on-premise and cloud machines, and a
walk-through of the exact order in which CHIA runs your commands when it brings
a cluster up and tears it down.

.. note::

   CHIA's YAML deliberately resembles the `Ray cluster launcher
   <https://docs.ray.io/en/latest/cluster/vms/references/ray-cluster-configuration.html>`_
   config so existing Ray configs feel familiar, but with additional support for heterogeneous on-premise setups as well as clusters split across on-premise and cloud providers. Chia currently does not support the following Ray autoscaler keys: ``min_workers`` / ``max_workers``, ``upscaling_speed``,
   ``idle_timeout_minutes``, ``cluster_synced_files``,
   ``file_mounts_sync_continuously``, ``provider.type``,
   ``provider.external_head_ip``, and ``provider.coordinator_address``.

Top-level structure
-------------------

At the top level a config is organized into a handful of sections:

.. code-block:: yaml

   cluster_name: MyCluster          # identifier for this cluster

   provider:                        # head machine (required)
       head_ip: ...

   auth:                            # how to SSH into the machines
       ssh_user: ...
       ssh_private_key: ...

   available_node_types:            # logical worker types + their resources
       my_worker:
           ...

   aws_nodes:                       # optional: provision EC2 instances
       ...
   gcp_nodes:                       # optional: provision GCP instances
       ...
   tunnel_defaults:                 # optional: tunnel/port tuning for cloud nodes
       ...

   # lifecycle command hooks (see "Command execution order" below)
   initialization_commands: [...]
   head_env_commands: [...]
   setup_commands: [...]
   head_setup_commands: [...]
   head_teardown_commands: [...]
   head_start_ray_commands: [...]
   worker_start_ray_commands: [...]

   # file syncing
   file_mounts: {...}
   rsync_exclude: [...]
   rsync_filter: [...]

Any ``${VAR}`` reference in a string value is expanded from your environment
when the config is loaded (e.g. ``${USER}``). A bare ``$VAR`` is left as-is so
it can be evaluated later on the remote shell.

Top-level keys
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Key
     - Default
     - Meaning
   * - ``cluster_name``
     - ``"default"``
     - Identifier for the head and workers of this cluster.
   * - ``provider``
     - *required*
     - Cluster head node. See `provider`_.
   * - ``auth``
     - ``{}``
     - SSH credentials for reaching the machines. See `auth`_.
   * - ``available_node_types``
     - ``{}``
     - Logical worker types, their Ray resources, and container images. See
       `available_node_types`_.
   * - ``initialization_commands``
     - ``[]``
     - Commands run first, each in its own SSH session, on the host (outside
       any container). They do not share an environment with each other or with
       later steps.
   * - ``setup_commands``
     - ``[]``
     - Global setup commands run inside the main script session on every nod (head and workers). Inside the container for containerized workers.
   * - ``head_env_commands``
     - ``[]``
     - Environment activation prepended to the head's main script on both ``chia up`` and ``chia down`` (e.g. ``source ~/.bashrc && conda activate chia_env``).
   * - ``head_setup_commands``
     - ``[]``
     - Head-only one-time setup, run during ``chia up`` in the head's main script.
   * - ``head_teardown_commands``
     - ``[]``
     - Head-only commands run during ``chia down`` before ``ray stop``.
   * - ``head_start_ray_commands``
     - ``[]``
     - Commands that start Ray on the head (typically ``ray stop`` then
       ``ray start --head ...``).
   * - ``worker_start_ray_commands``
     - ``[]``
     - Commands that start Ray on each worker. CHIA injects ``--resources`` (and,
       for tunneled cloud workers, pinned ports) automatically.
   * - ``file_mounts``
     - ``{}``
     - ``{remote_path: local_path}`` directories rsync'd to each node before the
       main script.
   * - ``rsync_exclude``
     - ``[]``
     - Patterns passed to rsync ``--exclude`` (e.g. ``**/.git``).
   * - ``rsync_filter``
     - ``[]``
     - Filter files (e.g. ``.gitignore``) passed to rsync ``--filter``.
   * - ``docker``
     - ``None``
     - A cluster-wide default container config (see `Container config`_), which
       individual node types can override. Specify at most one of the two.
   * - ``aws_nodes``
     - ``None``
     - Provision EC2 instances and tunnel them into the cluster. See `Cloud nodes`_.
   * - ``gcp_nodes``
     - ``None``
     - Provision GCP Compute Engine instances. See `Cloud nodes`_.
   * - ``tunnel_defaults``
     - ``None``
     - Tunnel/port-pinning defaults applied to every auto-tunneled cloud node.
       See `Cloud nodes`_.

provider
--------

The ``provider`` section declares the head machine.

.. code-block:: yaml

   provider:
       head_ip: ${HEAD_IP}

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Key
     - Default
     - Meaning
   * - ``head_ip``
     - *required*
     - Hostname or IP of the machine that manages the cluster (runs the Ray head).

auth
----

The ``auth`` section gives the SSH credentials CHIA uses to reach every machine,
with optional per-host overrides.

.. code-block:: yaml

   auth:
       ssh_user: ${USER}
       ssh_private_key: /home/${USER}/.ssh/${USER}   # omit if your key is in ssh-agent
       overrides:
           some-host:
               ssh_user: ubuntu
               ssh_private_key: ~/.ssh/other_key

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Key
     - Default
     - Meaning
   * - ``ssh_user``
     - ``""``
     - Default SSH username for all machines.
   * - ``ssh_private_key``
     - ``None``
     - Default private key path. Omit it if the relevant keys are already loaded
       into your SSH agent.
   * - ``overrides``
     - ``{}``
     - Per-IP overrides, keyed by hostname/IP (or a ``@node_type:index``
       placeholder). Each entry may set ``ssh_user``, ``ssh_private_key``, and a
       ``tunnel`` block (see `Cloud nodes`_). CHIA populates tunnel overrides for
       cloud nodes automatically.

available_node_types
--------------------

Each entry under ``available_node_types`` defines a *logical worker type*: the
Ray resources it advertises, how many of them to run, where they may run, and
the container (if any) they run in.

.. code-block:: yaml

   available_node_types:
       verilator_run:
           resources: {"verilator_run": 8}
           num_workers: 4
           compatible_ips: [machine9, machine10, machine11, machine12]
           worker_env_commands: ["source ~/.bashrc && conda activate chia_env"]
           docker:
               image: "ghcr.io/ucb-bar/chia-verilator-run:latest"
               container_name: "chia-verilator-run-${USER}"
               run_options:
                   - --ulimit nofile=65536:65536
                   - --shm-size=10.24gb

.. list-table::
   :header-rows: 1
   :widths: 22 13 65

   * - Key
     - Default
     - Meaning
   * - ``resources``
     - ``{}``
     - Custom Ray resources advertised by each worker of this type, e.g.
       ``{"verilator_run": 8}``. A ``@ChiaFunction`` requesting these resources is
       scheduled onto a matching worker, consuming the amount it requests.
   * - ``num_workers``
     - ``1``
     - How many workers of this type to launch. For Ray-config familiarity, if
       ``num_workers`` is absent CHIA falls back to ``max_workers``, then
       ``min_workers``, then ``1``.
   * - ``max_workers``
     - *optional*
     - Ray-config alias for ``num_workers``, used only when ``num_workers`` is
       absent (and it takes precedence over ``min_workers``). CHIA does not
       autoscale — this sets a single fixed worker count, not an upper bound.
   * - ``min_workers``
     - *optional*
     - Ray-config alias for ``num_workers``, used only when both
       ``num_workers`` and ``max_workers`` are absent. Not a lower bound; if you
       set ``min_workers`` and ``max_workers`` to different values, ``min_workers``
       is ignored and ``max_workers`` wins.
   * - ``compatible_ips``
     - *required if* ``num_workers > 0``
     - The machines this type's workers may run on. Accepts ``@node_type:index``
       placeholders.
   * - ``worker_env_commands``
     - ``[]``
     - Per-type environment activation prepended to the worker's main script on
       both ``chia up`` and ``chia down``. Runs inside the container for
       containerized types.
   * - ``worker_setup_commands``
     - ``[]``
     - Per-type one-time setup run during ``chia up`` in the worker's main script.
   * - ``docker``
     - ``None``
     - Container config for this type, overriding any cluster-wide default.
       Specify at most one. See `Container config`_.
   * - ``balance_level``
     - ``"cluster"``
     - How this type spreads across its eligible IPs: ``cluster`` packs around
       whatever other types already placed (fewest nodes globally); ``worker``
       distributes this type's own workers as evenly as possible across its IP
       pool, regardless of if the IP may be shared with another type.

Container config
~~~~~~~~~~~~~~~~

A ``docker:`` block may appear cluster-wide at the top level or
inside any node type; the node-type block overrides the cluster-wide one. 

.. code-block:: yaml

   docker:
       image: "ghcr.io/ucb-bar/chia-verilator-run:latest"
       container_name: "chia-verilator-run-${USER}"
       pull_before_run: False
       pull_timeout: 3600
       run_options:
           - --ulimit nofile=65536:65536
           - --shm-size=10.24gb
           - "-v $SSH_AUTH_SOCK:/ssh-agent"
       run_setup_commands:
           - cd /home/ray/ && git pull

.. list-table::
   :header-rows: 1
   :widths: 25 18 57

   * - Key
     - Default
     - Meaning
   * - ``image``
     - *required*
     - Container image URI
   * - ``container_name``
     - ``"chia_container"``
     - Base container name. CHIA appends the worker index (``-0``, ``-1``, …) so
       multiple workers of a type don't collide. Include ``${USER}`` on shared
       machines.
   * - ``pull_before_run``
     - ``True``
     - Pull the image before running. Set ``False`` to use a cached image.
   * - ``pull_timeout``
     - ``600``
     - Seconds to allow for the pull. Raise it for large images.
   * - ``run_options``
     - ``[]``
     - Extra flags passed to ``docker run`` (ulimits, shm size,
       volume mounts, ``--user``, env vars, …).
   * - ``run_setup_commands``
     - ``[]``
     - Commands run inside the container after it starts, before the worker's
       main script (e.g. clone/pull a repo, fix up ``/etc/passwd``).

.. note::

   Scripts run over SSH as a **non-interactive login shell** (``bash --login``):
   ``/etc/profile`` and ``~/.bash_profile`` are sourced, but ``~/.bashrc`` is
   not (and many ``~/.bashrc`` files bail out early for non-interactive shells).
   If you rely on conda/venv set up in ``~/.bashrc``, source it explicitly in
   ``head_env_commands`` / ``worker_env_commands``, e.g.
   ``source ~/.bashrc && conda activate chia_env``.

Cloud nodes
-----------

CHIA can provision public-cloud machines and reverse-tunnels them to the head. Declare them under ``aws_nodes`` (EC2) and/or
``gcp_nodes`` (Compute Engine). Everything downstream of provisioning —
placeholder expansion, tunnel injection, and the tunnels themselves — is
provider-agnostic.

aws_nodes
~~~~~~~~~

.. code-block:: yaml

   aws_nodes:
       region: us-east-1
       verilator_run_aws:
           KeyName: my-keypair          # an EC2 key pair in your account
           InstanceType: c5.9xlarge
           count: 3
           ImageId: ami-0ec10929233384c7f
           ssh_user: ubuntu
           ssh_private_key: /home/${USER}/my-keypair.pem
           setup_commands:
               - "echo ${GITHUB_TOKEN} | docker login ghcr.io -u myuser --password-stdin"
           BlockDeviceMappings:         # passed through to EC2 RunInstances
               - DeviceName: /dev/sda1
                 Ebs:
                     VolumeSize: 500
                     VolumeType: gp3

``region`` is a section-level key (default ``us-west-2``). Every other key lives
under a named node type:

.. list-table::
   :header-rows: 1
   :widths: 22 18 60

   * - Key
     - Default
     - Meaning
   * - ``KeyName``
     - *required*
     - EC2 key pair name (must already exist in the account).
   * - ``InstanceType``
     - *required*
     - EC2 instance type (e.g. ``c5.9xlarge``).
   * - ``count``
     - *required*
     - Number of instances to launch for this type.
   * - ``ImageId``
     - Ubuntu 22.04 AMI
     - AMI to launch.
   * - ``ssh_user``
     - ``None``
     - SSH user for the AMI (e.g. ``ubuntu``). Injected into the auth override
       for each provisioned IP.
   * - ``ssh_private_key``
     - ``None``
     - Private key path matching ``KeyName``. Injected into the auth override.
   * - ``skip_default_setup``
     - ``False``
     - Skip CHIA's default setup (git/conda/docker install) and run only your
       ``setup_commands``.
   * - ``setup_commands``
     - ``[]``
     - Commands run on the EC2 host before it joins the cluster (appended to the
       defaults unless skipped).
   * - ``setup_timeout``
     - ``1800``
     - Seconds allowed for setup.
   * - ``ssh_timeout``
     - ``120``
     - Seconds to wait for SSH to come up.
   * - *(anything else)*
     -
     - Unknown keys (e.g. ``BlockDeviceMappings``, ``UserData``) are passed
       straight through to the EC2 ``RunInstances`` call.

**AWS API access (always required).** CHIA picks up credentials by default from ``~/.aws/credentials``
/ ``~/.aws/config``. Override these paths with the environment variables ``AWS_CONFIG_FILE=/path/to/aws/config`` and ``AWS_SHARED_CREDENTIALS_FILE=/path/to/aws/credentials`` before calling ``chia up``. Instances launch into the account's default VPC.

gcp_nodes
~~~~~~~~~

``gcp_nodes`` is the Compute Engine analog of ``aws_nodes``.

.. code-block:: yaml

   gcp_nodes:
       project: your-gcp-project        # required
       zone: us-central1-a              # default
       gcp_worker:
           machine_type: n1-standard-1
           count: 2
           ssh_user: chia                          # local user created on the VM
           ssh_public_key: ${HOME}/.ssh/id_ed25519.pub
           ssh_private_key: ${HOME}/.ssh/id_ed25519
           spot: true
           disk_size_gb: 100

``project`` (required), ``zone`` (default ``us-central1-a``), and ``network`` /
``subnetwork`` (default to the project's ``default`` VPC) are section-level keys.
Every other key lives under a named node type:

.. list-table::
   :header-rows: 1
   :widths: 22 18 60

   * - Key
     - Default
     - Meaning
   * - ``machine_type``
     - *required*
     - GCE machine type (e.g. ``n1-standard-1``).
   * - ``count``
     - *required*
     - Number of instances to launch for this type.
   * - ``image``
     - Ubuntu image
     - Boot image (family or full image URL).
   * - ``zone``
     - section ``zone``
     - Per-type zone override.
   * - ``disk_size_gb``
     - image default
     - Boot disk size in GB.
   * - ``spot``
     - ``False``
     - Launch as Spot/preemptible VMs (cheaper, can be reclaimed).
   * - ``ssh_user``
     - ``None``
     - Login user CHIA connects as (see Authentication below).
   * - ``ssh_private_key``
     - ``None``
     - Private key path CHIA's SSH client uses. Recommended (otherwise it falls
       back to your ssh-agent / ``~/.ssh`` defaults).
   * - ``ssh_public_key``
     - ``None``
     - Public key injected into the VM (metadata method).
   * - ``use_os_login``
     - ``False``
     - Use OS Login instead of metadata SSH keys (see Authentication below).
   * - ``skip_default_setup``
     - ``False``
     - Skip CHIA's default host setup (git/conda/docker) and run only your
       ``setup_commands``.
   * - ``setup_commands``
     - ``[]``
     - Commands run on the host before it joins the cluster (appended to the
       defaults unless skipped).
   * - ``setup_timeout``
     - ``1800``
     - Seconds allowed for setup.
   * - ``ssh_timeout``
     - ``120``
     - Seconds to wait for SSH to come up.
   * - *(anything else)*
     -
     - Merged into the instance definition sent to the Compute API.

**Authentication.** A GCP bring-up uses two distinct credentials at two layers:

* **GCP API access (always required).** Set it up once with ``gcloud auth application-default login``
  (or point ``GOOGLE_APPLICATION_CREDENTIALS`` at a service-account JSON).
  A ``default`` VPC network must already exist (or set ``network``).
* **SSH into the instance**, chosen per node type by ``use_os_login``:

  * **Metadata SSH keys** (default). CHIA connects to the GCP instance as ``ssh_user`` with the matching private
    key to the public key ``ssh_public_key``.
    This is an ordinary keypair (the GCP analog of an AWS ``KeyName``), not tied
    to any Google identity, and it is silently ignored if the project or org
    enforces OS Login.
  * **OS Login** (``use_os_login: true``). Ties access to a GCP identity via IAM. You
    must register your ssh key manually (``gcloud compute os-login ssh-keys add``) and set
    ``ssh_user`` to the derived posix username. Use this when your org enforces OS Login.

Referencing cloud nodes (``@`` placeholders)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because cloud IPs aren't known until provisioning, you refer to them by
placeholder of the form ``@<node_type>:<index>``, where ``<node_type>`` is a key
under ``aws_nodes`` / ``gcp_nodes`` and ``<index>`` is 0-based. Placeholders are
valid anywhere an IP is — in a node type's ``compatible_ips`` and in
``auth.overrides`` keys:

.. code-block:: yaml

   available_node_types:
       verilator_run_aws:
           resources: {"verilator_run": 32}
           num_workers: 2
           compatible_ips: ["@verilator_run_aws:0", "@verilator_run_aws:1"]
           docker: {...}

tunnel_defaults
~~~~~~~~~~~~~~~

For every cloud IP, CHIA automatically adds an ``auth.overrides`` entry with a
tunnel (a per-IP ``auth.overrides[ip].tunnel`` you set yourself still wins).
``tunnel_defaults`` overrides the default
ports/behavior for *all* auto-tunneled nodes. It accepts any tunnel field except
``tunnel_ip`` (which CHIA assigns per-worker). Common ones:

.. code-block:: yaml

   tunnel_defaults:
       ray_worker_port_min: 20000
       ray_worker_port_max: 20001
       head_worker_port_min: 21000
       head_worker_port_max: 21001

Other tunnel fields (with defaults) include ``gcs_tunnel_port`` (16379),
``ray_node_manager_port`` (16800), ``ray_object_manager_port`` (16801),
``tool_port_min``/``max`` (18000/18010), ``head_tool_port_min``/``max``
(8000/8010), ``head_node_manager_port`` (29800), ``head_object_manager_port``
(29801), ``kill_orphaned_tunnels`` (true), and ``pre_tunnel_commands`` (sshd
``GatewayPorts`` + file-limit setup, run once per physical cloud IP). A typo in
any field name fails loudly at load time.

A mixed on-prem + cloud example
-------------------------------

This config keeps the head and several worker types on owned machines 
while bursting Verilator simulation onto AWS. To run purely on-prem,
delete the ``aws_nodes`` section and the ``@verilator_run_aws:*`` placeholders;
to add more cloud capacity, raise ``count`` and add matching placeholders.

.. code-block:: yaml

   cluster_name: ChiaClusterExample

   available_node_types:

       # On-prem Verilator workers, pinned to specific machines.
       verilator_run:
           resources: {"verilator_run": 8}
           num_workers: 4
           compatible_ips: [machine0, machine1, machine2, machine2]
           worker_env_commands: ["source ~/.bashrc && conda activate chia_env"]
           docker:
               image: "ghcr.io/ucb-bar/chia-verilator-run:latest"
               container_name: "chia-verilator-run-${USER}"
               run_options:
                   - --ulimit nofile=65536:65536
                   - --shm-size=10.24gb

       # Cloud Verilator workers — provisioned by the aws_nodes block below.
       verilator_run_aws:
           resources: {"verilator_run": 32}
           num_workers: 3
           compatible_ips:
               - "@verilator_run_aws:0"
               - "@verilator_run_aws:1"
               - "@verilator_run_aws:2"
           docker:
               image: "ghcr.io/ucb-bar/chia-verilator-run:latest"
               container_name: "chia-verilator-run-${USER}"
               pull_before_run: True
               run_options:
                   - --ulimit nofile=65536:65536
                   - --shm-size=10.24gb

       # On-prem VLSI workers (no container; uses the host conda env).
       vlsi:
           resources: {"VLSI": 1, "syn": 1, "cacti": 4}
           num_workers: 6
           compatible_ips: [machine1, machine2, machine3, machine4, machine5, machine6]
           worker_env_commands:
               - "source ~/.bashrc && source /ecad/tools/vlsi.bashrc && conda activate chia_env"

   # Provision the cloud half of the cluster.
   aws_nodes:
       region: us-east-1
       verilator_run_aws:
           KeyName: my-keypair
           InstanceType: c5.9xlarge
           count: 3
           ImageId: ami-0ec10929233384c7f
           ssh_user: ubuntu
           ssh_private_key: /home/${USER}/my-keypair.pem
           setup_commands:
               - "echo ${GITHUB_TOKEN} | docker login ghcr.io -u myuser --password-stdin"
           BlockDeviceMappings:
               - DeviceName: /dev/sda1
                 Ebs:
                     VolumeSize: 500
                     VolumeType: gp3

   # Pin the tunnel ports for the cloud nodes.
   tunnel_defaults:
       ray_worker_port_min: 20000
       ray_worker_port_max: 20001
       head_worker_port_min: 21000
       head_worker_port_max: 21001

   provider:
       type: local
       head_ip: machine7
       # No worker_ips: the worker pool is the union of every node type's
       # compatible_ips below (on-prem hosts + the cloud @-placeholders).

   auth:
       ssh_user: ${USER}
       ssh_private_key: /home/${USER}/.ssh/${USER}

   head_env_commands: ["source ~/.bashrc && conda activate chia_env"]

   head_start_ray_commands:
       - ray stop
       - ray start --head --port=6379 --include-dashboard=True --dashboard-agent-listen-port=0

   worker_start_ray_commands:
       - ray stop
       - ray start --address=$RAY_HEAD_IP:6379 --dashboard-agent-listen-port=0

Multiple heads on a single physical machine
-------------------------------------------

On shared lab machines it is common for two users (or two clusters) to want a
Ray *head* on the same host. A head binds several fixed TCP ports, so the
second cluster must move every one of them off the defaults or ``ray start``
(or the first cluster) will fail. Four ports matter — the GCS port (default
6379), the dashboard (8265), the Ray client server (10001), and the head's
dashboard agent (52365) — and the worker join address must follow the new GCS
port. Pick replacements that are free on the host, and note that an
explicitly assigned port must **not** fall inside Ray's worker-port range
(10002–19999 by default): ``ray start`` rejects the overlap, which is why the
client-server port below jumps to 20101 rather than 10101.

.. code-block:: yaml

   head_start_ray_commands:
       - ray stop
       - ray start --head --port=6479 --include-dashboard=True
         --dashboard-port=8365 --ray-client-server-port=20101
         --dashboard-agent-listen-port=52465

   worker_start_ray_commands:
       - ray stop
       - ray start --address=$RAY_HEAD_IP:6479 --dashboard-agent-listen-port=0

Nothing else needs to move: worker nodes already use
``--dashboard-agent-listen-port=0`` (OS-assigned) in the examples above, and
the remaining head ports (object manager, node manager, metrics export, …)
are randomized by default. Containerized workers coexist regardless. ``ray
stop`` is also safe on a shared host — it can only signal processes owned by
the invoking user, so it never touches the other cluster.

Two operational consequences of non-default ports:

* **Drivers must be pinned to the cluster address** — connect with
  ``ray.init(address="<head_ip>:6479")`` (or the ``RAY_ADDRESS`` environment
  variable), never ``address="auto"``: with several Ray instances alive on
  one machine, auto-discovery picks one arbitrarily, and it may be the other
  user's cluster.
* **Job submission must name the dashboard** — ``chia job submit --address
  http://127.0.0.1:8365 ...`` (the dashboard listens on localhost on the
  head, so submit from the head or tunnel to it).

Command execution order
-----------------------

When you run ``chia up``, CHIA sets up the head node, assigns each declared
worker to a machine (constrained ``compatible_ips`` types first, then
unconstrained — see ``assign_nodes`` in ``chia/cluster/config.py``), establishes
SSH tunnels for any cloud nodes, and then sets up the workers (in parallel
across machines, sequentially within a machine). 

``chia up`` — head node
~~~~~~~~~~~~~~~~~~~~~~~~

All head commands run on the host over SSH (the head node is never containerized):

.. code-block:: text

   1. initialization_commands        ← each in its own SSH session
   2. file_mounts rsync              ← separate rsync processes
   3. ┌─── single SSH session (env persists) ───┐
      │ head_env_commands                       │  e.g. conda activate
      │ setup_commands                          │  global
      │ head_setup_commands                     │
      │ head_start_ray_commands                 │  ray stop; ray start --head
      └─────────────────────────────────────────┘

``chia up`` — worker node
~~~~~~~~~~~~~~~~~~~~~~~~~~

Without a container, everything runs on the host:

.. code-block:: text

   1. initialization_commands        ← each in its own SSH session
   2. file_mounts rsync              ← separate rsync processes
   3. ┌─── single SSH session (env persists) ───────────────┐
      │ <type>.worker_env_commands                          │  per node type
      │ setup_commands                                      │  global
      │ <type>.worker_setup_commands                        │  per node type
      │ export RAY_HEAD_IP=...                              │
      │ worker_start_ray_commands  (--resources injected)   │
      └─────────────────────────────────────────────────────┘

With a container, the host pulls/starts the container first, then the main
script runs **inside** it:

.. code-block:: text

   1. initialization_commands        ← HOST, each in its own SSH session
   2. file_mounts rsync              ← HOST, separate rsync processes
   3. docker setup            ← HOST (pull, run, run_setup_commands inside)
   4. ┌─── single session INSIDE CONTAINER (env persists) ────┐
      │ <type>.worker_env_commands                            │
      │ setup_commands                                        │
      │ <type>.worker_setup_commands                          │
      │ export RAY_HEAD_IP=...                                │
      │ worker_start_ray_commands  (--resources injected)     │
      └───────────────────────────────────────────────────────┘

For cloud workers, CHIA additionally runs ``pre_tunnel_commands`` once per
physical cloud IP and brings up the reverse SSH tunnel before the worker's main
script, and pins the Ray ports in ``worker_start_ray_commands``.

``chia down``
~~~~~~~~~~~~~

Workers are torn down first (in parallel), then the head:

.. code-block:: text

   Workers (with container):              Workers (no container):
   1. docker exec:                        1. ┌─ single SSH session ───────┐
        <type>.worker_env_commands           │ <type>.worker_env_commands │
        ray stop                             │ ray stop                   │
   2. docker stop <container>                └────────────────────────────┘
   3. docker rm -f <container>

   Head (after all workers):
   ┌─── single SSH session ────┐
   │ head_env_commands         │
   │ head_teardown_commands    │
   │ ray stop                  │
   └───────────────────────────┘

.. note::

   ``head_env_commands`` and the per-type ``worker_env_commands`` run on **both**
   ``chia up`` and ``chia down`` — they are for environment activation. Use the
   ``*_setup_commands`` hooks for one-time setup. When ``head_ip`` is also listed
   in a node type's ``compatible_ips`` (so the head also hosts a worker) and that
   worker isn't containerized, CHIA skips ``ray stop`` on the worker so it doesn't
   kill the head's Ray process.
