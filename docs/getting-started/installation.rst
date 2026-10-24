Installation
============

CHIA can be installed easily using pip. We recommend using CHIA in a conda environment (see the `Miniconda install guide <https://www.anaconda.com/docs/getting-started/miniconda/main>`_). If you plan on using any of our provided docker containers, you should pin your conda environment to Python version 3.10.19, in order to match the python version used inside the Dockerized workers.

The easiest way to get that is a dedicated conda environment. We recommend naming the conda environment the same thing on all machines you plan on using CHIA for, as it makes the setup of CHIA clusters easier. We will assume throughout these docs that this environment is called ``chia_env``.

.. code-block:: bash

   conda create -n chia_env python=3.10.19
   conda activate chia_env

Then install CHIA from PyPI:

.. code-block:: bash

   pip install chialoops

This installs the ``chia`` package and its dependencies. Repeat these steps on every machine you plan on using CHIA on.

Installing for CHIA development
-------------------------------

If you are actively developing CHIA itself, install it from a clone of the repository in editable mode instead, so your changes take effect without reinstalling. Clone the CHIA repository onto any machines you plan on using:

.. code-block:: bash

   git clone https://github.com/ucb-bar/chia.git

Then install the package in editable mode from your clone (inside ``chia_env``):

.. code-block:: bash

   pip install -e /path/to/chia

.. note::
   Users in the ``ucb-bar`` organization should also clone the internal node library ``ucb-bar/chia-internal-nodes``. 
   Install ``ucb-bar/chia-internal-nodes`` in ``chia_env`` using ``pip install -e /path/to/chia-internal-nodes``.
   We encourage you to organize your project with the following structure:

   .. code-block:: bash

    your_repo
      └chia-internal-nodes
      └chia

Optional extras
---------------

.. list-table::
   :header-rows: 1

   * - Extra
     - Installs
     - Use for
   * - ``tensorboard``
     - ``tensorboardX``, ``tensorboard``
     - metrics logging
   * - ``wandb``
     - ``wandb``
     - metrics logging
   * - ``metrics``
     - both of the above
     - metrics logging
   * - ``postgres``
     - ``psycopg[binary]``
     - a Postgres-backed DatabaseNode

.. code-block:: bash

   # For one extra
   pip install "chialoops[metrics]"

   # For multiple extras
   pip install "chialoops[metrics,postgres]"

   # From a development clone
   pip install -e "/path/to/chia[metrics,postgres]"

Core dependencies
-----------------

Chia pins ``ray[default]==2.54.0`` along with, ``mcp``, ``pydantic``, ``fastapi``, ``boto3``, and the Google
GenAI / Vertex client libraries. See ``pyproject.toml`` for the full pinned set.

Next steps
----------

- :doc:`Quickstart </getting-started/quickstart>` — run the ``Hello World!`` example.
