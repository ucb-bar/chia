Installation
============

CHIA can be installed easily using pip. We recommend using CHIA in a conda environment (see the `Miniconda install guide <https://www.anaconda.com/docs/getting-started/miniconda/main>`_). If you plan on using any of our provided docker containers, you should pin your conda environment to Python version 3.10.19, in order to match the python version used inside the Dockerized workers.

The easiest way to get that is a dedicated conda environment. We recommend naming the conda environment the same thing on all machines you plan on using CHIA for, as it makes the setup of CHIA clusters easier. We will assume throughout these docs that this environment is called ``chia_env``.

.. code-block:: bash

   conda create -n chia_env python=3.10.19
   conda activate chia_env

Clone the CHIA repository onto any machines you plan on using

.. code-block:: bash

   git clone https://github.com/ucb-bar/chia.git

Then install the package in editable mode from your clone:

.. code-block:: bash

   pip install -e /path/to/chia

This installs the ``chia`` package and it's dependencies. We plan on releasing a PyPI CHIA release in the future.

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
   pip install -e "/path/to/chia[metrics]"
   
   # For multiple extras
   pip install -e "/path/to/chia[metrics,postgres]"

Core dependencies
-----------------

Chia pins ``ray[default]==2.54.0`` along with, ``mcp``, ``pydantic``, ``fastapi``, ``boto3``, and the Google
GenAI / Vertex client libraries. See ``pyproject.toml`` for the full pinned set.

Next steps
----------

- :doc:`Quickstart </getting-started/quickstart>` — run the ``Hello World!`` example.
