<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/_static/chia-logo-inv.png">
    <img alt="CHIA" src="docs/_static/chia-logo.png" width="420"/>
  </picture>
</p>

<h3 align="center">CHIA: An open-source framework for principled, agentic AI-driven hardware/software co-design research</h3>

<p align="center">
  <a href="https://chialoops.ai"><b>Documentation</b></a> &nbsp;•&nbsp;
  <a href="https://arxiv.org/abs/2606.27350v1"><b>Paper</b></a> &nbsp;•&nbsp;
  <a href="https://chialoops.ai"><b>Website</b></a>
</p>

---

## What is CHIA?

CHIA is an open-source framework for agile and principled hardware design using AI agents. Even though many of the steps of the hardware design process can be accelerated by AI, existing research using AI for hardware has been limited to small studies on isolated examples because it is still too hard to assemble more complex experiments. CHIA solves this problem. CHIA centers the whole co-design **workflow**---all of the steps of doing the co-design (nodes) and the connections between the steps (edges)---as a first class consideration. CHIA abstracts these workflows as graphs so it is easy for a user to define them, and provides an efficient, feature rich runtime system to execute these workflows. CHIA let's you incorporate AI agents into a workflow with all of the tools you already use, and we even provide hooks (primarily in the form of CHIA nodes) for many of them!

See the [documentation](docs/getting-started/chia-basics.rst) to run your first flow!

## Installation

CHIA requires **Python 3.10.19** (matching the Python in the Docker images). With conda:

```bash
conda create -n chia_env python=3.10.19
conda activate chia_env
pip install -e /path/to/chia
```

## Learn more about CHIA

- **[CHIA Basics](docs/getting-started/chia-basics.rst)** — the core ideas, start here.
- **[Architecture Overview](docs/concepts/overview.rst)** — how CHIA works under the hood.

User guides:

- [ChiaFunction](docs/user_guides/chia_function.rst)
- [ChiaTool](docs/user_guides/chia_tool.rst)
- [Cluster Configuration Reference](docs/user_guides/cluster_config_reference.rst)
- [Building a CHIA-compatible Docker image](docs/user_guides/docker_images.rst)
- [Caching and Bypass](docs/user_guides/caching_and_bypass.rst)
- [Profiling](docs/user_guides/profiling.rst)

## Overview of CHIA and Early Case Studies

<p align="center">
  <img src="docs/_static/overviewfig.png" alt="CHIA overview" width="820"/>
</p>

## Attribution

If you use CHIA in your research, please cite our paper:

```bibtex
@misc{cui2026chiaopensourceframeworkprincipled,
      title={CHIA: An open-source framework for principled, agentic AI-driven hardware/software co-design research}, 
      author={Angela Cui and Ferran Hermida-Rivera and Jack Toubes and Raghav Gupta and Jim Fang and Chengyi Lux Zhang and Ella Schwarz and Junha Kim and Yakun Sophia Shao and Borivoje Nikolic and Christopher W. Fletcher and Sagar Karandikar},
      year={2026},
      eprint={2606.27350},
      archivePrefix={arXiv},
      primaryClass={cs.AR},
      url={https://arxiv.org/abs/2606.27350}, 
}
```
