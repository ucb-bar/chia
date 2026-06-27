# -*- coding: utf-8 -*-
#
# Configuration file for the Sphinx documentation builder.
# Modeled on the FireSim docs setup (https://github.com/firesim/firesim).
# Full option list: https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import subprocess
import sys
import time

from sphinx.util import logging
logger = logging.getLogger(__name__)

# Make the `chia` package importable so autodoc can pull in docstrings.
# conf.py lives in docs/, the package root is one level up.
sys.path.insert(0, os.path.abspath('..'))

# -- Project information -----------------------------------------------------

project = u'CHIA'
this_year = time.strftime("%Y")
copyright = u'2025-' + this_year + ', Berkeley Architecture Research'
author = u'Berkeley Architecture Research'

on_rtd = os.environ.get("READTHEDOCS") == "True"
on_gha = os.environ.get("GITHUB_ACTIONS") == "true"


def get_git_branch_name():
    # When running locally, set the version to the current branch name so that
    # links into the repo can be generated against it.
    try:
        process = subprocess.Popen(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], stdout=subprocess.PIPE)
        branchname = process.communicate()[0].decode("utf-8").strip()
        if process.returncode == 0:
            return branchname
    except Exception:
        pass
    return None


# The RTD version can be 'latest' (the main branch), a tag ('stable'), or a
# branch name. When building locally or on CI, fall back to the git branch.
if on_rtd:
    rtd_version = os.environ.get("READTHEDOCS_VERSION")
    version = get_git_branch_name() if rtd_version == "latest" else rtd_version
    version = version or "latest"
else:
    version = get_git_branch_name() or "latest"

release = version
logger.info(f"Setting |version| to {version}.")

# -- General configuration ---------------------------------------------------

extensions = [
    'sphinx.ext.autodoc',       # pull docstrings from chia.* modules
    'sphinx.ext.napoleon',      # parse Google/NumPy-style docstrings
    'sphinx.ext.viewcode',      # add "[source]" links next to entries
    'sphinx_tabs.tabs',
    'sphinx_copybutton',
    'sphinx_substitution_extensions',
]

# -- Autodoc / Napoleon ------------------------------------------------------

autodoc_default_options = {
    'members': True,
    # Don't emit undocumented members. Dataclass fields are described in each
    # class's Napoleon ``Attributes:`` docstring section; with undoc-members on,
    # autodoc additionally lists every annotated field as a bare member.
    'show-inheritance': True,
}
# Document members in source order rather than alphabetically.
autodoc_member_order = 'bysource'
# Render both the class docstring and the __init__ docstring. Classes like
# ChiselBuildNode document their constructor args in __init__, which the
# default 'class' setting would drop.
autoclass_content = 'both'
# Don't fail the build if an optional runtime import is unavailable; mock it.
autodoc_mock_imports = []
napoleon_google_docstring = True
napoleon_numpy_docstring = True

# Several model backends (chia.models.*) each
# define their own identically-named exception classes (RateLimitError,
# AuthenticationError, ...). Napoleon `Raises:` sections turn those names into
# cross-references that can't resolve to a single target, which is expected
# here rather than a documentation bug. Silence just that ambiguity so it
# doesn't trip the -W / fail_on_warning build.
suppress_warnings = ['ref.python']

templates_path = ['_templates']
source_suffix = ['.rst']
master_doc = 'index'
language = 'en'
exclude_patterns = [u'_build', 'Thumbs.db', '.DS_Store']
pygments_style = 'sphinx'

# -- Options for HTML output -------------------------------------------------

html_theme = 'sphinx_rtd_theme'
html_theme_options = {
    'collapse_navigation': False,
    'navigation_depth': 4,
}
html_logo = 'chia-logo-inv.png'
html_static_path = ['_static']
html_css_files = ['custom.css']
html_context = {"version": version}

# -- Options for LaTeX (PDF) output ------------------------------------------

latex_documents = [
    (master_doc, 'CHIA.tex', u'CHIA Documentation', author, 'manual'),
]
