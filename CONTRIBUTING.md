Contributing to CHIA
=============================

### Branch management:

``main`` is the development branch and is write-protected (requires PR approval). Substantial code changes and feature additions must be based against this branch.

### Tests and docs

Contributions to ``chia`` should have tests in the corresponding ``chia/subfolder/tests`` folder. 
Contributions to ``chia`` that modify classes or user-facing functions (function names that do not start with an underscore _) should have docstrings describing arguments. 

New dockerfiles in ``dockerfiles`` should also have a corresponding ``.github/workflows`` action. 