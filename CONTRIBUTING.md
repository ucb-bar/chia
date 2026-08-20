# Contributing to CHIA

Thank you for your interest in contributing to Chia! We welcome contributions from the community and are grateful for your support.

This document provides guidelines and instructions for contributing.

## Table of Contents

- [AI-Assisted Contributions Policy](#ai-assisted-contributions-policy)
- [Reporting Security Vulnerabilities](#reporting-security-vulnerabilities)
- [Testing & Dockerfile Requirements](#testing--dockerfile-requirements)
- [Documentation](#documentation)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Features](#suggesting-features)
- [Getting Help](#getting-help)

## AI-Assisted Contributions Policy
You **MAY** use AI assistance for contributing to Chia, as long as you follow the principles described below.

**Accountability:** You MUST take the responsibility for your contribution. The contributor is always the author and is fully accountable for the entirety of these contributions.

**Transparency:** You MUST disclose the use of AI tools when the significant part of the contribution is taken from a tool without changes. 

**Contribution & Community Evaluation:** AI tools may be used to assist human reviewers by providing analysis and suggestions. You MUST NOT use AI as the sole or final arbiter in making a substantive or subjective judgment on a contribution.

## Reporting Security Vulnerabilities

**Please do not report security vulnerabilities through public GitHub issues.**

For information on how to report security vulnerabilities, please see our [SECURITY.md](SECURITY.md) file. We take security issues seriously and will respond promptly to your report.

## Testing & Dockerfile Requirements
Contributions to ``chia`` should have tests in the corresponding ``chia/subfolder/tests`` folder. 

New dockerfiles in ``dockerfiles`` should have a corresponding ``.github/workflows`` action. 

## Documentation

Good documentation is essential. Please update documentation under the `docs/` folder if your contributions affect the documented behavior. 

Contributions to ``chia`` that modify classes or user-facing functions (function names that do not start with an underscore _) should have docstrings describing arguments. 

## Reporting Bugs

Before reporting a bug, please:

1. Search existing [issues](https://github.com/ucb-bar/chia/issues) to see if the bug has already been reported.
2. Check if the bug has been fixed in the latest version.

<!-- When reporting a bug, please include:

- A clear and descriptive title
- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Environment details (OS, version, etc.)
- Any relevant logs or error messages
- Screenshots, if applicable -->

Use the bug report template when [opening a new issue](https://github.com/ucb-bar/chia/issues/new).

## Suggesting Features

We welcome feature suggestions! Before submitting a feature request:

1. Search existing [issues](https://github.com/ucb-bar/chia/issues) to see if the feature has already been requested.
2. Consider whether the feature aligns with the project's goals and scope.

<!-- When suggesting a feature, please include:

- A clear and descriptive title
- A detailed description of the proposed feature
- The problem it solves or the use case it addresses
- Any alternative solutions you've considered -->

Use the feature request template when [opening a new issue](https://github.com/ucb-bar/chia/issues/new).

## Getting Help

If you need help with your contribution:

- **Documentation**: Review the project [documentation](https://ucb.bar/chiadocs)
- **Issues**: Search or open an [issue](https://github.com/ucb-bar/chia/issues)
- **Mailing List**: Reach out on our [mailing list](https://groups.google.com/g/chialoops)

---

Thank you for contributing to Chia! Your efforts help make this project better for everyone.