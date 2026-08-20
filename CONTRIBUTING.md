# Contributing to CHIA

Thank you for your interest in contributing to Chia! We welcome contributions from the community and are grateful for your support.

This document provides guidelines and instructions for contributing.

## Table of Contents

- [AI-Assisted Contributions Policy]()
- [Reporting Security Vulnerabilities](#reporting-security-vulnerabilities)
- [Testing & Dockerfile Requirements](#testing--dockerfile-requirements)
- [Documentation](#documentation)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Features](#suggesting-features)
- [Review Process](#review-process)
- [Code of Conduct](#code-of-conduct)
- [Getting Help](#getting-help)

## AI-Assisted Contributions Policy
You **MAY** use AI assistance for contributing to Chia, as long as you follow the principles described below.

**Accountability:** You MUST take the responsibility for your contribution. Contributing to Chia means vouching for the quality, utility, and compliance of your submission. All contributions, whether from a human author or assisted by large language models (LLMs) or other generative AI tools, must meet the project's existing contribution guidelines. The contributor is always the author and is fully accountable for the entirety of these contributions.

**Transparency:** You MUST disclose the use of AI tools when the significant part of the contribution is taken from a tool without changes. You SHOULD disclose the other uses of AI tools, where it might be useful. Routine use of assistive tools for correcting grammar and spelling, or for clarifying language, does not require disclosure.

Information about the use of AI tools will help us evaluate their impact, build new best practices and adjust existing processes.

Disclosures are made where authorship is normally indicated. For contributions tracked in git, the recommended method is an Assisted-by: commit message trailer. For other contributions, disclosure may include document preambles, design file metadata, translation notes, or wiki page categories.

Examples:

`Assisted-by: generic LLM chatbot`

`Assisted-by: ChatGPTv5`

**Contribution & Community Evaluation:** AI tools may be used to assist human reviewers by providing analysis and suggestions. You MUST NOT use AI as the sole or final arbiter in making a substantive or subjective judgment on a contribution, nor may it be used to evaluate a person’s standing within the community (e.g., for funding, leadership roles, or Code of Conduct matters). This does not prohibit the use of automated tooling for objective technical validation, such as CI/CD pipelines, automated testing, or spam filtering. The final accountability for accepting a contribution, even if implemented by an automated system, always rests with the human contributor who authorizes the action.

**Interactions in PRs, Issues & with contributors:** Any discussions amost contributors, reviewers and collaborators in the Chia project MUST NOT be through an AI agent. Contributors cannot rely on AI to respond to review comments. If you cannot personally explain changes that AI helped generate, your PR will be closed. This requirement ensures that knowledge transfer happens and that contributors genuinely understand the code they're submitting.

**Exemptions:** Experienced/trusted contributors and members of the technical steering committee may be exempted from these guidelines. We define trusted contributors as those with direct WRITE permission to the Chia repository.

The key words “MAY”, “MUST”, “MUST NOT”, and “SHOULD” in this document are to be interpreted as described in RFC 2119.

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

## Review Process

After you submit a pull request:

<!-- 1. **Automated checks**: CI will run tests, linting, and other automated checks. -->
1. **Maintainer review**: A maintainer will review your changes, typically within [timeframe, e.g., "one week"].
2. **Feedback**: You may receive feedback or requests for changes. Please respond to comments and make requested updates.
3. **Approval**: Once approved, a maintainer will merge your pull request.
4. **Merge**: Your contribution will be included in the next release.

### Review Criteria

Reviewers will evaluate contributions based on:

- Code quality and adherence to coding standards
- Test coverage and quality
- Documentation completeness
- Alignment with project goals
- Security considerations
- Performance implications

## Code of Conduct

This project follows the [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to yf328@eecs.berkeley.edu.

## Getting Help

If you need help with your contribution:

- **Documentation**: Review the project [documentation](https://ucb.bar/chiadocs)
- **Issues**: Search or open an [issue](https://github.com/ucb-bar/chia/issues)
- **Mailing List**: Reach out on our [mailing list](https://groups.google.com/g/chialoops)

---

Thank you for contributing to Chia! Your efforts help make this project better for everyone.