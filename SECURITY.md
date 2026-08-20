# Security Policy

The Chia community takes security seriously. We appreciate your efforts to responsibly disclose your findings and will make every effort to acknowledge your contributions.

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues, discussions, or pull requests.**

If you believe you have found a security vulnerability in Chia, please report it to us privately. This allows us to assess the risk and prepare a fix before public disclosure.

### How to Report

Please report security vulnerabilities using GitHub's private vulnerability reporting feature:

1. Go to the [Security tab](project_repo/security) of the repository
2. Click "Report a vulnerability"
3. Fill out the vulnerability report form

### What to Include

To help us triage and respond to your report quickly, please include as much of the following information as possible:

- **Type of vulnerability**
- **Affected component(s)** (e.g., module, file, function)
- **Location of the vulnerability** (e.g., full paths of affected source files, tag/branch/commit, or direct URL)
- **Step-by-step instructions to reproduce the issue**
- **Proof-of-concept or exploit code** (if available)
- **Impact assessment** (what an attacker could achieve by exploiting this vulnerability)
- **Any special configuration required to reproduce the issue**
- **Your recommended fix** (if you have one)

### What to Expect

After you submit a report, you can expect the following:

| Timeline | Action |
|----------|--------|
| Within 48 hours | Acknowledgment of your report |
| Within 7 days | Initial assessment and severity determination |
| Within 14 days | Detailed response with remediation plan |
| Ongoing | Regular updates on progress (at least every 7 days) |

We will work with you to understand and validate the issue. Once validated, we will:

1. Develop and test a fix
2. Prepare a security advisory
3. Coordinate a disclosure timeline with you
4. Release the fix and publish the advisory
5. Credit you in the advisory (unless you prefer to remain anonymous)

## Supported Versions

The following versions of Chia are currently supported with security updates:

| Version | Supported |
|---------|-----------|
| main branch   | ✅ Yes    |

(We will update this table as we make releases of Chia)

We recommend always running the latest stable version to ensure you have the most recent security fixes.

## Security Update Policy

- **Critical vulnerabilities**: Patches released as soon as possible, typically within 48-72 hours of validation
- **High severity vulnerabilities**: Patches released within 7 days of validation
- **Medium severity vulnerabilities**: Patches included in the next scheduled release
- **Low severity vulnerabilities**: Patches included in a future release as prioritized by maintainers

## Disclosure Policy

We follow a coordinated disclosure process:

1. **Private disclosure**: The vulnerability is reported privately to the maintainers.
2. **Validation**: We validate and assess the severity of the vulnerability.
3. **Remediation**: We develop, test, and prepare a fix.
4. **Notification**: We notify affected users and downstream projects (if applicable).
5. **Public disclosure**: We publish a security advisory and release the fix.
6. **Credit**: We credit the reporter in the advisory (unless anonymity is requested).

We aim to complete this process within 90 days of the initial report.

### Embargo Policy

We request that you:

- Allow us reasonable time to investigate and address the vulnerability before any public disclosure
- Make a good faith effort to avoid privacy violations, data destruction, and service interruption
- Do not access or modify data that does not belong to you
- Do not exploit the vulnerability beyond what is necessary to demonstrate it

We will not pursue legal action against researchers who follow these guidelines.

## Security Best Practices

We recommend the following security best practices when using Chia:

- Always use the latest stable version
- Subscribe to security announcements [link to mailing list or notification channel]
- Review the [security advisories](https://github.com/ucb-bar/chia/security/advisories) regularly
- Follow the principle of least privilege when configuring access
- Keep dependencies up to date

## Security Advisories

Published security advisories are available at:

- [GitHub Security Advisories](https://github.com/ucb-bar/chia/security/advisories)
- [Chia mailing list](https://groups.google.com/g/chialoops)

To receive notifications about security advisories, you can:

- Watch the repository for security alerts
- Follow our announcements on the [Chia mailing list](https://groups.google.com/g/chialoops)

---

*This security policy is based on best practices from the [OpenSSF](https://openssf.org/) and is reviewed periodically by the TSC.*