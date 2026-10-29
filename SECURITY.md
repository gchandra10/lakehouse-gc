# Security Policy

## Supported versions

Security fixes go into the latest release. Please upgrade to the newest version from
[Releases](https://github.com/gchandra10/lakehouse-gc/releases).

## Reporting a vulnerability

Please report vulnerabilities **privately**, not in public issues. On the repository's
**Security** tab, choose **Report a vulnerability**. You will receive an acknowledgement
and can follow the fix there.

## What the project does to stay secure

- **Read-only by design.** The package only runs `SELECT`, `SHOW CATALOGS`, and
  `DESCRIBE DETAIL`. Policies cannot enable deletes, pauses, or any other changes, and
  tests guard against network, SDK, and process-spawning imports.
- **Minimal dependencies.** One runtime dependency: [PyYAML](https://pypi.org/project/PyYAML/).
- **Dependency audits.** [`pip-audit`](https://github.com/pypa/pip-audit), maintained by the
  Python Packaging Authority, checks every locked dependency (exact versions with hashes)
  against the [OSV](https://osv.dev) database, which includes the PyPA advisory database
  and GitHub security advisories. The audit runs:
  - weekly (Mondays). A failure opens an issue for the maintainer to decide on a fix.
  - whenever dependencies change.
  - before every release. A release is not published while a known vulnerability exists.
- **Pinned build tooling.** CI actions are pinned to commit SHAs, and uv is pinned by
  version and checksum.

## Verifying a release

Each GitHub Release, from 0.2.2 on, includes the wheel, the sdist, `SHA256SUMS.txt`, a
CycloneDX SBOM (`*.sbom.cdx.json`) for your own security scanners, and a signed build
provenance attestation.

```bash
# The files are exactly what the release published:
sha256sum -c SHA256SUMS.txt

# The wheel was built by this repository's release workflow (requires the GitHub CLI):
gh attestation verify lakehouse_gc-0.2.2-py3-none-any.whl --repo gchandra10/lakehouse-gc
```
