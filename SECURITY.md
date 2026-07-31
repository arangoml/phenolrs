# Security policy

phenolrs is a Rust library with Python bindings, published to PyPI as wheels
for Linux x86_64 and arm64, macOS x86_64 and arm64, and Windows x86_64. It
ships no service, no container image and no Helm chart, so this document is
about the dependency graph that ends up inside a wheel and about the code that
builds it.

## Reporting a vulnerability

Report privately, not in a public issue.

- GitHub private vulnerability reporting on this repository (Security tab,
  "Report a vulnerability"). This is the preferred channel; it needs no inbox
  and costs nothing.
- Failing that, contact the security owner listed in `.github/CODEOWNERS`.

What to expect:

| Stage | Target |
| --- | --- |
| Acknowledgement | 3 business days |
| Initial assessment with a severity | 10 business days |
| Fix or a dated remediation plan | per the severity windows below |
| Public disclosure | coordinated, and by default 90 days from acknowledgement or on the fix release, whichever is first |

Please do not run automated scanners against ArangoDB infrastructure as part of
a report. A reproducer against a local instance started with
`docker/start_db.sh` is enough.

## Supported versions

| Version | Supported | Notes |
| --- | --- | --- |
| 0.5.x | Yes | current line, released from `main` |
| < 0.5 | No | no backports; upgrade |

There is one supported line, so there are no maintenance branches to rescan
and the nightly runs on `main` only.

## Gate policy: two tiers

Every scan in `.circleci/config.yml` sits in one of two tiers, and the
distinction is deliberate rather than incidental.

**Blocking tier.** Fails the job and, once the ruleset exists, blocks the
merge. Calibrated to industry standard and no stricter:

| Gate | Scope | Band |
| --- | --- | --- |
| `dependency-cve-scan` | `Cargo.lock` | fixable CRITICAL and HIGH only, `ignore-unfixed` on |
| `sast-scan` | first-party Rust and Python, full repo | Semgrep ERROR |
| `sast-scan-diff` | newly introduced findings only, against `main` | Semgrep WARNING |
| `disposition-check` | waiver and suppression integrity | any undated, expired, over-window or unattributed entry |
| credential guard in `dockerfile-scan` | the three builder Dockerfiles | any credential-shaped `ARG` or `ENV` in a final stage |
| `nightly-dependency-scan` pass 1 | `Cargo.lock` | fixable CRITICAL and HIGH only |

**Report tier.** Publishes findings to the Tests tab and to job artifacts and
never fails a build:

| Gate | Scope | Why report-only |
| --- | --- | --- |
| `secret-scan` | whole repo, test paths included | one enumeration cycle before the flip, so genuine fixtures get dated allow-rules first. Open finding: `docker/server.pem` |
| misconfig half of `dockerfile-scan` | the three builder Dockerfiles | six HIGH findings on images that are never pushed. Flip criterion on the job |
| `nightly-dependency-scan` pass 2 | `Cargo.lock`, dev dependencies and licences included | MEDIUM, LOW, UNKNOWN, unfixed and licence findings are visibility, not gates |
| `required-checks-drift` | branch protection on `main` | there is no ruleset to compare against yet |

Deliberate calibration choices, so they are not read as omissions:

- UNKNOWN is never in a blocking band. It is a data-source property, not a
  risk level, so gating on it fails a build for the absence of a score.
- Unfixed CVEs are never in a blocking band. A gate nobody in this repository
  can clear becomes a gate that gets switched off.
- MEDIUM, LOW, licence and dev-dependency findings are report tier.
- `disposition-check` is blocking, and that is not strictness: it is whether
  the exception path can be trusted at all.

## Remediation windows (POA&M)

Windows run from `detected`, which is the date the finding first appeared in a
report, not the date someone looked at it. The same table is machine-enforced:
`waiver-windows` in `.circleci/config.yml` is passed to `validate-waivers` and
to every gating scan, and `poam-windows` to `disposition-check`. A waiver
cannot cure a breach, because a finding already past its window has no valid
expiry date left and the entry is rejected.

| Severity | Window | Enforced by |
| --- | --- | --- |
| CRITICAL | 30 days | `waiver-windows`, `poam-windows` |
| HIGH | 30 days | `waiver-windows`, `poam-windows` |
| MEDIUM | 90 days | `waiver-windows`, `poam-windows` |
| LOW | 180 days | `waiver-windows`, `poam-windows` |
| Design acceptance | 366 days, re-reviewed annually | `waiver-windows` |
| CISA KEV listed | 90 days, and never later than the CISA due date | `kev-epss-check` in the nightly |

The KEV row is the override: a KEV-listed finding is remediated by its CISA due
date regardless of its CVSS band, including when that band is below every gate.
This is why `kev-epss-check` runs against the nightly's full-severity report
rather than against the gate's report. CISA BOD 26-04 keys federal remediation
deadlines on exploitation evidence (KEV), not CVSS, with full agency use from
2026-12-07; EPSS is the complementary likelihood signal, mandated by nothing.

Open POA&M rows at the time of writing:

| Item | Severity | Detected | Window ends | State |
| --- | --- | --- | --- | --- |
| `docker/server.pem`, unencrypted RSA test-server key committed since the fixture was added | HIGH | 2026-07-29 | 2026-08-28 | reported by `secret-scan`, fix is to generate the key at fixture start-up |
| DS-0015, `yum clean all` missing, `Dockerfile-build` and `Dockerfile-build-linux-x86_64` | HIGH | 2026-07-29 | 2026-08-28 | reported by `dockerfile-scan`, blocks its flip to gating |
| DS-0029, `apt-get` without `--no-install-recommends`, `Dockerfile-build-linux-arm64` | HIGH | 2026-07-29 | 2026-08-28 | reported by `dockerfile-scan`, needs a build verification on arm64 |
| `dev-requirements.txt` and `test-requirements.txt` entirely unpinned | MEDIUM | 2026-07-29 | 2026-10-27 | see Known gaps |
| GitHub Actions in `.github/workflows/release.yml` referenced by mutable tags | MEDIUM | 2026-07-29 | 2026-10-27 | 10 findings, reported by `sast-scan` at WARNING |

Accepted risks live in `.circleci/security-waivers.yaml`, one entry today
(DS-0002, the builder images running as root, a design acceptance scoped to the
three Dockerfiles by path). Waivers are subtracted at gate time only and are
never passed to Trivy as an ignorefile, so the published report, table, JUnit
and SBOM keep every finding.

## Required status checks

The authoritative list is `.github/required-checks.txt`, and
`required-checks-drift` compares it against the live branch rules for `main` on
every PR and every night.

**Nothing is required today.** The only rulesets that apply to this repository
are the enterprise-sourced `stable-branch-no-force-push` and
`stable-branch-no-delete`, and neither carries a required-status-checks rule.
Every gate below runs and reports; none of them blocks a merge until an org
owner creates the ruleset.

**Suffix instability warning.** CircleCI appends an instance suffix when the
same job name appears in more than one workflow, and the scheme is not
predictable. This repository has already been bitten by it: `dependency-cve-scan`
ran unnamed in both the `ci` and `nightly` workflows and its status context was
`ci/circleci: dependency-cve-scan-1`. Every gate instance now carries an
explicit `name:`, so the strings in `.github/required-checks.txt` are stable,
but they must still be read off a live pipeline (`gh pr checks`) after any
topology change and never hand-derived.

## Scan coverage, honestly

What is covered:

- `Cargo.lock`, 201 packages, on every PR, on `main` and every night. The
  target list is asserted by name (`expect-targets: Cargo.lock`), so a moved or
  deleted lockfile fails the gate instead of passing it with nothing to scan.
  `lint-rust` also asserts the lockfile is current for `Cargo.toml`
  (`cargo metadata --locked`), before any other `cargo` command in the pipeline
  can quietly update it: a lock that has drifted from the manifest makes the
  gate report on versions a build does not resolve, and it does so at exit 0.
- First-party Rust in `src/` and Python in `python/`, including
  `python/tests/`. The committed `.semgrepignore` is what keeps the test tree in
  scope, since Semgrep's built-in list drops `tests/` wholesale, and it is
  guarded in both directions so scope cannot shrink unreviewed.
- Committed secrets across the whole tree including test paths, via
  `.circleci/trivy-secret.yaml` (`disable-allow-rules: [tests]`). Trivy's
  built-in allow rules exclude test code from secret detection entirely.
- The three builder Dockerfiles. They were invisible to Trivy before this
  change for a naming reason: the misconfiguration analyzer recognises
  `Dockerfile`, `Dockerfile.*`, `*.Dockerfile` and `Containerfile`, and these
  are `Dockerfile-build*`, so a plain `trivy fs --scanners misconfig .` found
  zero Dockerfile targets and exited 0. `file-patterns` fixes that.
- Detection itself, nightly. `nightly-self-test` scans a digest-pinned
  known-vulnerable image and fails if the finding count drops below a measured
  floor, because a gate that has never failed is indistinguishable from no gate.

What is not covered, and why:

- **The PyPI publish path.** `.github/workflows/release.yml` builds and uploads
  the wheels from GitHub Actions with `twine`, with no scan, no SBOM and no
  signature. Nothing in CircleCI publishes, so there was no publish job to wire
  a gate into. Closing this means either moving the publish path or adding the
  gate on the Actions side, and it is an owner decision, not a config edit.
- **`dev-requirements.txt` and `test-requirements.txt`.** Both are entirely
  unpinned, so Trivy's pip analyzer resolves no packages and reports nothing
  either way. The analyzer mapping is already wired
  (`file-patterns: pip:.*requirements\.txt`), so pinning them puts them in
  scope with no CI change.
- **The compiled artifact.** A built wheel contains a compiled Rust `cdylib`.
  Trivy cannot introspect a compiled binary on an `fs` scan, so coverage of
  what ships is coverage of `Cargo.lock`, the input, rather than of the `.so`,
  the output. The `rootfs` scan type reads compiled Go binaries but not Rust
  `cdylib` symbol data, so it would not help here.

## Artifact signing, attestation and SLSA level

Nothing this repository publishes is signed today, and the honest statement is
that there is no SLSA level to claim.

- No wheel carries a signature, an SBOM attestation or provenance. The
  `arangoml/supply-chain` orb signs OCI artifacts by digest, which does not
  apply to a wheel on PyPI, so the fleet pattern is not simply copyable here.
- The credible free path is PyPI Trusted Publishing with PEP 740 attestations
  from the release workflow, which would give a verifiable link from the wheel
  back to this repository and this workflow at zero cost. It needs a PyPI
  project-settings change and a release-workflow change, both owner actions.
- Consumers therefore have no verification command to run. When that changes,
  the exact command goes here rather than a claim that it exists.

An SBOM *is* produced: `dependency-cve-scan` renders CycloneDX and SPDX-2.3
from the same scan and stores both as build artifacts, structurally validated
against CISA's 2025 Minimum Elements by `validate-sbom` with a component floor.
It is not published next to the release artifacts, because there is no publish
step to attach it to.

## Cryptography

phenolrs performs no cryptography of its own. TLS to ArangoDB is provided by
the transitive `rustls` and `reqwest` stack pulled in through
`arangors-graph-exporter`, and no FIPS-validated mode is claimed or configured.
`docker/server.pem` is a fixture certificate for a local test database and is
not used by any released code path.

## Incident reporting obligations

The EU Cyber Resilience Act requires an actively exploited vulnerability in a
product with digital elements to be reported within 24 hours of awareness, and
a full notification within 72 hours. For this repository the named owner of
that clock is the security owner in `.github/CODEOWNERS`, and the trigger is
either a KEV match from `kev-epss-check` or a report received through the
channel above. This library is a component of ArangoDB products rather than a
standalone product, so the obligation is discharged through the product
security process; the row exists here so the escalation path is written down
where the finding surfaces.

## Known gaps

Free-first is the standing policy: paid tooling only where it clearly beats the
free baseline. Each gap below names the paid alternative and its internal-use
price so that "not done" is never confused with "not considered".

| Gap | Free baseline in place | Paid alternative and price | Residual risk |
| --- | --- | --- | --- |
| No required status checks on `main` | `.github/required-checks.txt` plus the drift job | None, this is a settings change | every gate reports and none blocks a merge |
| Wheels unsigned, no provenance | None | GitHub Artifact Attestations are Actions-only and, on private repos, Enterprise Cloud | a consumer cannot verify a wheel came from this repository |
| No SARIF in the GitHub UI | SARIF stored as a job artifact | GitHub Code Security, 30 USD per committer per month on private repos | triage happens in CircleCI rather than in the Security tab |
| No dataflow-grade taint analysis | Semgrep CE, `p/default` plus `p/rust`, 0 USD | Semgrep AppSec Platform, priced per contributor, quoted per deal | weaker interfile analysis than CodeQL |
| Evidence retention | `publish-evidence` and `dtrack-upload` are wired and inert | None; needs an S3 bucket with COMPLIANCE-mode retention and a Dependency-Track 5 host, both org actions | scan evidence persists only as CircleCI artifacts with finite retention |
| Unpinned dev and test requirements | None | None | the lint and test toolchain is not reproducible and is unscannable |
| `docker/server.pem` committed | reported every run by `secret-scan` | None | an unencrypted private key is in git history; rotating it means regenerating the fixture |
| Nightly runs on one branch | in-config `triggers:` schedule on `main` | None; Scheduled Pipelines is free but needs API setup | acceptable while there is exactly one supported line |
| No OpenVEX published | waivers carry the same statements | None | consumers cannot machine-read a not-affected claim; this is an internal library, so the customer-facing VEX cohort does not include it |

The machine-readable version of this posture, control by control with a
proven-versus-unproven flag on each, is `.security/baseline.yml`.

## Pinned components

Bumps to any of these are a reviewed one-line change:

| Component | Pin |
| --- | --- |
| `arangoml/trivy-scan` orb | 1.1.2 |
| `arangoml/semgrep-scan` orb | 1.0.0 |
| `arangoml/supply-chain` orb | 1.0.0 |
| Trivy | v0.72.0, checksum-verified in the orb |
| Semgrep CE | 1.168.0, from PyPI into the job venv |
| Scan executor | `cimg/python:3.12`, digest-pinned |
| Self-test fixture image | `python:3.9-slim`, digest-pinned |
| gitleaks pre-commit hook | commit `83d9cd68`, gitleaks v8.30.1 |
