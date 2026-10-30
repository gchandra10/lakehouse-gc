# FAQ

Short answers. Details live in [DETAILS.md](DETAILS.md); runnable notebooks in
[EXAMPLES.md](EXAMPLES.md).

**For data teams:** [Getting started](#getting-started) ·
[What it checks](#what-it-checks) · [Reading the results](#reading-the-results) ·
[Lakeflow jobs](#lakeflow-jobs) · [Tuning and customizing](#tuning-and-customizing) ·
[Troubleshooting](#troubleshooting)

**For security and governance teams:** [Safety](#safety) ·
[Access and permissions](#access-and-permissions) · [Data and privacy](#data-and-privacy) ·
[Supply chain and releases](#supply-chain-and-releases)

---

## Getting started

**What is it?**
A Python package you run in a Databricks notebook. It reports governance gaps and cost
waste, scores them, and explains every finding. It never changes anything.

**How do I install it?**
Download the wheel from [Releases](https://github.com/gchandra10/lakehouse-gc/releases),
upload it and `policy.yaml` to a volume, then `%pip install /Volumes/.../<wheel>`.

**What do I need?**
A Databricks notebook with Spark and Unity Catalog (Runtime 13.3 LTS or later). Python
3.10–3.14. One dependency: PyYAML.

**What's the fastest first run?**
Open [`examples/01_easy_quickstart.ipynb`](../examples/01_easy_quickstart.ipynb). It runs
`check_access` → `rules_dataframe` → `run` in three cells.

**Is there a CLI?**
No. It's a Python API: `run()`, `check_access()`, `rules_dataframe()`.

**Can I schedule it?**
Yes. Put the notebook in a Lakeflow job and `result.save(spark, table=...)` each run to
build history.

**How long does a run take?**
Mostly a few metadata queries. The slowest part is `DESCRIBE DETAIL`, run only for stale
table candidates and capped by `max_detail_queries` (default 200).

## What it checks

**What does the `uc` scanner check?**
Missing owners, missing descriptions, missing required tags, temporary-looking names,
empty catalogs/schemas, and large tables with no recent writes.

**What does the `jobs` scanner check?**
Jobs with no observed run, and jobs whose every run failed for N days in a row.

**Does it read my table data?**
No. Only metadata: `information_schema`, `DESCRIBE DETAIL`, and the Lakeflow system tables.

**Which catalogs does it scan?**
Whatever `scope:` in `policy.yaml` says. An empty include list means all; excludes always
win.

**Can I skip specific objects?**
Yes, with `exclusions:`. They still show up as `excluded`, with your reason, so nothing is
silently hidden. Add `expires_on` for a temporary skip.

**Does it estimate dollar savings?**
Not yet. It never invents a number; savings stay empty until there's real evidence.

**Does it look at clusters, warehouses, or billing?**
Not yet. Today: Unity Catalog and Lakeflow Jobs.

## Reading the results

**What's in the results table?**
One row per finding, most severe first: severity, resource, rule, recommendation,
confidence, and reasons in plain English.

**What does `severity_level` mean?**
A sortable number: 3 high, 2 medium, 1 low, 0 info.

**What do the recommendations mean?**

| Recommendation | Meaning |
|---|---|
| `keep` | The check passed |
| `review` | Look at it; a human decides |
| `tag_owner` | Add the missing owner or tag |
| `archive_candidate`, `delete_candidate`, `pause_candidate` | Strong evidence; still only a suggestion |
| `excluded` | Skipped by your policy, with the reason |

**What is confidence?**
How solid the evidence is: observed 100, inferred 50, unavailable 0. It is *not* "safe to
delete".

**Why is a high-severity row marked `keep`?**
Severity is how much the rule matters. `keep` means this resource passed it.

**Why does a stale table say `review`, not `archive_candidate`?**
"No writes" isn't proof of "no reads". Staleness is inferred (confidence 50), and anything
below 70 becomes `review`.

**What does "unknown" or `indeterminate` mean?**
The evidence couldn't be read. It's listed in `result.limitations` and costs part of a
failure in the score. It never counts as a pass.

**How is the score calculated?**
0–100, higher is healthier: `100 × (1 − weighted failures / weighted checks)`. Failures
weigh by severity. See [Scoring](DETAILS.md#scoring).

**Why did my score drop when nothing changed?**
Usually lost visibility: a table you can no longer read turns checks into unknowns. Check
`result.limitations`.

**Which checks cost me points?**
`result.scores.governance.deductions` (or `.cost`) lists each one and how much.

**How do I save results?**
`result.save(spark, table="cat.schema.findings")` appends to Delta;
`result.save(json_path="/Volumes/.../run.json")` writes a new file.

**Can I compare runs over time?**
Yes. Saved rows carry `run_id` and `run_time`; query the table.

## Lakeflow jobs

**Does "no run observed" mean it never ran?**
No. The system tables keep about a year. The finding states the date it looked back to.

**Why does it say "about N days old"?**
`create_time` is only filled from late November 2025. Older jobs use their earliest
recorded change, which may be younger than the real job.

**Why do I see jobs from other workspaces?**
The system tables cover the whole account. Set `workspace_ids: ["<id>"]` to limit it.

**What counts as a failing streak?**
Every finished run failed on each of the last `failing_days` days (UTC). A success, a
cancel, or a day with no runs breaks it.

**Is a paused job flagged?**
It can be flagged as never run, and the reason says it's paused.

**Will it pause or delete the job?**
No. It only says `review`.

## Tuning and customizing

**How do I change thresholds?**
Edit `rules.uc` / `rules.jobs` in `policy.yaml`. Every key is optional and validated.

**Can I try settings without editing the file?**
Yes: `loads_policy("""...""")` builds a policy from a string. See notebook 07, step 5.

**Can I make unknowns count as failures?**
Yes: `scoring.unknown_penalty: 1.0`. Default 0.5 counts an unknown as half a failure.

**Can I weight governance over cost?**
Yes: `governance_weight: 0.8`, `cost_weight: 0.2`.

**Can I add my own checks?**
Yes. A few plain functions with `@check` and `define_scanner`. See
[Examples §5](EXAMPLES.md#5-your-own-checks).

**What happens with a typo in `policy.yaml`?**
The run stops before scanning, with the exact key and a "did you mean" hint.

## Troubleshooting

**`check_access` says `unavailable`.**
You can't read that source. Ask for `SELECT`, or point `sources:` at views you can read.

**The `system` catalog is blocked for me.**
Use `information_schema: per_catalog`, or your org's views (`information_schema:
gov.uc_meta`, `lakeflow: gov.lakeflow`). Column names can be mapped too.

**`NameError` after a restart.**
`restartPython()` or a detach clears imports. Rerun the import cell.

**I reinstalled but still get the old version.**
`%pip install --force-reinstall <wheel>`, then `dbutils.library.restartPython()`.

**One scanner failed but the run continued.**
By design. Status becomes `partial`, the failed scanner adds no score, and
`scores.complete` is `False`.

**`save` says the file already exists.**
JSON files are never overwritten. Use a new name, e.g. with a date.

---

## Safety

**Can it delete, pause, or change anything?**
No. It only runs `SELECT`, `SHOW CATALOGS`, and `DESCRIBE DETAIL`. No setting can turn on
changes; the old switches were removed in 0.3.0 and are now rejected.

**How is that enforced?**
Tests fail the build if code imports network, SDK, or process-spawning modules, or if
anything outside `core/export.py` writes data.

**What does it write?**
Nothing, unless you call `result.save(...)`. Then only the table or file you name. Never a
scanned resource.

**Does a recommendation authorize action?**
No. Recommendations are advisory. A human decides.

**Can the policy file run code?**
No. YAML is parsed with a safe loader. Unknown or duplicate keys are rejected.

**What if it can't see something?**
It says so. Missing evidence is reported as unknown, never guessed and never passed.

## Access and permissions

**What permissions does it need?**
`SELECT` on what it reads: `system.information_schema` (or your views) for `uc`;
`system.lakeflow.jobs` and `job_run_timeline` for `jobs`. Plus `CREATE TABLE`/`MODIFY` on
the target, only if you save to a table.

**Whose identity does it run as?**
Yours, or the job's run-as principal. It has no credentials of its own.

**Does it bypass Unity Catalog permissions?**
No. It sees only what the running identity can see. Results are limited to that.

**Should it run as an admin?**
No need. Run as a principal with read access to the scope you want covered. Less access
means more unknowns, reported as such.

**Can we keep `system` locked down?**
Yes. Grant curated views instead and point `sources:` at them.

**How do I check access before a scan?**
`check_access(spark, POLICY_PATH)`. It reads column names only (`LIMIT 0`, no rows).

## Data and privacy

**Does any data leave the workspace?**
No. No network calls, no telemetry. Everything runs inside your Spark session.

**What do the results contain?**
Metadata: object and job names, owners or creators, rule results, and reasons. No table
contents.

**Should the results table be protected?**
Yes. It lists names and owners across your scope. Grant it like any governance report.

**Does it store credentials or tokens?**
No. It has none and asks for none.

**Is the output reproducible for audits?**
Yes. Same inputs give the same results. Each finding records its evidence and confidence,
and scores have a deduction trail.

## Supply chain and releases

**What are the dependencies?**
One at runtime: PyYAML. Spark is used if present but isn't a dependency.

**Are dependencies scanned?**
Yes. `pip-audit` checks the locked versions against OSV weekly, on any dependency change,
and before each release. A known vulnerability blocks a release.

**How do I verify a wheel?**
`sha256sum -c SHA256SUMS.txt`, then
`gh attestation verify <wheel> --repo gchandra10/lakehouse-gc`. See
[SECURITY.md](../SECURITY.md#verifying-a-release).

**Is there an SBOM?**
Yes. A CycloneDX SBOM ships with every release from 0.2.2.

**Who builds the wheels?**
GitHub Actions, from a tagged commit. Never a laptop, never committed to git. Actions are
pinned to commit SHAs.

**How do I report a vulnerability?**
Privately, from the repo's **Security** tab. See [SECURITY.md](../SECURITY.md).

**What's the license?**
The [Databricks License](../LICENSE): source-available, for use with Databricks services.
