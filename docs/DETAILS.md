# Details

- [Installation](#installation)
- [Safety guarantees](#safety-guarantees)
- [Data sources and restricted access](#data-sources-and-restricted-access)
- [Unity Catalog scanner (`uc`)](#unity-catalog-scanner-uc)
- [Policy reference](#policy-reference)
- [Scoring](#scoring)
- [Architecture](#architecture)
- [Writing scanners: guidelines](#writing-scanners-guidelines)
- [Logging](#logging)
- [Development](#development)
- [Releasing](#releasing)

---

## Installation

- **Requirements:** Python 3.10–3.14 (tested; Databricks Runtime 13.3 LTS or later). The only runtime
  dependency is PyYAML. pyspark is used when present but is not a dependency.
- **Get the wheel:** download `lakehouse_gc-<version>-py3-none-any.whl` from the
  project's GitHub Releases, or build it yourself with `uv build` (it is written to
  `dist/`).
- **Install in Databricks:** upload the wheel and your `policy.yaml` to a volume, then run
  `%pip install /Volumes/<catalog>/<schema>/<volume>/<wheel file>` in a notebook cell.
- **Reinstalling the same version:** use `%pip install --force-reinstall ...` and then
  `dbutils.library.restartPython()`. Otherwise the old copy may be kept.
- **Outside Databricks:** `pip install <wheel file>`. The `example` scanner runs anywhere.
  The `uc` scanner needs Spark; without it, `uc` fails on its own with a clear message
  while the other scanners still run.

Ready-made notebooks (`.ipynb`, saved without outputs), from easy to advanced: see
[Examples](EXAMPLES.md). Start with
[`examples/01_easy_quickstart.ipynb`](../examples/01_easy_quickstart.ipynb).

**Recommended first run:** `check_access(spark, POLICY_PATH)` (what can I read?), then
`rules_dataframe(...)` (what will be checked?), then `run(...)`.

## Safety guarantees

- **Report-only.** The package cannot change resources, and no setting can enable a
  change. It has no SDK or network dependencies, and the scanners and `check_access` only run
  `SELECT`, `SHOW CATALOGS`, and `DESCRIBE DETAIL`. The keys `actions.mode`,
  `allow_delete`, and `allow_pause` were removed in 0.3.0; a policy that still has them is
  rejected with a message to delete the line.
- **Results are saved only where you say.** `result.save(...)` writes the findings to a
  table or file you name, and nothing else. It never writes to a scanned resource.
- **Recommendations are advisory.** `archive_candidate`, `pause_candidate`, and
  `delete_candidate` are lowered to `review` unless confidence is known and at least
  `actions.min_candidate_confidence` (default 70). The finding records `downgraded_from`.
- **Unknown stays unknown.** Evidence is `observed`, `inferred`, or `unavailable`.
  Unavailable evidence carries no value, missing confidence is `None`, and missing savings
  are `None`, never zero.

## Data sources and restricted access

Organizations differ in what end users may read. Some block the `system` catalog, and some
publish curated views in their own schemas instead. The tool adapts in three ways.

**1. Check first.** `check_access(spark, POLICY_PATH, scanners=["uc"])` probes every source
for its column names only (`SELECT * ... LIMIT 0` returns no rows). It reports each one:

| Status | Meaning |
|---|---|
| `available` | Readable, with all expected columns |
| `partial` | Readable, but some optional columns are missing; only the checks that need them become unknown |
| `unavailable` | Not readable, or a required column is missing; essential sources stop the scanner |
| `unverified` | Couldn't be probed (e.g. `per_catalog` with no visible catalog in scope) |

Each row also shows `impact` (the checks affected) and `how_to_fix`. Use
`report.summary()`, `report.to_dataframe(spark)`, `report.to_rows()`, or
`report.can_run("uc")`.

**2. Point the scanner at what you can read** under `sources:` in `policy.yaml`:

```yaml
sources:
  information_schema: system.information_schema  # default
  # information_schema: per_catalog              # each catalog's own information_schema
  # information_schema: governance.uc_meta       # a schema of your org's views, same shape
  tables:  # repoint a single view and map renamed columns
    table: governance.uc_meta.all_tables
    columns: {table_owner: owner_principal}  # logical name: your column name
```

- `per_catalog` lists catalogs with `SHOW CATALOGS` and reads
  `<catalog>.information_schema.<view>` for each catalog in scope.
- A single source can be a plain name (`tables: org.meta.tables`) or a mapping with
  `table` and/or `columns`.
- Names are validated: unknown sources or columns fail before scanning, with "did you
  mean" hints.

**3. Missing data is reported, never guessed.** After a run, `result.limitations` (also
shown in `summary()`) lists what couldn't be seen and which checks became unknown.
Unknown checks count against the score through `unknown_penalty`; they are never treated
as passing.

`uc` sources and their columns. Required columns are in **bold**; the other columns are
optional, and the rule in brackets becomes unknown without them.

| Source | Default object | Columns | If unavailable |
|---|---|---|---|
| `catalogs` | `system.information_schema.catalogs` | **catalog_name**, catalog_owner [owner_missing], comment [comment_missing], created, last_altered | `uc` cannot run |
| `schemata` | `system.information_schema.schemata` | **catalog_name**, **schema_name**, schema_owner [owner_missing], comment [comment_missing], created, last_altered | `uc` cannot run |
| `tables` | `system.information_schema.tables` | **table_catalog**, **table_schema**, **table_name**, table_type [stale_table], table_owner [owner_missing], comment [comment_missing], created, last_altered [stale_table], data_source_format | `uc` cannot run |
| `table_tags` | `system.information_schema.table_tags` | **catalog_name**, **schema_name**, **table_name**, **tag_name**, tag_value | required_tags_missing unknown |
| `volumes` | `system.information_schema.volumes` | **volume_catalog**, **volume_schema**, **volume_name** | empty_container unknown |

`DESCRIBE DETAIL` runs per stale-table candidate and can't be probed up front. Its
failures appear in `limitations`.

## Unity Catalog scanner (`uc`)

The scanner reads Unity Catalog `information_schema` views (`catalogs`, `schemata`,
`tables`, `table_tags`, `volumes`), from `system.information_schema` by default or wherever
`sources:` points (see [above](#data-sources-and-restricted-access)). For stale-table
candidates only, it also runs `DESCRIBE DETAIL`.
It skips `information_schema` schemas. Its resource types are `catalog`, `schema`, and
`table`.

| Rule | Applies to | Fails when | On fail |
|---|---|---|---|
| `uc.owner_missing` | catalog, schema, table | no owner | tag_owner |
| `uc.comment_missing` | catalog, schema, table | no comment (`require_comment`) | review |
| `uc.required_tags_missing` | table | any of `required_tags` is missing | tag_owner |
| `uc.temporary_name` | schema, table | name matches `temp_name_patterns` | review |
| `uc.empty_container` | catalog, schema | no visible tables or volumes | review |
| `uc.stale_table` | managed/external table | no data written in `stale_table_days` and size ≥ `min_table_size_gb` | archive_candidate |

`rules.uc` settings (all optional):

| Setting | Default | Meaning |
|---|---|---|
| `stale_table_days` | 180 | No data written for this many days counts as stale |
| `min_table_size_gb` | 10 | Only flag stale tables at least this big (1 GB = 1024³ bytes; 0 = any size) |
| `required_tags` | `[]` | Tags every table must have (empty turns the check off) |
| `require_comment` | `true` | Check for descriptions |
| `temp_name_patterns` | `tmp_*`, `temp_*`, `test_*`, `*_bak`, ... | Names that look temporary |
| `check_empty_containers` | `true` | Check for empty catalogs and schemas |
| `max_detail_queries` | 200 | Limit on `DESCRIBE DETAIL` calls; tables beyond it are reported as unknown |

How evidence limits are reported:

- **Staleness is inferred.** `last_altered` tracks metadata changes, and `lastModified`
  (from `DESCRIBE DETAIL`) tracks data writes. Neither is a read time, so confidence is 50
  and `archive_candidate` becomes `review`. Confirm with lineage or audit logs before
  acting.
- **"Empty" means nothing you can see.** `information_schema` only shows objects the
  running user can access, and functions and models are not counted. Empty containers are
  inferred and get `review`.
- **Unreadable data is unknown.** If tags or volumes can't be read, or `DESCRIBE DETAIL`
  fails or hits the limit, the check is `indeterminate`. It counts as partially failed
  (`unknown_penalty`) and is listed in `limitations`. If catalogs, schemas, or tables can't
  be listed, `uc` fails with the reason, and other scanners still run.
- **Naming is a heuristic,** so temporary-looking names are inferred evidence.

## Lakeflow Jobs scanner (`jobs`)

The scanner reads `system.lakeflow.jobs` and `system.lakeflow.job_run_timeline`, or your
organization's views (`sources: {lakeflow: governance.lakeflow}` for a schema with views
of the same names, or `jobs:` / `job_run_timeline:` for single views and renamed
columns). Its resource type is `job`, so exclusions use `resource_type: job` and match the
job name. Each job appears as `name (workspace_id/job_id)`.

| Rule | Severity | Fails when | On fail |
|---|---|---|---|
| `jobs.never_run` | medium | the job is at least `never_run_days` old and no run is observed | review |
| `jobs.failing_streak` | high | each of the last `failing_days` days (UTC) had finished runs, and every one ended in `failure_states` | review |

`rules.jobs` settings (all optional):

| Setting | Default | Meaning |
|---|---|---|
| `never_run_days` | 30 | Minimum job age before "no run observed" is flagged |
| `failing_days` | 3 | Days in a row of only failed runs before flagging (1–90) |
| `failure_states` | `FAILED`, `ERROR`, `TIMED_OUT` | `result_state` values that count as a failure |
| `workspace_ids` | `[]` | Limit to these workspaces; empty = every workspace the tables show |

| Source | Default object | Columns | If unavailable |
|---|---|---|---|
| `jobs` | `system.lakeflow.jobs` | **workspace_id**, **job_id**, **change_time**, name, creator_user_name, creator_id, create_time, delete_time, paused | `jobs` cannot run |
| `job_run_timeline` | `system.lakeflow.job_run_timeline` | **workspace_id**, **job_id**, **period_start_time**, period_end_time [failing_streak], result_state [failing_streak] | both rules unknown |

How evidence limits are reported:

- **"No run observed", not "never ran".** The timeline keeps about a year, so an older run
  would not show. The finding names the date it looked back to.
- **Job age.** `create_time` is only filled for rows from late November 2025 on. Without
  it, age comes from the earliest recorded change: inferred, shown as "about N days", and
  possibly younger than the real job.
- **The latest row describes a job.** `jobs` has one row per change; jobs with a
  `delete_time` are skipped.
- **Streak rules.** A day with no finished run, a success, or another result such as
  `CANCELLED` or `SKIPPED` ends the streak. Today counts once it has a finished run;
  otherwise the streak is counted from yesterday.
- **Other workspaces.** The system tables can include every workspace in the account; use
  `workspace_ids` to limit the scan.

## Policy reference

[`examples/policy.yaml`](../examples/policy.yaml) documents every key. The rules:

- `policy_version: 1` is required. Other versions are rejected.
- Unknown keys, wrong types, and **duplicate YAML keys** are errors. Messages name the
  exact location, e.g. `scoring.cost_wieght: unknown key (did you mean 'cost_weight'?)`.
- `rules.<scanner>` is validated by that scanner. A section for a scanner that isn't
  registered is an error, to catch typos. Only add `rules.<name>` for a custom scanner
  to a policy that always runs with that scanner, or rely on its `config` defaults.
- Policies can also be built in code (`Policy(...)`) with the same validation. See
  [Examples §8](EXAMPLES.md#8-build-the-policy-in-code).

### Scope

```yaml
scope:
  include_catalogs: ["main", "prod_*"]
  exclude_catalogs: ["system", "samples", "__databricks_internal"]
  include_schemas: ["main.sales"]  # catalog.schema
  exclude_schemas: ["main.scratch"]
  include_tables: ["main.sales.fact_*"]  # catalog.schema.table
  exclude_tables: []
```

- Names must be fully qualified (1, 2, or 3 parts). Patterns are case-insensitive globs.
- An empty include list means "all", and excludes always win.
- Narrower includes also narrow the levels above them. For example,
  `include_tables: ["main.sales.orders"]` alone scans only catalog `main`, schema
  `main.sales`, and that table.

### Exclusions

- **Fields:** `resource_type`, `pattern` (a case-insensitive glob on the full name),
  `reason`, and an optional `expires_on`.
- **`resource_type`** must be a type a scanner produces (`catalog`, `schema`, or `table`
  for `uc`). Internal and event-log tables are type `table`. An exclusion whose type no
  scanner produces can never match, and the run warns about it.
- **`expires_on`** makes an exclusion temporary, for example a migration window or a
  pending review. It applies through that date. Afterwards it is ignored, the resources are
  checked again, and a warning notes the expiry. `null` means permanent.
- **Scope vs. exclusions:** objects outside the scope are never read. Excluded objects are
  read but not analyzed, and they appear as `excluded` findings.

### Scoring and actions settings

| Key | Default | Meaning |
|---|---|---|
| `scoring.governance_weight`, `scoring.cost_weight` | 0.5 / 0.5 | Weights of the two scores in the overall score (normalized) |
| `scoring.unknown_penalty` | 0.5 | Share of a failure that an unknown check costs, in (0, 1]; 1.0 treats unknown as failed |
| `scoring.severity_weights` | info 0, low 1, medium 3, high 5 | Weight of each severity |
| `scoring.grade_boundaries` | A 90, B 80, C 70, D 60, F 0 | Minimum score for each grade (one must be 0) |
| `actions.min_candidate_confidence` | 70 | `*_candidate` below this becomes `review` |

## Scoring

Scores range from 0 to 100, and higher is healthier. Checks from all successful scanners
are pooled. Each check has weight *w*: the policy weight of its rule's severity.

```text
penalty p:  PASS = 0,  FAIL = 1,  INDETERMINATE = unknown_penalty   (0 < u <= 1)

score       = 100 * (1 - sum(w * p) / sum(w))     None if nothing was assessable
deduction_i = 100 * w_i * p_i / sum(w)            deductions add up to 100 - score
coverage    = checks with PASS/FAIL / all checks

overall     = weighted mean of governance and cost (weights normalized)
              None if a weighted dimension has no score
grade       = highest grade whose minimum <= overall
```

**Example.** 10 checks of equal weight: 6 pass, 2 fail, 2 unknown. With
`unknown_penalty` 0.5 the score is 100 − (2 + 2 × 0.5) × 10 = 70; with 0.8 it is 64; with
1.0 it is 60, the same as if both unknowns had failed. With weights 0.8 / 0.2, overall =
0.8 × governance + 0.2 × cost.

- **Missing visibility is never rewarded.** Unknown checks always cost points, and
  `unknown_penalty: 0` is rejected. A dimension with nothing to check is `None`, not 100.
- **Deterministic.** Inputs are sorted and arithmetic uses exact fractions. Only the
  displayed scores are rounded, to 2 decimals.
- **Audit trail.** `result.scores.<governance|cost>.deductions` lists every check that cost
  points.
- **Confidence** measures evidence quality, not the probability that acting is safe. By
  default it is the average over evidence items (observed 100, inferred 50,
  unavailable 0), or `None` when there is no evidence.
- **Savings** are `None` unless a finding carries an estimate with stated assumptions and
  coverage. Estimates on the same resource are not added together. The built-in scanners
  produce no estimates yet.

## Results: sorting and saving

`result.to_dataframe(spark)` and `result.to_rows()` return one row per finding, most
severe first. Columns: `severity_level` (3 high, 2 medium, 1 low, 0 info), `severity`,
`scanner`, `resource_type`, `resource`, `rule`, `recommendation`, `confidence`, `reasons`,
`downgraded_from`, `excluded_reason`, `run_id`, and `run_time`.

`result.to_json()` returns the whole run (status, scores, limitations, warnings, errors,
findings) as JSON text. Saving is optional and goes only where you say:

```python
result.save(spark, table="main.governance.lakehouse_gc_findings")  # append to Delta
result.save(json_path="/Volumes/main/default/lakehouse_gc/run-1.json")  # new file
```

- A table must be `catalog.schema.table`. Each run appends its rows (created if missing),
  so `run_id` and `run_time` give you history. You need `CREATE TABLE` or `MODIFY` there.
- A JSON file is never overwritten; an existing path is an error. The folder must exist.
- With neither argument, `save` raises a `ValueError` and nothing is written.

## Architecture

```text
src/lakehouse_gc/
  core/
    models.py     Resource, RuleEvaluation, ScoredEvaluation, Finding, enums
    evidence.py   Evidence, EvidenceStatus, derive_confidence
    policy.py     Policy model and strict YAML loader
    config.py     validation of scanner rules sections
    scanner.py    Scanner base class, RuleSpec, ScannerContext
    registry.py   ScannerRegistry
    scoring.py    scores and deduction audit trail
    savings.py    savings summary (no double counting)
    results.py    ScannerResult, RunResult (summary, to_rows, to_dataframe, save)
    export.py     to_json and save: the only code that writes, and only results
    sql.py        read-only SqlClient protocol and Spark adapter
    sources.py    configurable sources, column probing, source reader
    log.py        structured logging helpers
  scanners/
    example.py    mock-data demo scanner
    uc.py         Unity Catalog scanner
    jobs.py       Lakeflow Jobs scanner
  simple.py       check / define_scanner (function-based scanners)
  access.py       check_access
  rules.py        describe_rules, rules_dataframe, rules_text
  runner.py       run()
```

### Scanner lifecycle

Each scanner runs `discover → analyze → score → recommend`. Values pass between phases
explicitly and are immutable, and every run gets a fresh scanner instance.

| Phase | Input | Output | Notes |
|---|---|---|---|
| `discover` | context | resources | Unique resource IDs only |
| *(framework)* | resources | included / excluded | Policy exclusions applied |
| `analyze` | included resources | rule evaluations | `PASS`, `FAIL`, or `INDETERMINATE` |
| `score` | evaluations | scored evaluations | Default: severity weights and evidence-based confidence |
| `recommend` | scored evaluations | findings | At most one per evaluation |
| *(framework)* | findings | final findings | Confidence gate and aggregate scoring |

The framework checks every phase's output. A contract violation (wrong type, duplicate
resource, undeclared rule, evaluating an excluded resource, and so on) fails that scanner,
the same way an exception does.

### Failure isolation

- Configuration errors are raised **before** any scanner runs: an invalid policy, unknown
  scanner names, rules for unregistered scanners, or invalid scanner rules.
- During a run, an error in one scanner marks only that scanner `failed` and records the
  phase and message. Other scanners still run.
- Run status is `succeeded`, `partial` (some scanners failed), or `failed` (all failed).
- Failed scanners add nothing to the scores. The score report sets `complete=False` and
  lists the failed scanners, so a crash never looks like a clean result.
- Scanners run in name order, and results are sorted, so runs are reproducible.

## Writing scanners: guidelines

- Use [`@check` / `define_scanner`](EXAMPLES.md#6-add-your-own-checks) for most needs, and
  [subclass `Scanner`](EXAMPLES.md#7-advanced-subclass-scanner) for full control.
- Never modify resources; use read-only access only.
- When a signal can't be obtained, report it as unknown (`None` / `unknown()`, or
  `EvidenceStatus.UNAVAILABLE` with `Outcome.INDETERMINATE`). Never guess timestamps,
  counts, or traffic.
- Use the fully qualified name as the resource name, because exclusions match against it.
- Don't apply exclusions or the confidence gate yourself; the framework does both.
- Rule IDs start with `<scanner name>.`, and scanner names use lowercase letters, digits,
  and underscores.
- Registry errors: `DuplicateScannerError`, `UnknownScannerError` (with "did you mean"
  hints), and `InvalidScannerError`. All errors derive from `LakehouseGCError`.

## Logging

The library logs through the standard `lakehouse_gc` logger and never configures the root
logger. Records carry structured fields (`event`, `run_id`, `scanner`, `phase`,
`status`, ...). `lakehouse_gc.core.log.JsonFormatter` writes them as JSON lines; see
[Examples §9](EXAMPLES.md#9-registry-errors-and-logging).

## Development

```bash
uv sync                                   # create .venv with all dependencies
uv run pytest                             # run tests (no Databricks needed)
uv run ruff check . && uv run ruff format --check .
uv run mypy                               # strict type checks
uv build                                  # build the wheel and sdist into dist/
```

Tests use a frozen policy in `tests/fixtures/policy.yaml`, so editing
`examples/policy.yaml` never changes expected results.

## Releasing

Wheels are built by GitHub Actions, never from a laptop and never committed to git.

- **[`ci.yml`](../.github/workflows/ci.yml)** runs on every push to `main` and every pull
  request. On Python 3.10–3.14 it runs ruff, mypy, and pytest, builds the wheel, and checks
  that the wheel installs in a clean environment.
- **[`security.yml`](../.github/workflows/security.yml)** runs
  [`pip-audit`](https://github.com/pypa/pip-audit) (Python Packaging Authority) on the exact,
  hashed versions in `uv.lock`, against the [OSV](https://osv.dev) database. It runs weekly
  (Mondays), whenever `uv.lock` or `pyproject.toml` changes, and on demand. It audits the
  runtime dependencies (shipped with the wheel) and all dev tools. If the weekly run fails,
  it opens, or comments on, a **"Weekly security audit failed"** issue, so you can decide
  whether to upgrade, wait for a fix, or accept the risk.
- **[`release.yml`](../.github/workflows/release.yml)** runs when a `v*` tag is pushed. It
  runs CI **and** the security audit, checks that the tag equals `v` + `__version__`,
  builds, and creates a GitHub Release with the wheel, the sdist, `SHA256SUMS.txt`, a
  CycloneDX SBOM, and a signed build provenance attestation. Versions with letters (e.g.
  `0.3.0rc1`) become pre-releases. Nothing is published if any check fails.
- **[`use-public-pypi.sh`](../.github/scripts/use-public-pypi.sh)** points `uv.lock` at
  public PyPI on GitHub runners, for locks generated behind a corporate PyPI mirror. The
  hashes still verify every file.

**One-time repository settings** (Settings → Code security): enable *Private vulnerability
reporting* (used by [SECURITY.md](../SECURITY.md)), *Dependabot alerts*, and *Secret
scanning*. Optionally enable *Code scanning → CodeQL default setup*, which needs no
workflow file.

To release:

```bash
# 1. set __version__ = "0.4.0" in src/lakehouse_gc/__init__.py, then:
git commit -am "Release 0.4.0"
git push
git tag v0.4.0
git push origin v0.4.0      # starts release.yml
```

If the tag and version don't match, the release fails before anything is published. Delete
the tag (`git tag -d v0.4.0 && git push origin :v0.4.0`), fix the version, and tag again.
