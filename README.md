# lakehouse-gc

Report-only governance, cost, and hygiene assessment framework for Databricks lakehouses.
Distribution name `lakehouse-gc`, import name `lakehouse_gc`. The Python package is the
primary interface; there is no CLI.

> **Status: 0.2.1.** Ships the framework, a credential-free `example` scanner (mock data),
> and a read-only Unity Catalog scanner (`uc`) for catalogs, schemas, and tables. AI Search,
> Apps, and Volumes scanners, cost estimation, and HTML/JSON/Delta exporters are not built
> yet.

## Safety guarantees

- **Report-only.** The package cannot mutate resources. It has no SDK or network
  dependencies, the `uc` scanner only issues `SELECT` and `DESCRIBE DETAIL` statements, and
  policies that set `actions.mode` to anything but `report_only`, or
  `allow_delete`/`allow_pause` to anything but `false`, are rejected.
- **Recommendations are advisory.** `ARCHIVE_CANDIDATE`, `PAUSE_CANDIDATE`, and
  `DELETE_CANDIDATE` are downgraded to `REVIEW` unless confidence is known and at least
  `actions.min_candidate_confidence` (default 70). The finding records `downgraded_from`.
- **Unknown stays unknown.** Evidence is `observed`, `inferred`, or `unavailable`.
  Unavailable evidence carries no value, missing confidence is `None`, and missing savings
  are `None`, never zero.

## Quick start: scan Unity Catalog from a notebook

1. Build the wheel (`uv build`) and upload `dist/lakehouse_gc-0.2.1-py3-none-any.whl` and
   [`examples/policy.yaml`](examples/policy.yaml) to a volume.
2. In `policy.yaml`, list what to scan under `scope` (see [Scope](#scope)) and adjust
   `rules.uc`.
3. Import [`examples/custom_scanner.py`](examples/custom_scanner.py) as a notebook (it is in
   Databricks notebook source format), set the two `/Volumes/...` paths, and run all cells:

```python
%pip install /Volumes/main/default/lakehouse_gc/lakehouse_gc-0.2.1-py3-none-any.whl
```

```python
from lakehouse_gc import rules_dataframe, run

POLICY_PATH = "/Volumes/main/default/lakehouse_gc/policy.yaml"
display(rules_dataframe(spark, POLICY_PATH, scanners=["uc"]))  # what each check means

result = run(POLICY_PATH, scanners=["uc"], spark=spark)
print(result.summary())
display(result.to_dataframe(spark))
```

For a tour of **every** option, with comments listing the allowed values, see
[`examples/all_options.py`](examples/all_options.py). It covers `run()` arguments, result
fields, `@check` / `define_scanner`, subclassing `Scanner`, policy in code, the registry,
errors, and JSON logging. It is also a Databricks notebook, and a test runs it end to end.

`rules_dataframe(spark, POLICY_PATH, scanners=[...])` lists every rule the run would apply.
Its columns are: scanner, rule, title, meaning, severity (with its scoring weight), affects
(governance or cost), on_fail, and settings, which shows the effective values from your
policy. The last row lists your exclusions. It validates the policy exactly as `run()` does
but scans nothing. Outside notebooks, use `print(rules_text(POLICY_PATH))`, or
`describe_rules(...)` for plain dicts.

`result.summary()` prints scores, coverage, finding counts, failures, and warnings.
`result.to_dataframe(spark)` returns one row per finding with these columns: scanner,
resource_type, resource, rule, severity, recommendation, downgraded_from, confidence,
reasons, and excluded_reason. `result.to_rows()` returns the same data as plain dicts.

If you reinstall a rebuilt wheel with the same version, use `%pip install --force-reinstall
...` followed by `dbutils.library.restartPython()`.

### `run()` arguments

`run(config_path | policy=..., scanners=None, spark=None, registry=None, clock=None,
run_id=None)`. Provide exactly one of `config_path` or `policy` (a `Policy` object).

- `scanners`: registered names (`"uc"`, `"example"`) and/or scanner classes. Defaults to
  all registered scanners.
- `spark`: a SparkSession, or any object with `query(statement, params)`. Defaults to the
  active session.
- `/Volumes/...` paths work as `config_path` because they are local file paths.

### Local development

```bash
uv sync               # create .venv with runtime + dev dependencies
uv run pytest         # run tests (no Databricks needed)
uv build              # build sdist and wheel into dist/
```

`run("examples/policy.yaml", scanners=["example"])` runs anywhere. Without Spark, the `uc`
scanner fails in isolation and says what to do.

Requires Python 3.10+ (Databricks Runtime 13.3 LTS and later). The only runtime dependency
is PyYAML. pyspark is used only when present, at runtime, and is not a dependency.

## Unity Catalog scanner (`uc`)

The scanner reads `system.information_schema` (`catalogs`, `schemata`, `tables`,
`table_tags`, `volumes`). For stale-table candidates only, it also runs `DESCRIBE DETAIL`.
It skips `information_schema` schemas.

| Rule | Applies to | Fails when | On fail |
|---|---|---|---|
| `uc.owner_missing` | catalog, schema, table | no owner | tag_owner |
| `uc.comment_missing` | catalog, schema, table | no comment (`require_comment`) | review |
| `uc.required_tags_missing` | table | any of `required_tags` is absent | tag_owner |
| `uc.temporary_name` | schema, table | name matches `temp_name_patterns` | review |
| `uc.empty_container` | catalog, schema | no visible tables or volumes | review |
| `uc.stale_table` | managed/external table | no data written in `stale_table_days` and size ≥ `min_table_size_gb` | archive_candidate |

`rules.uc` settings (all optional): `stale_table_days` (180), `min_table_size_gb` (10, where
1 GB = 1024³ bytes; 0 means any size), `required_tags` ([]), `require_comment` (true),
`temp_name_patterns`, `check_empty_containers` (true), and `max_detail_queries` (200).

How evidence limits are reported:

- **Staleness is inferred.** `last_altered` tracks metadata changes, and `lastModified`
  (from `DESCRIBE DETAIL`) tracks data writes. Neither is a read or access time, so
  confidence is 50 and `archive_candidate` is downgraded to `review`. Confirm with lineage
  or audit logs before acting.
- **Empty means "nothing visible to you".** `information_schema` only shows objects the
  running principal can access, and functions and models are not counted. Empty containers
  are inferred evidence and get `review`.
- **Unreadable data becomes unknown.** If tags or volumes can't be read, or `DESCRIBE DETAIL`
  fails or exceeds `max_detail_queries`, the check is `indeterminate`. It is scored as
  partially failed (`unknown_penalty`) and listed in `RunResult.warnings`. If listing
  catalogs, schemas, or tables fails, the `uc` scanner fails and the other scanners still
  run.
- **Naming is a heuristic,** so temporary-looking names are inferred evidence.

## Architecture

```
src/lakehouse_gc/
  core/
    models.py     Resource, RuleEvaluation, ScoredEvaluation, Finding, enums
    evidence.py   Evidence + EvidenceStatus, derive_confidence
    policy.py     Policy model + strict YAML loader
    config.py     scanner rules-section validation
    scanner.py    Scanner ABC, RuleSpec, ScannerContext
    registry.py   ScannerRegistry
    scoring.py    aggregate scores + deduction audit trail
    savings.py    savings summary (no double counting)
    results.py    ScannerResult, RunResult (+ summary, to_rows, to_dataframe)
    sql.py        read-only SqlClient protocol, Spark adapter
    log.py        structured logging helpers
  scanners/example.py   deterministic fixture-backed scanner
  scanners/uc.py        Unity Catalog scanner
  simple.py             check / define_scanner (function-based scanners)
  runner.py             run()
```

### Scanner lifecycle

Each scanner runs `discover → analyze → score → recommend`. Values pass explicitly between
phases and are immutable, and every run gets a fresh scanner instance.

| Phase | Input | Output | Notes |
|---|---|---|---|
| `discover` | context | `Resource`s | Unique `ResourceRef`s only |
| *(framework)* | resources | excluded / included | Policy exclusions applied; excluded resources get an `EXCLUDED` finding |
| `analyze` | included resources | `RuleEvaluation`s | `PASS`, `FAIL`, or `INDETERMINATE`; at most one per (resource, rule) |
| `score` | evaluations | `ScoredEvaluation`s | Default: severity weights from policy, confidence from evidence |
| `recommend` | scored evaluations | `Finding`s | One per evaluation at most; never `EXCLUDED` |
| *(framework)* | findings | gated findings | Candidate-confidence gate; aggregate scoring |

The framework validates every phase's output. A contract violation (wrong type, duplicate
resource, undeclared rule, evaluating an excluded resource, and so on) fails that scanner
the same way an exception does.

### Failure isolation

- Configuration errors raise **before** any scanner runs: an invalid policy, unknown
  scanner names, `rules` for unregistered scanners, or invalid scanner rules.
- During a run, any `Exception` in a scanner (including in `__init__`) marks only that
  scanner `FAILED`, records `phase`, `error_type`, and `message`, and logs the traceback.
  Other scanners still run. `KeyboardInterrupt` and `SystemExit` are not caught.
- Run status: `SUCCEEDED` (no failures), `PARTIAL` (some failed), `FAILED` (all failed).
- Failed scanners contribute nothing to scores. The `ScoreReport` sets `complete=False` and
  lists `failed_scanners`, so a crashed scanner never looks like a clean bill of health.
- Scanners run in sorted name order; resources and findings are sorted by stable keys.

## Scoring

Scores range from 0 to 100, and higher is healthier. Evaluations from all successful scanners
are pooled. For each dimension *d* (governance, cost), each evaluation whose rule affects
*d* has weight *w* = the policy severity weight of the rule's severity:

```
penalty p:  PASS = 0,  FAIL = 1,  INDETERMINATE = scoring.unknown_penalty  (0 < u ≤ 1)

score_d     = 100 × (1 − Σ w·p / Σ w)        None if Σ w = 0 (not assessable)
deduction_i = 100 × w_i·p_i / Σ w            Σ deductions = 100 − score_d exactly
coverage_d  = (# PASS/FAIL) / (# evaluations)

overall     = Σ W_d·score_d / Σ W_d          W = governance_weight, cost_weight (normalized)
              None if any dimension with W_d > 0 is None
grade       = highest grade whose minimum ≤ overall (scoring.grade_boundaries)
```

- **No reward for missing visibility.** Indeterminate outcomes always cost points, and
  `unknown_penalty: 0` is rejected. A dimension with nothing to evaluate is `None`, not 100.
- **Deterministic.** Inputs are sorted and arithmetic uses exact fractions
  (`exact_score`, `Deduction.points`). Only `score` and `overall` are rounded to 2 decimals.
- **Audit trail.** `ScoreReport.<dimension>.deductions` lists every evaluation that cost
  points: scanner, resource, rule, outcome, weight, penalty factor, and points.
- **Confidence** is evidence quality, not the probability that acting is safe. By default it
  is the mean of per-evidence status scores (observed 100, inferred 50, unavailable 0),
  rounded half up, or `None` when there is no evidence. Scanners may override `score()`.

Defaults: severity weights info 0, low 1, medium 3, high 5, critical 8. Grades A ≥ 90,
B ≥ 80, C ≥ 70, D ≥ 60, F ≥ 0. Dimension weights 0.5 / 0.5. Unknown penalty 0.5.

Savings: `RunResult.savings.total` is `None` unless findings carry a `SavingsEstimate`,
which requires assumptions and coverage. Overlapping estimates on the same resource are
not summed (the largest is used). The built-in scanners emit no estimates yet.

## Policy

See [`examples/policy.yaml`](examples/policy.yaml) for every key. The rules are:

- `policy_version: 1` is required. Other versions raise `UnsupportedPolicyVersionError`.
- Unknown keys, wrong types, and **duplicate YAML keys** are errors. Messages name the
  dotted path, e.g. `scoring.cost_wieght: unknown key (did you mean 'cost_weight'?)`.
- `rules.<scanner>` is validated by that scanner's `config_model`. A section for a scanner
  that isn't registered is an error, to catch typos.
- `exclusions[]`: `resource_type`, `pattern` (case-insensitive glob on `Resource.name`),
  `reason`, and optional `expires_on`.
  - `resource_type` must be a type a scanner produces. For `uc` these are `catalog`,
    `schema`, and `table`; tables include internal or event-log tables. An exclusion whose
    type no scanner produces can never match, so the run reports it in
    `RunResult.warnings`, e.g. `resource_type 'internal_tables', which no scanner
    produces`.
  - `expires_on` makes an exclusion temporary, for holds like a migration window or a
    pending review. It applies through that date (inclusive). Afterwards it is ignored, the
    resources are checked again, and a warning notes the expiry. `null` means permanent.
- `scope` selects Unity Catalog objects (see below) and is exposed as `ctx.policy.scope`.
- The `governance` and `outputs` sections from the vision document are not part of the
  schema yet.

### Scope

```yaml
scope:
  include_catalogs: ["main", "prod_*"]
  exclude_catalogs: ["system", "samples", "__databricks_internal"]
  include_schemas: ["main.sales"]           # catalog.schema
  exclude_schemas: ["main.scratch"]
  include_tables: ["main.sales.fact_*"]     # catalog.schema.table
  exclude_tables: []
```

- Names must be fully qualified (1, 2, or 3 parts). Patterns are case-insensitive globs.
- An empty include list means "all". Excludes always win.
- Narrower includes also narrow the levels above them. For example, `include_tables:
  ["main.sales.orders"]` alone scans only catalog `main`, schema `main.sales`, and that
  table.
- **Scope vs. exclusions:** objects outside the scope are never read. Objects matched by
  `exclusions` are read but not analyzed, and appear as `excluded` findings.

Policies can also be built in code (`Policy(...)`), and they enforce the same invariants.

## Writing a scanner

### Simple: functions plus `define_scanner`

For most scanners you only write check functions. A check receives one resource (your
dict) and, optionally, the config mapping. It returns `True` (pass), `False` (fail), or
`None` (data unavailable, which is never counted as a pass). For a custom reason, return
`ok("...")`, `fail("...")`, or `unknown("...")`. Pass `inferred=True` to `ok`/`fail` when
the verdict rests on indirect evidence; this lowers confidence.

```python
from lakehouse_gc import check, define_scanner, run, unknown

TABLES = [
    {
        "name": "main.sales.orders",
        "owner": "team-sales@example.com",
        "tags": {"cost_center": "cc1"},
    },
    {"name": "main.tmp.scratch", "owner": None, "tags": {}},
    {"name": "main.legacy.unknown", "owner": None, "tags": None},
]


@check(severity="low")
def has_cost_center(table, config):
    if table["tags"] is None:
        return unknown("Tags could not be read.")
    return config["required_tag"] in table["tags"]


@check(severity="medium", on_fail="tag_owner")
def has_owner(table):
    return bool(table["owner"])


tags = define_scanner(
    "tags",
    resources=TABLES,  # or a function returning the list
    checks=[has_cost_center, has_owner],
    config={"required_tag": "cost_center"},  # defaults; override under rules.tags
    resource_type="table",
)

result = run("/Volumes/admin/config/policy.yaml", scanners=["example", tags])
```

- `@check` options: `severity` (info, low, medium, high, critical), `dimension`
  (`governance`, `cost`, or a list), `on_fail` (review, tag_owner, archive_candidate,
  pause_candidate, delete_candidate), `rule`, and `title`. Undecorated functions default to
  medium / governance / review. The rule ID is `<scanner>.<function name>`.
- Resource dicts need `name`. They may also set `id` (default: name), `type` (default:
  `resource_type`, otherwise the scanner name), `scope` (default `default`), and `owner`.
- `config` defaults set the type of each key (bool, int, float, str, or list of str). The
  policy's `rules.<scanner>` section may override them, and wrong types or unknown keys are
  rejected.
- Scanner classes passed to `run()` are registered for that run only.
- A check that raises or returns anything else fails its scanner, isolated like any other
  failure.

### Custom scanners in a notebook

Custom scanners work in notebooks the same way. Define them in a cell and pass them next to
the built-ins: `run(POLICY_PATH, scanners=["uc", tags], spark=spark)`. To view findings
from all scanners as a table:

```python
display(result.to_dataframe(spark))
```

A policy section `rules.<name>` for a scanner that isn't part of the run is rejected, to
catch typos. Only add `rules.tags` to a policy that is always run with the `tags` scanner,
or rely on the `config` defaults.

### Advanced: subclass `Scanner`

Subclass when you need full control: custom `Evidence` values, several pieces of evidence
per evaluation, or your own confidence in `score()`. See
`src/lakehouse_gc/scanners/example.py`.

```python
from dataclasses import dataclass
from lakehouse_gc.core import Dimension, RuleSpec, Scanner, Severity


@dataclass(frozen=True)
class MyConfig:  # fields: bool/int/float/str/date, tuple[scalar, ...], Optional
    idle_days: int = 30


class MyScanner(Scanner):
    name = "mine"  # ^[a-z][a-z0-9_]*$
    rules = (RuleSpec("mine.idle", "Idle", Severity.HIGH, frozenset({Dimension.COST})),)
    config_model = MyConfig  # validates policy `rules.mine`

    def discover(self, ctx): ...  # -> Sequence[Resource]
    def analyze(self, ctx, resources): ...  # -> Sequence[RuleEvaluation]
    # score() is inherited; override only to refine confidence
    def recommend(self, ctx, scored): ...  # -> Sequence[Finding]


result = run("policy.yaml", scanners=[MyScanner])  # or register it in a ScannerRegistry
```

Guidelines for both styles:

- Never mutate resources. Read-only access only.
- When a signal can't be obtained, report it as unknown (`None`/`unknown()`, or
  `EvidenceStatus.UNAVAILABLE` with `Outcome.INDETERMINATE`). Never guess timestamps,
  counts, or traffic.
- Set the resource name to the fully qualified name, because exclusions match against it.
- Don't apply exclusions or the candidate gate yourself; the framework does both.

Registry API: `register` (also works as a decorator), `copy`, `get`, `names`, and
`resolve`. It raises `DuplicateScannerError`, `UnknownScannerError` (with "did you mean"
hints), and `InvalidScannerError`.

## Logging

The library logs through standard `logging` under the `lakehouse_gc` logger. It adds a
`NullHandler` and never configures the root logger. Records carry structured `extra` fields
(`event`, `run_id`, `scanner`, `phase`, `status`, ...). For JSON lines:

```python
import logging
from lakehouse_gc.core.log import JsonFormatter

handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logging.getLogger("lakehouse_gc").addHandler(handler)
logging.getLogger("lakehouse_gc").setLevel(logging.INFO)
```

## Development

```bash
uv sync
uv run pytest
uv run ruff check . && uv run ruff format --check .
uv run mypy
uv build
```

Exceptions all derive from `lakehouse_gc.core.LakehouseGCError`.

## License

Source-available under the [Databricks License](LICENSE). See [NOTICE](NOTICE) for
third-party attributions.
