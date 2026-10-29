# Examples

Every example below uses the public API. To try them all at once, import
[`examples/all_options.py`](../examples/all_options.py) as a Databricks notebook; a test
runs it end to end. The examples assume a notebook, where `spark` and `display` are
available.

- [1. Explain the rules](#1-explain-the-rules)
- [2. Run a scan](#2-run-a-scan)
- [3. Read the results](#3-read-the-results)
- [4. Choose what to scan](#4-choose-what-to-scan)
- [5. Exclude resources](#5-exclude-resources)
- [6. Add your own checks](#6-add-your-own-checks)
- [7. Advanced: subclass `Scanner`](#7-advanced-subclass-scanner)
- [8. Build the policy in code](#8-build-the-policy-in-code)
- [9. Registry, errors, and logging](#9-registry-errors-and-logging)

---

## 1. Explain the rules

These helpers show what each rule checks and the settings your policy applies. They
validate the policy just as `run()` does, but scan nothing.

```python
from lakehouse_gc import describe_rules, rules_dataframe, rules_text

POLICY_PATH = "/Volumes/main/default/lakehouse_gc/policy.yaml"

display(rules_dataframe(spark, POLICY_PATH, scanners=["uc"]))  # table
print(rules_text(POLICY_PATH, scanners=["uc"]))  # plain text
rows = describe_rules(POLICY_PATH, scanners=["uc"])  # list of dicts
```

Columns: `scanner`, `rule`, `title`, `meaning`, `severity` (with its scoring weight),
`affects` (governance or cost), `on_fail`, and `settings`. The last row lists your
exclusions.

## 2. Run a scan

```python
from datetime import datetime, timezone

from lakehouse_gc import run

result = run(
    POLICY_PATH,  # or policy=Policy(...) (see section 8), never both
    scanners=["uc"],  # names and/or scanner classes; None = all registered
    spark=spark,  # SparkSession or any object with query(statement, params)
    run_id="manual-check-001",  # optional; default: random
    clock=lambda: datetime.now(timezone.utc),  # optional; must be timezone-aware
)
```

| Argument | Default | Notes |
|---|---|---|
| `config_path` | — | Path to `policy.yaml`; `/Volumes/...` paths work |
| `policy` | — | A `Policy` object instead of a file |
| `scanners` | all registered | `"uc"`, `"example"`, or your own scanner classes |
| `spark` | active session | Needed by `uc` |
| `registry` | built-in | A custom `ScannerRegistry` (section 9) |
| `run_id`, `clock` | random, UTC now | Useful for tests and reproducibility |

Built-in scanners: `uc` (Unity Catalog) and `example` (mock data, for demos).

## 3. Read the results

```python
print(result.summary())  # scores, coverage, counts, failures, warnings
display(result.to_dataframe(spark))  # one row per finding
rows = result.to_rows()  # same rows as plain dicts

print(result.status.value)  # succeeded | partial | failed

scores = result.scores
print(scores.governance.score, scores.cost.score, scores.overall, scores.grade)
print(scores.governance.coverage)  # share of checks with known evidence
for d in scores.cost.deductions:  # audit trail: what cost points
    print(d.resource.resource_id, d.rule_id, d.outcome.value, round(float(d.points), 2))

for f in result.findings:
    print(
        f.resource.resource_id,  # e.g. main.sales.orders
        f.rule_id,  # e.g. uc.stale_table
        f.recommendation.value,  # keep | review | tag_owner | archive_candidate | ...
        f.confidence,  # 0-100 evidence quality, or None
        f.reasons,
    )
    for e in f.evidence:  # observed | inferred | unavailable
        print("  ", e.key, e.status.value, e.value, e.source)

print(result.excluded_findings)  # resources protected by policy
print(result.warnings)  # expired exclusions, unreadable data, typos
print(result.errors)  # scanners that failed, and in which phase
print(result.savings.total)  # None = unknown (never a guess)
print(result.scanner_result("uc").resources)
```

Recommendation values: `keep`, `review`, `tag_owner`, `archive_candidate`,
`pause_candidate`, `delete_candidate`, `excluded`. They are advisory only. `*_candidate`
values are lowered to `review` (shown in `downgraded_from`) unless confidence is at least
`actions.min_candidate_confidence`.

## 4. Choose what to scan

```yaml
scope:
  include_catalogs: ["main", "prod_*"]  # empty = all
  exclude_catalogs: ["system", "samples", "__databricks_internal"]
  include_schemas: ["main.sales"]  # catalog.schema
  exclude_schemas: ["main.scratch"]
  include_tables: ["main.sales.fact_*"]  # catalog.schema.table
  exclude_tables: []
```

| You want to scan | Use |
|---|---|
| One catalog | `include_catalogs: ["main"]` |
| Two schemas | `include_schemas: ["main.sales", "main.marketing"]` |
| Specific tables only | `include_tables: ["main.sales.orders", "main.sales.fact_*"]` |
| Everything except a schema | `exclude_schemas: ["main.scratch"]` |

Patterns are case-insensitive globs, and excludes always win. Listing only tables also
limits the scan to those tables' schemas and catalogs.

## 5. Exclude resources

Excluded resources are read but not analyzed. They still appear as `excluded` findings,
so nothing is hidden.

```yaml
exclusions:
  - resource_type: table  # uc types: catalog, schema, table
    pattern: "main.finance.*"
    reason: "Retention policy"
    expires_on: null  # permanent
  - resource_type: table
    pattern: "*.__materialization*"  # internal tables behind materialized views
    reason: "Internal tables"
    expires_on: null
  - resource_type: schema
    pattern: "main.migration_*"
    reason: "Migration hold"
    expires_on: 2026-12-31  # applies through this date, then ignored with a warning
```

`resource_type` must match a type the scanner produces. If it doesn't (for example
`internal_tables`), the run warns that the exclusion never matches.

## 6. Add your own checks

Write plain functions and turn them into a scanner. A check returns `True` (pass),
`False` (fail), or `None` (data unavailable). Return `ok(...)`, `fail(...)`, or
`unknown(...)` to add a reason.

```python
from lakehouse_gc import check, define_scanner, fail, ok, run, unknown


@check(
    rule="has_cost_center",  # default: function name -> "tags.has_cost_center"
    title="Has a cost center tag",  # default: from the function name
    description="Every table needs a cost_center tag.",  # default: docstring
    severity="low",  # info | low | medium | high | critical (default: medium)
    dimension="governance",  # governance | cost | ["governance", "cost"]
    on_fail="tag_owner",  # review | tag_owner | archive_candidate |
    #                       pause_candidate | delete_candidate (default: review)
)
def has_cost_center(table, config):  # config is optional
    if table["tags"] is None:
        return unknown("Tags could not be read.")  # or: return None
    if config["required_tag"] in table["tags"]:
        return ok("Cost center present.")  # or: return True
    return fail("Missing cost center.")  # or: return False


@check  # all defaults
def has_owner(table):
    """Tables need an owner."""
    return bool(table["owner"])


@check(dimension="cost", on_fail="archive_candidate")
def recently_written(table):
    if table["days_since_write"] is None:
        return None
    # inferred=True: indirect evidence -> lower confidence -> candidate becomes review
    return ok(inferred=True) if table["days_since_write"] <= 365 else fail(inferred=True)


def load_tables(config):  # a list also works; a function may take the config
    return [
        {
            "name": "main.sales.orders",  # required (exclusions match on it)
            "id": "main.sales.orders",  # optional, default: name
            "type": "table",  # optional, default: resource_type below
            "scope": "my-workspace",  # optional, default: "default"
            "owner": "team-sales@example.com",  # optional
            "tags": {"cost_center": "cc1"},  # any other keys go to your checks
            "days_since_write": 3,
        },
        {"name": "main.tmp.scratch", "owner": None, "tags": {}, "days_since_write": 500},
    ]


tags = define_scanner(
    "tags",  # lowercase letters, digits, underscores
    resources=load_tables,
    checks=[has_cost_center, has_owner, recently_written],
    config={"required_tag": "cost_center"},  # defaults; override under rules.tags
    resource_type="table",
)

result = run(POLICY_PATH, scanners=["uc", tags], spark=spark)
```

Config values may be a bool, int, float, str, or list of str. The policy's `rules.tags`
section may override them, and wrong types or unknown keys are rejected. A check that
raises an error fails only its own scanner.

## 7. Advanced: subclass `Scanner`

Subclass `Scanner` for full control over evidence and confidence. Its lifecycle is
`discover → analyze → score → recommend`, and `score` is inherited.

```python
from dataclasses import dataclass

from lakehouse_gc import run
from lakehouse_gc.core import (
    Dimension,
    Evidence,
    EvidenceStatus,
    Finding,
    Outcome,
    Recommendation,
    Resource,
    ResourceRef,
    RuleEvaluation,
    RuleSpec,
    Scanner,
    Severity,
)


@dataclass(frozen=True)
class JobsConfig:  # typed settings for rules.jobs_demo
    max_idle_days: int = 90


class JobsScanner(Scanner):
    name = "jobs_demo"
    resource_types = ("job",)
    config_model = JobsConfig
    rules = (
        RuleSpec(
            "jobs_demo.idle",  # must start with "<scanner name>."
            "Job has not run recently",
            Severity.MEDIUM,
            frozenset({Dimension.COST}),
            description="Jobs idle longer than max_idle_days.",
            settings=("max_idle_days",),
            on_fail=Recommendation.REVIEW,
        ),
    )

    def discover(self, ctx):
        return [
            Resource(
                ResourceRef("my-workspace", "job", "job-1"),
                name="nightly_etl",
                evidence=(Evidence("days_idle", EvidenceStatus.OBSERVED, "demo", value=2),),
            ),
            Resource(
                ResourceRef("my-workspace", "job", "job-2"),
                name="old_export",
                evidence=(Evidence("days_idle", EvidenceStatus.UNAVAILABLE, "demo"),),
            ),
        ]

    def analyze(self, ctx, resources):
        evaluations = []
        for resource in resources:
            evidence = resource.get_evidence("days_idle")
            if evidence.status is EvidenceStatus.UNAVAILABLE:
                outcome = Outcome.INDETERMINATE  # unknown, never guessed
            elif evidence.value > ctx.config.max_idle_days:
                outcome = Outcome.FAIL
            else:
                outcome = Outcome.PASS
            evaluations.append(RuleEvaluation(resource.ref, "jobs_demo.idle", outcome, (evidence,)))
        return evaluations

    def recommend(self, ctx, scored):
        return [
            Finding(
                resource=s.evaluation.resource,
                rule_id=s.evaluation.rule_id,
                severity=s.severity,
                recommendation=Recommendation.KEEP
                if s.evaluation.outcome is Outcome.PASS
                else Recommendation.REVIEW,
                evidence=s.evaluation.evidence,
                confidence=s.confidence,
                impacts=frozenset({Dimension.COST}),
            )
            for s in scored
        ]


result = run(POLICY_PATH, scanners=["uc", JobsScanner], spark=spark)
```

## 8. Build the policy in code

These objects have the same fields and validation as `policy.yaml`, and every field is
optional.

```python
from datetime import date

from lakehouse_gc import run
from lakehouse_gc.core import Policy, load_policy, loads_policy
from lakehouse_gc.core.policy import ActionsConfig, Exclusion, Scope, ScoringConfig

policy = Policy(
    scope=Scope(include_catalogs=("main",), exclude_tables=("main.sales.tmp_*",)),
    exclusions=(
        Exclusion("table", "*.__materialization*", "Internal tables"),
        Exclusion("table", "main.sales.legacy_*", "Migration hold", date(2026, 12, 31)),
    ),
    rules={"uc": {"stale_table_days": 90, "required_tags": ["cost_center"]}},
    scoring=ScoringConfig(
        governance_weight=0.6,  # normalized against cost_weight
        cost_weight=0.4,
        unknown_penalty=0.5,  # cost of an unknown check, in (0, 1]
        grade_boundaries=(("A", 90), ("B", 75), ("C", 50), ("D", 0)),  # must include 0
    ),
    actions=ActionsConfig(min_candidate_confidence=80),  # always report-only
)
result = run(policy=policy, scanners=["uc"], spark=spark)

from_file = load_policy(POLICY_PATH)  # parse without running
from_text = loads_policy("policy_version: 1\nscope:\n  include_catalogs: [main]\n")
```

## 9. Registry, errors, and logging

```python
import logging

from lakehouse_gc import run
from lakehouse_gc.core import PolicyValidationError, get_default_registry, loads_policy
from lakehouse_gc.core.log import JsonFormatter

# Registered scanners, and a private registry with your own added.
print(get_default_registry().names())  # ('example', 'uc')
registry = get_default_registry().copy()
registry.register(JobsScanner)
result = run(POLICY_PATH, scanners=["uc", "jobs_demo"], spark=spark, registry=registry)

# Configuration mistakes fail before scanning and name the exact key.
try:
    loads_policy("policy_version: 1\nscoring:\n  cost_wieght: 1\n")
except PolicyValidationError as error:
    print(error.path, "->", error.message)  # scoring.cost_wieght -> unknown key (did you mean ...

# Structured JSON logs (fields: event, run_id, scanner, phase, status, ...).
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logging.getLogger("lakehouse_gc").addHandler(handler)
logging.getLogger("lakehouse_gc").setLevel(logging.INFO)
```
