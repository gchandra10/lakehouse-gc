# Examples

Eight ready-to-run notebooks (`.ipynb`), from easy to advanced. They render on GitHub, and
Databricks imports them directly. Import any file from
[`examples/`](../examples) into your workspace, set the `/Volumes/...` paths in the first
two cells, and run all. Each notebook stands on its own, and every one is run end to end
by the test suite.

| # | Level | Notebook | You learn |
|---|---|---|---|
| 1 | Easy | [`01_easy_quickstart.ipynb`](../examples/01_easy_quickstart.ipynb) | Check access, see the rules, scan, view results |
| 2 | Easy | [`02_easy_scope_and_exclusions.ipynb`](../examples/02_easy_scope_and_exclusions.ipynb) | Choose catalogs, schemas, and tables; exclude resources |
| 3 | Easy | [`03_easy_reading_results.ipynb`](../examples/03_easy_reading_results.ipynb) | Scores, audit trail, findings, evidence, limitations |
| 4 | Intermediate | [`04_intermediate_restricted_access.ipynb`](../examples/04_intermediate_restricted_access.ipynb) | No `system` access, `per_catalog`, your org's views |
| 5 | Intermediate | [`05_intermediate_custom_checks.ipynb`](../examples/05_intermediate_custom_checks.ipynb) | Your own checks with `@check` and `define_scanner` |
| 6 | Intermediate | [`06_intermediate_policy_in_code.ipynb`](../examples/06_intermediate_policy_in_code.ipynb) | Policy as Python objects; what-if runs |
| 7 | Advanced | [`07_advanced_scanner_class.ipynb`](../examples/07_advanced_scanner_class.ipynb) | A full `Scanner` subclass |
| 8 | Advanced | [`08_advanced_registry_errors_logging.ipynb`](../examples/08_advanced_registry_errors_logging.ipynb) | `run()` options, registry, errors, JSON logs |

The snippets below show the key idea from each notebook. They assume a notebook, where
`spark` and `display` are available, and this policy path:

```python
POLICY_PATH = "/Volumes/main/default/lakehouse_gc/policy.yaml"
```

---

## Easy

### 1. Quick start

```python
from lakehouse_gc import check_access, rules_dataframe, run

print(check_access(spark, POLICY_PATH, scanners=["uc"]).summary())  # what can I read?
display(rules_dataframe(spark, POLICY_PATH, scanners=["uc"]))  # what will be checked?

result = run(POLICY_PATH, scanners=["uc"], spark=spark)
print(result.summary())  # scores, grade, counts, limitations
display(result.to_dataframe(spark))  # one row per finding
```

### 2. Choose what to scan

```yaml
scope:
  include_catalogs: ["main"]  # empty = all
  include_schemas: ["main.sales"]  # catalog.schema
  exclude_tables: ["main.sales.tmp_*"]  # catalog.schema.table
exclusions:  # read but not analyzed, still shown as "excluded"
  - resource_type: table  # uc types: catalog, schema, table
    pattern: "main.sales.legacy_*"
    reason: "Migration hold"
    expires_on: 2026-12-31  # applies through this date; null = permanent
```

| You want to scan | Use |
|---|---|
| One catalog | `include_catalogs: ["main"]` |
| Two schemas | `include_schemas: ["main.sales", "main.marketing"]` |
| Specific tables only | `include_tables: ["main.sales.orders", "main.sales.fact_*"]` |
| Everything except a schema | `exclude_schemas: ["main.scratch"]` |

### 3. Reading the results

```python
scores = result.scores
print(scores.governance.score, scores.cost.score, scores.overall, scores.grade)
print(scores.cost.coverage)  # share of checks with known evidence
for d in scores.cost.deductions:  # audit trail
    print(d.resource.resource_id, d.rule_id, round(float(d.points), 2))

for f in result.findings:
    print(f.resource.resource_id, f.rule_id, f.recommendation.value, f.confidence, f.reasons)

print(result.limitations)  # what couldn't be seen; those checks are unknown, not passed
print(result.warnings, result.errors, result.savings.total)
```

Recommendations (`keep`, `review`, `tag_owner`, `archive_candidate`, `pause_candidate`,
`delete_candidate`, `excluded`) are advisory only. A `*_candidate` is lowered to `review`
unless confidence is at least `actions.min_candidate_confidence`.

---

## Intermediate

### 4. Restricted access and organization views

| Your situation | `sources:` in `policy.yaml` |
|---|---|
| You can read `system.information_schema` | nothing (default) |
| The `system` catalog is blocked | `information_schema: per_catalog` |
| Your org publishes a schema of metadata views | `information_schema: governance.uc_meta` |
| One view is different or renames columns | see below |

```yaml
sources:
  information_schema: per_catalog
  tables:
    table: governance.uc_meta.all_tables
    columns: {table_owner: owner_principal, last_altered: changed_at}  # logical: yours
```

```python
report = check_access(spark, POLICY_PATH, scanners=["uc"])
display(report.to_dataframe(spark))  # status, missing columns, impact, how to fix
print(report.can_run("uc"))
```

Each source is `available`, `partial` (some optional columns missing, so only the checks
that need them become unknown), or `unavailable`.

### 5. Your own checks

```python
from lakehouse_gc import check, define_scanner, run, unknown


@check(severity="low", on_fail="tag_owner")
def has_cost_center(table, config):
    if table["tags"] is None:
        return unknown("Tags could not be read.")
    return config["required_tag"] in table["tags"]  # True = pass, False = fail


tags = define_scanner(
    "tags",
    resources=[{"name": "main.sales.orders", "tags": {"cost_center": "cc1"}}],
    checks=[has_cost_center],
    config={"required_tag": "cost_center"},
    resource_type="table",
)
result = run(POLICY_PATH, scanners=["uc", tags], spark=spark)
```

The `@check` options are `rule`, `title`, `description`, `severity`, `dimension`, and
`on_fail`. The notebook shows every option and return style.

### 6. Policy in code

```python
from lakehouse_gc.core import Policy
from lakehouse_gc.core.policy import Exclusion, Scope

policy = Policy(
    scope=Scope(include_catalogs=("main",)),
    exclusions=(Exclusion("table", "*.__materialization*", "Internal tables"),),
    sources={"information_schema": "per_catalog"},
    rules={"uc": {"stale_table_days": 90}},
)
result = run(policy=policy, scanners=["uc"], spark=spark)
```

---

## Advanced

### 7. A scanner class

Subclass `Scanner` and implement `discover`, `analyze`, and `recommend` (`score` is
inherited). Declare `rules` with `RuleSpec`, and optionally a typed `config_model`, your
`resource_types`, and `sources`. See the notebook for a complete, working example.

### 8. Registry, run options, errors, and logging

```python
from lakehouse_gc.core import PolicyValidationError, get_default_registry, loads_policy

registry = get_default_registry().copy()  # built-ins plus your own
registry.register(tags)
run(POLICY_PATH, scanners=["uc", "tags"], spark=spark, registry=registry, run_id="check-1")

try:
    loads_policy("policy_version: 1\nsources:\n  tabels: org.meta.tables\n")
except PolicyValidationError as error:
    print(error.path, error.message)  # sources.tabels: unknown source (did you mean 'tables'?)
```
