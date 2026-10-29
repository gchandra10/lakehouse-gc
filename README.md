# Lakehouse Governance & Cost

[![CI](https://github.com/gchandra10/lakehouse-gc/actions/workflows/ci.yml/badge.svg)](https://github.com/gchandra10/lakehouse-gc/actions/workflows/ci.yml)

**lakehouse-gc** is a read-only Python package that checks your Databricks lakehouse for
governance gaps and cost waste. It explains every result, and it never changes anything.

## What it does

- **Scans Unity Catalog:** checks the catalogs, schemas, and tables you choose in a simple
  `policy.yaml`.
- **Checks governance:** finds missing owners, missing descriptions, missing required tags,
  temporary-looking names, and empty catalogs or schemas.
- **Checks cost:** finds large tables with no recent data writes.
- **Scores your estate:** gives a Governance, Cost, and Overall score from 0 to 100, plus a
  grade, with an audit trail of what cost points.
- **Explains itself:** every finding includes reasons, evidence, and a confidence level.
  `rules_dataframe()` shows what each rule means and the settings your policy applies.
- **Stays honest:** missing data is reported as *unknown*, never guessed. Inactivity is
  never treated as proof that something is unused.
- **Stays safe:** report-only. It recommends `keep`, `review`, `tag_owner`, and similar,
  but never deletes, pauses, or modifies anything.
- **Is extensible:** you can add your own checks with a few plain Python functions.

## Usage

In a Databricks notebook, after installing the wheel:

```python
from lakehouse_gc import rules_dataframe, run

POLICY_PATH = "/Volumes/main/default/lakehouse_gc/policy.yaml"

display(rules_dataframe(spark, POLICY_PATH, scanners=["uc"]))  # what will be checked

result = run(POLICY_PATH, scanners=["uc"], spark=spark)
print(result.summary())  # scores, grade, counts
display(result.to_dataframe(spark))  # one row per finding
```

Choose what to scan in `policy.yaml`:

```yaml
policy_version: 1
scope:
  include_catalogs: ["main"]
  include_schemas: ["main.sales"]
exclusions:
  - resource_type: table
    pattern: "main.sales.archive_*"
    reason: "Retention policy"
    expires_on: null
rules:
  uc:
    stale_table_days: 180
    required_tags: ["cost_center"]
```

Start from [`examples/policy.yaml`](examples/policy.yaml), which documents every key.

Example output:

```text
Run 3f9c...: succeeded (uc)
  Governance: 68.83  (43 checks, 100% with known evidence)
  Cost: 70  (5 checks, 80% with known evidence)
  Overall: 69.42  grade: D
  Findings: excluded=1, keep=34, review=8, tag_owner=6
```

## Learn more

- **[Examples](docs/EXAMPLES.md):** custom checks, policy in code, reading results, and more.
- **[Details](docs/DETAILS.md):** installation, rules, scoring, policy reference, and
  architecture.

## License

Source-available under the [Databricks License](LICENSE). See [NOTICE](NOTICE).
