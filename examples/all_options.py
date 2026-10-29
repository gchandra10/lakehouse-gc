# Databricks notebook source
# MAGIC %md
# MAGIC # lakehouse-gc: every available option
# MAGIC
# MAGIC A tour of the whole public API. Each cell runs on its own after cells 1-2.
# MAGIC Everything is read-only: nothing in your workspace is changed.
# MAGIC
# MAGIC | Cell | Topic |
# MAGIC |---|---|
# MAGIC | 1 | Install |
# MAGIC | 2 | Explain the rules (`rules_dataframe`, `rules_text`, `describe_rules`) |
# MAGIC | 3 | `run()` and all of its arguments |
# MAGIC | 4 | Reading results: summary, tables, scores, findings, evidence |
# MAGIC | 5 | Custom checks with `@check` / `define_scanner` (all options) |
# MAGIC | 6 | Advanced: subclass `Scanner` |
# MAGIC | 7 | Policy in code instead of YAML |
# MAGIC | 8 | Registry, errors, and structured logging |
# MAGIC
# MAGIC Every `policy.yaml` key is documented in `examples/policy.yaml`.

# COMMAND ----------

# MAGIC %pip install /Volumes/main/default/lakehouse_gc/lakehouse_gc-0.2.1-py3-none-any.whl

# COMMAND ----------

# 2. Explain the rules. This validates the policy like run() does, but scans nothing.
from lakehouse_gc import describe_rules, rules_dataframe, rules_text

POLICY_PATH = "/Volumes/main/default/lakehouse_gc/policy.yaml"

display(rules_dataframe(spark, POLICY_PATH, scanners=["uc"]))  # table for notebooks
print(rules_text(POLICY_PATH, scanners=["uc"]))  # same, as text
rule_rows = describe_rules(POLICY_PATH, scanners=["uc"])  # same, as a list of dicts

# COMMAND ----------

# 3. run() and all of its arguments.
from datetime import datetime, timezone

from lakehouse_gc import run

result = run(
    POLICY_PATH,  # path to policy.yaml; or policy=Policy(...) (cell 7), never both
    scanners=["uc"],  # registered names ("uc", "example") and/or scanner classes (cells 5-6);
    #                   None = all registered scanners
    spark=spark,  # a SparkSession, or any object with query(statement, params);
    #               default: the active Spark session
    run_id="manual-check-001",  # optional; default: a random ID
    clock=lambda: datetime.now(timezone.utc),  # optional; must return timezone-aware times
    # registry=...,  # optional ScannerRegistry (cell 8); default: the built-in registry
)

# COMMAND ----------

# 4. Reading results.
print(result.summary())  # scores, coverage, counts, failures, warnings
display(result.to_dataframe(spark))  # one row per finding
rows = result.to_rows()  # the same rows as plain dicts

print(result.status.value)  # succeeded | partial | failed
print(result.run_id, result.started_at, result.finished_at, result.scanners)

scores = result.scores
for dimension in (scores.governance, scores.cost):
    # score: 0-100 or None (nothing assessable); coverage: share of checks with known evidence
    print(dimension.dimension.value, dimension.score, dimension.coverage, dimension.evaluations)
    for deduction in dimension.deductions[:5]:  # audit trail: what cost points, and how many
        print(
            "  -",
            deduction.resource.resource_id,
            deduction.rule_id,
            deduction.outcome.value,
            round(float(deduction.points), 2),
        )
print("overall", scores.overall, "grade", scores.grade, "complete", scores.complete)

for finding in result.findings[:5]:
    print(
        finding.scanner,
        finding.resource.resource_type,  # catalog | schema | table | your own types
        finding.resource.resource_id,  # e.g. main.sales.orders
        finding.rule_id,
        finding.severity.value,  # info | low | medium | high | critical
        finding.recommendation.value,  # keep | review | tag_owner | archive_candidate |
        #                                pause_candidate | delete_candidate | excluded
        finding.downgraded_from,  # set when a *_candidate was lowered to review
        finding.confidence,  # 0-100 evidence quality, or None if unknown
        finding.reasons,
    )
    for evidence in finding.evidence:
        # status: observed | inferred | unavailable (unavailable never carries a value)
        print("    ", evidence.key, evidence.status.value, evidence.value, evidence.source)

print([f.resource.resource_id for f in result.excluded_findings])  # protected by policy
print(result.warnings)  # e.g. expired exclusions, unreadable tags, exclusion type typos
print(result.errors)  # scanners that failed: scanner, phase, error_type, message
print(result.savings.total, result.savings.notes)  # None = unknown, never a guess
uc_result = result.scanner_result("uc")  # per scanner: status, resources, findings, error
print(uc_result.status.value, len(uc_result.resources))

# COMMAND ----------

# 5. Custom checks: plain functions plus define_scanner. Every option is shown.
from lakehouse_gc import check, define_scanner, fail, ok, unknown


@check(
    rule="has_cost_center",  # default: the function name -> rule id "tags.has_cost_center"
    title="Has a cost center tag",  # default: built from the function name
    description="Every table needs a cost_center tag for chargeback.",  # default: docstring
    severity="low",  # info | low | medium | high | critical (default: medium)
    dimension="governance",  # governance | cost | ["governance", "cost"] (default: governance)
    on_fail="tag_owner",  # review | tag_owner | archive_candidate | pause_candidate |
    #                       delete_candidate (default: review)
)
def has_cost_center(table, config):  # config is optional: def has_cost_center(table)
    if table["tags"] is None:
        return unknown("Tags could not be read.")  # or just: return None
    if config["required_tag"] in table["tags"]:
        return ok("Cost center present.")  # or just: return True
    return fail(f"Missing tag {config['required_tag']}.")  # or just: return False


@check  # bare decorator: all defaults
def has_owner(table):
    """Tables need an owner."""
    return bool(table["owner"])


@check(severity="medium", dimension="cost", on_fail="archive_candidate")
def recently_used(table):
    # inferred=True: the verdict rests on indirect evidence (lower confidence, so
    # *_candidate is lowered to review by the confidence gate)
    if table["days_since_write"] is None:
        return unknown("No write history.")
    if table["days_since_write"] > 365:
        return fail("No writes for over a year.", inferred=True)
    return ok(inferred=True)


def load_tables(config):  # resources may be a list, or a function (optionally taking config)
    # In a real notebook, e.g.:
    #   return [r.asDict() for r in spark.sql("SELECT ... FROM ...").collect()]
    return [
        {
            "name": "main.sales.orders",  # required: full name (exclusions match on it)
            "id": "main.sales.orders",  # optional, default: name
            "type": "table",  # optional, default: resource_type below
            "scope": "my-workspace",  # optional, default: "default"
            "owner": "team-sales@example.com",  # optional
            "tags": {"cost_center": "cc1"},  # any other keys are passed to your checks
            "days_since_write": 3,
        },
        {"name": "main.tmp.scratch", "owner": None, "tags": {}, "days_since_write": 500},
        {"name": "main.legacy.unknown", "owner": None, "tags": None, "days_since_write": None},
    ]


tags = define_scanner(
    "tags",  # scanner name: lowercase letters, digits, underscores
    resources=load_tables,
    checks=[has_cost_center, has_owner, recently_used],
    config={"required_tag": "cost_center"},  # defaults; override in policy under rules.tags
    #   (types allowed: bool, int, float, str, list of str)
    resource_type="table",  # default type for resources (default: the scanner name)
)

# COMMAND ----------

# 6. Advanced: subclass Scanner for full control over evidence and confidence.
from collections.abc import Sequence
from dataclasses import dataclass

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
    ScannerContext,
    ScoredEvaluation,
    Severity,
)


@dataclass(frozen=True)
class JobsConfig:  # typed settings for rules.jobs_demo in the policy
    max_idle_days: int = 90


class JobsScanner(Scanner):
    name = "jobs_demo"
    resource_types = ("job",)  # lets the run flag exclusions with a misspelled type
    config_model = JobsConfig
    rules = (
        RuleSpec(
            "jobs_demo.idle",  # must start with "<scanner name>."
            "Job has not run recently",
            Severity.MEDIUM,
            frozenset({Dimension.COST}),
            description="Jobs idle longer than max_idle_days may be candidates for review.",
            settings=("max_idle_days",),
            on_fail=Recommendation.REVIEW,
        ),
    )

    def discover(self, ctx: ScannerContext) -> Sequence[Resource]:  # 1. find resources
        source = "demo-data"
        return [
            Resource(
                ResourceRef("my-workspace", "job", "job-1"),
                name="nightly_etl",
                owner="team-data",
                evidence=(Evidence("days_idle", EvidenceStatus.OBSERVED, source, value=2),),
            ),
            Resource(
                ResourceRef("my-workspace", "job", "job-2"),
                name="old_export",
                evidence=(Evidence("days_idle", EvidenceStatus.UNAVAILABLE, source),),
            ),
        ]

    def analyze(  # 2. evaluate each rule; unknown data -> INDETERMINATE, never a guess
        self, ctx: ScannerContext, resources: Sequence[Resource]
    ) -> Sequence[RuleEvaluation]:
        evaluations = []
        for resource in resources:
            evidence = resource.get_evidence("days_idle")
            if evidence is None or evidence.status is EvidenceStatus.UNAVAILABLE:
                outcome, reason = Outcome.INDETERMINATE, "Run history unavailable."
            elif evidence.value > ctx.config.max_idle_days:
                outcome, reason = Outcome.FAIL, f"Idle for {evidence.value} days."
            else:
                outcome, reason = Outcome.PASS, f"Ran {evidence.value} days ago."
            evaluations.append(
                RuleEvaluation(
                    resource.ref,
                    "jobs_demo.idle",
                    outcome,
                    evidence=(evidence,) if evidence else (),
                    reasons=(reason,),
                )
            )
        return evaluations

    # 3. score() is inherited: severity weights from the policy, confidence from evidence.

    def recommend(  # 4. turn scored evaluations into findings
        self, ctx: ScannerContext, scored: Sequence[ScoredEvaluation]
    ) -> Sequence[Finding]:
        return [
            Finding(
                resource=item.evaluation.resource,
                rule_id=item.evaluation.rule_id,
                severity=item.severity,
                recommendation=Recommendation.KEEP
                if item.evaluation.outcome is Outcome.PASS
                else Recommendation.REVIEW,
                reasons=item.evaluation.reasons,
                evidence=item.evaluation.evidence,
                confidence=item.confidence,
                impacts=frozenset({Dimension.COST}),
            )
            for item in scored
        ]


# Built-in, function-based, and subclassed scanners together in one run:
combined = run(POLICY_PATH, scanners=["uc", tags, JobsScanner], spark=spark)
print(combined.summary())
display(rules_dataframe(spark, POLICY_PATH, scanners=["uc", tags, JobsScanner]))

# COMMAND ----------

# 7. Policy in code instead of YAML (same validation; every field is optional).
from datetime import date

from lakehouse_gc.core import Policy, load_policy, loads_policy
from lakehouse_gc.core.policy import ActionsConfig, Exclusion, Scope, ScoringConfig

policy = Policy(
    scope=Scope(
        include_catalogs=("main",),  # empty = all; globs like "prod_*" work
        exclude_catalogs=("system", "samples"),
        include_schemas=("main.sales",),  # catalog.schema
        exclude_schemas=(),
        include_tables=(),  # catalog.schema.table
        exclude_tables=("main.sales.tmp_*",),
    ),
    exclusions=(
        Exclusion("table", "*.__materialization*", "Internal tables"),  # permanent
        Exclusion("table", "main.sales.legacy_*", "Migration hold", date(2026, 12, 31)),
    ),
    rules={  # one section per scanner; unknown keys or wrong types are errors
        "uc": {"stale_table_days": 90, "min_table_size_gb": 1, "required_tags": ["cost_center"]},
        "tags": {"required_tag": "cost_center"},
    },
    scoring=ScoringConfig(
        governance_weight=0.6,  # normalized against cost_weight
        cost_weight=0.4,
        unknown_penalty=0.5,  # cost of a check with unavailable evidence, in (0, 1]
        grade_boundaries=(("A", 90), ("B", 75), ("C", 50), ("D", 0)),  # must include 0
        # severity_weights={Severity.INFO: 0, Severity.LOW: 1, ...},  # all five if set
    ),
    actions=ActionsConfig(
        min_candidate_confidence=80,  # *_candidate below this becomes review
        # mode must stay "report_only"; allow_delete / allow_pause must stay False
    ),
)
in_code = run(policy=policy, scanners=["uc", tags], spark=spark)
print(in_code.summary())

policy_from_file = load_policy(POLICY_PATH)  # parse a file without running
policy_from_text = loads_policy("policy_version: 1\nscope:\n  include_catalogs: [main]\n")

# COMMAND ----------

# 8. Registry, errors, and structured logging.
import logging

from lakehouse_gc.core import PolicyValidationError, get_default_registry
from lakehouse_gc.core.log import JsonFormatter

print(get_default_registry().names())  # built-in scanners: ('example', 'uc')

registry = get_default_registry().copy()  # a private registry with your scanners added
registry.register(JobsScanner)
print(registry.names())
with_registry = run(POLICY_PATH, scanners=["uc", "jobs_demo"], spark=spark, registry=registry)
# To add a scanner for the whole session instead:
#   from lakehouse_gc.core import register_scanner; register_scanner(JobsScanner)

try:  # configuration problems fail before anything is scanned, naming the exact key
    loads_policy("policy_version: 1\nscoring:\n  cost_wieght: 1\n")
except PolicyValidationError as error:
    print(error.path, "->", error.message)  # scoring.cost_wieght -> unknown key (did you mean...

json_handler = logging.StreamHandler()  # structured JSON logs (event, run_id, scanner, ...)
json_handler.setFormatter(JsonFormatter())
logging.getLogger("lakehouse_gc").addHandler(json_handler)
logging.getLogger("lakehouse_gc").setLevel(logging.INFO)
run(POLICY_PATH, scanners=["uc"], spark=spark)
logging.getLogger("lakehouse_gc").removeHandler(json_handler)
