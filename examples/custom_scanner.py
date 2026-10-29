# Databricks notebook source
# MAGIC %md
# MAGIC # lakehouse-gc: scan Unity Catalog
# MAGIC
# MAGIC 1. Upload `lakehouse_gc-0.2.1-py3-none-any.whl` and `policy.yaml` to a volume.
# MAGIC 2. In `policy.yaml`, list the catalogs / schemas / tables to scan under `scope`.
# MAGIC 3. Set the two paths below and run all cells. The scan is read-only.

# COMMAND ----------

# MAGIC %pip install /Volumes/main/default/lakehouse_gc/lakehouse_gc-0.2.1-py3-none-any.whl

# COMMAND ----------

from lakehouse_gc import rules_dataframe, run

POLICY_PATH = "/Volumes/main/default/lakehouse_gc/policy.yaml"

# What each check means and the settings your policy applies (scans nothing).
display(rules_dataframe(spark, POLICY_PATH, scanners=["uc"]))

# COMMAND ----------

result = run(POLICY_PATH, scanners=["uc"], spark=spark)
print(result.summary())

# COMMAND ----------

display(result.to_dataframe(spark))
