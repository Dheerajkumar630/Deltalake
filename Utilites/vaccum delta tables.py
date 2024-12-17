# Databricks notebook source
dbutils.widgets.text("tableName", "")
tableName = dbutils.widgets.get("tableName")

# COMMAND ----------

# MAGIC %sql
# MAGIC use silver;
# MAGIC show tables;

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

from delta.tables import DeltaTable
if tableName: spark.sql(""" use {0}""".format(tableName))
df=spark.sql(""" show tables""").select("tableName").collect()
list_tables=[i.tableName for i in df]
for i in list_tables:
    deltatable=DeltaTable.forName(spark,i)
    deltatable.vacuum(4)

