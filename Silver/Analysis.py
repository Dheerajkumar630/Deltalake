# Databricks notebook source
# MAGIC %sql
# MAGIC select * from silver.result;

# COMMAND ----------

df=spark.table("silver.result")

# COMMAND ----------

df.dtypes

# COMMAND ----------

-- how many wins by each drivers/teams and how many points he/she gained

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.race

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.raceId,b.name,sum(case when a.position = 1 then 1 else 0 end) as total_wins from silver.result a inner join silver.race b on a.raceId = b.raceId group by a.raceId,b.name;
