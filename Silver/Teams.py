# Databricks notebook source
# MAGIC %run /Workspace/Delta/Utilites/Utils

# COMMAND ----------

from pyspark.sql.functions import *
base_path = "/mnt/sinkmediallian/bronze/Teams_New/"  # Adjust the mount point if needed

# COMMAND ----------

dbutils.fs.ls(base_path)

# COMMAND ----------

base_path = "/mnt/sinkmediallian/bronze/Teams_New/"
try:
    files = dbutils.fs.ls(base_path)
    display(files)
except Exception as e:
    print(f"Error: {e}")

# COMMAND ----------

df=spark.read.format('delta').option('header',True).load(base_path)

# COMMAND ----------

display(df)

# COMMAND ----------

merge_condition= find_primary_key(df)
print(merge_condition)

# COMMAND ----------

df.schema

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace table silver.teams(
# MAGIC id integer
# MAGIC ,name string
# MAGIC ,logo string
# MAGIC ,base string
# MAGIC ,first_team_entry integer
# MAGIC ,world_championships integer
# MAGIC ,position integer
# MAGIC ,number integer
# MAGIC ,pole_positions integer
# MAGIC ,fastest_laps integer
# MAGIC ,president string
# MAGIC ,director string
# MAGIC ,technical_manager string
# MAGIC ,chassis string
# MAGIC ,engine string
# MAGIC ,tyres string
# MAGIC )
# MAGIC using delta
# MAGIC location '/mnt/sinkmediallian/silver/teams/'

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# COMMAND ----------

def f_merge_1(source_path,target_table,merge_condition):
    df_staging=spark.read.format("delta").load(f"{source_path}")
    deltatable=DeltaTable.forName(spark,target_table)
    deltatable.alias("tgt").merge(df_staging.alias("src"),f"{merge_condition}").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

f_merge_1("/mnt/sinkmediallian/bronze/Teams_New/","silver.teams",merge_condition=merge_condition)
