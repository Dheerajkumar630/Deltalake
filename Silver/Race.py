# Databricks notebook source
# MAGIC %run /Workspace/Delta/Utilites/Utils

# COMMAND ----------

from pyspark.sql.functions import *
base_path = "/mnt/sinkmediallian/bronze/Race_New"  # Adjust the mount point if needed

latest_file_path = get_latest_file(base_path)

if latest_file_path:
    print(f"The latest file is: {latest_file_path}")
else:
    print("No files found in the specified path.")

# COMMAND ----------

df=spark.read.format('delta').load(latest_file_path)
df.schema

# COMMAND ----------

display(df)

# COMMAND ----------

merge_condition= find_primary_key(df)
print(merge_condition)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table if not exists silver.Race(
# MAGIC raceId string,
# MAGIC year string,
# MAGIC round string,
# MAGIC circuitId string,
# MAGIC name string,
# MAGIC date string,
# MAGIC time string,
# MAGIC url string,
# MAGIC fp1_date string,
# MAGIC fp1_time string,
# MAGIC fp2_date string,
# MAGIC fp2_time string,
# MAGIC fp3_date string,
# MAGIC fp3_time string,
# MAGIC quali_date string,
# MAGIC quali_time string,
# MAGIC sprint_date string,
# MAGIC sprint_time string
# MAGIC )
# MAGIC using delta
# MAGIC location '/mnt/sinkmediallian/silver/Race/'

# COMMAND ----------

f_merge(source_path="/mnt/sinkmediallian/bronze/Race_New/", target_table="silver.Race", merge_condition=merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.race
