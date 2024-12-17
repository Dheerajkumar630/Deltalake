# Databricks notebook source
# MAGIC %run /Workspace/Delta/Utilites/Utils

# COMMAND ----------

from pyspark.sql.functions import *
base_path = "/mnt/sinkmediallian/bronze/Constructor_New"  # Adjust the mount point if needed

latest_file_path = get_latest_file(base_path)

if latest_file_path:
    print(f"The latest file is: {latest_file_path}")
else:
    print("No files found in the specified path.")

# COMMAND ----------

df=spark.read.format('delta').load(latest_file_path)
df.schema

# COMMAND ----------

merge_condition= find_primary_key(df)

# COMMAND ----------

print(merge_condition)

# COMMAND ----------

df.createOrReplaceTempView('constructor_check')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from constructor_check

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table if not exists silver.Constructor(
# MAGIC constructorId integer,
# MAGIC constructorRef string,
# MAGIC name string,
# MAGIC nationality string,
# MAGIC url string
# MAGIC )
# MAGIC using delta
# MAGIC location '/mnt/sinkmediallian/silver/Constructor/'

# COMMAND ----------

f_merge(source_path="/mnt/sinkmediallian/bronze/Constructor_New/", target_table="silver.Constructor", merge_condition=merge_condition)
