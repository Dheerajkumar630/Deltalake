# Databricks notebook source
# MAGIC %run /Workspace/Delta/Utilites/Utils

# COMMAND ----------

from pyspark.sql.functions import *
base_path = "/mnt/sinkmediallian/bronze/Laptime_New"  # Adjust the mount point if needed

latest_file_path = get_latest_file(base_path)

if latest_file_path:
    print(f"The latest file is: {latest_file_path}")
else:
    print("No files found in the specified path.")

# COMMAND ----------

df.createOrReplaceTempView('laptime_check')

# COMMAND ----------

df=spark.read.format('delta').load(latest_file_path)
display(df)

# COMMAND ----------

merge_condition= find_primary_key(df)
print(merge_condition)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table if not exists silver.Laptime(
# MAGIC  raceid string,
# MAGIC  driverid string,
# MAGIC  laptime string,
# MAGIC  position string,
# MAGIC  time string,
# MAGIC  milliseconds string
# MAGIC )
# MAGIC using delta
# MAGIC location '/mnt/sinkmediallian/silver/Laptime/'

# COMMAND ----------

f_merge(source_path="/mnt/sinkmediallian/bronze/Laptime_New/", target_table="silver.Laptime", merge_condition=merge_condition)
