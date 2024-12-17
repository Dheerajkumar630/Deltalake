# Databricks notebook source
# MAGIC %run /Workspace/Delta/Utilites/Utils

# COMMAND ----------

from pyspark.sql.functions import *
base_path = "/mnt/sinkmediallian/bronze/Result_New/"  # Adjust the mount point if needed

latest_file_path = get_latest_file(base_path)

if latest_file_path:
    print(f"The latest file is: {latest_file_path}")
else:
    print("No files found in the specified path.")

# COMMAND ----------

df=spark.read.format('delta').option('header',True).load(latest_file_path)

# COMMAND ----------

display(df)

# COMMAND ----------

merge_condition= find_primary_key(df)
print(merge_condition)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table if not exists silver.result(
# MAGIC resultId long, 
# MAGIC raceId long,
# MAGIC driverId long,
# MAGIC constructorId long,
# MAGIC number long,
# MAGIC grid long,
# MAGIC position long,
# MAGIC positionText string,
# MAGIC positionOrder long,
# MAGIC points double, 
# MAGIC laps long,
# MAGIC time string,
# MAGIC milliseconds long,
# MAGIC fastestLap long,
# MAGIC rank long,
# MAGIC fastestLapTime string,
# MAGIC fastestLapSpeed double, 
# MAGIC statusId long
# MAGIC )
# MAGIC using delta
# MAGIC location '/mnt/sinkmediallian/silver/Result/'

# COMMAND ----------

f_merge(source_path="/mnt/sinkmediallian/bronze/Result_New/", target_table="silver.Result", merge_condition=merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.Result
