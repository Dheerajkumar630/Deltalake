# Databricks notebook source
# MAGIC %run /Workspace/Delta/Utilites/Utils

# COMMAND ----------

from pyspark.sql.functions import *
base_path = "/mnt/sinkmediallian/bronze/Pitstop_New/"  # Adjust the mount point if needed

latest_file_path = get_latest_file(base_path)

if latest_file_path:
    print(f"The latest file is: {latest_file_path}")
else:
    print("No files found in the specified path.")

# COMMAND ----------

df=spark.read.format('delta').option('header',True).load(latest_file_path)

# COMMAND ----------



# COMMAND ----------

display(df)

# COMMAND ----------

merge_condition= find_primary_key(df)
print(merge_condition)

# COMMAND ----------

df.schema

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table if not exists silver.pitstop(
# MAGIC raceId integer,
# MAGIC driverId integer,
# MAGIC stop integer,
# MAGIC lap integer,
# MAGIC time date,
# MAGIC duration string,
# MAGIC milliseconds integer
# MAGIC )
# MAGIC using delta
# MAGIC location '/mnt/sinkmediallian/silver/pitstop/'

# COMMAND ----------

f_merge(source_path="/mnt/sinkmediallian/bronze/Pitstop_New/", target_table="silver.pitstop", merge_condition=merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.pitstop
