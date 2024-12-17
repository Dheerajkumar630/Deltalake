# Databricks notebook source
# MAGIC %run /Workspace/Delta/Utilites/Utils

# COMMAND ----------

base_path = "/mnt/sinkmediallian/bronze/Circuit_New"  # Adjust the mount point if needed

latest_file_path = get_latest_file(base_path)

if latest_file_path:
    print(f"The latest file is: {latest_file_path}")
else:
    print("No files found in the specified path.")

# COMMAND ----------

df=spark.read.format('delta').load(latest_file_path)

# COMMAND ----------

merge_condition= find_primary_key(df)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table if not exists silver.Circuit(
# MAGIC circuitId string, 
# MAGIC circuitRef string, 
# MAGIC name string, 
# MAGIC location string, 
# MAGIC country string, 
# MAGIC lat string,
# MAGIC lng string, 
# MAGIC alt string, 
# MAGIC url string
# MAGIC )
# MAGIC using delta
# MAGIC location '/mnt/sinkmediallian/silver/Circuit/'

# COMMAND ----------

f_merge(source_path="/mnt/sinkmediallian/bronze/Circuit_New", target_table="silver.Circuit",merge_condition=merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.circuit
