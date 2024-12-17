# Databricks notebook source
from pyspark.sql.functions import col,when,concat,lit
df_drivers=spark.read.table("silver.driver")

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

df_final=df_drivers.select("*").\
         withColumn("FullName",concat(col("forename"),lit(" "),col("surname"))).\
         withColumn("number",when(col("number")=="\\N",None).otherwise(col("number")).cast("int")).\
         withColumn("code",when(col("code")=="\\N",None).otherwise(col("code"))).\
         drop("url","forename","surname")

df_final.repartition(1).write.format("delta").mode("overwrite").save("/mnt/sinkmediallian/gold/dim_drivers")

# COMMAND ----------

display(df_final)
