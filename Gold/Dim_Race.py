# Databricks notebook source
from pyspark.sql.functions import col,when,concat,lit
df_race=spark.read.table("silver.Race")


# COMMAND ----------

display(df_race)

# COMMAND ----------

df_race=df_race.select([when(col(i)=="\\N",None).otherwise(col(i)).alias(i) for i in df_race.columns])
display(df_race)

# COMMAND ----------

df_final=df_race.select("raceId","round","circuitId","name","time")
df_final.repartition(1).write.format("delta").mode("overwrite").save("/mnt/sinkmediallian/gold/dim_race")
