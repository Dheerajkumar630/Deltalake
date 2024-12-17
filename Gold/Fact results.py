# Databricks notebook source
df_result=spark.read.table('silver.result')
df_race=spark.read.table('silver.race')
df_driver=spark.read.table('silver.driver')

# COMMAND ----------

df_driver

# COMMAND ----------

df_final=df_result.join(broadcast(df_race),df_result.raceId==df_race.raceId,how="inner").join(df_driver,df_result.driverId==df_driver.driverId,how="inner").select(df_race.raceId,df_driver.driverId,df_race.circuitId,df_race.date,df_result.points,df_result.position)
display(df_final)

# COMMAND ----------

from pyspark.sql.functions import *
df_final_agg=df_final.groupBy("raceId","driverId","circuitId","date").agg(sum(col('points')).alias('total_points'),count(when(col('position')==1,1)).alias("total_wins"))

# COMMAND ----------

display(df_final_agg)

# COMMAND ----------

df_final_agg.write.mode("overwrite").save('/mnt/sinkmediallian/gold/Fact_results')
