# Databricks notebook source
# MAGIC %run /Workspace/Delta/Utilites/Utils

# COMMAND ----------

source_folder = "dbfs:/mnt/sourcedheerajgen2/Results"
destination_dir="/mnt/sinkmediallian/bronze/Result_New"
file_format="parquet"
logs_dir ="/mnt/sinkmediallian/bronze/Logs/Result_New"


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,LongType,DoubleType

expected_schema = StructType([StructField('resultId', LongType(), True), StructField('raceId', LongType(), True), StructField('driverId', LongType(), True), StructField('constructorId', LongType(), True), StructField('number', LongType(), True), StructField('grid', LongType(), True), StructField('position', LongType(), True), StructField('positionText', StringType(), True), StructField('positionOrder', LongType(), True), StructField('points', DoubleType(), True), StructField('laps', LongType(), True), StructField('time', StringType(), True), StructField('milliseconds', LongType(), True), StructField('fastestLap', LongType(), True), StructField('rank', LongType(), True), StructField('fastestLapTime', StringType(), True), StructField('fastestLapSpeed', DoubleType(), True), StructField('statusId', LongType(), True)])

# COMMAND ----------

ingest_data(source_folder,destination_dir,file_format,logs_dir,expected_schema,"Result_New")
