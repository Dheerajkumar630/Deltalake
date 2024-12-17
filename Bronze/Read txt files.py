# Databricks notebook source
# MAGIC %run /Workspace/Delta/Utilites/Utils

# COMMAND ----------

"""Laptimes"""
source_folder_Laptimes = "dbfs:/mnt/sourcedheerajgen2/Laptimes"
destination_dir_Laptimes="/mnt/sinkmediallian/bronze/Laptime_New"
file_format="csv"
logs_dir_Laptimes ="/mnt/sinkmediallian/bronze/Logs_New/Laptime_New"

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,LongType,DoubleType

expected_schema_Laptimes = StructType([StructField('raceid', StringType(), True), StructField('driverid', StringType(), True), StructField('laptime', StringType(), True), StructField('position', StringType(), True), StructField('time', StringType(), True), StructField('milliseconds', StringType(), True)])

ingest_data(source_folder_Laptimes,destination_dir_Laptimes,file_format,logs_dir_Laptimes,expected_schema_Laptimes,"Laptime_New")
