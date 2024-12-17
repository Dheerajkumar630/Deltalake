# Databricks notebook source
# MAGIC %run /Workspace/Delta/Utilites/Utils

# COMMAND ----------

"""Circuits"""
source_folder_circuits = "dbfs:/mnt/sourcedheerajgen2/Circuits"
destination_dir_circuits="/mnt/sinkmediallian/bronze/Circuit_New"
file_format="csv"
logs_dir_circuits ="/mnt/sinkmediallian/bronze/Logs_New/Circuit_New"

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,LongType,DoubleType

expected_schema_circuits= StructType([StructField('circuitId', StringType(), True), StructField('circuitRef', StringType(), True), StructField('name', StringType(), True), StructField('location', StringType(), True), StructField('country', StringType(), True), StructField('lat', StringType(), True), StructField('lng', StringType(), True), StructField('alt', StringType(), True), StructField('url', StringType(), True)])

ingest_data(source_folder_circuits,destination_dir_circuits,file_format,logs_dir_circuits,expected_schema_circuits,"Circuit_New")

# COMMAND ----------

"""Drivers"""
source_folder_Drivers = "dbfs:/mnt/sourcedheerajgen2/Drivers"
destination_dir_Drivers="/mnt/sinkmediallian/bronze/Driver_New"
file_format="csv"
logs_dir_Drivers ="/mnt/sinkmediallian/bronze/Logs_New/Driver_New"

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,LongType,DoubleType,DateType

expected_schema_Drivers= StructType([StructField('driverId', StringType(), True), StructField('driverRef', StringType(), True), StructField('number', StringType(), True), StructField('code', StringType(), True), StructField('forename', StringType(), True), StructField('surname', StringType(), True), StructField('dob', StringType(), True), StructField('nationality', StringType(), True), StructField('url', StringType(), True)])

ingest_data(source_folder_Drivers,destination_dir_Drivers,file_format,logs_dir_Drivers,expected_schema_Drivers,"Driver_New")

# COMMAND ----------

"""Race"""
source_folder_Race= "dbfs:/mnt/sourcedheerajgen2/Race"
destination_dir_Race="/mnt/sinkmediallian/bronze/Race_New"
file_format="csv"
logs_dir_Race ="/mnt/sinkmediallian/bronze/Logs_New/Race_New"

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,LongType,DoubleType,DateType

expected_schema_Race= StructType([StructField('raceId', StringType(), True), StructField('year', StringType(), True), StructField('round', StringType(), True), StructField('circuitId', StringType(), True), StructField('name', StringType(), True), StructField('date', StringType(), True), StructField('time', StringType(), True), StructField('url', StringType(), True), StructField('fp1_date', StringType(), True), StructField('fp1_time', StringType(), True), StructField('fp2_date', StringType(), True), StructField('fp2_time', StringType(), True), StructField('fp3_date', StringType(), True), StructField('fp3_time', StringType(), True), StructField('quali_date', StringType(), True), StructField('quali_time', StringType(), True), StructField('sprint_date', StringType(), True), StructField('sprint_time', StringType(), True)])

ingest_data(source_folder_Race,destination_dir_Race,file_format,logs_dir_Race,expected_schema_Race,"Race_New")
