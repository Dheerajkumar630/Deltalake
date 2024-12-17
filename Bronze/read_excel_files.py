# Databricks notebook source
# MAGIC %run /Workspace/Delta/Utilites/Utils

# COMMAND ----------

"""Constructors"""
source_folder_Constructors = "dbfs:/mnt/sourcedheerajblob/Constructors"
destination_dir_Constructors="/mnt/sinkmediallian/bronze/Constructor_New"
file_format="xlsx"
logs_dir_Constructors ="/mnt/sinkmediallian/bronze/Logs_New/Constructor_New"

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,LongType,DoubleType

expected_schema_Constructors=  StructType([StructField("constructorId", IntegerType(), True),StructField("constructorRef", StringType(), True),StructField("name", StringType(), True),StructField("nationality", StringType(), True),StructField("url", StringType())])

ingest_data(source_folder_Constructors,destination_dir_Constructors,file_format,logs_dir_Constructors,expected_schema_Constructors,"Constructor_New")
