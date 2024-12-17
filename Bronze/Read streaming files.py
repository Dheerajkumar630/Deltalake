# Databricks notebook source
from pyspark.sql.types import *
schema= StructType([StructField('raceId', IntegerType(), True), StructField('driverId', IntegerType(), True), StructField('stop', IntegerType(), True), StructField('lap', IntegerType(), True), StructField('time', TimestampType(), True), StructField('duration', StringType(), True), StructField('milliseconds', IntegerType(), True)])

# COMMAND ----------

df=spark.readStream.format('csv').option('header',True).option('maxfilesperTrigger',1).schema(schema).load('dbfs:/mnt/sourcedheerajgen2/Pitstop')

# COMMAND ----------

import datetime
import re
from pyspark.sql.functions import col

# Function to extract date from filename
def extract_date_from_filename(filename):
    match = re.search(r"(.+)_(\d{4})_(\d{1,2})_(\d{1,2})\.csv", filename)
    if match:
        year = int(match.group(2))
        month = int(match.group(3))
        day = int(match.group(4))
        return datetime.date(year, month, day)
    return None

def process_batch(df, batch_id, destination_dir, expected_schema,source_dir):
    # Get the list of files from the current batch
    files = dbutils.fs.ls(source_dir)  # Use dbutils.fs.ls to get files in the source directory

    for file in files:
        filename = file.name

        # Extract date from filename
        file_date = extract_date_from_filename(filename)
        if file_date is None:
            print(f"Warning: Could not extract date from filename {filename}. Skipping.")
            continue

        # Format the date for partitioning
        year = file_date.strftime("%Y")
        month = file_date.strftime("%m")
        day = file_date.strftime("%d")

        # Construct the blob path
        blob_path = f"{destination_dir}/year={year}/month={month}/day={day}/{filename}"

        # Check if the file has already been processed
        try:
            existing_df = spark.read.format("delta").load(blob_path)
            if existing_df.count() > 0:
                print(f"File {filename} already processed at {blob_path}. Skipping.")
                continue
        except:  # If the path doesn't exist, it hasn't been processed
            pass

        # Schema validation
        try:
            original_schema = df.schema
            if expected_schema != original_schema:
                raise Exception("Schema mismatch")
        except Exception as err:
            print(f"Error processing {filename}: {err}")
            continue  # Skip to the next file

        # Write the data
        try:
            df.write.option('header',True).format("delta").mode('append').save(blob_path)
            print(f"Data from {filename} uploaded to {blob_path}")
        except Exception as err:
            print(f"Error writing data to {blob_path}: {err}")


# Create SparkSession

# Define source directory and destination directory
source_dir = "/mnt/sourcedheerajgen2/Pitstop"  # Replace with your actual source directory
destination_dir = "/mnt/sinkmediallian/bronze/Pitstop_New"  # Replace with your actual destination directory

# Define expected schema
expected_schema = StructType([StructField('raceId', IntegerType(), True), StructField('driverId', IntegerType(), True), StructField('stop', IntegerType(), True), StructField('lap', IntegerType(), True), StructField('time', TimestampType(), True), StructField('duration', StringType(), True), StructField('milliseconds', IntegerType(), True)])

# Read streaming data from the source directory
df_stream = spark.readStream \
    .option('header',True) \
    .format("csv") \
    .option("header", True) \
    .schema(expected_schema) \
    .load(source_dir)

# Process the streaming data
df_stream.writeStream \
    .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, destination_dir, expected_schema,source_dir)) \
    .start()
