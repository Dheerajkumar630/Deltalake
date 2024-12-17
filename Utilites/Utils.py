# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

import datetime
import os
import re
import openpyxl
import pandas as pd
  # The name of the table to store processed file information

# Function to extract date from filename
def extract_date_from_filename(filename):
    match = re.search(r"(.+)_(\d{4})_(\d{1,2})_(\d{1,2})\.(csv|xlsx|parquet|txt)", filename)
    if match:
        year = int(match.group(2))
        month = int(match.group(3))
        day = int(match.group(4))
        return datetime.date(year, month, day)
    return None

# Function to ingest data from a given source directory
def ingest_data(source_dir,destination_dir,file_format,logs_dir,expected_schema,table_name):
    # Create a Spark DataFrame to store processed file information (if it doesn't exist)
    spark.sql(f"""
      CREATE OR REPLACE TABLE {table_name} (
        filename STRING,
        processed_date TIMESTAMP
      )
      using DELTA
      location "{logs_dir}"
    """)

    # Get already processed files from the table
    processed_files_df = spark.table(table_name)
    processed_files = set(row.filename for row in processed_files_df.collect())

    # Iterate through files in the source directory
    for file in dbutils.fs.ls(source_dir):
        filename=file.name
        file_path = file.path

        # Check if it's a file (not a subdirectory) and has a valid extension
        if filename.endswith((".csv", ".xlsx", ".parquet",".txt")):
            # Read the data using Spark with schema validation
            try:
                if filename.endswith((".csv", ".parquet",".txt")):
                    original_schema=spark.read.option('header',True).format(file_format).load(file_path).schema
                    if expected_schema == original_schema:
                        df = spark.read.option('header',True).format(file_format).schema(expected_schema).load(file_path)
                    else:
                        raise Exception("Schema mismatch")
                elif filename.endswith((".xlsx")):
                    file_path_1=file_path.replace("dbfs:","/dbfs")
                    print(file_path_1)
                    sheet_excel = openpyxl.load_workbook(file_path_1)
                    df=spark.createDataFrame([],schema=expected_schema)
                    for sheet in sheet_excel.sheetnames:
                        df_1 = pd.read_excel(file_path_1,engine='openpyxl',sheet_name=f"{sheet}")
                        df_spark=spark.createDataFrame(df_1,schema=expected_schema)
                        df=df.union(df_spark)
            except Exception as err:
                raise err

            # Assuming the filename is in a column named 'filename' (adjust if different)
            if filename not in processed_files:
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

                df.write.option('header',True).format("delta").mode('overwrite').save(blob_path)
                print(f"Data from {source_dir} uploaded to {blob_path}")

                    # Add processed file information to the DataFrame
                new_row_df = spark.createDataFrame([(filename, datetime.datetime.now())], ["filename", "processed_date"])
                processed_files_df = processed_files_df.union(new_row_df)

    # Save the updated processed files information to the table
                processed_files_df.write.format('delta').mode("overwrite").save(logs_dir)
                print("successfully saved the file")


# COMMAND ----------

def get_latest_file(base_path):
    
    latest_file = None
    latest_date = None

    try:
        # List all year folders
        year_folders = dbutils.fs.ls(base_path)
        year_folders.sort(key=lambda file_info: file_info.name, reverse=True)  # Sort year folders in descending order

        for year_folder in year_folders:

            if year_folder.isDir():
                # List all month folders within the year folder
                month_folders = dbutils.fs.ls(year_folder.path)
                month_folders.sort(key=lambda file_info: file_info.name, reverse=True)  # Sort month folders in descending order

                for month_folder in month_folders:
                    if month_folder.isDir():
                        # List all day folders within the month folder
                        day_folders = dbutils.fs.ls(month_folder.path)
                        day_folders.sort(key=lambda file_info: file_info.name, reverse=True)  # Sort day folders in descending order

                        for day_folder in day_folders:
                            if day_folder.isDir():
                                # List all files within the day folder
                                files = dbutils.fs.ls(day_folder.path)
                                files=[f for f in files if not f.name.startswith("_delta_log")]
                                if files:
                                    # Assuming the latest file is the first one lexicographically
                                    latest_file = files[0].path
                                    return latest_file  # Found the latest file, so return

    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            print(f"Path not found: {base_path}")
        else:
            raise e

    return latest_file


# COMMAND ----------

from delta.tables import DeltaTable
def f_merge(source_path,target_table,merge_condition):
    path=get_latest_file(source_path)
    df_staging=spark.read.format("delta").load(f"{path}")
    deltatable=DeltaTable.forName(spark,target_table)
    deltatable.alias("tgt").merge(df_staging.alias("src"),f"{merge_condition}").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

import mack as mk
def find_primary_key(df):
    try:
        primary_key =mk.find_composite_key_candidates(df)
        merge_condition=" "
        for i in range(len(primary_key)):
            if(i==len(primary_key)-1):
                merge_condition+=f"tgt."+primary_key[i]+" = src."+primary_key[i]
            else:
                merge_condition+=f"tgt."+primary_key[i]+"=src."+primary_key[i]+" AND "

        return merge_condition
    except Exception as err:
        print("Error occured: ",err)

