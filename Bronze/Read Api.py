# Databricks notebook source
# MAGIC %run /Workspace/Users/sai91035@gmail.com/MountPoints

# COMMAND ----------

import requests

# COMMAND ----------

headers= {
    'x-rapidapi-host':f_get_secret('api-host'),
    'x-rapidapi-key':f_get_secret('api-key')
}


response = requests.get("https://v1.formula-1.api-sports.io/teams",headers=headers)
data=response.json()['response']

# COMMAND ----------

import pandas as pd
df=pd.DataFrame(data)
df_spark = spark.createDataFrame(df)

# COMMAND ----------

df=pd.DataFrame(data)
df_spark = spark.createDataFrame(df)

# COMMAND ----------

df_spark.write.format('delta').mode('overwrite').save('/mnt/sinkmediallian/bronze/Teams_New')
