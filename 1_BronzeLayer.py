# Databricks notebook source
# DBTITLE 1,Import the lib
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Create Catalog,Schema,Volume

spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.api_project")
base_path ='/Volumes/workspace/default/api_project';

# COMMAND ----------

# DBTITLE 1,Calling api
API_KEY = 'ffbd5de5-53c4-4f6d-843d-d9a429ced8f3'
api_url=f"https://api.cricapi.com/v1/currentMatches?apikey={API_KEY}&offset=0"
response=requests.get(api_url)
response.raise_for_status()
api_data=response.json()
print(api_data.keys())

print(json.dumps(api_data,indent=2)[:2000])

# COMMAND ----------

# DBTITLE 1,Save raw data in volumes
raw_file_path=f'{base_path}/current_matches_raw.json'
with open(raw_file_path,'w')as file:
  json.dump(api_data,file)
print("Raw API data is saved to {raw_file_path}")



# COMMAND ----------

# DBTITLE 1,BronzeLayer
bronze_data=[{
    "source_api":api_url,
    "raw_json":json.dumps(api_data),
    "ingest_time":None
}]

bronze_schema=StructType([
  StructField("source_api",StringType(),True),
  StructField("raw_json",StringType(),True),
  StructField("ingest_time",TimestampType(),True)
])

bronze_df=spark.createDataFrame(bronze_data,schema=bronze_schema).withColumn("ingest_time",current_timestamp())
display(bronze_df)

# COMMAND ----------

# DBTITLE 1,SaveBronzeData
bronze_df.write.format('delta').mode('overwrite').saveAsTable("workspace.default.cricket_bronze_data")

print("BRONZE TABLE CREATED SUCCESSFULLY")