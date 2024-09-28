# Databricks notebook source
#Install Required Packages
%pip install azure-storage-blob
# dbutils.library.restartPython()

# COMMAND ----------

#connection to blob storage
storage_account_name = "sagarhackathon"
container_name = "bronze"
storage_account_key = dbutils.secrets.get(scope="sagaradbscope", key="az-storage-key")
# api_access_token = dbutils.secrets.get(scope="sagaradbscope", key="api-access-token")
#currently SODA API2.0 doesn't require access token to fetch complete data, if required in future for authentication this token can be enabled.

spark.conf.set(
    "fs.azure.account.key.sagarhackathon.blob.core.windows.net",storage_account_key)

# COMMAND ----------

import requests
from time import sleep
from azure.storage.blob import BlobServiceClient
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F
import json

# COMMAND ----------

# Azure Blob Storage configuration
account_url = f"https://{storage_account_name}.blob.core.windows.net/"
blob_service_client = BlobServiceClient(account_url=account_url, credential=storage_account_key)

# define Azure Blob Storage file name parameters
folder_path = "business_location_data/"
blob_prefix = "business_"

# COMMAND ----------

# Initialize Spark Session
spark = SparkSession.builder.appName("APIDataFetch").getOrCreate()

# Set API Endpoint and other parameters
api_url = "https://data.sfgov.org/resource/g8m3-pdis.json"  # Replace with actual API endpoint
# headers = {"X-App-Token": api_access_token}
limit = 50000  # Set the limit of records to fetch per API call
max_retries = 3  # Max number of retries per API call
retry_delay = 10  # Seconds between retries

# Function to fetch data from API with retries
def fetch_data_from_api(offset, limit):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(f"{api_url}?$offset={offset}&$limit={limit}")
            response.raise_for_status()  # Raises an exception for 4XX/5XX responses
            return response.json()
        except requests.exceptions.RequestException as e:
            retries += 1
            print(f"Error fetching data from API: {e}. Retrying {retries}/{max_retries}...")
            sleep(retry_delay)
            if retries == max_retries:
                raise Exception(f"Max retries reached. Failed to fetch data from API at offset {offset}")

# Function to upload data to Azure Blob Storage
def upload_to_blob(data, file_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
    blob_client.upload_blob(data, overwrite=True)
    print(f"Uploaded {file_name} to Blob Storage.")

# COMMAND ----------

# Main pipeline execution
def execute_pipeline():
    offset = 0 #Always start from beginning for full load
    while True:
        try:
            # Fetch data from the API
            data = fetch_data_from_api(offset, limit)

            # Break if no data is returned (end of records)
            if not data:
                print("No more data to fetch. Exiting...")
                break

            # Convert data to JSON string or another format
            data_str = "\n".join([json.dumps(record) for record in data])

            # Upload to blob storage
            file_name = f"{folder_path}{blob_prefix}{offset // limit}.json"
            upload_to_blob(data_str, file_name)

            # Update the offset for checkpointing
            fetched_count = len(data)
            offset += fetched_count

        except Exception as e:
            print(f"Pipeline error: {e}. Stopping execution.")
            break

# COMMAND ----------

# DBTITLE 1,Execute the Data Pull
execute_pipeline()

# COMMAND ----------

#Drop unnecessary columns and load as delta table
df = spark.read.json("wasbs://bronze@sagarhackathon.blob.core.windows.net/business_location_data/")
df = df.withColumn("latitude", F.col("location.coordinates")[1])
df = df.withColumn("longitude", F.col("location.coordinates")[0])
df = df.drop("location",":@computed_region_26cr_cadq",":@computed_region_6qbp_sg9q",":@computed_region_ajp5_b2md",":@computed_region_jwn9_ihcz",":@computed_region_qgnn_b9vv")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("wasbs://bronze@sagarhackathon.blob.core.windows.net/processed_data/business_location_data")
