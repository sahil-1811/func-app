from typing import List
import logging
import json
from datetime import datetime 

import azure.functions as func
from applicationinsights import TelemetryClient

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError

from azure.identity import ClientSecretCredential
import pandas as pd

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import os
# # make sure pyspark tells workers to use python3 not 2 if both are installed
# os.environ['PYSPARK_PYTHON'] = 'C:\Program Files\Python39\python.exe'

# from deltalake import DeltaTable
# import pyarrow as pa

# from azure.eventhub import EventHubConsumerClient


# Initialize Application Insights
telemetry_client = TelemetryClient("e2c5c379-90a0-41dd-aad8-1d23e753e003")

# def main(events: List[func.EventHubEvent]):
#     for event in events:
#         logging.info('Python EventHub trigger processed an event: %s',
#                          event.get_body().decode('utf-8'))

#         send_to_data_lake(event.get_body().decode('utf-8'))
    

def insert_to_data_lake(data: str):

    event_data = json.loads(data)
    exclude_key = "type"
    if exclude_key in event_data:
        del event_data[exclude_key]

    # Access specific fields from the data
    country = event_data.get("Country", "N/A")
    # Azure Data Lake Storage account information
    account_name = "ssahildemo123"
    filesystem_name = "test"
    # directory_name = f"{country}"
    directory_name = "test"
    # file_name = f"{country}.json"
    file_name = "test.json"
    file_path = f"{directory_name}/"

     # Azure AD authentication information
    tenant_id = "f5f02767-508e-4b78-bc05-2b16c8a081a1"
    client_id = "e8e858b6-27fd-4069-85cf-efa2fc7dc157"
    client_secret = "52o8Q~V-1i.WRm8QDxNp92r7d1HNMBv7A~YHAa1-"

    # Create a ClientSecretCredential
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)

    # Create a Data Lake Service Client
    service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", credential=credential)

    # Get a file system client
    filesystem_client = service_client.get_file_system_client(file_system=filesystem_name)

    # Get a directory client
    directory_client = filesystem_client.get_directory_client(directory_name)
    try:
    # Check if the directory exists
        directory_properties = directory_client.get_directory_properties()
    
    except ResourceNotFoundError:
    # Create the directory if it doesn't exist
        directory_client.create_directory()

    # Get a file client
    
    file_client = directory_client.get_file_client(file_name)
   
    spark = SparkSession.builder.appName("DeltaLakeWriter").getOrCreate()
    df = spark.read.json(spark.sparkContext.parallelize([data]))
    logging.info(df)

    file_path_delta = f"{file_path}.delta"
    delta_table = DeltaTable.forPath(spark, file_path_delta)



    try:
        delta_table.alias("old").merge(df.alias("new"), "event_id") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        logging.info('Data appended to Delta table successfully.')
    
    except Exception as e:
        logging.error(f"Error writing to Delta table: {e}")
        # Create the Delta table if it doesn't exist
        df.write.format("delta").save(file_path_delta)
        logging.info('Initial Delta table created successfully.')

    finally:
        spark.stop() 

    
