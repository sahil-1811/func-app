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

# from .insert import insert_to_data_lake
from .insert1 import insert_to_data_lake
from .update import update_to_data_lake
from .delete import delete_from_data_lake


# from deltalake import DeltaTable
# import pyarrow as pa

# from azure.eventhub import EventHubConsumerClient


# Initialize Application Insights
telemetry_client = TelemetryClient("e2c5c379-90a0-41dd-aad8-1d23e753e003")
delta_table_path = "abfss://test@ssahildemo123.dfs.core.windows.net/test/"

def main(events: List[func.EventHubEvent]):
    for event in events:
        logging.info('Python EventHub trigger processed an event: %s',
                         event.get_body().decode('utf-8'))
        data = event.get_body().decode('utf-8')
        event_data = json.loads(data)
        modification_type = event_data.get("type")
        if modification_type == 'I':
            insert_to_data_lake(event.get_body().decode('utf-8'))
        # elif modification_type == 'U':
        #     update_to_data_lake(event.get_body().decode('utf-8'))
        # elif modification_type == 'D':
        #     delete_from_data_lake(event.get_body().decode('utf-8'))
        

        # send_to_data_lake(event.get_body().decode('utf-8'))

    

