import json
import logging
from delta import DeltaTable
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

def insert_to_data_lake(data: str):
    delta_table_path = "abfss://test@ssahildemo123.dfs.core.windows.net/test/"
    try:
        event_data = json.loads(data)
        exclude_key = "type"
        if exclude_key in event_data:
            del event_data[exclude_key]

        formatted_data = json.dumps(event_data)

        # Azure Data Lake Storage account information
        account_name = "ssahildemo123"
        filesystem_name = "test"
        directory_name= 'test'
        delta_file_name = "delta_file.parquet"  # Adjust the file name as needed
        delta_file_path = f"{directory_name}/{delta_file_name}"

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
        directory_client = filesystem_client.get_directory_client(directory_name)

        # Get a file client for the Delta file
        delta_file_client = directory_client.get_file_client(delta_file_path)

        # Create or get Delta table
        delta_table = DeltaTable(delta_table_path)

        # Append data to the Delta table
        delta_table.alias("source").merge(
            formatted_data.alias("delta"), "source.id = delta.id"
        ).whenNotMatchedInsertAll().execute()

        # Append data to the Delta file for historical purposes
        offset = delta_file_client.get_file_properties().size or 0
        delta_file_client.append_data(formatted_data + '\n', offset=offset, length=len(formatted_data) + 1, flush=True)

        logging.info('Data sent to Azure Delta Lake Storage successfully.')
    except Exception as e:
        logging.error(f"Error sending data to Delta Lake Storage: {str(e)}")
