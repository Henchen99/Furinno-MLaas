import datetime
import logging
import azure.functions as func
import numpy as np
import pandas as pd
from azure.storage.queue import QueueMessage
from azure.storage.blob import BlobServiceClient
import io

import time
unix=time.time()



def main(mytimer: func.TimerRequest, msg: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info("**********ANNNNNNNDDDD NEWWWWWWW****************")


    # Define your connection string and container name
    connection_string = "DefaultEndpointsProtocol=https;AccountName=sftpagentgroupa4ac;AccountKey=4V95xpvoK2THUxuxlBqFDfLstMO/UbTG3Ot8jYuH559fYPXe+DVujiLtcatxyI00NU/2rbL4Crgo+AStDfvRaw==;EndpointSuffix=core.windows.net"
    container_name = "furinno-forecast-upload-data"

    # Create a BlobServiceClient object to interact with the Blob Storage service
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # list blobs sorted by creation time
    blobs = blob_service_client.get_container_client(container_name).list_blobs()
    blobs = sorted(blobs, key=lambda b: b.creation_time, reverse=True)

    # download newest blob
    blob = next(iter(blobs))
    blob_client = blob_service_client.get_blob_client(container_name, blob.name)
    download_stream = blob_client.download_blob()
    
    download_data = download_stream.readall()

    # Convert bytes to file-like object
    download_data = io.BytesIO(download_data)

    logging.info(type(download_data))
    data = pd.read_csv(download_data)


    logging.info(data)
    logging.info(type(data))

    # # Load dataset
    # sales_df = pd.read_excel('22-21daily.xlsx')
    # inv_df = pd.read_excel('Furinno + Amazon Inventory.xlsx')

    # # Drop unnecessary columns
    # sales_df.drop(columns=['Unnamed: 28'], inplace=True)

    # # Drop rows with NaN values
    # sales_df.dropna(inplace=True)   

    # # Drop rows with weird characters like â€”, Â—, Ã¢Â€Â—
    # sales_df = sales_df[~sales_df.stack().str.contains('â€”').groupby(level=0).any()]
    # sales_df = sales_df[~sales_df.stack().str.contains('Â—').groupby(level=0).any()]
    # sales_df = sales_df[~sales_df.stack().str.contains('Ã¢Â€Â”').groupby(level=0).any()]

    # # Code below replaces all cells that have $, %, or , to nothing so can be converted to floats
    # replacement = {
    #     "$": "",
    #     "%": "",
    #     ",": ""
    # }
    # for i in (sales_df.columns):
    #     sales_df[i] = sales_df[i].replace(replacement,regex=True)
    #     sales_df['Shipped COGS'] = sales_df['Shipped COGS'].str.replace('$','')
    #     sales_df['Average Sales Price'] = sales_df['Average Sales Price'].str.replace('$','')

    # # Convert all columns to floats
    # for i in sales_df.columns:
    
    #     try:
    #         sales_df[i]=sales_df[i].astype('float64')
    #     except ValueError as ve:
    #         sales_df[i]=sales_df[i].astype('object')
    #     except TypeError as te:
    #         sales_df[i]=sales_df[i].astype('datetime64[ns]')


    # # Merge sales and inventory dataframes
    # sal_inv_data = pd.merge(sales_df, inv_df, on = ['Date', 'ASIN'])



    # # Define your connection string and container name
    # connection_string = "DefaultEndpointsProtocol=https;AccountName=sftpagentgroupa4ac;AccountKey=4V95xpvoK2THUxuxlBqFDfLstMO/UbTG3Ot8jYuH559fYPXe+DVujiLtcatxyI00NU/2rbL4Crgo+AStDfvRaw==;EndpointSuffix=core.windows.net"
    # container_name = "ff-process-data"
    # # container_name = "furinno-forecast-upload-data"

    # # Create a BlobServiceClient object to interact with the Blob Storage service
    # blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # # Get a reference to the container
    # container_client = blob_service_client.get_container_client(container_name)

    # # Create a BlobClient object to represent the Excel file you want to upload
    # # filename = f"azure_excel_furinno_forecast_test{datetime.datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
    # filename = f"cleaned_sal_inv_data.csv"
    # blob_client = container_client.get_blob_client(filename)

    # # Upload the file to Blob Storage
    # logging.info("blob uploaded")
    # blob_client.upload_blob(sal_inv_data.to_csv(index=False, encoding="utf-8"), overwrite=True)

    msg.set("****Data cleaning DONE****")
