import logging
import pysftp
import azure.functions as func
import datetime
import os
# from time import time
from azure.storage.queue import QueueMessage
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import io

# unix=time.time()

# connectOptions = pysftp.CnOpts()
# connectOptions.hostkeys = None

def main(allProd: func.ServiceBusMessage) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                 allProd.get_body().decode('utf-8'))
    
    # Define your connection string and container name
    connection_string = "DefaultEndpointsProtocol=https;AccountName=sftpagentgroupa4ac;AccountKey=4V95xpvoK2THUxuxlBqFDfLstMO/UbTG3Ot8jYuH559fYPXe+DVujiLtcatxyI00NU/2rbL4Crgo+AStDfvRaw==;EndpointSuffix=core.windows.net"
    container_name = "sftp-upload-data"

    # Create a BlobServiceClient object to interact with the Blob Storage service
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Create an empty list to store blobs
    blob_list = []
    dataframes = []

    # list blobs sorted by creation time
    blobs = blob_service_client.get_container_client(container_name).list_blobs()
    blobs = sorted(blobs, key=lambda b: b.creation_time, reverse=True)
    # Append the blobs to the list
    for blob in blobs:
        blob_list.append(blob)
    # logging.info(blob_list)

    for blob in blob_list:
        blob_client = blob_service_client.get_blob_client(container_name, blob.name)
        logging.info("******BLOB NAME ******")
        logging.info(blob.name)

        download_stream = blob_client.download_blob()

        data = download_stream.readall()

        # Convert bytes to file-like object
        data_file = io.BytesIO(data)

        # Read the CSV using the file-like object and append to the list
        df = pd.read_excel(data_file)

        dataframes.append(df)
    

    # Load the transformed dataset
    tsal_inv_df = dataframes[0]
    test_df = dataframes[1]

    logging.info("*****SFTP_tsal_inv_df*****")
    logging.info(tsal_inv_df)
    logging.info("*****SFTP_test_df*****")
    logging.info(test_df)





    # local_file_path = r"C:\Users\henry\OneDrive\Desktop\Furinno MLaas Internship\Jupyter Notebook Forecasts\cleaned_sal_inv_data.xlsx"
    
    # with pysftp.Connection(host='localhost', port=22, username='henry', password='9Redgraveclose', cnopts=connectOptions) as sftp:
    #     logging.info("connection success!")
    #     sftp.cwd("./SFTP_Furinno")
    #     logging.info("We are in: %s", sftp.pwd) # print working directory
    #     logging.info("Files in Directory: %s", sftp.listdir()) # Print files in current directory
        
    #     filename = f"cleaned_sal_inv_data_{datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
    #     logging.info("Filename: %s", filename)
        
    #     sftp.put(local_file_path, filename)
        
    #     logging.info("Files in Directory: %s", sftp.listdir()) # Print files in current directory


    logging.info("******THIS HAS TRIGGERD AFTER ALL PRODUCT FORECAST*****")