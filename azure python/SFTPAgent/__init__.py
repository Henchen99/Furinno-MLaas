import os
import pysftp
import logging
import time
import azure.functions as func
from datetime import datetime
# from azure.storage.queue import QueueMessage
# from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

unix=time.time()

connectOptions = pysftp.CnOpts()
connectOptions.hostkeys = None


def main(allProd: func.QueueMessage) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                 allProd.get_body().decode('utf-8'))
    logging.info("******THIS HAS TRIGGERD AFTER ALL PRODUCT FORECAST*****")


    # # Define your connection string and container name
    # connection_string = "DefaultEndpointsProtocol=https;AccountName=sftpagentgroupa4ac;AccountKey=4V95xpvoK2THUxuxlBqFDfLstMO/UbTG3Ot8jYuH559fYPXe+DVujiLtcatxyI00NU/2rbL4Crgo+AStDfvRaw==;EndpointSuffix=core.windows.net"
    # container_name = "sftp-upload-data"

    # # Create a BlobServiceClient object to interact with the Blob Storage service
    # blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # # list blobs sorted by creation time
    # blobs = blob_service_client.get_container_client(container_name).list_blobs()
    # blobs = sorted(blobs, key=lambda b: b.creation_time, reverse=True)

    # # download newest blob
    # blob = next(iter(blobs))
    # blob_client = blob_service_client.get_blob_client(container_name, blob.name)
    # download_path = r"C:\Users\henry\OneDrive\Desktop\Furinno MLaas Internship\Jupyter Notebook Forecasts\blob-test-download"
    # with open(os.path.join(download_path, blob.name), "wb") as f:
    #     download_stream = blob_client.download_blob()
    #     f.write(download_stream.readall())






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
