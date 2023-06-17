import logging
# Import relevant libraries from environment
import numpy as np
import pandas as pd
from time import time
import os
from azure.storage.queue import QueueMessage
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import io

# Any results you write to the current directory are saved as output.
import warnings

warnings.filterwarnings("ignore")
from datetime import datetime, timedelta
import azure.functions as func


def main(msg: func.ServiceBusMessage, predSamp: func.Out[str]) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))

    logging.info("*****THIS HAS TRIGGERD AFTER DATA CLEANING****")

    # Define your connection string and container name
    connection_string = "DefaultEndpointsProtocol=https;AccountName=sftpagentgroupa4ac;AccountKey=4V95xpvoK2THUxuxlBqFDfLstMO/UbTG3Ot8jYuH559fYPXe+DVujiLtcatxyI00NU/2rbL4Crgo+AStDfvRaw==;EndpointSuffix=core.windows.net"
    container_name = "ff-process-stage1"

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

    logging.info("*****PREDICT_SAMP_tsal_inv_df*****")
    logging.info(tsal_inv_df)
    logging.info("*****PREDICT_SAMP_test_df*****")
    logging.info(test_df)

    tsal_inv_df += 1
    test_df += 1

    logging.info("*****PREDICT_SAMP_tsal_inv_df*****")
    logging.info(tsal_inv_df)
    logging.info("*****PREDICT_SAMP_test_df*****")
    logging.info(test_df)

    # # drop the total inv column to make the dataset consistent with the test dataset
    # tsal_inv_df.drop(["Total inv"], inplace=True, axis=1)   

    # # sorting the dataset by date
    # tsal_inv_df.sort_values(by="Date", inplace=True, ignore_index=True)
    # test_df.sort_values(by="Date", inplace=True, ignore_index=True)

    # # removing outliers from the dataset
    # tsal_inv_df = tsal_inv_df[tsal_inv_df["Ordered Units"] < 2500]
    # tsal_inv_df = tsal_inv_df[tsal_inv_df["Ordered Units"] > 0]
    # tsal_inv_df = tsal_inv_df[
    #     (tsal_inv_df["Date"] != "2022-04-13") & (tsal_inv_df["Ordered Units"] < 2000)
    # ]
    # test_df = test_df[test_df["Ordered Units"] > 0]

    # # resetting the index of the dataset for later combination
    # test_df.reset_index(inplace=True, drop=True)
    # tsal_inv_df.reset_index(inplace=True, drop=True)


    # # combining the two datasets
    # combined_df = pd.concat([tsal_inv_df, test_df], ignore_index=True)

    # """# First Component: obtaining 6-months average dataset and superpose it with last year same month dataset for selected features

    # """

    # # extracting dataset after 2022-05 to compute the 6 months and previous year target month average
    # ave_sample = combined_df[(combined_df["Date"] > "2022-05")]
    # nov_21_sample = combined_df[
    #     (combined_df["Date"] >= "2021-11-01") & (combined_df["Date"] <= "2021-11-30")
    # ]
    # dec_21_sample = combined_df[
    #     (combined_df["Date"] >= "2021-12-01") & (combined_df["Date"] <= "2021-12-31")
    # ]
    # jan_22_sample = combined_df[
    #     (combined_df["Date"] >= "2022-01-01") & (combined_df["Date"] <= "2022-01-31")
    # ]

    # # resetting the index of the dataset for later combination
    # ave_sample.reset_index(inplace=True, drop=True)
    # nov_21_sample.reset_index(inplace=True, drop=True)
    # dec_21_sample.reset_index(inplace=True, drop=True)
    # jan_22_sample.reset_index(inplace=True, drop=True)

    # # combining the two datasets
    # nov_ave_sample = pd.concat([ave_sample, nov_21_sample], ignore_index=True)
    # dec_ave_sample = pd.concat([ave_sample, dec_21_sample], ignore_index=True)
    # jan_ave_sample = pd.concat([ave_sample, jan_22_sample], ignore_index=True)

    # # sorting the dataset by ASIN and date
    # nov_ave_sample.sort_values(by=["ASIN", "Date"], inplace=True, ignore_index=True)
    # dec_ave_sample.sort_values(by=["ASIN", "Date"], inplace=True, ignore_index=True)
    # jan_ave_sample.sort_values(by=["ASIN", "Date"], inplace=True, ignore_index=True)

    # # inserting the day column into the data structure
    # nov_ave_sample["day"] = nov_ave_sample["Date"].dt.day
    # dec_ave_sample["day"] = dec_ave_sample["Date"].dt.day
    # jan_ave_sample["day"] = jan_ave_sample["Date"].dt.day

    # def average (dataset):
    #     dataset = dataset.groupby(["ASIN", "day"])[
    #         "Ordered Units",
    #         "Shipped COGS",
    #         "Shipped COGS - % of Total",
    #         "Shipped Units",
    #         "Shipped Units - % of Total",
    #         "Ordered Units - % of Total",
    #         "Customer Returns",
    #         "Free Replacements",
    #         "Subcategory (Sales Rank)",
    #         "Average Sales Price",
    #         "Glance Views",
    #         "Conversion Rate",
    #         "Rep OOS",
    #         "Rep OOS - % of Total",
    #         "LBB (Price)",
    #         "AMZ Inv",
    #         "Furinno Inv",
    #     ].mean()
    #     return dataset

    # nov_ave_sample = average(nov_ave_sample)
    # dec_ave_sample = average(dec_ave_sample)
    # jan_ave_sample = average(jan_ave_sample)

    # """# Second Component: obtaining prior period dataset

    # """

    # nov_pp_sample = combined_df[
    #     (combined_df["Date"] >= "2022-08-01") & (combined_df["Date"] <= "2022-08-31")
    # ]
    # dec_pp_sample = combined_df[
    #     (combined_df["Date"] >= "2022-09-01") & (combined_df["Date"] <= "2022-09-30")
    # ]
    # jan_pp_sample = combined_df[
    #     (combined_df["Date"] >= "2022-10-01") & (combined_df["Date"] <= "2022-10-31")
    # ]

    # nov_pp_sample["day"] = nov_pp_sample["Date"].dt.day
    # dec_pp_sample["day"] = dec_pp_sample["Date"].dt.day
    # jan_pp_sample["day"] = jan_pp_sample["Date"].dt.day

    # def drop_pp (dataset):

    #     dataset.drop(
    #         [
    #             "Product Title",
    #             "Shipped COGS - % of Total",
    #             "Shipped COGS - Prior Period",
    #             "Shipped COGS - Last Year",
    #             "Shipped Units - % of Total",
    #             "Shipped Units - Prior Period",
    #             "Shipped Units - Last Year",
    #             "Ordered Units - % of Total",
    #             "Ordered Units - Prior Period",
    #             "Ordered Units - Last Year",
    #             "Customer Returns",
    #             "Free Replacements",
    #             "Subcategory (Sales Rank)",
    #             "Average Sales Price - Prior Period",
    #             "Change in Glance View - Prior Period",
    #             "Change in GV Last Year",
    #             "Conversion Rate",
    #             "Date",
    #             "Rep OOS - % of Total",
    #             "Rep OOS - Prior Period",
    #             "LBB (Price)",
    #             "UPC",
    #             "Model No",
    #             "AMZ Inv",
    #             "Furinno Inv",
    #         ],
    #         inplace=True,
    #         axis=1,
    #     ) 
    #     return dataset

    # nov_pp_sample = drop_pp(nov_pp_sample)
    # dec_pp_sample = drop_pp(dec_pp_sample)
    # jan_pp_sample = drop_pp(jan_pp_sample)

    # # renaming column name for later concatenating
    # def rename_pp (dataset):
    #     dataset.rename(
    #         columns={
    #             "Shipped COGS": "Shipped COGS - Prior Period",
    #             "Shipped Units": "Shipped Units - Prior Period",
    #             "Ordered Units": "Ordered Units - Prior Period",
    #             "Average Sales Price": "Average Sales Price - Prior Period",
    #             "Glance Views": "Change in Glance View - Prior Period",
    #             "Rep OOS": "Rep OOS - Prior Period",
    #         },
    #         inplace=True,
    #     )
    #     return dataset

    # nov_pp_sample = rename_pp(nov_pp_sample)
    # dec_pp_sample = rename_pp(dec_pp_sample)
    # jan_pp_sample = rename_pp(jan_pp_sample)

    # def merge (dataset_pp, dataset_ave):

    #     prediction_df = pd.merge(
    #         dataset_ave,
    #         dataset_pp,
    #         on=["ASIN", "day"],
    #         how="inner",
    #     )
    #     return prediction_df

    # nov_pred_df = merge(nov_pp_sample, nov_ave_sample)
    # dec_pred_df = merge(dec_pp_sample, dec_ave_sample)
    # jan_pred_df = merge(jan_pp_sample, jan_ave_sample)

    # """# Third Component: obtaining last year dataset

    # """

    # # obtain last year dataset
    # nov_ly_sample = combined_df[
    #     (combined_df["Date"] >= "2021-11-01") & (combined_df["Date"] <= "2021-11-30")
    # ]
    # dec_ly_sample = combined_df[
    #     (combined_df["Date"] >= "2021-12-01") & (combined_df["Date"] <= "2021-12-31")
    # ]
    # jan_ly_sample = combined_df[
    #     (combined_df["Date"] >= "2022-01-01") & (combined_df["Date"] <= "2022-01-31")
    # ]

    # nov_ly_sample["day"] = nov_ly_sample["Date"].dt.day
    # dec_ly_sample["day"] = dec_ly_sample["Date"].dt.day
    # jan_ly_sample["day"] = jan_ly_sample["Date"].dt.day

    # def drop_ly (dataset):
    #     dataset.drop(
    #         [
    #             "Product Title",
    #             "Shipped COGS - % of Total",
    #             "Shipped COGS - Prior Period",
    #             "Shipped COGS - Last Year",
    #             "Shipped Units - % of Total",
    #             "Shipped Units - Prior Period",
    #             "Shipped Units - Last Year",
    #             "Ordered Units - % of Total",
    #             "Ordered Units - Prior Period",
    #             "Ordered Units - Last Year",
    #             "Customer Returns",
    #             "Free Replacements",
    #             "Subcategory (Sales Rank)",
    #             "Average Sales Price - Prior Period",
    #             "Average Sales Price",
    #             "Change in Glance View - Prior Period",
    #             "Change in GV Last Year",
    #             "Conversion Rate",
    #             "Rep OOS",
    #             "Rep OOS - % of Total",
    #             "Rep OOS - Prior Period",
    #             "LBB (Price)",
    #             "UPC",
    #             "Model No",
    #             "AMZ Inv",
    #             "Furinno Inv",
    #         ],
    #         inplace=True,
    #         axis=1,
    #     )
    #     return dataset

    # nov_ly_sample = drop_ly(nov_ly_sample)
    # dec_ly_sample = drop_ly(dec_ly_sample)
    # jan_ly_sample = drop_ly(jan_ly_sample)

    # # renaming column name for later concatenating
    # def rename_ly (dataset):

    #     dataset.rename(
    #         columns={
    #             "Shipped COGS": "Shipped COGS - Last Year",
    #             "Shipped Units": "Shipped Units - Last Year",
    #             "Ordered Units": "Ordered Units - Last Year",
    #             "Average Sales Price": "Average Sales Price - Last Year",
    #             "Glance Views": "Change in GV Last Year",
    #         },
    #         inplace=True,
    #     )
    #     return dataset

    # # renaming all the columns
    # nov_ly_sample = rename_ly(nov_ly_sample)
    # dec_ly_sample = rename_ly(dec_ly_sample)
    # jan_ly_sample = rename_ly(jan_ly_sample)

    # # merging the dataset with the last year dataset to form the final prediction data
    # def merge_ly (dataset, dataset_ly):
        
    #     prediction = pd.merge(
    #         dataset, dataset_ly, on=["ASIN", "day"], how="inner"
    #     )
    #     return prediction

    # nov_pred_df = merge_ly(nov_pred_df, nov_ly_sample)
    # dec_pred_df = merge_ly(dec_pred_df, dec_ly_sample)
    # jan_pred_df = merge_ly(jan_pred_df, jan_ly_sample)

    # """# Final transformation """

    # # Convert datetime object to string in format YYYY-MM-DD
    # nov_pred_df["Date"] = nov_pred_df["Date"].dt.strftime(
    #     "%Y-%m-%d"
    # )
    # dec_pred_df["Date"] = dec_pred_df["Date"].dt.strftime(
    #     "%Y-%m-%d"
    # )  
    # jan_pred_df["Date"] = jan_pred_df["Date"].dt.strftime(
    #     "%Y-%m-%d"
    # )

    # # Replace all occurrences of "2021" with "2022"
    # nov_pred_df["Date"] = nov_pred_df["Date"].str.replace(
    #     "2021", "2022"
    # )
    # dec_pred_df["Date"] = dec_pred_df["Date"].str.replace(
    #     "2021", "2022"
    # )
    # jan_pred_df["Date"] = jan_pred_df["Date"].str.replace(
    #     "2021", "2022"
    # )

    # # Convert back to pandas datetime object
    # nov_pred_df["Date"] = pd.to_datetime(
    #     nov_pred_df["Date"]
    # )
    # dec_pred_df["Date"] = pd.to_datetime(
    #     dec_pred_df["Date"]
    # )
    # jan_pred_df["Date"] = pd.to_datetime(
    #     jan_pred_df["Date"]
    # )

    # nov_pred_df.drop(["day"], inplace=True, axis=1)
    # dec_pred_df.drop(["day"], inplace=True, axis=1)
    # jan_pred_df.drop(["day"], inplace=True, axis=1)

    # nov_pred_df.to_excel("nov_pred_df.xlsx")
    # dec_pred_df.to_excel("dec_pred_df.xlsx")
    # jan_pred_df.to_excel("jan_pred_df.xlsx")

    # jan_pred_df

    # jan_pred_df[jan_pred_df["Ordered Units"] > 2500]

    # # get particular index information
    # def get_index (dataset, index):
    #     dataset = dataset.loc[index]
    #     return dataset

    # get_index(nov_pred_df, 3)

    # # get all the row that have NaN ordered units value
    # def get_nan (dataset):
    #     dataset = dataset[dataset["Ordered Units"].isna()]
    #     return dataset

    logging.info("**************HERE***************")
    passing_dataframes = [tsal_inv_df, test_df]
    logging.info("**************HERE2***************")
    # Define your connection string and container name
    connection_string = "DefaultEndpointsProtocol=https;AccountName=sftpagentgroupa4ac;AccountKey=4V95xpvoK2THUxuxlBqFDfLstMO/UbTG3Ot8jYuH559fYPXe+DVujiLtcatxyI00NU/2rbL4Crgo+AStDfvRaw==;EndpointSuffix=core.windows.net"
    container_name = "ff-process-stage2"

    # Create a BlobServiceClient object to interact with the Blob Storage service
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    for index, df in enumerate(passing_dataframes):
        
        # Get a reference to the container
        container_client = blob_service_client.get_container_client(container_name)
        
        # Create a BytesIO object to hold the Excel data
        excel_data = io.BytesIO()
        
        # Write the DataFrame to the BytesIO object as an Excel file
        df.to_excel(excel_data, index=False)
        excel_data.seek(0)  # Reset the position of the BytesIO object to the beginning
        
        # Create a BlobClient object to represent the Excel file you want to upload
        filename = f"df{index + 1}.xlsx"
        blob_client = container_client.get_blob_client(filename)
        
        # Upload the file to Blob Storage
        logging.info("blob uploaded")
        blob_client.upload_blob(excel_data, overwrite=True)

    predSamp.set("*****THIS HAS TRIGGERD AFTER DATA CLEANING****")