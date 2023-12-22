import json 
import boto3
import random
import uuid
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
# create s3 client
import os
session = boto3.Session(profile_name='adt')
s3 = session.client('s3')
SOURCE_BUCKET_NAME = 'csv-data-comp8157'
TARGET_BUCKET_NAME = 'parquet-data-comp8157'

# read all csv files
def convert_csv_to_parquet(input_csv, output_parquet):
    # Define the schema with data types
    schema = pa.schema([
        ("Date", pa.string()),
        ("Level", pa.string()),
        ("ProcessId", pa.string()),
        ("Component", pa.string()),
        ("Message", pa.string())
    ])

    # Read CSV and convert to Parquet with specified schema
    df = pd.read_csv(input_csv, delimiter=',')
    df.columns = ["Date", "Level", "ProcessId", "Component", "Message"]
    df["ProcessId"] = df["ProcessId"].astype(str)

    # Convert DataFrame to Arrow Table
    table = pa.Table.from_pandas(df, schema=schema)

    # Write Parquet file
    pq.write_table(table, output_parquet)

def process():
    # list all csv files in path ../split_data recursively
    csv_files = []
    for root, dirs, files in os.walk("./split_data"):
        for file in files:
            if file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))
    # print(csv_files)
    return csv_files

# main function
if __name__ == "__main__":
    csv_files=process()
    for file in csv_files:
        # print(file)
        # print(file.split('/')[-1].split('.')[0])
        _,original_folder, date, level, *_ = file.split('/')
        # print(date, level)
        # create folder ./split_data_parquet/date/level if not exist
        if not os.path.exists('./split_data_parquet/'+date+'/'+level):
            os.makedirs('./split_data_parquet/'+date+'/'+level)
        convert_csv_to_parquet(file, './split_data_parquet/'+date+'/'+level+'/'+file.split('/')[-1].split('.')[0]+'.parquet')
        # input("Press Enter to continue...")
    