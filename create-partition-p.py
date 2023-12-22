import boto3
from datetime import datetime
session = boto3.Session(profile_name='adt')
import time
def list_object_paths(bucket_name, prefix=''):
    s3_client = session.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return [obj['Key'] for obj in response.get('Contents', [])]

def extract_dates_and_levels(object_paths):
    unique_dates = set()
    unique_levels = set()

    for path in object_paths:
        # Extract date and level from the path
        # print(path)
        date, level, *_ = path.split('/')
        unique_dates.add(date)
        unique_levels.add(level)
    # unique_levels.remove('.DS_Store',)
    return sorted(list(unique_dates)), sorted(list(unique_levels))

def create_athena_partitions(database, table, s3_location, dates, levels):
    athena_client = session.client('athena')

    for date in dates:
        for level in levels:
            time.sleep(0.5)
            partition_query = (
                f"ALTER TABLE {database}.{table} "
                f"ADD PARTITION (date_partition='{date}', level_partition='{level}') "
                f"LOCATION '{s3_location}/{date}/{level}/'"
            )
            response = athena_client.start_query_execution(
                QueryString=partition_query,
                QueryExecutionContext={'Database': database},
                ResultConfiguration={'OutputLocation': 's3://query-output-adt/'}
            )
            query_execution_id = response['QueryExecutionId']
            print(f"Partition query for date={date} and level={level} submitted. QueryExecutionId: {query_execution_id}")

if __name__ == "__main__":
    # Replace with your S3 bucket name and prefix
    bucket_name = 'parquet-data-comp8157'
    prefix = ''

    # Replace with your Athena database, table, and S3 location
    athena_database = 'adt'
    athena_table = 'parquet_table_with_partition'
    athena_s3_location = 's3://parquet-data-comp8157'

    # List object paths in S3 bucket
    object_paths = list_object_paths(bucket_name, prefix)
    # print(f"Object paths: {object_paths}")
    # Extract unique dates and levels
    unique_dates, unique_levels = extract_dates_and_levels(object_paths)
    print(f"Unique dates: {unique_dates}")
    print(f"Unique levels: {unique_levels}")
    input("Press Enter to continue...")
    # Create Athena partitions
    create_athena_partitions(athena_database, athena_table, athena_s3_location, unique_dates, unique_levels)
