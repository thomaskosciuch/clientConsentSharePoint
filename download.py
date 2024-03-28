import os
import urllib
import csv

from sqlalchemy import create_engine, MetaData, Table
import boto3


SQL_USERNAME = os.environ['SQL_USERNAME']
SQL_PASSWORD = urllib.parse.quote_plus(os.environ['SQL_PASSWORD'])
SQL_DATABASE = os.environ['SQL_DATABASE']

def download_table():
    connection_url = f'mysql+pymysql://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_DATABASE}.criwycoituxs.ca-central-1.rds.amazonaws.com/qw_prod'
    engine = create_engine(connection_url)
    metadata = MetaData()
    metadata.reflect(bind=engine)
    client_consent_table = metadata.tables['clientConsent']
    query = client_consent_table.select()

    with engine.connect() as connection:
        result_proxy = connection.execute(query)
        rows = result_proxy.fetchall()
    
    csv_filename = "ClientConsent/ClientConsent.csv"
    
    with open(csv_filename, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(result_proxy.keys())
        csv_writer.writerows(rows)

def download_files_from_s3(bucket_name, prefix, local_directory):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name, 'Prefix': prefix}

    for page in paginator.paginate(**operation_parameters):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if not key.endswith('/'):  # Ignore directories
                    download_path = os.path.join(local_directory, key)  # Include directory in filename
                    if not os.path.exists(download_path):  # Check if file already exists
                        os.makedirs(os.path.dirname(download_path), exist_ok=True)  # Create directory if not exists
                        s3.download_file(bucket_name, key, download_path)
                        print(f"Downloaded {key} to {download_path}")
                    else:
                        print(f"Skipping {key}. File already exists at {download_path}")

# Specify your bucket name, prefix, and local directory to save files
bucket_name = 'qsuite'
prefix = 'ClientConsent/'
local_directory = '.'  # Current directory

if __name__ == "__main__":
    download_files_from_s3(bucket_name, prefix, local_directory)
    download_table()