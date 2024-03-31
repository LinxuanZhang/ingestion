# helper functions
import boto3
from botocore.exceptions import ClientError
import json
import polars as pl
from io import BytesIO
import requests
import gzip

def get_df_from_url(url, file_name=None):
    response = requests.get(url)
    if file_name:
        print(f'Downloading {file_name}...')
    if response.status_code == 200:
        compressed_data = BytesIO(response.content)
        # decomprese
        decompressed_data = gzip.GzipFile(fileobj=compressed_data).read()
        # load into io to read
        data_io = BytesIO(decompressed_data)
        df = pl.read_csv(data_io, separator='\t')
        return df
    else:
        print(f"Failed to download the file: status code {response.status_code}")



def get_secret(secret_name="datalake-access", region_name="eu-west-2"):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except ClientError as e:
        raise e

def check_file_exists(bucket, key):
    s3_client = boto3.client('s3')
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise e

def get_parquet_from_s3(object_key):
    secret = get_secret()
    s3_client = boto3.client('s3', aws_access_key_id=secret['s3_access_key_secret_name'], aws_secret_access_key=secret['s3_secret_key_secret_name'])
    bucket_name = secret['s3_bucket_name_secret_name']
    obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    # Read data into a pandas DataFrame
    print('loading parquet...')
    df = pl.read_parquet(BytesIO(obj['Body'].read()))
    return df


def get_gz_from_s3(object_key):
    secret = get_secret()
    s3_client = boto3.client('s3', aws_access_key_id=secret['s3_access_key_secret_name'], aws_secret_access_key=secret['s3_secret_key_secret_name'])
    bucket_name = secret['s3_bucket_name_secret_name']
    obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    # Read data into a pandas DataFrame
    print('loading resource...')
    df = pl.read_csv(BytesIO(obj['Body'].read()), truncate_ragged_lines=True, separator='\t')   
    return df



