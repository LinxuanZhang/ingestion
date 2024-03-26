# helper functions
import boto3
from botocore.exceptions import ClientError
import json
import polars as pl
from io import BytesIO
import requests
import gzip
import aiohttp
import aioboto3


async def get_df_from_url_async(url, file_name=None):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if file_name:
                print(f'Downloading {file_name}...')
            if response.status == 200:
                compressed_data = BytesIO(await response.read())
                decompressed_data = gzip.GzipFile(fileobj=compressed_data).read()
                data_io = BytesIO(decompressed_data)
                df = pl.read_csv(data_io, sep='\t')
                return df
            else:
                print(f"Failed to download the file: status code {response.status}")


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
    print('loading mapping...')
    df = pl.read_parquet(BytesIO(obj['Body'].read()))
    print('saving mapping...')
    df.write_parquet('build_mapping.parquet')
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


