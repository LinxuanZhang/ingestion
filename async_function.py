# async_function.py
import aiohttp
import aioboto3
import asyncio
import boto3
from botocore.exceptions import ClientError
import json
import polars as pl
from io import BytesIO
import gzip

async def get_df_from_url_async(session, url, file_name=None):
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

async def get_secret_async(secret_name="datalake-access", region_name="eu-west-2"):
    async with aioboto3.client(service_name='secretsmanager', region_name=region_name) as client:
        response = await client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])

async def check_file_exists_async(bucket, key):
    async with aioboto3.client('s3') as s3_client:
        try:
            await s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except s3_client.exceptions.NoSuchKey:
            return False
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise e

async def get_gz_from_s3_async(session, object_key):
    secret = await get_secret_async()
    async with session.client('s3', aws_access_key_id=secret['s3_access_key_secret_name'], aws_secret_access_key=secret['s3_secret_key_secret_name']) as s3_client:
        bucket_name = secret['s3_bucket_name_secret_name']
        obj = await s3_client.get_object(Bucket=bucket_name, Key=object_key)
        df = pl.read_csv(BytesIO(obj['Body'].read()), truncate_ragged_lines=True, sep='\t')
        return df

def clean_soma_df(df, annotation_df):
    df = (
        df
        .with_columns(pl.col('Chrom').str.extract('chr(.*)', 1).alias('chr'))
        .with_columns(
            pl.when(pl.col('chr') == 'X')
            .then(23)
            .otherwise(pl.col('chr'))
            .cast(pl.Int32)
            .alias('chr'))
        .rename({'Pos': 'pos',
                 'otherAllele':'other_allele', 
                 'effectAllele':'effect_allele', 
                 'rsids':'SNP', 
                 'Beta':'beta',
                 'SE':'se', 
                 'Pval':'pval',
                 'minus_log10_pval':'mlogp'})
        .filter(pl.col('SNP').str.starts_with('rs'))
        .join(annotation_df, on='Name', how='left')
        .select(['SNP', 'chr', 'pos', 'effect_allele', 'other_allele', 'eaf', 'beta', 'se', 'pval', 'mlogp', 'file_name'])
    )
    return df


