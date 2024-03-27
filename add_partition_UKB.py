import boto3
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from function import get_secret
from io import BytesIO
import re
from function import check_file_exists

secret = get_secret()
bucket_name = secret['s3_bucket_name_secret_name']
s3_client = boto3.client('s3', aws_access_key_id=secret['s3_access_key_secret_name'], aws_secret_access_key=secret['s3_secret_key_secret_name'])
source_prefix = 'TER/UKB_Olink/'
destination_prefix = 'chr'

def list_s3_objects(bucket, prefix):
    """List objects in an S3 bucket with pagination."""
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    return [content['Key'] for page in page_iterator for content in page.get('Contents', []) if 'chr' not in content['Key'] and 'parquet' in content['Key']]

def partition_and_transfer_file(file_key):
    try:
        # Download the file to process
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pl.read_parquet(BytesIO(response['Body'].read()))
        pattern = r'UKB_Olink/(.*?)\.parquet'
        file_name = re.search(pattern, file_key).group(1)
        
        # Assuming a 'date' column for partitioning - adjust as needed
        for chrom in df['chr'].unique().to_list():
            partition_key = f"{source_prefix}{destination_prefix}{chrom}/{file_name}.parquet"
            if check_file_exists(bucket_name, partition_key):
                continue
            
            partition_df = df.filter(pl.col('chr') == chrom)
            
            buffer = BytesIO()
            partition_df.write_parquet(buffer)
            buffer.seek(0)
            s3_client.put_object(Bucket=bucket_name, Key=partition_key, Body=buffer)

        # Delete original file after successful partitioning
        s3_client.delete_object(Bucket=bucket_name, Key=file_key)
        return f"Processed and deleted {file_key}"
    except Exception as e:
        return f"Failed to process {file_key}: {e}"

def main():
    file_keys = list_s3_objects(bucket_name, source_prefix)
    
    # Use ThreadPoolExecutor to parallelize processing
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_file = {executor.submit(partition_and_transfer_file, key): key for key in file_keys}
        for future in as_completed(future_to_file):
            result = future.result()
            print(result)

if __name__ == "__main__":
    main()
