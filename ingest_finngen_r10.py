import boto3
import polars as pl
import io
import re
import concurrent.futures
from botocore.exceptions import ClientError
from function import get_secret, check_file_exists


# Configuration and Utilities
def extract_substring(string):
    pattern = r'[^/]+\.gz$'
    match = re.search(pattern, string)
    return match.group() if match else None


# Main Processing Function
def process_and_upload_file(url, secret, bucket_name, base_s3_key):
    file_name = extract_substring(url)
    print(f'Ingesting {file_name}')
    if file_name is None:
        return

    s3_key = f'{base_s3_key}/{file_name.replace(".gz", ".parquet")}'
    if check_file_exists(bucket_name, s3_key):
        return

    df = pl.read_csv(url, separator='\t')
    df = df.rename({'#chrom':'chr', 'ref':'other_allele', 'alt':'effect_allele', 'rsids':'SNP', 'sebeta':'se', 'af_alt':'eaf'})
    df = df.select(['SNP', 'chr', 'pos', 'effect_allele', 'other_allele', 'eaf', 'beta', 'se', 'pval', 'mlogp'])
    df = df.filter(pl.col('SNP').is_not_null()).filter(pl.col('SNP').str.starts_with('rs'))

    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    s3_client = boto3.client('s3', aws_access_key_id=secret['s3_access_key_secret_name'], aws_secret_access_key=secret['s3_secret_key_secret_name'])
    s3_client.upload_fileobj(buffer, bucket_name, s3_key)


if __name__ == "__main__":
    # Load Configuration
    secret = get_secret()
    bucket_name = secret['s3_bucket_name_secret_name']
    base_s3_key = 'TER/FinnGen_r10'

    # Load Manifest and Process Files
    manifest = pl.read_csv("summary_stats_R10_manifest.tsv", separator='\t')
    url_list = manifest['path_https'].to_list()
    url_list = url_list[:2]

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        futures = [executor.submit(process_and_upload_file, url, secret, bucket_name, base_s3_key) for url in url_list]

    # Handle Failed Uploads
    failed_uploads = [future.result() for future in futures if future.exception()]
    if failed_uploads:
        print("Failed uploads:", failed_uploads)
