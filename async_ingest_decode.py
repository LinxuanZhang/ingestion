import boto3
import polars as pl
from io import BytesIO
import concurrent
from async_function import get_df_from_url_async, get_secret, check_file_exists, get_gz_from_s3
import aiohttp
import asyncio


def clean_soma_df(df, annotation_df):
    df = (
        df
        .with_columns(pl.col('Chrom').str.extract('chr(.*)', 1).alias('chr'))
        .with_columns(
            pl.when(pl.col('chr') == 'X')  # Check if 'chr' is "X"
            .then(23)  # Then assign 23
            .otherwise(pl.col('chr'))  # Otherwise, keep the original value
            .cast(pl.Int32)  # Now cast the entire column to integers
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


def process_and_upload_file(url, file_name, annotation_df, secret, bucket_name, base_s3_key):
    print(f'Ingesting {file_name}')
    if file_name is None:
        return

    s3_key = f'{base_s3_key}/{file_name}.parquet'
    if check_file_exists(bucket_name, s3_key):
        return

    # processing
    df = get_df_from_url_async(url)
    df = df.with_columns(pl.lit(file_name).alias('file_name'))
    df = clean_soma_df(df, annotation_df)

    buffer = BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    s3_client = boto3.client('s3', aws_access_key_id=secret['s3_access_key_secret_name'], aws_secret_access_key=secret['s3_secret_key_secret_name'])
    s3_client.upload_fileobj(buffer, bucket_name, s3_key)


async def process_and_upload_file_async(url, file_name, annotation_df, secret, bucket_name, base_s3_key):
    print(f'Ingesting {file_name}')
    if file_name is None:
        return

    s3_key = f'{base_s3_key}/{file_name}.parquet'
    if check_file_exists(bucket_name, s3_key):
        return
    
    # use async function
    df = await get_df_from_url_async(url, file_name)
    # Assume you adapt your processing and uploading code to be async as well
    df = df.with_columns(pl.lit(file_name).alias('file_name'))
    df = clean_soma_df(df, annotation_df)

    buffer = BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    s3_client = boto3.client('s3', aws_access_key_id=secret['s3_access_key_secret_name'], aws_secret_access_key=secret['s3_secret_key_secret_name'])
    s3_client.upload_fileobj(buffer, bucket_name, s3_key)

async def main():
    annotation_df = get_gz_from_s3('Resource/assocvariants.annotated.txt.gz')
    annotation_df = (
        annotation_df
        .select(['Name', 'effectAlleleFreq'])
        .rename({'effectAlleleFreq':'eaf'})
    )
    print('loading configs')
    # Load Configuration
    secret = get_secret()
    bucket_name = secret['s3_bucket_name_secret_name']

    # base key
    base_s3_key = 'TER/deCODE_SomaScan'

    # get manifest
    manifest = pl.read_csv('manifest/decode_protein_manifest.csv')
    urls = manifest['urls'].to_list()
    file_names = manifest['filename'].to_list()

    # You would get your manifest and annotation_df asynchronously or prepare them before entering the async context
    # Then loop through your tasks
    tasks = [process_and_upload_file_async(url, file_name, annotation_df, secret, bucket_name, base_s3_key) for url, file_name in zip(urls, file_names)]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
