import boto3
import polars as pl
from io import BytesIO
import concurrent
from function import get_df_from_url, get_secret, check_file_exists, get_gz_from_s3



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
    
    if file_name is None:
        return

    s3_key = f'{base_s3_key}/{file_name}.parquet'
    if check_file_exists(bucket_name, s3_key):
        print(f'{file_name} completed, skipping')
        return

    in_chr = [check_file_exists(bucket_name, f"TER/deCODE_SomaScan/chr{chrom}/{file_name}.parquet") for chrom in range(1, 24)]
    if all(in_chr):
        print(f'{file_name} completed, skipping')
        return

    print(f'Ingesting {file_name}')
    # processing
    df = get_df_from_url(url)
    df = df.with_columns(pl.lit(file_name).alias('file_name'))
    df = clean_soma_df(df, annotation_df)
    for chrom in df['chr'].unique().to_list():
        partition_df = df.filter(pl.col('chr') == chrom)
        partition_key = f"TER/deCODE_SomaScan/chr{chrom}/{file_name}.parquet"
        buffer = BytesIO()
        partition_df.write_parquet(buffer)
        buffer.seek(0)
        s3_client.put_object(Bucket=bucket_name, Key=partition_key, Body=buffer)

    # buffer = BytesIO()
    # df.write_parquet(buffer)
    # buffer.seek(0)

    
    # s3_client.upload_fileobj(buffer, bucket_name, s3_key)


if __name__ == "__main__":
    print('loading annotation files')
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
    s3_client = boto3.client('s3', aws_access_key_id=secret['s3_access_key_secret_name'], aws_secret_access_key=secret['s3_secret_key_secret_name'])

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_and_upload_file, url, file_name, annotation_df, secret, bucket_name, base_s3_key) for url, file_name in zip(urls, file_names)]

    # Handle Failed Uploads
    failed_uploads = [future.result() for future in futures if future.exception()]
    if failed_uploads:
        print("Failed uploads:", failed_uploads)
