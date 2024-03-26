# Your main script
import aioboto3
from io import BytesIO
import polars as pl
from async_function import get_df_from_url_async, get_secret_async, check_file_exists_async, get_gz_from_s3_async, clean_soma_df
import aiohttp
import asyncio

async def process_and_upload_file_async(session, url, file_name, annotation_df, secret, bucket_name, base_s3_key):
    print(f'Ingesting {file_name}')
    if file_name is None:
        return

    s3_key = f'{base_s3_key}/{file_name}.parquet'
    if await check_file_exists_async(bucket_name, s3_key):
        return
    
    df = await get_df_from_url_async(session, url, file_name)
    df = df.with_columns(pl.lit(file_name).alias('file_name'))
    df = clean_soma_df(df, annotation_df)

    buffer = BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)

    async with aioboto3.client('s3', aws_access_key_id=secret['s3_access_key_secret_name'], aws_secret_access_key=secret['s3_secret_key_secret_name']) as s3_client:
        await s3_client.upload_fileobj(buffer, bucket_name, s3_key)

async def main():
    async with aiohttp.ClientSession() as session:
        secret = await get_secret_async()
        bucket_name = secret['s3_bucket_name_secret_name']
        base_s3_key = 'TER/deCODE_SomaScan'

        annotation_df = await get_gz_from_s3_async(session, 'Resource/assocvariants.annotated.txt.gz')
        annotation_df = (
            annotation_df
            .select(['Name', 'effectAlleleFreq'])
            .rename({'effectAlleleFreq':'eaf'})
        )
        print('loading configs')
            # Load Configuration
        bucket_name = secret['s3_bucket_name_secret_name']

        # Base key
        base_s3_key = 'TER/deCODE_SomaScan'

        # Get manifest
        manifest = pl.read_csv('manifest/decode_protein_manifest.csv')
        urls = manifest['urls'].to_list()
        file_names = manifest['filename'].to_list()

        tasks = [process_and_upload_file_async(session, url, file_name, annotation_df, secret, bucket_name, base_s3_key) for url, file_name in zip(urls, file_names)]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
