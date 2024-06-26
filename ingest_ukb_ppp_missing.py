import synapseclient
import synapseutils 
import tarfile
import polars as pl
import os
import tempfile
from io import BytesIO
import boto3
import gzip
import concurrent.futures
from function import get_secret, check_file_exists, get_parquet_from_s3
import shutil

def get_ukb_concat_df(cur_id, file_name):
    temp_dir = f'/home/ubuntu/ingestion/{tempfile.mkdtemp()}'
    os.makedirs(temp_dir, exist_ok=True)
    if True: 
        # print(f"Temporary directory created at: {temp_dir}")
        files = synapseutils.syncFromSynapse(syn, cur_id, path=temp_dir)
        concatenated_df = None
        with tarfile.open(os.path.join(temp_dir, file_name), 'r') as tar:
            gz_files = [m for m in tar.getmembers() if m.name.endswith('.gz')]
            for member in gz_files:
                # Ensure the member is a file before proceeding
                if member.isfile():
                    # Use tar.extractfile() to get a file-like object
                    file_obj = tar.extractfile(member)
                    # Check if the file object is not None
                    if file_obj is not None:
                        # Decompress the gzip content
                        with gzip.open(file_obj, 'rt', encoding='utf-8') as gz:
                            # Since the content is now decompressed and treated as text,
                            # we read it into a string and then use BytesIO so Polars can read it as if it were a file
                            buffer = BytesIO(gz.read().encode('utf-8'))
                            # Read the buffer into a Polars DataFrame specifying the separator
                            df = pl.read_csv(buffer, separator=' ')
                            # Concatenate to the accumulating DataFrame
                            if concatenated_df is None:
                                concatenated_df = df
                            else:
                                concatenated_df = pl.concat([concatenated_df, df], how='vertical')
    concatenated_df = concatenated_df.with_columns(pl.lit(file_name).alias('file_name'))
    shutil.rmtree(temp_dir)
    return concatenated_df

def clean_df(df, mapping_df):
    # merge with maping file to get position and rsid
    df = df.join(mapping_df.select(['ID', 'rsid', 'POS38']), on='ID', how='left')
    # rename columns
    df = df.rename({'CHROM':'chr', 'ALLELE0':'other_allele', 
                    'ALLELE1':'effect_allele', 'rsid':'SNP', 
                    'SE':'se', 'A1FREQ':'eaf', 'BETA':'beta', 
                    'POS38':'pos', 'LOG10P':'mlogp'})
    # calculate pval
    df = df.with_columns((10**(-pl.col('mlogp'))).alias('pval'))
    # select standard columns
    df = df.select(['SNP', 'chr', 'pos', 'effect_allele', 'other_allele', 'eaf', 'beta', 'se', 'pval', 'mlogp', 'file_name'])
    # delete empty SNP row
    df = df.filter(pl.col('SNP').is_not_null())
    return df

def process_and_upload_file(mapping_df, cur_id, file_name, bucket_name, base_s3_key):
    # check if the file as already been ingested
    s3_key = f'{base_s3_key}/{file_name.replace(".tar", ".parquet").lower()}'
    if check_file_exists(bucket_name, s3_key):
        print(f'{file_name.replace(".tar", ".parquet").lower()} already exists, skipping')
        return
    
    in_chr = [check_file_exists(bucket_name, f'TER/UKB_Olink/chr{chrom}/{file_name.replace(".tar", ".parquet").lower()}') for chrom in range(1, 24)]
    if all(in_chr):
        print(f'{file_name.replace(".tar", ".parquet").lower()} completed, skipping')
        return
    
    # start ingestion
    print(f'Ingesting {file_name.replace(".tar", ".parquet").lower()}')
    # download and merge form synapseclient
    df = get_ukb_concat_df(cur_id, file_name)
    # clean and merge to get rsid
    df = clean_df(df, mapping_df) 
    for chrom in df['chr'].unique().to_list():
        partition_key = f'TER/UKB_Olink/chr{chrom}/{file_name.replace(".tar", ".parquet").lower()}'
        if check_file_exists(bucket_name, partition_key):
            continue
        
        partition_df = df.filter(pl.col('chr') == chrom)
        
        buffer = BytesIO()
        partition_df.write_parquet(buffer)
        buffer.seek(0)
        s3_client.put_object(Bucket=bucket_name, Key=partition_key, Body=buffer)

    print(f'{file_name} ingestion finished')

if __name__ == "__main__":
    print('loading mapping files')
    mapping_df = get_parquet_from_s3('Resource/build_mapping.parquet')
    mapping_df = mapping_df.select(['ID', 'rsid', 'POS38'])
    print(mapping_df.head())
    print('loading configs')
    # Load Configuration
    secret = get_secret()
    bucket_name = secret['s3_bucket_name_secret_name']
    token = secret['UKB_synapseclient_token']
    syn = synapseclient.Synapse() 
    syn.login(authToken=token)
    # get manifest
    query = syn.tableQuery("SELECT * FROM syn53038826 WHERE ( ( \"parentId\" = 'syn51365308' ) )")
    ids = list(query.asDataFrame().id)
    file_names = list(query.asDataFrame().name)
    # base key
    base_s3_key = 'TER/UKB_Olink'
    #for cur_id, file_name in zip(ids, file_names):
     #   process_and_upload_file(mapping_df, cur_id, file_name, bucket_name, base_s3_key)
    # submitting jobs
    s3_client = boto3.client('s3', aws_access_key_id=secret['s3_access_key_secret_name'], aws_secret_access_key=secret['s3_secret_key_secret_name'])
    cur_id = 'syn52357899'
    file_name = 'PTPRB_P23467_OID30324_v1_Cardiometabolic_II.tar'
    process_and_upload_file(mapping_df, cur_id, file_name, bucket_name, base_s3_key)

