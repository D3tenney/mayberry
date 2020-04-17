from constants import VOTER_SUBSET, VOTER_DTYPES
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv
from datetime import datetime
from zipfile import ZipFile
import pandas as pd
import s3fs
import os

load_dotenv(override=True)

PG_USER = os.environ.get("PG_USER")
PG_PASS = os.environ.get("PG_PASS")
PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT")
PG_DB = os.environ.get("PG_DB")

engine_string = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(engine_string)
conn = engine.connect()

NC_BUCKET = os.environ.get("NC_BUCKET")
MAYBERRY_BUCKET = os.environ.get("MAYBERRY_BUCKET")

s3 = s3fs.S3FileSystem(anon=False)

zip_archive_location = f's3://{NC_BUCKET}/data/ncvoter_Statewide.zip'

with s3.open(zip_archive_location, 'rb') as access:
    access.seek(0)
    with ZipFile(access) as archive:
        print(archive.infolist())
        shapes = []
        first = True
        counter = 0
        file_month = str(datetime.today())[:7]
        for chunk in pd.read_csv(archive.open('ncvoter_Statewide.txt'),
                                 sep='\t',
                                 encoding="ISO-8859-1",
                                 nrows=500000,
                                 dtype=voter_dtypes,
                                 parse_dates=['registr_dt'],
                                 chunksize=500000):
            # write raw file
            counter += 1
            filename = f"chunk_{counter:02}.parquet"
            raw_prefix = f'/voterfile/raw_parquet/{file_month}/'
            chunk.to_parquet(f's3://{MAYBERRY_BUCKET}{raw_prefix}{filename}')

            # drop duplicates
            chunk = chunk[voter_subset]
            print(f"chunk_original: {chunk.shape[0]}")
            chunk = (chunk.drop_duplicates(subset=['ncid'])
                          .reset_index(drop=True))
            print(f"chunk_distinct: {chunk.shape[0]}")

            # indicate which month the file is from
            chunk['vintage_month'] = file_month

            # insert to SQL database
            chunk.to_sql('voter_staging',
                         con=conn,
                         if_exists='append',
                         method='multi',
                         chunksize=5000)

        with open('vf_insert_logic.sql', 'r') as f:
