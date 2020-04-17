from sqlalchemy import create_engine
from dotenv import load_dotenv
from zipfile import Zipfile
from datetime import date
import pandas as pd
import s3fs
import os

load_dotenv(override=True)

PG_USER = os.environ.get("PG_USER")
PG_PASS = os.environ.get("PG_PASS")
PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT")
PG_DB = os.environ.get("PG_DB")

NC_BUCKET = os.environ.get("NC_BUCKET")
MAYBERRY_BUCKET = os.environg.get("MAYBERRY_BUCKET")

zip_archive_location = f's3://{NC_BUCKET}/data/ncvoter_Statewide.zip'

year_month = str(date.today()).replace('-', '')[:6]
local_file_name = f'./voterfile/voterfile_nc_{year_month}.csv'

with s3.open(zip_archive_location, 'rb') as access:
    access.seek(0)
    with ZipFile(access) as archive:
        print(archive.infolist())
        shapes = []
        first = True
        for chunk in pd.read_csv(archive.open('ncvoter_Statewide.txt'),
                                 sep='\t', encoding="ISO-8859-1",
                                 chunksize=20000, dtype=object):
            shapes.append(chunk.shape[0])

            # write to local disk
            if first is True:
                chunk.to_csv(local_file_name,
                             index=False, header=True)
                first = False
            else:
                chunk.to_csv(local_file_name,
                             index=False, header=False, mode='a')

            # make helper tables
            county_df = (chunk[['county_id', 'county_desc']]
                         .drop_duplicates().reset_index(drop=True))
            ethnic_df = (chunk[['ethnic_code']]
                         .drop_duplicates().reset_index(drop=True))
            race_df = (chunk[['race_code']]
                       .drop_duplicates().reset_index(drop=True))
            party_df = (chunk[['party_cd']]
                        .drop_duplicates().reset_index(drop=True))
            precinct_df = (chunk[['county_id', 'precinct_abbrv', 'precinct_desc']]
                           .drop_duplicates().reset_index(drop=True))
            status_df = (chunk[['status_cd', 'voter_status_desc']]
                         .drop_duplicates().reset_index(drop=True))
            reason_df = (chunk[['reason_cd', 'voter_status_reason_desc']]
                         .drop_duplicates().reset_index(drop=True))

            HELPER_TABLES = [county_df, ethnic_df, race_df, party_df,
                             precinct_df, status_df, reason_df]

            # relevant columns to mayberry database
            chunk.to_sql('voter_staging',
                         if_exists='append',
                         index=False,
                         method='multi',
                         chunksize=20000,
                         con=conn)
