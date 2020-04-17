import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import s3fs
import pandas as pd

load_dotenv(override=True)

# DATABASE CONNECTION
PG_USER = os.environ.get("PG_USER")
PG_PASS = os.environ.get("PG_PASS")
PG_HOST = os.environ.get("PG_HOST")
PG_DB = os.environ.get("PG_DB")
PG_PORT = os.environ.get("PG_PORT")

NC_BUCKET = os.environ.get("NC_BUCKET")

conn_string = "postgresql://{0}:{1}@{2}:{3}/{4}".format(
    PG_USER, PG_PASS, PG_HOST, PG_PORT, PG_DB)
engine = create_engine(conn_string)
conn = engine.connect()

s3_object = 's3://'+NC_BUCKET+'/data/PartyChange/2020_party_change_list.csv'

for chunk in pd.read_csv(s3_object,
                         chunksize=10000):
    chunk = chunk[['voter_reg_num', 'change_dt', 'party_from',
                   'party_to', 'county_id']]
    chunk.to_sql('party_change',
                 if_exists='append',
                 index=False,
                 method='multi',
                 chunksize=2000,
                 con=conn)
