import os
import s3fs
import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

s3 = s3fs.S3FileSystem(anon=False)

NC_BUCKET = os.environ.get("NC_BUCKET")

object = 's3://'+NC_BUCKET+'/data/PartyChange/2020_party_change_list.csv'

first = True
for chunk in pd.read_csv(object, chunksize=100000):
    if first:
        chunk.to_csv('party_change_20200320.csv',
                     index=False)
        first = False
    else:
        chunk.to_csv('party_change_20200320.csv',
                     index=False,
                     header=False,
                     mode='a')
