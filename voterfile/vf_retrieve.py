from dotenv import load_dotenv
from zipfile import ZipFile
import pandas as pd
import s3fs
import os

load_dotenv(override=True)

NC_BUCKET = os.environ.get('NC_BUCKET')

s3 = s3fs.S3FileSystem(anon=False)

pull_date = ''
local_file_name = 'voterfile_nc_'+pull_date+'.csv'

zip_archive_location = 's3://'+NC_BUCKET+'/data/ncvoter_Statewide.zip'

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
            if first is True:
                chunk.to_csv(local_file_name,
                             index=False, header=True)
                first = False
            else:
                chunk.to_csv(local_file_name,
                             index=False, header=False, mode='a')
