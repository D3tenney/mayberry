import s3fs
import boto3
from zipfile import ZipFile
import pandas as pd
import io

s3 = s3fs.S3FileSystem(anon=False)

with s3.open('s3://dl.ncsbe.gov/data/ncvoter_Statewide.zip', 'rb') as access:
    access.seek(0)
    with ZipFile(access) as archive:
        print(archive.infolist())
        shapes = []
        first = True
        for chunk in pd.read_csv(archive.open('ncvoter_Statewide.txt'),
                                 sep='\t', encoding = "ISO-8859-1", chunksize=20000, dtype=object):
            shapes.append(chunk.shape[0])
            if first is True:
                chunk.to_csv('voterfile_nc_20200201.csv', index=False, header=True)
                first = False
            else:
                chunk.to_csv('voterfile_nc_20200201.csv', index=False, header=False, mode='a')
