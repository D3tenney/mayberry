from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv
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

s3 = s3fs.S3FileSystem(anon=False)

zip_archive_location = f's3://{NC_BUCKET}/data/ncvoter_Statewide.zip'

with s3.open(zip_archive_location, 'rb') as access:
    access.seek(0)
    with ZipFile(access) as archive:
        print(archive.infolist())
        shapes = []
        first = True
        df = pd.read_csv(archive.open('ncvoter_Statewide.txt'),
                         sep='\t',
                         encoding="ISO-8859-1",
                         dtype=object,
                         usecols=['county_id', 'county_desc',
                                  'ethnic_code',
                                  'race_code',
                                  'party_cd',
                                  'county_id', 'precinct_abbrv',
                                               'precinct_desc',
                                  'status_cd', 'voter_status_desc',
                                  'reason_cd', 'voter_status_reason_desc'])

# make helper tables
county_df = (df[['county_id', 'county_desc']]
             .rename(columns={'county_id': 'id', 'county_desc': 'name'})
             .sort_values('id')
             .drop_duplicates().reset_index(drop=True))

ethnic_df = (df[['ethnic_code']]
             .rename(columns={'ethnic_code': 'code'})
             .sort_values('code')
             .drop_duplicates().reset_index(drop=True)
             )
ethnic_df.name = 'ethnic_df'

race_df = (df[['race_code']]
           .rename(columns={'race_code': 'code'})
           .sort_values('code')
           .drop_duplicates().reset_index(drop=True))
race_df.name = 'race_df'

party_df = (df[['party_cd']]
            .rename(columns={'party_cd': 'cd'})
            .sort_values('cd')
            .drop_duplicates().reset_index(drop=True))
party_df.name = 'party_df'

precinct_df = (df[['county_id', 'precinct_abbrv', 'precinct_desc']]
               .rename(columns={'precinct_abbrv': 'abbrv',
                                'precinct_desc': 'description'})
               .sort_values(['county_id', 'abbrv'])
               .drop_duplicates().reset_index(drop=True))

status_df = (df[['status_cd', 'voter_status_desc']]
             .rename(columns={'status_cd': 'cd',
                              'voter_status_desc': 'description'})
             .sort_values('cd')
             .drop_duplicates().reset_index(drop=True))

reason_df = (df[['reason_cd', 'voter_status_reason_desc']]
             .rename(columns={'reason_cd': 'cd',
                              'voter_status_reason_desc': 'description'})
             .sort_values('cd')
             .drop_duplicates().reset_index(drop=True))

description_dfs = [ethnic_df, race_df, party_df]
description_rows = {'ethnic_df': ['HISPANIC OR LATINO',
                                  'NOT HISPANIC OR LATINO',
                                  'NOT PROVIDED'],
                    'race_df': [None, 'ASIAN',
                                'BLACK OR AFRICAN AMERICAN',
                                'AMERICAN INDIAN OR ALASKA NATIVE',
                                'TWO OR MORE RACES', 'OTHER',
                                'UNDESIGNATED', 'WHITE'],
                    'party_df': ['CONSTITUTION PARTY',
                                 'DEMOCRATIC PARTY',
                                 'GREEN PARTY', 'LIBERTARIAN PARTY',
                                 'REPUBLICAN PARTY', 'UNAFFILIATED']}

for entry in description_dfs:
    if entry.shape[0] == len(description_rows[entry.name]):
        entry['description'] = description_rows[entry.name]
    else:
        exec('global '+entry.name)
        exec(entry.name+' = None')

# Insert into db tables
# TODO: Some of these might be combined into a single loop.
# TODO: Handle new rows for race, ethnicity, and party

if type(county_df) == pd.core.frame.DataFrame:
    for idx, row in county_df.iterrows():
        insert_statement = ("INSERT INTO county (id, name)"
                            "VALUES "
                            f"({row['id']}, '{row['name']}')")
        try:
            conn.execute(insert_statement)
        except IntegrityError:
            continue
        except Exception as e:
            print(f"error: {e}")
else:
    print("No insert for county")

if type(ethnic_df) == pd.core.frame.DataFrame:
    for idx, row in ethnic_df.iterrows():
        insert_statement = ("INSERT INTO ethnicity (code, description)"
                            "VALUES "
                            f"('{row['code']}', '{row['description']}')")
        try:
            conn.execute(insert_statement)
        except IntegrityError:
            continue
        except Exception as e:
            print(f"error: {e}")
else:
    print("No insert for ethnicity")

if type(race_df) == pd.core.frame.DataFrame:
    for idx, row in race_df.iterrows():
        insert_statement = ("INSERT INTO race (code, description)"
                            "VALUES "
                            f"('{row['code']}', '{row['description']}')")
        try:
            conn.execute(insert_statement)
        except IntegrityError:
            continue
        except Exception as e:
            print(f"error: {e}")
else:
    print("No insert for race")

if type(party_df) == pd.core.frame.DataFrame:
    for idx, row in party_df.iterrows():
        insert_statement = ("INSERT INTO party (cd, description)"
                            "VALUES "
                            f"('{row['cd']}', '{row['description']}')")
        try:
            conn.execute(insert_statement)
        except IntegrityError:
            continue
        except Exception as e:
            print(f"error: {e}")
else:
    print("No insert for party")

if type(precinct_df) == pd.core.frame.DataFrame:
    for idx, row in precinct_df.iterrows():
        insert_statement = ("INSERT INTO precinct "
                            "(county_id, abbrv, description) "
                            "VALUES "
                            f"({row['county_id']}, '{row['abbrv']}', "
                            f"'{row['description']}')")
        try:
            conn.execute(insert_statement)
        except IntegrityError:
            continue
        except Exception as e:
            print(f"error: {e}")
else:
    print("No insert for precinct")

if type(status_df) == pd.core.frame.DataFrame:
    for idx, row in status_df.iterrows():
        insert_statement = ("INSERT INTO voter_status (cd, description)"
                            "VALUES "
                            f"('{row['cd']}', '{row['description']}')")
        try:
            conn.execute(insert_statement)
        except IntegrityError:
            continue
        except Exception as e:
            print(f"error: {e}")
else:
    print("No insert for voter_status")

if type(reason_df) == pd.core.frame.DataFrame:
    for idx, row in reason_df.iterrows():
        insert_statement = ("INSERT INTO voter_status_reason (cd, description)"
                            "VALUES "
                            f"('{row['cd']}', '{row['description']}')")
        try:
            conn.execute(insert_statement)
        except IntegrityError:
            continue
        except Exception as e:
            print(f"error: {e}")
else:
    print("No insert for voter_status_reason")

conn.close()
engine.dispose()
