import s3fs
import pandas as pd
from zipfile import ZipFile

from sqlalchemy.dialects.postgresql import insert
'''
from app import app, db, celery, County, Ethnicity, Party, Precinct, Race, \
                VoterStatus, VoterStatusReason
'''
from constants import HELPER_SUBSET, HELPER_DESCRIPTIONS

"""
@celery.task()
def process_ingestion_helper(message):
    print('----------PROCESSING------------------')
    # Read in file, chunk and push to helper tables
    s3 = s3fs.S3FileSystem(anon=False)

    vf_key = f's3://{app.config["NC_BUCKET"]}/data/{app.config["VF_FILE"]}.zip'
    try:
        with s3.open(vf_key, 'rb') as access:
            access.seek(0)
            with ZipFile(access) as archive:
                df = pd.read_csv(archive.open(f'{app.config["VF_FILE"]}.txt'),
                                 sep='\t',
                                 encoding="ISO-8859-1",
                                 dtype=object,
                                 usecols=HELPER_SUBSET)

        # The pattern is to create a dataframe of just the relevant
        # columns, drop duplicates, and insert to database.
        # On Conflict Do Nothing is key here.
        print('----------FILE READ-------------------')
        "COUNTY"
        county_df = (df[['county_id', 'county_desc']]
                     .rename(columns={'county_id': 'id',
                                      'county_desc': 'name'})
                     .drop_duplicates())
        cnty_statement = insert(County).values(county_df.to_dict('records'))
        cnty_statement = cnty_statement.on_conflict_do_nothing()
        results = db.session.execute(cnty_statement)
        print('----------COUNTY----------------------')
        "PRECINCT"
        precinct_df = (df[['county_id', 'precinct_abbrv', 'precinct_desc']]
                       .rename(columns={'precinct_abbrv': 'abbrv',
                                        'precinct_desc': 'description'})
                       .drop_duplicates())
        pct_statement = insert(Precinct).values(precinct_df.to_dict('records'))
        pct_statement = pct_statement.on_conflict_do_nothing()
        results = db.session.execute(pct_statement)
        print('----------PRECINCT--------------------')
        "VOTER STATUS"
        status_df = (df[['status_cd', 'voter_status_desc']]
                     .rename(columns={'status_cd': 'cd',
                                      'voter_status_desc': 'description'})
                     .drop_duplicates())
        stat_statement = insert(VoterStatus).values(status_df.to_dict('records'))
        stat_statement = stat_statement.on_conflict_do_nothing()
        results = db.session.execute(stat_statement)
        print('----------VOTER STATUS----------------')
        "VOTER STATUS REASON"
        reason_df = (df[['reason_cd', 'voter_status_reason_desc']]
                     .rename(columns={'reason_cd': 'cd',
                                      'voter_status_reason_desc': 'description'})
                     .drop_duplicates())
        rsn_statement = (insert(VoterStatusReason)
                         .values(reason_df.to_dict('records')))
        rsn_statement = rsn_statement.on_conflict_do_nothing()
        results = db.session.execute(rsn_statement)
        print('----------VOTER STATUS REASON---------')
        # These three tables need 'description' columns added manually
        "ETHNICITY"
        ethnic_df = (df[['ethnic_code']]
                     .rename(columns={'ethnic_code': 'code'})
                     .drop_duplicates())
        ethnic_df['description'] = (ethnic_df['code']
                                    .map(HELPER_DESCRIPTIONS['ethnicity']))
        eth_statement = (insert(Ethnicity)
                         .values(ethnic_df.to_dict('records')))
        eth_statement = eth_statement.on_conflict_do_nothing()
        results = db.session.execute(eth_statement)
        print('----------ETHNICITY-------------------')
        "RACE"
        race_df = (df[['race_code']]
                   .rename(columns={'race_code': 'code'})
                   .drop_duplicates())
        race_df['description'] = race_df['code'].map(HELPER_DESCRIPTIONS['race'])
        race_statement = (insert(Race)
                          .values(race_df.to_dict('records')))
        race_statement = race_statement.on_conflict_do_nothing()
        results = db.session.execute(race_statement)
        print('----------RACE------------------------')
        "PARTY"
        party_df = (df[['party_cd']]
                    .rename(columns={'party_cd': 'cd'})
                    .drop_duplicates())
        party_df['description'] = party_df['cd'].map(HELPER_DESCRIPTIONS['party'])
        prty_statement = (insert(Party)
                          .values(party_df.to_dict('records')))
        prty_statement = prty_statement.on_conflict_do_nothing()
        results = db.session.execute(prty_statement)
        print('----------PARTY-----------------------')
        # Commit transaction
        db.session.commit()
        print('----------INSERT SUCCESSFUL-----------')
        return "Helper Insert Successful."
    except Exception as e:
        return f"error: {e}"
    return 'processing helper tables'
"""
