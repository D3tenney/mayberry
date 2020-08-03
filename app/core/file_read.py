import s3fs
import pandas as pd
from zipfile import ZipFile
import time

from app import app, s3client
from constants import VOTE_HISTORY_READ_COLS, VOTER_DTYPES, VOTER_READ_COLS

s3 = s3fs.S3FileSystem(anon=False)


def read_vh_for_vf_ingestion(timestamp):
    vh_raw = f's3://{app.config["MAYBERRY_BUCKET"]}/vote_history/raw_copy/updated_at={timestamp}/{app.config["VH_FILE_ZIP"]}'
    try:
        # db.session.query(VoterStaging).delete()
        with s3.open(vh_raw, 'rb') as access:
            access.seek(0)
            with ZipFile(access) as archive:
                vh_df = pd.read_csv(archive.open(app.config["VH_FILE"]),
                                    sep='\t',
                                    encoding="ISO-8859-1",
                                    # nrows=100000,
                                    usecols=VOTE_HISTORY_READ_COLS,
                                    parse_dates=['election_lbl'])
                print(f'vh pulled, shape: {vh_df.shape[0]} rows')

                # Filter to primary/general elections in even years since 2012
                vh_df = vh_df[((vh_df['election_desc'].str.contains('GENERAL')) |
                               (vh_df['election_desc'].str.contains('PRIMARY'))) &
                              (vh_df['election_desc'].str.len() == 18) &
                              (vh_df['election_lbl'] >= '2012-01-01') &
                              (vh_df['election_lbl'].dt.year % 2 == 0)]
                print(f'relevant rows: {vh_df.shape[0]}')

                # Indicate primary/general election activity
                # TODO: add flag for midterm voters
                vh_df.loc[(vh_df['election_desc'].str.contains('GENERAL')),
                          'general'] = 1
                vh_df.loc[(vh_df['election_desc'].str.contains('PRIMARY')),
                          'primary'] = 1
                # Indicate whether an individual participated in primary/general
                vh_df = (vh_df[['ncid', 'general', 'primary']]
                         .groupby('ncid')
                         .max()
                         .reset_index())

                # prior_voter: Has participated in a primary OR general election
                vh_df.loc[(vh_df['general'] >= 1) |
                          (vh_df['primary'] >= 1), 'prior_voter'] = 1

                # primary_voter: Has participated in a primary election
                vh_df.loc[(vh_df['primary'] >= 1), 'primary_voter'] = 1

                # primary/general columns no longer needed
                vh_df.drop(columns=['general', 'primary'], inplace=True)
                return vh_df

    except Exception as e:
        print(f'vh error, {e}')


def read_vf_for_vf_ingestion(timestamp):
    vf_raw = f's3://{app.config["MAYBERRY_BUCKET"]}/voterfile/raw_copy/updated_at={timestamp}/{app.config["VF_FILE_ZIP"]}'
    try:
        with s3.open(vf_raw, 'rb') as access:
            access.seek(0)
            with ZipFile(access) as archive:
                vf_df = pd.read_csv(archive.open(app.config["VF_FILE"]),
                                    sep='\t',
                                    encoding="ISO-8859-1",
                                    # nrows=50000,
                                    dtype=VOTER_DTYPES,
                                    usecols=VOTER_READ_COLS)
                vf_df['absent_ind'] = vf_df['absent_ind'].replace(' ', '')
                vf_df = vf_df.fillna('')
                print(f'vf shape: {vf_df.shape[0]} rows')
                return vf_df
    except Exception as e:
        print(f'vf error, {e}')


def read_combine_vf_vh_files(vh_timestamp, vf_timestamp):
    print('vh read')
    vh_counter = 0
    vh_read = False
    while not vh_read and vh_counter < 5:
        try:
            vh_df = read_vh_for_vf_ingestion(vh_timestamp)
            vh_read = True
            print('vh read success')
        except Exception as e:
            vh_counter += 1
            time.sleep(5)

    print('vf read')
    vf_counter = 0
    vf_read = False
    while not vf_read and vf_counter < 5:
        try:
            vf_df = read_vf_for_vf_ingestion(vf_timestamp)
            vf_read = True
            print('vf read success')
        except Exception as e:
            vf_counter += 1
            time.sleep(5)

    vf_df = vf_df.merge(vh_df, on='ncid', how='left')
    print(f'merge successful. rows :{vf_df.shape[0]}')
    print('filling_nas')
    vf_df[['prior_voter', 'primary_voter']] = vf_df[['prior_voter', 'primary_voter']].fillna(0)
    vf_df['prior_voter'] = vf_df['prior_voter'].astype(int)
    vf_df['primary_voter'] = vf_df['primary_voter'].astype(int)
    return vf_df


def poll_raw_copy_files(vf_time, vh_time):
    vf_exists = False
    vh_exists = False

    counter = 0

    # check to make sure both voterfile and voter history have finished copying
    while counter < 5 or (vf_exists is False or vh_exists is False):
        try:
            vh_key = f'vote_history/raw_copy/updated_at={vh_time}/{app.config["VH_FILE_ZIP"]}'
            metadata_vh = s3client.head_object(Bucket=app.config["MAYBERRY_BUCKET"],
                                               Key=vh_key)
            if metadata_vh:
                vh_exists = True
        except Exception as e:
            pass
        try:
            vf_key = f'voterfile/raw_copy/updated_at={vf_time}/{app.config["VF_FILE_ZIP"]}'
            metadata_vf = s3client.head_object(Bucket=app.config["MAYBERRY_BUCKET"],
                                               Key=vf_key)
            if metadata_vf:
                vf_exists = True
        except Exception as e:
            pass

        if vf_exists and vh_exists:
            return True
            break
        else:
            time.sleep(20)

    return False
