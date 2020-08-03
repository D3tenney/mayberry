from app import app, celery, S3FS, athenaclient

from app.core.file_read import poll_raw_copy_files, read_combine_vf_vh_files

from app.utils import date_from_string

from constants import VOTER_PARTITION_COLS

import time


@celery.task()
def process_ingestion_voter(message):
    # Check that files have finished copying from NC bucket to Mayberry bucket
    files_confirmed = poll_raw_copy_files(vf_time=message["vf_timestamp"],
                                          vh_time=message["vh_timestamp"])
    if files_confirmed is False:
        return f'Raw files do not exist to ingest vf {message["vf_timestamp"]}, vh {message["vh_timestamp"]}'

    # Add prior_voter and primary_voter to voterfile
    vf_df = read_combine_vf_vh_files(message["vh_timestamp"],
                                     message["vf_timestamp"])

    vf_date = date_from_string(str(message["vf_timestamp"]))

    # Write an individual-level file to s3.
    ind_file_prefix = f's3://{app.config["MAYBERRY_BUCKET"]}/vf_vh_ind_files'
    ind_file_loc = f'vf_updated_on={vf_date}/vf_vh_ind_file.parquet'
    vf_df.to_parquet(f'{ind_file_prefix}/{ind_file_loc}', index=False)

    print(f'File written to {ind_file_loc}. Rows: {vf_df.shape[0]}.')

    # Create counts grouped by VOTER_PARTITION_COLS
    print('grouping')
    vf_df = (vf_df.groupby(VOTER_PARTITION_COLS)
                  .count()['ncid']
                  .reset_index()
                  .rename(columns={'ncid': 'count'}))
    print(f'writing {vf_df.shape[0]} rows')

    # Write grouped counts to s3
    count_file_prefix = f's3://{app.config["MAYBERRY_BUCKET"]}/vf_vh_count_files'
    count_file_loc = f'vf_updated_on={vf_date}'
    count_file_name = 'vf_vh_file.parquet'
    vf_df.to_parquet(f'{count_file_prefix}/{count_file_loc}/{count_file_name}',
                     index=False)

    print(f'File written to {count_file_loc}. Rows: {vf_df.shape[0]}. IDs: {vf_df["count"].sum()}')

    # Add partition to Athena Table
    print('adding partition')
    partition = f"PARTITION (vf_updated_on='{vf_date}')"
    location = f"LOCATION '{count_file_prefix}/{count_file_loc}/'"
    query = f'ALTER TABLE {app.config["ATHENA_COUNT_TABLE"]} ADD {partition} {location}'
    print(query)
    query_id = athenaclient.start_query_execution(QueryString=query,
                                                  QueryExecutionContext={
                                                   'Database': app.config["ATHENA_DB"]
                                                  },
                                                  ResultConfiguration={
                                                   'OutputLocation': f's3://{app.config["MAYBERRY_BUCKET"]}/add_partition/'
                                                  })['QueryExecutionId']

    counter = 0
    while counter < 5:
        status = status = (athenaclient
                           .get_query_execution(QueryExecutionId=query_id)
                           ['QueryExecution']['Status']['State'])
        if status == 'SUCCEEDED':
            break
        if status in ['FAILED', 'CANCELLED']:
            print('Error adding partition to Athena table.')
        else:
            counter += 1
            time.sleep(2)
    print('partition added')
    return 'Individual and Grouped files successfully written.'
