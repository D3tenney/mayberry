import pandas as pd

from app import app, celery, S3FS


@celery.task
def process_compare_voter(message):
    # set up output variables

    output_prefix = f's3://{app.config["MAYBERRY_BUCKET"]}/comparisons'
    output_file_name = f'{message["output_id"]}.parquet'

    # Read in the individual files and join
    ind_file_prefix = f's3://{app.config["MAYBERRY_BUCKET"]}/vf_vh_ind_files'

    vf_1_path = f'vf_updated_on={message["vf_1_date"]}/vf_vh_ind_file.parquet'
    vf_1_df = pd.read_parquet(f'{ind_file_prefix}/{vf_1_path}')

    vf_2_path = f'vf_updated_on={message["vf_2_date"]}/vf_vh_ind_file.parquet'
    vf_2_df = pd.read_parquet(f'{ind_file_prefix}/{vf_2_path}')

    merged_df = pd.merge(vf_1_df, vf_2_df,
                         on=['ncid'],
                         how='outer',
                         indicator=True,
                         suffixes=('_1', '_2'))

    # Identify voters new to the file and write out
    new_voters = (merged_df[[col for col in merged_df.columns
                             if col == 'ncid'
                             or '_2' in col]]
                           [merged_df['_merge'] == 'right_only'])
    print(f'new voters: {new_voters.shape[0]}')
    new_voters.to_parquet(f'{output_prefix}/new_voters/{output_file_name}')

    # Identify voters dropped from the file and write out
    removed_voters = (merged_df[[col for col in merged_df.columns
                                 if col == 'ncid'
                                 or '_1' in col]]
                               [merged_df['_merge'] == 'left_only'])
    print(f'removed voters: {removed_voters.shape[0]}')
    removed_voters.to_parquet(f'{output_prefix}/removed_voters/{output_file_name}')

    # Identify voters who have had a status change and write out
    status_change_voters = merged_df[(merged_df['_merge'] == 'both') &
                                     (merged_df['status_cd_1'] != merged_df['status_cd_2'])]
    print(f'status change: {status_change_voters.shape[0]}')
    status_change_voters.to_parquet(f'{output_prefix}/status_change/{output_file_name}')

    # Identify voters who have changed party and write out
    party_change_voters = merged_df[(merged_df['_merge'] == 'both') &
                                    (merged_df['party_cd_1'] != merged_df['party_cd_2'])]
    print(f'party change voters: {party_change_voters.shape[0]}')
    party_change_voters.to_parquet(f'{output_prefix}/party_change/{output_file_name}')
