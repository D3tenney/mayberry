from app import app, s3client


def max_previous_timestamp(file_type):
    if file_type == 'voterfile':
        file_name = app.config["VF_FILE_ZIP"]
    elif file_type == 'vote_history':
        file_name = app.config["VH_FILE_ZIP"]

    files = s3client.list_objects_v2(Bucket=app.config["MAYBERRY_BUCKET"],
                                     Prefix=f'{file_type}/raw_copy/')
    if 'Contents' in files.keys():
        timestamp_list = [int((obj["Key"].lstrip(f'{file_type}/raw_copy/updated_at=')
                                         .rstrip(f'/{file_name}')))
                          for obj in files["Contents"]]
        return max(timestamp_list)
    else:
        return 0


def current_timestamp(file_type):
    if file_type == 'voterfile':
        file_name = app.config["VF_FILE_ZIP"]
    elif file_type == 'vote_history':
        file_name = app.config["VH_FILE_ZIP"]

    metadata = s3client.head_object(Bucket=app.config["NC_BUCKET"],
                                    Key=f'data/{file_name}')
    timestamp = (str(metadata["LastModified"]).replace('-', '')
                                              .replace(':', '')
                                              .replace(' ', '')[:14])
    return int(timestamp)
