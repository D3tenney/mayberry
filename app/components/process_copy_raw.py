from app import app, s3resource, celery

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


@celery.task()
def process_copy_raw(message):
    if message["file_type"] == 'voterfile':
        file_name = app.config["VF_FILE_ZIP"]
    elif message["file_type"] == 'vote_history':
        file_name = app.config["VH_FILE_ZIP"]

    # Copy from NC bucket to Mayberry Bucket
    copy_source = {'Bucket': app.config["NC_BUCKET"],
                   'Key': f'data/{file_name}'}
    copy_destination_key = f'{message["file_type"]}/raw_copy/updated_at={message["timestamp"]}/{file_name}'
    s3resource.Bucket = app.config["MAYBERRY_BUCKET"]
    print('starting copy...')
    logger.debug('starting copy...')
    s3resource.meta.client.copy(copy_source,
                                app.config["MAYBERRY_BUCKET"],
                                copy_destination_key)
    print(f'{message["file_type"]} successfully copied to {copy_destination_key}')
    logger.debug(f'{message["file_type"]} successfully copied to {copy_destination_key}')
