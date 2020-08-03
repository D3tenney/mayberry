# from app import app, s3client, celery
from app import celery

# import logging

import time

from app.core.queue import send_to_queue
from app.core.polling import max_previous_timestamp, current_timestamp

# logger = logging.getLogger(__name__)
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


@celery.task()
def process_file_polling(message):
    new_vf = False
    new_vh = False
    file_timestamps = {}

    counter = 0

    file_types = ['vote_history', 'voterfile']

    while counter < 5:
        # check if timestamp on NC bucket file > most recent Mayberry timestamp
        for file in file_types:
            logger.debug(f"Polling {file}")
            print(f"Polling {file}")
            max_file_timestamp = max_previous_timestamp(file)
            new_timestamp = current_timestamp(file)

            if new_timestamp > max_file_timestamp:
                if file == 'vote_history':
                    new_vh = True
                elif file == 'voterfile':
                    new_vf = True
                file_timestamps[file] = new_timestamp
                logger.debug(f'New {file} data found.')
                print(f'New {file} data found.')
            else:
                logger.debug(f"No new data for {file}. Most recent: {new_timestamp}")
                print(f"No new data for {file}. Most recent: {new_timestamp}")
        if new_vf == new_vh:
            break
        else:
            counter += 1
            time.sleep(30)

    # Because the ingestion and other processes are dependent on joining
    # a voter file to a vote history file, we only proceed if we have both new
    # files.
    if new_vf is True and new_vh is True:
        for file in file_types:
            send_to_queue({"type": "copy_raw",
                           "file_type": f"{file}",
                           "timestamp": f"{file_timestamps[file]}"})
        send_to_queue({"type": "ingest_voterfile",
                       "vh_timestamp": file_timestamps["vote_history"],
                       "vf_timestamp": file_timestamps["voterfile"]})
        return 'New files sent to queue.'
    elif new_vf is False and new_vh is False:
        return 'No new file data found.'
    elif new_vf is True and new_vh is False:
        return 'New votefile data found, no new vote history found.'
    elif new_vf is False and new_vh is True:
        return 'New vote history found, no new voter file found.'
