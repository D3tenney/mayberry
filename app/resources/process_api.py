from flask import request
from flask_restful import Resource

# from app.components.process_ingestion_helper import process_ingestion_helper
from app.components.process_ingestion_voter import process_ingestion_voter
from app.components.process_compare_voter import process_compare_voter
from app.components.process_query_counts import process_query_counts
from app.components.process_file_polling import process_file_polling
from app.components.process_copy_raw import process_copy_raw

import logging
logger = logging.getLogger(__name__)


class Process(Resource):
    def post(self):
        # TODO: payload validation (marshmellow?)
        payload = request.get_json()

        if not payload:
            logger.debug('no payload')
            return 'no payload'

        if payload['type'] == 'polling':
            task = process_file_polling.delay(payload)
            logger.debug('polling job running')
            return "polling..."
        if payload['type'] == 'copy_raw':
            task = process_copy_raw.delay(payload)
            logger.debug(f"copying {payload['file_type']}")
            return f"copying {payload['file_type']}"
        # TODO: set up reference tables (full county name, etc..)
        """
        if payload['type'] == 'ingest_helper':
            task = process_ingestion_helper.delay(payload)
            logger.debug('ingesting helper tables')
            return "processing helper tables..."
        """
        if payload['type'] == 'ingest_voterfile':
            task = process_ingestion_voter.delay(payload)
            logger.debug('ingesting voter file')
            return "processing voter tables..."
        if payload['type'] == 'vf_compare':
            task = process_compare_voter.delay(payload)
            logger.debug("comparing voter files...")
            return "comparing voter files..."
        if payload['type'] == 'query_counts':
            task = process_query_counts.delay(payload)
            logger.debug("pulling counts")
            return "pulling counts..."
