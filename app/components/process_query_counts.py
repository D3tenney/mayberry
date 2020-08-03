from app import app, celery, athenaclient, s3resource, S3FS

import time
from io import BytesIO
import pandas as pd


@celery.task()
def process_query_counts(message):
    print('starting count')
    WHERE_COLS = [col for col in message['parameters'].keys()
                  if len(message['parameters'][col]) != 0]

    select = f"""
             SELECT vf_updated_on, SUM(count) AS \"count\"
             FROM \"{app.config["ATHENA_DB"]}\".{app.config["ATHENA_COUNT_TABLE"]}
             """
    where = f"""
             WHERE vf_updated_on BETWEEN
                   CAST('{message['parameters']['vf_updated_on']['min_date']}' AS DATE)
                   AND
                   CAST('{message['parameters']['vf_updated_on']['max_date']}' AS DATE)
             """

    for entry in WHERE_COLS:
        if entry == 'vf_updated_on':
            continue
        else:
            statement = f"""
                         AND {entry} IN {tuple(message['parameters'][entry])}
                         """
            where += statement

    group = f"GROUP BY vf_updated_on"

    query = select + where + group

    # hacky fix for single-entry tuples
    query = query.replace(',)', ')')

    query_id = athenaclient.start_query_execution(QueryString=query,
                                                  QueryExecutionContext={
                                                   'Database': app.config["ATHENA_DB"]
                                                  },
                                                  ResultConfiguration={
                                                   'OutputLocation': f's3://{app.config["MAYBERRY_BUCKET"]}/count_test/'
                                                  })['QueryExecutionId']

    counter = 0
    while counter < 10:
        counter += 1
        status = (athenaclient.get_query_execution(QueryExecutionId=query_id)
                  ['QueryExecution']['Status']['State'])

        if status == 'SUCCEEDED':
            object = (s3resource.Bucket(app.config["MAYBERRY_BUCKET"])
                                .Object(key=f'count_test/{query_id}.csv')
                                .get())
            df = pd.read_csv(BytesIO(object['Body'].read()), encoding='utf-8')
            df.to_parquet(f's3://{app.config["MAYBERRY_BUCKET"]}/count_results_parquet/{message["audience_id"]}.parquet', index=False)
            break
        if status in ['FAILED', 'CANCELLED']:
            break
            return None
        else:
            time.sleep(2)
    print('counts file written')
