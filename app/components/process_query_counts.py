from app import app, celery, athenaclient, s3resource, s3client, S3FS

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
                                                   'OutputLocation': f's3://{app.config["MAYBERRY_BUCKET"]}/count_results_csv/'
                                                  })['QueryExecutionId']

    counter = 0
    while counter < 10:
        counter += 1
        status = (athenaclient.get_query_execution(QueryExecutionId=query_id)
                  ['QueryExecution']['Status']['State'])

        if status == 'SUCCEEDED':
            # NOTES: transient TypeError here. 'str' object not callable
            # seems to go away if Celery worker restarted...
            '''
            object = (s3resource.Bucket(app.config["MAYBERRY_BUCKET"])
                                .Object(key=f'count_results_csv/{query_id}.csv')
                                .get())
            df = pd.read_csv(BytesIO(object['Body'].read()), encoding='utf-8')
            '''
            # NOTES: This code _seems_ to fix the above problem. Want to leave
            # it all in to test, though.
            object = s3client.get_object(Bucket=app.config["MAYBERRY_BUCKET"],
                                         Key=f'count_results_csv/{query_id}.csv')
            df = pd.read_csv(object['Body'])
            df.to_parquet(f's3://{app.config["MAYBERRY_BUCKET"]}/count_results_parquet/{message["audience_id"]}.parquet', index=False)
            print('counts file written')
            break
        if status in ['FAILED', 'CANCELLED']:
            print('count query failed')
            break
            return None
        else:
            time.sleep(2)
