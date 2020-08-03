# Mayberry

Mayberry is a web application using the [Flask](https://flask.palletsprojects.com/en/1.1.x/) framework with Redis as a queue and Celery handling asynchronous tasks. It is useful for copying and investigating the North Carolina voter file, updated weekly and provided by the North Carolina State Board of Elections (NC SBOE).
It uses [Amazon Web Service](https://aws.amazon.com/) S3 and Athena services.

You will need an AWS account, and an IAM user with permissions for S3 and Athena. You can learn more about IAM and permissioning [here](https://aws.amazon.com/iam/).

AWS costs money, and while this project was designed to be cost-effective, nothing is free. I strongly suggest setting up automated cost warnings in AWS billing.

Mayberry is still in development.

## Installation and Setup

Install  and configure [Redis](https://redis.io/) which will act as a queue service.
This [digitalocean](https://www.digitalocean.com/community/tutorials/how-to-install-and-secure-redis-on-ubuntu-18-04) article may be useful.

After installing Redis and cloning the repository, create a virtual environment.
```bash
python3 -m venv venv
```

Start the environment.
```bash
. venv/bin/activate
```

Install required packages
```bash
pip3 install -r requirements.txt
```

Save the `.env.example` file as `.env`. Then edit the `.env` file to use your Redis and AWS settings and credentials.
```bash
cp .env.example .env
```

Create an S3 bucket and set it as `MAYBERRY_BUCKET` in `.env`. I recommend using a clean, new bucket. Mayberry will automatically create folders, which could result in overwriting data if you have folders with the same name in your bucket. Your bucket should be set to Private. The data used for this project is public record, but it's generally best practice to default to private.

In Athena, create a database and create a counts table by modifying `LOCATION` and running the following
```SQL
CREATE EXTERNAL TABLE `vf_vh_counts`(
  `county_id` int,
  `cong_dist_abbrv` string,
  `nc_senate_abbrv` string,
  `nc_house_abbrv` string,
  `res_city_desc` string,
  `party_cd` string,
  `gender_code` string,
  `race_code` string,
  `ethnic_code` string,
  `status_cd` string,
  `absent_ind` string,
  `prior_voter` int,
  `primary_voter` int,
  `count` int)
PARTITIONED BY (
  `vf_updated_on` date)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://<YOUR_BUCKET>/vf_vh_count_files'
TBLPROPERTIES (
  'has_encrypted_data'='false')
```

## USE
To start up Mayberry, activate your virtual environment and run
```bash
python3 mayberry.py
```

In a separate shell, start the celery worker
```bash
celery worker -A mayberry.celery
```

Mayberry offers two endpoints:
`health` to check that the app is running, and
`process` to run tasks.

To hit `health`, alter the below to use `PORT` from your `.env`. It should return "OK".
```bash
curl http://localhost:4242/health
```

`process` accepts POST requests. It has 5 components, currently.
- `process_file_polling` compares files in your S3 bucket to the current file in the NC State Board of Election bucket. When it finds new files, it sends payloads to `process_copy_raw` and `process_ingestion_voter`.
- `process_copy_raw` copies vote history and voterfile data from the NC SBOE bucket to your bucket.
- `process_ingestion_voter` joins voterfile and vote history data, creates a dataframe using a narrow selection of variables (defined as `VOTER_READ_COLS` in `constants.py`), and writes both an individual-level and a grouped file to s3.
- `process_query_counts` uses Athena to query the grouped files written by `process_ingestion_voter` to create counts by date for audiences defined by the user.
- `process_compare_voter` compares two individual-level files produced by `process_ingestion_voter` and writes out files of new voters, removed voters, voters whose status has changed, and voters who have changed party registration.

## Sample Payloads
Below are sample payloads for each component in `process`. You can send these by using Curl. [This](https://www.educative.io/edpresso/how-to-perform-a-post-request-using-curl) post gives some good examples of sending payloads with Curl. You may also find [postman](https://www.postman.com/) to be a useful environment for saving and sending requests to Mayberry.

`process_file_polling`:
```json
{"type": "polling"}
```
`process_copy_raw`:
```json
{
  "type": "copy_raw",
  "file_type": "voterfile",
  "timestamp": "20200711110313"
}
```
`process_ingestion_voter`:
```json
{
    "type": "ingest_voterfile",
    "vf_timestamp": "20200711110313",
    "vh_timestamp": "20200711110247"
}
```
`process_query_counts`:
```json
{
    "type": "query_counts",
    "audience_id": "test",
    "parameters": {
        "vf_updated_on": {
            "min_date": "2020-01-01",
            "max_date": "2020-08-01"
        },
        "county_id": [1,2,3,4,5,6,7,8,9,10],
        "cong_dist_abbrv": [],
        "nc_senate_abbrv": [],
        "nc_house_abbrv": [],
        "res_city_desc": [],
        "party_cd": [],
        "gender_code": ["F"],
        "race_code": ["B"],
        "ethnic_code": [],
        "status_cd": ["A"],
        "absent_ind": [],
        "prior_voter": [],
        "primary_voter": []
    }
}
```
`process_compare_voter`:
```json
{
    "type": "vf_compare",
    "output_id": "test_file2",
    "vf_1_date": "2020-06-20",
    "vf_2_date": "2020-08-01"
}
```

## Future
Testing, payload validation, improved logging, improved error handling, and cloud deployment are all forthcoming. A little more cleanup is also needed in constants and requirements.
