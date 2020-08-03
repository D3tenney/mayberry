from flask import Flask
from flask_restful import Api

from celery import Celery

import boto3
import s3fs

import logging
import sys

# app setup
app = Flask(__name__)
app.config.from_object('app.config.Config')

# queue setup
celery = Celery(app.name, broker=app.config["CELERY_BROKER_URL"])
celery.conf.update(app.config)

# api setup
api = Api(app)

# logging
logging.basicConfig(stream=sys.stdout,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=app.config["LOG_LEVEL"])
logger = logging.getLogger(__name__)
logger.debug('logging started')
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

# aws setup
s3client = boto3.client('s3',
                        aws_access_key_id=app.config["AWS_ACCESS_KEY_ID"],
                        aws_secret_access_key=app.config["AWS_SECRET_ACCESS_KEY"])
s3resource = boto3.resource('s3',
                            aws_access_key_id=app.config["AWS_ACCESS_KEY_ID"],
                            aws_secret_access_key=app.config["AWS_SECRET_ACCESS_KEY"])
athenaclient = boto3.client('athena',
                            aws_access_key_id=app.config["AWS_ACCESS_KEY_ID"],
                            aws_secret_access_key=app.config["AWS_SECRET_ACCESS_KEY"])

# TODO: just use boto3 throughout, rather than mixing in s3fs
# s3fs setup
S3FS = s3fs.S3FileSystem(anon=False,
                         key=app.config["AWS_ACCESS_KEY_ID"],
                         secret=app.config["AWS_SECRET_ACCESS_KEY"])
