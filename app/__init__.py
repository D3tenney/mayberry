from flask import Flask
from flask_restful import Api
from flask_sqlalchemy import SQLAlchemy

from celery import Celery

from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base

import boto3

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
                    level='DEBUG')
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
# db setup
db = SQLAlchemy(app)
engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
metadata = MetaData(engine)

Model = declarative_base(metadata=metadata, bind=engine)
Base = automap_base(metadata=metadata, declarative_base=Model)

Base.prepare(engine, reflect=True)
tables = Base.classes

County = tables.county
Ethnicity = tables.ethnicity
Party = tables.party
Precinct = tables.precinct
Race = tables.race
Voter = tables.voter
VoterPast = tables.voter_past
VoterStaging = tables.voter_staging
VoterStatus = tables.voter_status
VoterStatusReason = tables.voter_status_reason
