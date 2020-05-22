from flask import Flask
from flask_restful import Api
from flask_sqlalchemy import SQLAlchemy

from celery import Celery

from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base

# TODO: Add Logging

# app setup
app = Flask(__name__)
app.config.from_object('app.config.Config')

# queue setup
celery = Celery(app.name, broker=app.config["CELERY_BROKER_URL"])
celery.conf.update(app.config)

# api setup
api = Api(app)

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
