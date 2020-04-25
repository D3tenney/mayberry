from sqlalchemy import create_engine
from dotenv import load_dotenv
from os import environ

load_dotenv(override=True)


class Config(object):
    '''
    Base config class
    '''
    DEBUG = True
    TESTING = False

    PG_USER = environ.get("PG_USER")
    PG_PASS = environ.get("PG_PASS")
    PG_HOST = environ.get("PG_HOST")
    PG_PORT = environ.get("PG_PORT")
    PG_DB = environ.get("PG_DB")

    PG_CXN = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    engine = create_engine(PG_CXN)

    SQLALCHEMY_DATABASE_URI = PG_CXN
    SQLALCHEMY_TRACK_MODIFICATIONS = False
