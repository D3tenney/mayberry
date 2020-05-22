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

    SQLALCHEMY_DATABASE_URI = PG_CXN
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    MAYBERRY_BUCKET = environ.get("MAYBERRY_BUCKET")
    NC_BUCKET = environ.get("NC_BUCKET")
    VF_FILE = environ.get("VF_FILE")

    VF_CHUNKSIZE = int(environ.get("VF_CHUNKSIZE"))

    REDIS_HOST = environ.get("REDIS_HOST")
    REDIS_PASSWORD = environ.get("REDIS_PASSWORD")
    REDIS_PORT = environ.get("REDIS_PORT")

    CELERY_BROKER_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}"
    CELERY_RESULT_BACKEND = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}"
