from dotenv import load_dotenv
from os import environ

load_dotenv(override=True)


class Config(object):
    '''
    Base config class
    '''
    DEBUG = True
    TESTING = False

    HOST = environ.get("HOST")
    PORT = int(environ.get("PORT"))

    LOG_LEVEL = environ.get("LOG_LEVEL")

    # aws setup
    AWS_ACCESS_KEY_ID = environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = environ.get("AWS_SECRET_ACCESS_KEY")
    MAYBERRY_BUCKET = environ.get("MAYBERRY_BUCKET")
    NC_BUCKET = environ.get("NC_BUCKET")
    ATHENA_DB = environ.get("ATHENA_DB")
    ATHENA_COUNT_TABLE = environ.get("ATHENA_COUNT_TABLE")

    # file names
    VF_FILE = environ.get("VF_FILE")
    VF_FILE_ZIP = environ.get("VF_FILE_ZIP")
    VH_FILE = environ.get("VH_FILE")
    VH_FILE_ZIP = environ.get("VH_FILE_ZIP")
    PC_FILE = environ.get("PC_FILE")

    # redis setup (queue)
    REDIS_HOST = environ.get("REDIS_HOST")
    REDIS_PASSWORD = environ.get("REDIS_PASSWORD")
    REDIS_PORT = environ.get("REDIS_PORT")

    # celery setup (asynchronous)
    CELERY_BROKER_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}"
    CELERY_RESULT_BACKEND = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}"
