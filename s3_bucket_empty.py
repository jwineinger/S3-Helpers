from eventlet import patcher
patcher.monkey_patch(all=True)

import os, sys, time
from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket

import logging
from eventlet import *

logging.basicConfig(filename="s3_download.log", level=logging.INFO)

from S3 import CallingFormat
AWS_CALLING_FORMAT = CallingFormat.SUBDOMAIN
AWS_S3_SECURE_URLS = False
AWS_QUERYSTRING_AUTH = False

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_STORAGE_BUCKET_NAME = ''


def delete_file(key):
    key.delete()
    logging.info("del:%s" % key.name)

if __name__ == "__main__":
    conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = Bucket(connection=conn, name=AWS_STORAGE_BUCKET_NAME)

    logging.info("Fetching bucket list")
    bucket_list = bucket.list()

    logging.info("Creating a pool")
    pool = GreenPool(size=20)

    logging.info("Deleting files from bucket...")
    for x in pool.imap(delete_file, bucket_list):
        pass
