from eventlet import patcher
patcher.monkey_patch(all=True)

import os, sys, time
from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket
from boto.s3.key import Key
import boto

import logging
from eventlet import *

logging.basicConfig(filename="s3_upload.log", level=logging.INFO)

from S3 import CallingFormat
AWS_CALLING_FORMAT = CallingFormat.SUBDOMAIN
AWS_S3_SECURE_URLS = False
AWS_QUERYSTRING_AUTH = False

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_STORAGE_BUCKET_NAME = ''

if __name__ == "__main__":
    conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = Bucket(connection=conn, name=AWS_STORAGE_BUCKET_NAME)

    def upload_file(filename):
        if filename.startswith(sys.argv[1]):
            upload_filename = filename[len(sys.argv[1]):]
        key = Key(bucket)
        key.key = upload_filename
        key.set_contents_from_filename(filename)
        key.set_acl('public-read')
        logging.info("ul:%s" % key.name)

    logging.info("Fetching file list")

    file_list = []
    for dirname, dirnames, filenames in os.walk(sys.argv[1]):
        for filename in filenames:
            file_list.append(os.path.join(dirname, filename))

    logging.info("Creating a pool")
    pool = GreenPool(size=20)
 
    logging.info("Saving files in bucket...")
    for x in pool.imap(upload_file, file_list):
        pass
