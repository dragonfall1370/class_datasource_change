
# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import os
import re
import common.s3 as s3
import psycopg2
import numpy as np
import pymssql
import datetime
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_common as vincere_common
import pandas as pd
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %%
# loading configuration
cf = configparser.RawConfigParser()
cf.read('tt_config.ini')
data_folder = cf['default'].get('data_folder')
upload_folder = cf['default'].get('upload_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
fr = cf[cf['default'].get('src_db')]
to = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(cf['default'].get('log_file'))




# %% test s3
from boto.s3.connection import S3Connection
s3_bucket = 'fra-vc-p1-file2'
s3_bucket = 'file-server-prod-fra'
s3_key = cf['default'].get('direct_s3_key')
REGION_HOST = 's3.eu-central-1.amazonaws.com'
AWS_KEY = 'AKIAJUNYY2WD7YF6A53A'
AWS_SECRET = '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'

conn = S3Connection(aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET, host=REGION_HOST)
from_bucket = conn.get_bucket(s3_bucket)

temp = from_bucket.list(prefix=r'christymedia-review.vincere.io/documents')
uploaded_files = [re.search(r'\/((?:.(?!\/))+$)', i.key).group(1) for i in temp]
uploaded_filesf = pd.DataFrame(pd.Series(uploaded_files))
uploaded_filesf.columns = ['file_name',]
uploaded_filesf['uploaded'] = 'yes'

filenames = []
keys = []
for i in temp:
    filenames.append(re.search(r'\/((?:.(?!\/))+$)', i.key).group(1))
    keys.append(i.key)
pd.concat([pd.Series(keys, name='key'), pd.Series(filenames, name='filename')] ,axis=1)



# s3://file-server-prod-fra/christymedia-review.vincere.io/documents/

s3_bucket = cf['default'].get('direct_s3_bucket')
s3_key = cf['default'].get('direct_s3_key')
REGION_HOST = 's3.eu-central-1.amazonaws.com'

from common import s3_add_thread_pool
s3_add_thread_pool.upload1('filenaytaoup.pdf', 'filenaytaoup.pdf', 'D:/', s3_bucket, s3_key, log=None, region_host=REGION_HOST, reup=0)