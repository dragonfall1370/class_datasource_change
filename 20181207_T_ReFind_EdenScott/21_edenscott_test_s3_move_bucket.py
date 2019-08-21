# -*- coding: UTF-8 -*-
import configparser
import os
import pandas as pd
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 2000)
pd.set_option('display.width', 1000)
from common import s3_add_thread_pool as s3
from common import logger_config
from os import listdir
from os.path import isfile, join

#
# read config info
cf = configparser.RawConfigParser()
cf.read('_edenscott_config.ini', encoding='utf8')
data_folder = cf['default'].get('data_folder')
destfolder = os.path.join(data_folder, 'bulk_upload_metadata')

upload_folder = cf['default'].get('upload_folder')
s3_key = cf['default'].get('s3_key')
s3_bucket = cf['default'].get('s3_bucket')
overwrite = True if int(cf['default'].get('overwrite')) else False
mylog = logger_config.get_info_logger("edenscott.log")

s3.move_from_bucket_to_bucket(from_bucket=s3_bucket , to_bucket=s3_bucket, log=mylog)
