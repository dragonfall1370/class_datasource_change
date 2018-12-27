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

#
# list all file in folder / directory does not include sub folder
onlyfiles = [join(destfolder, f) for f in listdir(destfolder) if isfile(join(destfolder, f))]

#
# files meta data
files_metadata = pd.concat(
    [pd.read_csv(f) for f in onlyfiles], sort=False
)

files_metadata['rn'] = files_metadata.groupby('file').cumcount()
test1 = files_metadata[(files_metadata['rn']>0)]
test1[test1['file'].notnull()]
if True:
    u1 = files_metadata[:1]
    REGION_HOST = 's3.eu-central-1.amazonaws.com'
    # s3.upload1('6.4.doc', '6.4.doc', r'd:\vincere\data_import\eden\zip\CompProfile', s3_bucket, s3_key, mylog, REGION_HOST)
    s3.upload_multi_files_parallelism_1_2(files_metadata, 'file', 'alter_file1', 'root'
                                    , bucket=s3_bucket
                                    , key=s3_key, log=mylog, region_host=REGION_HOST)
