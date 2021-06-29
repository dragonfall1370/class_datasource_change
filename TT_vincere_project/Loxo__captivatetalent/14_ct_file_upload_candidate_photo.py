# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import sqlalchemy
import datetime
from functools import reduce
from common import vincere_job_application
import pandas as pd

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('ct_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre_review.raw_connection()
document_path_candidate = r'D:\Tony\File\Loxo\prod\captivate-talent\people\profile_pictures'

# %% extract data
candidate = pd.read_sql("""select people_id as id,* from people_pic""", engine_sqlite)
# candidate['id'] = candidate['id'].astype(str)
# candidate['path'] = candidate['pic'].apply(lambda x: eval(x)['path'])
# candidate['filename'] = candidate['pic'].apply(lambda x: eval(x)['filename'])
# candidate['content_type'] = candidate['pic'].apply(lambda x: eval(x)['content_type'])
# candidate['upload_date'] = candidate['pic'].apply(lambda x: eval(x)['upload_date'])
temp_msg_metadata_candidate = vincere_common.get_folder_structure(document_path_candidate)
assert False
temp_msg_metadata_candidate['matcher'] = temp_msg_metadata_candidate['file_fullpath'].apply(lambda x: x.split('captivate-talent')[1])
temp_msg_metadata_candidate['matcher'] = temp_msg_metadata_candidate['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
candidate['matcher'] = candidate['path'].apply(lambda x: x.split('captivate-talent')[1])
candidate['matcher'] = candidate['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

# assert False
temp_msg_metadata_candidate = temp_msg_metadata_candidate.merge(candidate, on='matcher')
temp_msg_metadata_candidate['ext'] = temp_msg_metadata_candidate['content_type'].apply(lambda x: x.split('/')[-1])
temp_msg_metadata_candidate['alter_file2'] = temp_msg_metadata_candidate['alter_file2']+'.'+temp_msg_metadata_candidate['ext']
temp_msg_metadata_candidate['file_name'] = temp_msg_metadata_candidate['alter_file2']
temp_msg_metadata_candidate['external_id'] = temp_msg_metadata_candidate['id']
temp_msg_metadata_candidate['uploaded_filename'] = temp_msg_metadata_candidate['file']+'.'+temp_msg_metadata_candidate['ext']
temp_msg_metadata_candidate['created'] = pd.to_datetime(temp_msg_metadata_candidate['upload_date'])
# assert False
# %% candidate
candidate_file = vincere_custom_migration.insert_candidate_photos_candidate(temp_msg_metadata_candidate, ddbconn, dest_db, mylog)
candidate_file = candidate_file.drop_duplicates()

# %% upload files to s3
s3_bucket = cf['default'].get('s3_bucket')
s3_key = cf['default'].get('s3_key')
REGION_HOST = 's3.us-east-1.amazonaws.com'

from common import s3_add_thread_pool

s3_add_thread_pool.upload_multi_files_parallelism_1_2(candidate_file, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host
assert False

