# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('yc_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
document_path = r'C:\Users\tony\Downloads\vectorstock_13227832\vectorstock_13227832'
# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# %% extract data
file = pd.read_sql("""
select
      c.Id as candidate_externalid,
      'vectorstock_13227832.png' as image_name
from Contact c
join RecordType r on (c.RecordTypeId || 'AA2') = r.Id
left join User u on c.OwnerId = u.Id
left join "User" m on c.LastModifiedById = m.Id
where c.IsDeleted = 0
and r.Name = 'Candidate'
and c.No_Touch__c = '1'
""", engine_sqlite)

temp_msg_metadata = vincere_common.get_folder_structure(document_path)
temp_msg_metadata = temp_msg_metadata.merge(file, left_on='file', right_on='image_name')


assert False

# %% transform data
temp_msg_metadata['file_name'] = temp_msg_metadata['alter_file2']
temp_msg_metadata['external_id'] = temp_msg_metadata['candidate_externalid']
# temp_msg_metadata.to_csv('temp_msg_metadata.csv')

# %% candidate
candidate_file = vincere_custom_migration.insert_bulk_upload_photo_mapping_4_candidate(temp_msg_metadata, connection)

# assert False

# %% upload files to s3
s3_bucket = cf['default'].get('s3_bucket')
s3_key = cf['default'].get('s3_key')
REGION_HOST = 's3.eu-central-1.amazonaws.com'

from common import s3_add_thread_pool

s3_add_thread_pool.upload_multi_files_parallelism_1_2(candidate_file, 'file', 'file_name', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

