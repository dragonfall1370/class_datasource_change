# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
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
cf.read('lh_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
document_path = r'D:\Tony\project\vincere_project\CSV__linkinghuman\Data\Attachments_all'
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre.raw_connection()

# %% extract data
candidate = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__linkinghuman\Data\candidate.csv')
candidate['candidate_externalid'] = candidate['Candidate ID']
candidate = candidate.where(candidate.notnull(),None)

temp_msg_metadata = vincere_common.get_folder_structure(document_path)
temp_msg_metadata['matcher'] = temp_msg_metadata['file_fullpath'].apply(lambda x: x.split('Attachments_all')[1])
temp_msg_metadata['matcher'] = temp_msg_metadata['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# assert False
tem = candidate[['candidate_externalid','CV File Name (+extension)']].dropna()
tem['matcher'] = tem['CV File Name (+extension)'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())


temp_msg_metadata_cand = temp_msg_metadata.merge(tem, on='matcher')
# tem.loc[~tem['candidate_externalid'].isin(temp_msg_metadata_cand['candidate_externalid'])]
temp_msg_metadata_cand['file_name'] = temp_msg_metadata_cand['alter_file2']
temp_msg_metadata_cand['external_id'] = temp_msg_metadata_cand['candidate_externalid']
temp_msg_metadata_cand['uploaded_filename'] = temp_msg_metadata_cand['CV File Name (+extension)']
temp_msg_metadata_cand['primary_document'] = 1
# %% candidate
candidate_file = vincere_custom_migration.insert_candidate_documents_candidate(temp_msg_metadata_cand, ddbconn, dest_db, mylog)
candidate_file = candidate_file.drop_duplicates()

# %% upload files to s3
s3_bucket = cf['default'].get('s3_bucket')
s3_key = cf['default'].get('s3_key')
REGION_HOST = 's3.eu-west-2.amazonaws.com'

from common import s3_add_thread_pool
s3_add_thread_pool.upload_multi_files_parallelism_1_2(candidate_file, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host



