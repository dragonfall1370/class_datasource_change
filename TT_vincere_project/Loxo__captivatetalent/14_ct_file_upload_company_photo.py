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
document_path_company = r'D:\Tony\File\Loxo\prod\captivate-talent\companies\logos'
# %% extract data
company = pd.read_sql("""select id, logo from company where logo is not null""", engine_sqlite)
company['id'] = company['id'].astype(str)
company['path'] = company['logo'].apply(lambda x: eval(x)['path'])
company['filename'] = company['logo'].apply(lambda x: eval(x)['filename'])
company['content_type'] = company['logo'].apply(lambda x: eval(x)['content_type'])
company['upload_date'] = company['logo'].apply(lambda x: eval(x)['upload_date'])
temp_msg_metadata_company = vincere_common.get_folder_structure(document_path_company)
# assert False
temp_msg_metadata_company['matcher'] = temp_msg_metadata_company['file_fullpath'].apply(lambda x: x.split('captivate-talent')[1])
temp_msg_metadata_company['matcher'] = temp_msg_metadata_company['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
company['matcher'] = company['path'].apply(lambda x: x.split('captivate-talent')[1])
company['matcher'] = company['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

# assert False
temp_msg_metadata_client = temp_msg_metadata_company.merge(company, on='matcher')
temp_msg_metadata_client['ext'] = temp_msg_metadata_client['content_type'].apply(lambda x: x.split('/')[-1])
temp_msg_metadata_client['alter_file2'] = temp_msg_metadata_client['alter_file2']+'.'+temp_msg_metadata_client['ext']
temp_msg_metadata_client['file_name'] = temp_msg_metadata_client['alter_file2']
temp_msg_metadata_client['external_id'] = temp_msg_metadata_client['id']
temp_msg_metadata_client['uploaded_filename'] = temp_msg_metadata_client['file']+'.'+temp_msg_metadata_client['ext']
temp_msg_metadata_client['created'] = pd.to_datetime(temp_msg_metadata_client['upload_date'])
# assert False
# %% company
company_file = vincere_custom_migration.insert_candidate_documents_company_logo(temp_msg_metadata_client, ddbconn, dest_db, mylog)
company_file = company_file.drop_duplicates()


# %% upload files to s3
s3_bucket = cf['default'].get('s3_bucket')
s3_key = cf['default'].get('s3_key')
REGION_HOST = 's3.us-east-1.amazonaws.com'

from common import s3_add_thread_pool

s3_add_thread_pool.upload_multi_files_parallelism_1_2(company_file, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host
assert False

