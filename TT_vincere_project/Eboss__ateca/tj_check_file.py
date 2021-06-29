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
import datetime
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('tj_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
document_path_cand = r'D:\Tony\File\tanjong\SQLDATA\CVs (admin)'
temp_msg_metadata_cand = vincere_common.get_folder_structure(document_path_cand)
temp_msg_metadata_cand['matcher'] = temp_msg_metadata_cand['file'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

mapping = pd.read_csv('MalaysianCandidatesCVsDirectoryReference.csv')
mapping['matcher'] = mapping['CVs Directory Document FileName'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
assert False
map = temp_msg_metadata_cand.merge(mapping,on='matcher')
map['rn'] = map.groupby('file').cumcount()
map = map.loc[map['rn']==0]

cand_db_2 = pd.read_sql("""SELECT CandidateID from Candidates c""", engine_mssql)
# map.loc[~map['SQL Database CandidateID'].isin(cand_db_2['CandidateID'])]