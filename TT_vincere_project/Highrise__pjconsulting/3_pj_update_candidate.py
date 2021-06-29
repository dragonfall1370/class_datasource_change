# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
import os
import string
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
cf.read('pj_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)
# assert False
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% connect data
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

from common import vincere_candidate
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())
# %% candidate
candidate = pd.read_csv(os.path.join(standard_file_upload, 'candidate_2.csv'))
candidate['candidate-externalId'] = candidate['candidate-externalId'].astype(str)
candidate['candidate_externalid'] = candidate['candidate-externalId']
assert False
# %% location name
location = candidate[['candidate_externalid', 'candidate-locationName-inject']]\
    .rename(columns={'candidate-locationName-inject': 'location_name'}).dropna()
vcand.update_common_location_location_name(location, mylog)

# %% work phone
tem = candidate[['candidate_externalid', 'candidate-workPhone-inject']].dropna().rename(columns={'candidate-workPhone-inject': 'work_phone'})
tem2 = vincere_common.splitDataFrameList_1(tem,'work_phone',',')
tem2['work_phone'] = tem2['work_phone'].apply(lambda x: re.sub("[A-Za-z-()]+", "", x))
tem2['work_phone'] = tem2['work_phone'].apply(lambda x: x.replace(" ",""))
tem2 = tem2.groupby(['candidate_externalid'])['work_phone'].apply(lambda x: ', '.join(x)).reset_index()
# tem2['regex'] = tem2['work_phone'].map(lambda x: re.match(r"^[+]*[(]{0,1}[0-9]{1,4}[)]{0,1}[-\s\\./0-9]*$",x))
# tem2.loc[tem2['work_phone']=='(08) 9284 0444 HR Dept']
# tem2['regex_res'] = tem2['regex'].apply(lambda x: x.group(0))
tem2.to_csv('wp2.csv')
tem2['length'] = tem2['work_phone'].apply(lambda x: len(x))
vcand.update_work_phone(tem2, mylog)

# %% reg date
reg_date = candidate[['candidate_externalid', 'candidate-regDate-inject']]\
    .rename(columns={'candidate-regDate-inject': 'reg_date'}).dropna()
reg_date['reg_date'] = pd.to_datetime(reg_date['reg_date'])
cp1 = vcand.update_reg_date(reg_date, mylog)

