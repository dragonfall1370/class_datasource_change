# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
# from edenscott._edenscott_dtypes import *
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
cf.read('departer_config.ini')
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

# %% connect db
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# %%
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
candidate = pd.read_csv('D:\Tony\File\Departer\candidate import departer.csv', encoding = 'unicode_escape')
candidate['candidate_externalid'] = candidate['candidate-externalId'].apply(lambda x: str(x) if x else x)
candidate.loc[(candidate['candidate-firstName']=='Mohamed') & (candidate['candidate-Lastname']=='Hafez')]  #72038
candidate.loc[candidate['candidate-externalId']==72038]
assert False #country citizenship jobtype
# %% job type
tem = candidate[['candidate-jobTypes','candidate_externalid']].dropna()
tem['desired_job_type'] = 'permanent'
cp = vcand.update_desired_job_type(tem, mylog)

# %% country
from common import vincere_company
vcom = vincere_company.Company(connection)
tem = candidate[['candidate_externalid','candidate-Country']].dropna()
tem['candidate-Country'].unique()
tem['country_code'] = tem['candidate-Country'].map(vcom.get_country_code)
cp6 = vcand.update_location_country_code(tem, mylog)

# %% citizen
tem = candidate[['candidate_externalid','candidate-citizenship']].dropna()
tem['nationality'] = tem['candidate-citizenship'].map(vcom.get_country_code)
a= tem[tem['nationality']=='']
a['candidate-citizenship'].unique()
cp6 = vcand.update_nationality(tem, mylog)

# %% mobile
tem = candidate[['candidate_externalid','candidate-mobile']].dropna()
tem['mobile_phone'] = tem['candidate-mobile']
tem['rn'] = tem['mobile_phone'].apply(lambda x: len(x))
tem = tem.loc[tem['rn']<100]
cp6 = vcand.update_mobile_phone(tem, mylog)

# %% home phone
tem = candidate[['candidate_externalid','candidate-homePhone']].dropna()
tem['home_phone'] = tem['candidate-homePhone']
tem['rn'] = tem['home_phone'].apply(lambda x: len(x))
tem = tem.loc[tem['rn']<100]
cp6 = vcand.update_home_phone(tem, mylog)