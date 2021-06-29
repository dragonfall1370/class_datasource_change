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
cf.read('at_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()
function = pd.read_csv('fe-sfe.csv')
function = function.where(function.notnull(), None)
function['id'] = function.index
# assert False
func = function['Functional Skill'].map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(function['id'], left_index=True, right_index=True) \
    .melt(id_vars=['id'], value_name='skill') \
    .drop('variable', axis='columns') \
    .dropna()
func = func.loc[func['skill']!='']
func1 = func[['skill']].drop_duplicates().dropna()
# func1 = func1.drop_duplicates()
func1.loc[func1['skill'].str.contains('CRM')]
func1['matcher'] = func1['skill'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
func1['rn'] = func1.groupby('matcher').cumcount()
func1 = func1.loc[func1['rn']==0]
# assert False
cp = vincere_custom_migration.inject_functional_expertise_subfunctional_expertise(func1, 'skill', None, None, connection)


