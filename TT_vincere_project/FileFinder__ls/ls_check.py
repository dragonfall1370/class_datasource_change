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
cf.read('ls_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
# data_folder = '/Users/truongtung/Desktop'
sqlite_path = cf['default'].get('sqlite_path')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
assert False
#%%
value = pd.read_sql("""
select a.translate, b.field_value
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='d5b6dff5678f5c48b0559fbcd4571760')
        and form_id in (select form_id from configurable_form_field where  field_key ='d5b6dff5678f5c48b0559fbcd4571760')
""", engine_postgre_review)

cand = pd.read_sql("""
select id ,first_name, last_name, contact_id from candidate where deleted_timestamp is null
""", engine_postgre_review)

cand_value = pd.read_sql("""
select additional_id as candidate_id
     , field_value from additional_form_values
where field_id = 11267
and nullif(field_value,'') is not null
""", engine_postgre_review)

cand_value1 = cand_value.field_value.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cand_value[['candidate_id']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_id'], value_name='value') \
    .drop('variable', axis='columns') \
    .dropna()

cand_value2 = cand_value1.merge(value, left_on='value', right_on='field_value')
cand_value2 = cand_value2.groupby('candidate_id')['translate'].apply(lambda x: ', '.join(x)).reset_index()
cand1 = cand.merge(cand_value2, left_on='id', right_on='candidate_id', how='left')
cand1 = cand1.where(cand1.notnull(),None)
#cand1.to_csv('ls_candidate.csv',index=False)


value2 = pd.read_sql("""
select a.translate, b.field_value
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='7ef65742e491c9553e5a3a3ec387a22d')
        and form_id in (select form_id from configurable_form_field where  field_key ='7ef65742e491c9553e5a3a3ec387a22d')
""", engine_postgre_review)

cont = pd.read_sql("""
select id, first_name, last_name from contact where deleted_timestamp is null
""", engine_postgre_review)

cont_value = pd.read_sql("""
select additional_id
     , field_value from additional_form_values
where field_id = 11268
and nullif(field_value,'') is not null
""", engine_postgre_review)

cont_value1 = cont_value.field_value.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cont_value[['additional_id']], left_index=True, right_index=True) \
    .melt(id_vars=['additional_id'], value_name='value') \
    .drop('variable', axis='columns') \
    .dropna()

cont_value2 = cont_value1.merge(value2, left_on='value', right_on='field_value')
cont_value2 = cont_value2.groupby('additional_id')['translate'].apply(lambda x: ', '.join(x)).reset_index()

cont1 = cont.merge(cont_value2, left_on='id', right_on='additional_id', how='left')
cont1 = cont1.where(cont1.notnull(),None)
#cont1.to_csv('ls_contact.csv',index=False)

cand1['contact_id'] = cand1['contact_id'].apply(lambda x: int(str(x).split('.')[0]) if x else x)
tem = cand1.merge(cont1, left_on='contact_id',right_on='id',how='left')
#tem.to_csv('ls_contact_candidate_link.csv',index=False)

cont2 = cont1.loc[~cont1['id'].isin(cand1['contact_id'])]
cont2.to_csv('ls_contact_not_map.csv',index=False)