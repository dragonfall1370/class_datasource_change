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
cf.read('dn_config.ini')
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
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
# %%
candidate = pd.read_csv('Danos Regions_Batch 3.csv')
api = 'f95fe7ff3d434594e8c49d8bec4f5594'
candidate=candidate.dropna().rename(columns={'candidate_id':'additional_id'})
candidate = candidate.value.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(candidate[['additional_id']], left_index=True, right_index=True) \
    .melt(id_vars=['additional_id'], value_name='value') \
    .drop('variable', axis='columns') \
    .dropna()
candidate['value'] = candidate['value'].str.strip()
# assert False
# vincere_custom_migration.append_candidate_muti_selection_checkbox(candidate, 'candidate_externalid', 'DirectTel', api, connection)

df = candidate
values_colname='value'
field_key = api
ddbconn = connection
logger=None



additional_form_values = pd.read_sql("""
        select * from additional_form_values 
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key), ddbconn)

configurable_form_language = pd.read_sql("""
    select * from configurable_form_language 
    where language_code in (
    select title_language_code from configurable_form_field_value 
    where field_id in (select id from configurable_form_field where  field_key ='%s')
    and form_id in (select form_id from configurable_form_field where  field_key ='%s') 
    )""" % (field_key, field_key), ddbconn)

configurable_form_field_value = pd.read_sql("""
    select * from configurable_form_field_value 
    where field_id in (select id from configurable_form_field where  field_key ='%s')
    and form_id in (select form_id from configurable_form_field where  field_key ='%s') 
    """ % (field_key, field_key), ddbconn)

df = df[df[values_colname].notnull()]

test = df[[values_colname]].drop_duplicates()
test = test.merge(configurable_form_language, left_on=values_colname, right_on='translate', how='outer', indicator=True)
test = test.query("_merge == 'left_only'")

drpd_vals = test[values_colname].unique()
drpd_vals = [x for x in drpd_vals if str(x) != 'nan']
drpd_vals.sort()
vincere_custom_migration.insert_drop_down_list_values(drpd_vals, field_key, ddbconn)

sql = """
    select a.translate, 'add_cand_info' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.field_id as constraint_id
    from configurable_form_language a
    join configurable_form_field_value b on a.language_code=b.title_language_code
    where field_id in (select id from configurable_form_field where  field_key ='%s')
    and form_id in (select form_id from configurable_form_field where  field_key ='%s')
    """ % (field_key, field_key)
df = df.merge(pd.read_sql(sql, ddbconn), left_on=values_colname, right_on='translate')
# insert to: additional_form_values
cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp', 'constraint_id']

temp_series = df.groupby(['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'])['field_value'].apply(lambda x: ','.join(x))  # group by groupby_colname, join all the contents by ','
df = pd.DataFrame(temp_series).reset_index()  #

df['constraint_id'] = df['constraint_id'].astype(str)
test2 = df.merge(additional_form_values, on=['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'], how='left', suffixes=('', '_y'))
tem1 = test2.loc[test2['insert_timestamp'].isnull()]
tem2 = test2.loc[test2['insert_timestamp'].notnull()]
tem1['insert_timestamp'] = datetime.datetime.now()

vincere_custom_migration.psycopg2_bulk_insert_tracking(tem1, ddbconn, cols, 'additional_form_values', logger)

vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, ddbconn, ['field_value'], ['additional_id'], 'additional_form_values', mylog)
