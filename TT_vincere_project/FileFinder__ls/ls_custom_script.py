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
cf.read('ls_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
assert False
# %%
cand_cont = pd.read_sql("""
select id ,first_name, last_name, contact_id from candidate where deleted_timestamp is null and contact_id is not null
""", engine_postgre_review)

value = pd.read_sql("""
select a.translate, b.field_value
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='d5b6dff5678f5c48b0559fbcd4571760')
        and form_id in (select form_id from configurable_form_field where  field_key ='d5b6dff5678f5c48b0559fbcd4571760')
""", engine_postgre_review)

cand_value = pd.read_sql("""
select additional_id
     , field_value from additional_form_values
where field_id = 11267
and nullif(field_value,'') is not null
""", engine_postgre_review)

cand_value1 = cand_value.field_value.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cand_value[['additional_id']], left_index=True, right_index=True) \
    .melt(id_vars=['additional_id'], value_name='value') \
    .drop('variable', axis='columns') \
    .dropna()

cand_value2 = cand_value1.merge(value, left_on='value', right_on='field_value')

value2 = pd.read_sql("""
select a.translate, b.field_value
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='7ef65742e491c9553e5a3a3ec387a22d')
        and form_id in (select form_id from configurable_form_field where  field_key ='7ef65742e491c9553e5a3a3ec387a22d')
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

# %% cand
cand_value2['translate'].unique()
tem_cand1 = cand_value2.loc[cand_value2['translate']=='Hot Candidate']
tem_cand2 = cand_value2.loc[cand_value2['translate']=='Paula Hot Candidate']
tem_cand3 = cand_value2.loc[cand_value2['translate']=='Fabiana Hot Candidate']
tem_cand4 = cand_value2.loc[cand_value2['translate']=='Andrea Hot Candidate']
temcand = pd.concat([tem_cand1[['candidate_id']],tem_cand2[['candidate_id']],tem_cand3[['candidate_id']],tem_cand4[['candidate_id']]])
temcand = temcand.drop_duplicates()

cont_value2['translate'].unique()
temcont1 = cont_value2.loc[cont_value2['translate']=='Hot Candidate']
temcont2 = cont_value2.loc[cont_value2['translate']=='Paula Hot Candidate']
temcont = pd.concat([temcont1[['candidate_id']],temcont2[['candidate_id']]])
temcont = temcont.drop_duplicates()
temcont = temcont.merge(cand_cont, left_on='additional_id', right_on='contact_id')
temcand2 = temcont[['id']].drop_duplicates()
temcand2 = temcand2.rename(columns={'id':'candidate_id'})

cand = pd.concat([temcand,temcand2])
cand = cand.drop_duplicates()
cand['additional_id'] = cand['candidate_id']
cand['additional_type'] = 'add_cand_info'
cand['form_id'] = 1005
cand['field_value'] = 1
cand['field_value'] = cand['field_value'].astype(str)
cand['field_id'] = 11275
cand['constraint_id'] = 11275

#-------------------------------------------------------------------

cand_value2['translate'].unique()
tem_cand1_1 = cand_value2.loc[cand_value2['translate']=='Start up Culture']
tem_cand1_1 = tem_cand1_1[['candidate_id']].drop_duplicates()

cont_value2['translate'].unique()
temcont1_1 = cont_value2.loc[cont_value2['translate']=='Start up Culture']
temcont1_1 = temcont1_1.drop_duplicates()
temcont1_1 = temcont1_1.merge(cand_cont, left_on='additional_id', right_on='contact_id')
temcont1_2 = temcont1_1[['id']].drop_duplicates()
temcont1_2 = temcont1_2.rename(columns={'id':'candidate_id'})

cand2 = pd.concat([tem_cand1_1,temcont1_2])
cand2 = cand2.drop_duplicates()
cand2['additional_id'] = cand2['candidate_id']
cand2['additional_type'] = 'add_cand_info'
cand2['form_id'] = 1005
cand2['field_value'] = 2
cand2['field_value'] = cand2['field_value'].astype(str)
cand2['field_id'] = 11275
cand2['constraint_id'] = 11275

df = pd.concat([cand,cand2])
df = df.drop_duplicates()

cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp', 'constraint_id']

temp_series = df.groupby(['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'])['field_value'].apply(lambda x: ','.join(x))  # group by groupby_colname, join all the contents by ','
df = pd.DataFrame(temp_series).reset_index()  #

df['insert_timestamp'] = datetime.datetime.now()
vincere_custom_migration.psycopg2_bulk_insert_tracking(df, connection, cols, 'additional_form_values', mylog)




























# %% CONTACT
cand_value2['translate'].unique()
tem_cand_cont1 = cand_value2.loc[cand_value2['translate']=='Hot Client']
tem_cand_cont1 = tem_cand_cont1[['candidate_id']].drop_duplicates()
tem_cand_cont1 = tem_cand_cont1.merge(cand_cont, left_on='candidate_id', right_on='id')
tem_cand_cont = tem_cand_cont1[['contact_id']].drop_duplicates()
tem_cand_cont = tem_cand_cont.rename(columns={'contact_id':'additional_id'})

cont_value2['translate'].unique()
temcont1 = cont_value2.loc[cont_value2['translate']=='Hot Client']
temcont2 = cont_value2.loc[cont_value2['translate']=='Paula Hot Client']
temcont3 = cont_value2.loc[cont_value2['translate']=='Fabiana Hot Client']
temcont = pd.concat([temcont1[['additional_id']],temcont2[['additional_id']],temcont3[['additional_id']]])
temcont = temcont.drop_duplicates()


cont1 = pd.concat([tem_cand_cont,temcont])
cont1 = cont1.drop_duplicates()
cont1['additional_type'] = 'add_con_info'
cont1['form_id'] = 1007
cont1['field_value'] = 1
cont1['field_value'] = cont1['field_value'].astype(str)
cont1['field_id'] = 11276
cont1['constraint_id'] = 11276

#-------------------------------------------------------------------

cand_value2['translate'].unique()
tem_cand_cont1 = cand_value2.loc[cand_value2['translate']=='Hot Potential Client']
tem_cand_cont2 = cand_value2.loc[cand_value2['translate']=='Paula Potential Client']
tem_cand_cont = pd.concat([tem_cand_cont1[['candidate_id']],tem_cand_cont2[['candidate_id']]])
tem_cand_cont = tem_cand_cont.drop_duplicates()
tem_cand_cont = tem_cand_cont.merge(cand_cont, left_on='candidate_id', right_on='id')
tem_cand_cont = tem_cand_cont[['contact_id']].drop_duplicates()
tem_cand_cont = tem_cand_cont.rename(columns={'contact_id':'additional_id'})

cont_value2['translate'].unique()
temcont1 = cont_value2.loc[cont_value2['translate']=='Hot Potential Client']
temcont2 = cont_value2.loc[cont_value2['translate']=='Paula Potential Client']
temcont3 = cont_value2.loc[cont_value2['translate']=='Fabiana Hot Potential Client']
temcont = pd.concat([temcont1[['additional_id']],temcont2[['additional_id']],temcont3[['additional_id']]])
temcont = temcont.drop_duplicates()


cont2 = pd.concat([tem_cand_cont,temcont])
cont2 = cont2.drop_duplicates()
cont2['additional_type'] = 'add_con_info'
cont2['form_id'] = 1007
cont2['field_value'] = 3
cont2['field_value'] = cont2['field_value'].astype(str)
cont2['field_id'] = 11276
cont2['constraint_id'] = 11276


#-------------------------------------------------------------------

cand_value2['translate'].unique()
tem_cand_cont1 = cand_value2.loc[cand_value2['translate']=='Hot Networker']
tem_cand_cont1 = tem_cand_cont1[['candidate_id']].drop_duplicates()
tem_cand_cont1 = tem_cand_cont1.merge(cand_cont, left_on='candidate_id', right_on='id')
tem_cand_cont = tem_cand_cont1[['contact_id']].drop_duplicates()
tem_cand_cont = tem_cand_cont.rename(columns={'contact_id':'additional_id'})

cont_value2['translate'].unique()
temcont1 = cont_value2.loc[cont_value2['translate']=='Hot Networker']
temcont1 = temcont1[['additional_id']].drop_duplicates()

cont3 = pd.concat([tem_cand_cont,temcont1])
cont3 = cont3.drop_duplicates()
cont3['additional_type'] = 'add_con_info'
cont3['form_id'] = 1007
cont3['field_value'] = 2
cont3['field_value'] = cont3['field_value'].astype(str)
cont3['field_id'] = 11276
cont3['constraint_id'] = 11276




df = pd.concat([cont1, cont2,cont3])
df = df.drop_duplicates()

cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp', 'constraint_id']

temp_series = df.groupby(['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'])['field_value'].apply(lambda x: ','.join(x))  # group by groupby_colname, join all the contents by ','
df = pd.DataFrame(temp_series).reset_index()  #

df['insert_timestamp'] = datetime.datetime.now()
vincere_custom_migration.psycopg2_bulk_insert_tracking(df, connection, cols, 'additional_form_values', mylog)