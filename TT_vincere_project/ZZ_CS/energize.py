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
from pandas.io.json import json_normalize
import json

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
contact = pd.read_sql("""
select id
     , first_name
     , last_name
     , email
     , phone
     , parent_industry
     , industry
     , functional_expertise
     , sub_functional_expertise
     , brand
     , contact_owners
     , last_activity_date
     , insert_timestamp as reg_date
from contact c
left join
(select contact_id, string_agg(parent,' ,') as parent_industry, string_agg(industry,' ,') as industry
from contact_industry ci
left join (select v1.id, v1.name as industry, v2.name as parent from vertical v1
left join vertical v2 on v1.parent_id = v2.id) v on ci.industry_id = v.id
group by contact_id) i on c.id = i.contact_id
left join
(select contact_id, string_agg(fe.name,' ,') as functional_expertise, string_agg(sfe.name,' ,') as sub_functional_expertise
from contact_functional_expertise cfe
left join functional_expertise fe on cfe.functional_expertise_id = fe.id
left join sub_functional_expertise sfe on cfe.sub_functional_expertise_id = sfe.id
group by contact_id) func on c.id = func.contact_id
left join
(select contact_id, string_agg(name,' ,') as brand from team_group_contact tgc
left join team_group tg on tgc.team_group_id = tg.id
group by contact_id) b on c.id = b.contact_id
left join contact_extension ce on c.id = ce.contact_id
where c.deleted_timestamp is null""",engine_postgre_review)


user = pd.read_sql("""select id as user_id, name from user_account where deleted_timestamp is null""",engine_postgre_review)

owner = contact[['id','contact_owners']].dropna()
owner['contact_owners'] = owner['contact_owners'].apply(lambda x: x.replace('[','').replace(']','').replace('"',''))


owner = owner.contact_owners.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(owner[['id']], left_index=True, right_index=True) \
    .melt(id_vars=['id'], value_name='owner') \
    .drop('variable', axis='columns') \
    .dropna()
# owner.loc[owner['id']==120979]
owner = owner.loc[owner['owner']!='']
owner['owner'] = owner['owner'].astype(int)
owner = owner.merge(user, left_on='owner',right_on='user_id')
owner = owner.groupby('id')['name'].apply(', '.join).reset_index()

contact = contact.merge(owner, on ='id', how='left')
contact['contact_owners'] = contact['name']
contact = contact.drop( columns='name')
contact.loc[(contact['parent_industry'].isnull()), 'parent_industry'] = contact['industry']
contact.loc[(contact['parent_industry']==contact['industry']), 'industry'] = None
contact = contact.rename(columns={
    'first_name':'Contact First Name'
    ,'last_name':'Contact Last Name'
    ,'email':'Primary Email Address'
    ,'phone':'Primary Phone Number'
    ,'parent_industry':'Industry'
    ,'industry':'Sub Industry'
    ,'functional_expertise':'Functional Expertise'
    ,'sub_functional_expertise':'Sub Functional Expertise'
    ,'brand':'Brands'
    ,'contact_owners':'Contact Owners Full Name'
    ,'last_activity_date':'Last Activity Date'
    ,'reg_date':'Reg Date'})
contact.to_csv('energize_contact_export.csv', index=False)