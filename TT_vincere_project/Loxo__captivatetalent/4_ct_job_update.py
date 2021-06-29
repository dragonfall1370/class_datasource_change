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
from datetime import date, timedelta
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
connection = engine_postgre_review.raw_connection()

from common import vincere_job
vjob = vincere_job.Job(connection)

# %%
job = pd.read_sql(""" 
select id, address, city, zip, country, state, comp,status, created, publishedAt, contacts, desc, notes, owners  from jobs
""", engine_sqlite)
job['job_externalid'] = job['id'].astype(str)
assert False
j_prod = pd.read_sql("""select id, name from position_description""", engine_postgre_review)
j_prod['name'] = j_prod['name'].apply(lambda x: x.split('_')[0])
vincere_custom_migration.psycopg2_bulk_update_tracking(j_prod, vjob.ddbconn, ['name'], ['id'], 'position_description', mylog)
# vjob.set_job_location_by_company_location(mylog)

# %% currency country
tem = job[['job_externalid']]
tem['currency_type'] = 'usd'
tem['country_code'] = 'US'
vjob.update_currency_type(tem, mylog)
vjob.update_country_code(tem, mylog)

# %% owner
tem = job[['job_externalid', 'owners']].dropna().drop_duplicates()
tem['owners'] = tem['owners'].apply(lambda x: x.replace('\'','').replace(']','').replace('[','') if x else x)
tem['owners'] = tem['owners'].apply(lambda x: x.replace(', ',',') if x else x)

tem1 = tem.owners.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(tem[['job_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['job_externalid'], value_name='email') \
    .drop('variable', axis='columns') \
    .dropna()

cp8 = vjob.insert_owner(tem1, mylog)

# %% public_description
tem = job[['job_externalid', 'desc']].dropna().drop_duplicates()
tem['public_description'] = tem['desc']
cp8 = vjob.update_public_description(tem, mylog)

# %% internal_description
tem = job[['job_externalid', 'notes']].dropna().drop_duplicates()
tem['internal_description'] = tem['notes']
cp8 = vjob.update_internal_description(tem, mylog)

# %% job type
# tem = job[['job_externalid', 'EMPLOYMENT TYPE']].dropna()
# tem.loc[(tem['EMPLOYMENT TYPE'] == 'CONTRACT'), 'job_type'] = 'contract'
# tem = tem[['job_externalid', 'job_type']].dropna()
# cp5 = vjob.update_job_type(tem, mylog)

# %% start date close date reg date
tem = job[['job_externalid', 'created']].dropna().rename(columns={'created': 'start_date'})
tem['start_date'] = pd.to_datetime(tem['start_date'])
tem['reg_date'] = tem['start_date']
vjob.update_start_date(tem, mylog)
vjob.update_reg_date(tem, mylog)

# tem = job[['job_externalid', 'SCHEDULED END DATE']].dropna().rename(columns={'SCHEDULED END DATE': 'close_date'})
# tem['close_date'] = pd.to_datetime(tem['close_date'])
# vjob.update_close_date(tem, mylog)

 # %% status list
tem = job[['job_externalid', 'status']].dropna()
tem.loc[tem['status'] == 'Inactive', 'name'] = 'Closed'
tem.loc[tem['status'] == 'Active', 'name'] = 'Open'
vjob.add_job_status(tem, mylog)

# %% note
tem = job[['job_externalid','address','city','zip','country','state','comp','publishedAt','contacts']]
tem['zip'] = tem['zip'].apply(lambda x: str(x).split('.')[0] if x else x)
tem['location'] = tem[['address', 'city','zip','state','country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)

tem1 = tem[['job_externalid','contacts']].dropna()
tem1['contacts'] = tem1['contacts'].apply(lambda x: x.replace('[','').replace(']','').replace(', ',','))
tem1 = tem1.contacts.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(tem1[['job_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['job_externalid'], value_name='contacts') \
    .drop('variable', axis='columns') \
    .dropna()
cont = pd.read_sql("""select id as contacts, name from people""",engine_sqlite)
cont['contacts'] = cont['contacts'].astype(str)
tem1['contacts'] = tem1['contacts'].astype(str)
tem1 = tem1.merge(cont,on='contacts')
tem1.loc[tem1['job_externalid']=='218662']
tem1 = tem1.groupby('job_externalid')['name'].apply(', '.join).reset_index()

tem = tem.merge(tem1,on='job_externalid',how='left')
tem = tem.where(tem.notnull(),None)
tem['note'] = tem[['location','name','comp','publishedAt']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Location','Compant contacts','Salary','Publish date'], x) if e[1]]), axis=1)
vjob.update_note(tem, mylog)

# %% close job
tem = job[['job_externalid', 'status']].dropna()
tem = tem.loc[tem['status'] == 'Inactive']
today = date.today()
tem['close_date'] = today
vjob.update_close_date(tem, mylog)

# %% salary
tem=pd.read_csv('salary.csv')
tem['actual_salary']=tem['comp']
tem['actual_salary'] = tem['actual_salary'].apply(lambda x: x.replace(',',''))
tem['actual_salary'] = tem['actual_salary'].astype(int)
tem['job_externalid']=tem['job_externalid'].astype(str)
vjob.update_actual_salary(tem, mylog)