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
cf.read('dj_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_company
vcom = vincere_company.Company(connection)
# %%
sql ="""select c.ID, KEY1
from company c
join CONTACT1 c1 on c.COMPANY = c1.COMPANY"""
company = pd.read_sql(sql, engine_mssql)
company['company_externalid'] = company['ID'].astype(str)
assert False

# %% web
web = pd.read_sql("""
select c1.ACCOUNTNO as contact_externalid, CONTSUPREF, ID from CONTACT1 c1
left join CONTSUPP c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
left join company c on c.COMPANY = c1.COMPANY
where c2.CONTACT = 'Web Site'
and KEY1 in (
'Vendor'
,'Suspect'
,'Supplier'
,'Prospect'
,'Moved on'
,'Internal'
,'Community'
,'Closed'
,'Client'
,'Accounts'
,'6 Soul Mate'
,'5 Raving Fan'
,'4 Client'
,'3 Customer'
,'2 Prospect'
,'1 Suspect')
""", engine_mssql)
web['company_externalid'] = web['ID'].astype(str)
tem = web[['company_externalid','CONTSUPREF']].dropna().drop_duplicates()
tem['website'] = tem['CONTSUPREF']
vcom.update_website(tem, mylog)

# %% note
tem1 = company[['company_externalid','KEY1']].dropna().drop_duplicates()
tem1 = tem1.groupby('company_externalid')['KEY1'].apply(lambda x: ', '.join(x)).reset_index()
tem1['note'] = tem1['KEY1']
vcom.update_note_2(tem1, dest_db, mylog)

# %% last activity
tem = pd.read_sql("""select company_id, max(a.insert_timestamp) as last_activity_date
from  activity_company a
group by company_id""", engine_postgre_review)
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, connection, ['last_activity_date', ], ['company_id', ],'company_extension', mylog)

# %% industry
industries = pd.read_csv('industry.csv')
from common import vincere_candidate
import datetime
vcand = vincere_candidate.Candidate(connection)
tem = industries[['Vincere Industry']].drop_duplicates()
tem['insert_timestamp'] = datetime.datetime.now()
tem['name'] = tem['Vincere Industry']
tem = tem.loc[tem.name!='']
vcand.append_industry(tem, mylog)

com_ind = pd.read_sql("""select c.ID, nullif(U_KEY2,'') as industry
from CONTACT1 c1
join company c on c1.COMPANY = c.COMPANY
where KEY1 in (
'Vendor'
,'Suspect'
,'Supplier'
,'Prospect'
,'Moved on'
,'Internal'
,'Community'
,'Closed'
,'Client'
,'Accounts'
,'6 Soul Mate'
,'5 Raving Fan'
,'4 Client'
,'3 Customer'
,'2 Prospect'
,'1 Suspect')
""", engine_mssql)
com_ind = com_ind.dropna().drop_duplicates()
com_ind['company_externalid'] = com_ind['ID'].astype(str)
com_ind = com_ind.merge(industries, left_on='industry', right_on='GM Value')
com_ind['name'] = com_ind['Vincere Industry']
# com_ind['name'] = com_ind['name'].apply(lambda x:  x.title())
# com_ind.loc[com_ind['name'].str.contains('Debt Collection')]
cp10 = vcom.insert_company_industry(com_ind, mylog)
