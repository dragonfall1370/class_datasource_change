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
cf.read('mc_config.ini')
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
sql ="""select nullif(USERDEF10,'') as VAT, c.*, nullif(KEY2,'') as industry, KEY1
from CONTACT1 c1
join CONTACT2 c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
join company c on c.COMPANY = c1.COMPANY
where KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'BANKING'
,'CANDIDATE / CLIENT'
,'CANDIDATE/CLIENT'
,'CLIENT'
,'CLIENT /'
,'CLIENT/CANDIDATE'
,'CLIENT/EXTRAORDINARY'
,'COMPANY'
,'COMPETITOR'
,'CONSULTING CLIENT'
,'CONTRACTOR'
,'EXTRAORDINARY CLIEN'
,'EXTRAORDINARY CLIENT'
,'FINANCE'
,'HOTEL'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'POSSIBLE BUSINESS PA'
,'POTENTIAL AQUISITION'
,'POTENTIAL BUSINESS P'
,'POTENTIAL CLIENT'
,'SOURCE'
,'SOURCING')"""
company = pd.read_sql(sql, engine_mssql)
company['company_externalid'] = company['ID'].astype(str)
assert False
# %% note
tem1 = company[['company_externalid','VAT']].dropna().drop_duplicates()
tem2 = company[['company_externalid','industry']].dropna().drop_duplicates()
tem3 = company[['company_externalid','KEY1']].dropna().drop_duplicates()

tem2 = tem2.groupby('company_externalid')['industry'].apply(lambda x: ', '.join(x)).reset_index()
tem1 = tem1.groupby('company_externalid')['VAT'].apply(lambda x: ', '.join(x)).reset_index()

tem_1 = tem3.merge(tem2, on='company_externalid', how='left')
tem = tem_1.merge(tem1, on='company_externalid', how='left')
tem = tem.where(tem.notnull(),None)
tem = tem.drop_duplicates()
tem['note'] = tem[['VAT','industry','KEY1']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['VAT No.','Industry','Record Type'], x) if e[1]]), axis=1)
# tem['rn'] = tem.groupby('company_externalid').cumcount()
# tem.loc[tem['rn']>0]
# company.to_csv(os.path.join(standard_file_upload, 'dt_out.csv'), index=False)
# tem = tem.loc[tem['note']!='']
vcom.update_note_2(tem, dest_db, mylog)



tem1 = tem1.merge(vcom.company, on=['company_externalid'])
tem1['company_id'] = tem1['id']
tem1['business_number'] = tem1['VAT']
tem1.loc[tem1['VAT'].str.contains(',')]
vincere_custom_migration.psycopg2_bulk_update_tracking(tem1, vcom.ddbconn, ['business_number', ], ['company_id', ],'company_location', mylog)

