# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
# from edenscott._edenscott_dtypes import *
import os
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
cf.read('dj_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% data connections
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# assert False
# %%
company = pd.read_sql("""
select * from company
""", engine_mssql)
company = company.drop_duplicates()

contact = pd.read_sql("""
select c1.ACCOUNTNO, c1.CONTACT, trim(LASTNAME) as LASTNAME,c.ID,  u.EMC_ACC_EMAILS from CONTACT1 c1
left join company c on c.COMPANY = c1.COMPANY
left join USERS u on u.NAME = c1.KEY4
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
tem = contact.loc[contact['CONTACT'].notnull()]

tem2 = tem.loc[tem['LASTNAME'].isnull()]
tem2['contact-firstName'] = None

tem3 = tem.loc[tem['LASTNAME'].notnull()]
tem3['contact-firstName'] = [a.replace(b, '').strip() for a, b in zip(tem3['CONTACT'], tem3['LASTNAME'])]

contact = pd.concat([tem2, tem3]) #10516+773 11289

tem = contact[['ACCOUNTNO','EMC_ACC_EMAILS']]
tem['EMC_ACC_EMAILS'] = tem['EMC_ACC_EMAILS'].fillna('')
tem = tem.groupby('ACCOUNTNO')['EMC_ACC_EMAILS'].apply(lambda x: ','.join(x)).reset_index()
tem['EMC_ACC_EMAILS'] = tem['EMC_ACC_EMAILS'].apply(lambda x: x.replace('.com,','.com'))
tem.to_csv('tem_cont.csv')
contact = contact.drop('EMC_ACC_EMAILS', axis = 1)
contact = contact.drop_duplicates()
contact = contact.merge(tem,on='ACCOUNTNO')

candidate = pd.read_sql("""
select c1.ACCOUNTNO, c1.CONTACT, LASTNAME, u.EMC_ACC_EMAILS
from CONTACT1 c1
left join USERS u on u.NAME = c1.KEY4
where KEY1 in (
'Placed'
,'Do not Use'
,'Candidate')
""", engine_mssql) #59359

tem2 = candidate.loc[candidate['LASTNAME'].isnull()]
tem2['candidate-firstName'] = tem2['CONTACT'].apply(lambda x: x.split(' ')[0])
tem2['LASTNAME'] = tem2['CONTACT'].apply(lambda x: x.split(' ')[1])

tem3 = candidate.loc[candidate['LASTNAME'].notnull()]
tem3['candidate-firstName'] = [a.replace(b, '').strip() for a, b in zip(tem3['CONTACT'], tem3['LASTNAME'])]

candidate = pd.concat([tem2, tem3])
candidate['Email_Address'] = candidate['ACCOUNTNO']+'@noemial.com'

# candidate['EMC_ACC_EMAILS'] = candidate['EMC_ACC_EMAILS'].apply(lambda x: x or None)
tem = candidate[['ACCOUNTNO','EMC_ACC_EMAILS']]
tem['EMC_ACC_EMAILS'] = tem['EMC_ACC_EMAILS'].fillna('')
tem = tem.groupby('ACCOUNTNO')['EMC_ACC_EMAILS'].apply(lambda x: ','.join(x)).reset_index()
tem['EMC_ACC_EMAILS'] = tem['EMC_ACC_EMAILS'].apply(lambda x: x.replace('.com,','.com'))

candidate = candidate.drop('EMC_ACC_EMAILS', axis = 1)
candidate = candidate.drop_duplicates()
candidate = candidate.merge(tem,on='ACCOUNTNO')

# %% transpose
company.rename(columns={
    'ID': 'company-externalId',
    'COMPANY': 'company-name',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'ACCOUNTNO': 'contact-externalId',
    'ID': 'contact-companyId',
    'LASTNAME': 'contact-lastName',
    # 'middlename': 'contact-middleName',
    # 'email': 'contact-email',
     'EMC_ACC_EMAILS': 'contact-owners',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

candidate.rename(columns={
    'ACCOUNTNO': 'candidate-externalId',
    # 'Forename': 'candidate-firstName',
    'LASTNAME': 'candidate-lastName',
    # 'Middle': 'candidate-middleName',
    'Email_Address': 'candidate-email',
    'EMC_ACC_EMAILS': 'candidate-owners',
}, inplace=True)
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)

# %% to csv files
candidate.to_csv(os.path.join(standard_file_upload, '6_candidate.csv'), index=False)

contact.to_csv(os.path.join(standard_file_upload, '4_contact.csv'), index=False)
company.to_csv(os.path.join(standard_file_upload, '2_company.csv'), index=False)

if len(default_company):
    default_company.to_csv(os.path.join(standard_file_upload, '1_default_company.csv'), index=False)
elif len(tem):
    default_company = pd.DataFrame({'company-externalId': ['DEFAULT_COMPANY'], 'company-name': ['DEFAULT COMPANY']})
    default_company.to_csv(os.path.join(standard_file_upload, '1_default_company.csv'), index=False)