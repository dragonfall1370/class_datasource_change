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
cf.read('ct_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
# src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% data connections
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
# def get_email(exp):
#     exp = exp.replace('\'', '"')
#     df = pd.read_json(exp)
#     df = df.loc[(df['type']=='Main') | (df['type']=='Work')]
#     if df.empty:
#         return ''
#     return df[['email']].iloc[0,0]
#
# def get_email_cand(exp):
#     exp = exp.replace('\'', '"')
#     exp = exp.replace('Clinton.O"Brien@mixpanel.com', 'Clinton.O\'Brien@mixpanel.com')
#     df = pd.read_json(exp)
#     df = df.loc[(df['type']=='Main') | (df['type']=='Personal')]
#     if df.empty:
#         return ''
#     return df[['email']].iloc[0,0]
#
# com = """[{'type': 'Main', 'email': 'clinton.m.obrien@gmail.com'}, {'type': 'Work', 'email': "Clinton.O'Brien@mixpanel.com"}, {'type': 'Work', 'email': 'Clinton@mixpanel.com'}]"""
# exp = com
# exp = exp.replace('\'', '"')
# df = pd.read_json(exp)
# df = df.loc[(df['type'] == 'Main') | (df['type'] == 'Work')]
#
# def get_company_job_title_2(exp):
#     exp = exp.replace('true', '"true"')
#     exp = exp.replace('\n', '')
#     exp = exp.replace('\"', '')
#     val = eval(exp)
#     return val[0]['company']
assert False
# %%
company = pd.read_sql("""
select id, name, ownedBy, createdBy from company
""", engine_sqlite)
company['owner'] = company[['ownedBy', 'createdBy']].apply(lambda x: ','.join(set([e for e in x if e])), axis=1)

contact = pd.read_sql("""
select id, name, ownedBy, createdBy from people
where types like '%Contact%'
""", engine_sqlite)
contact_info = pd.read_sql("""
select * from people_experience
""", engine_sqlite)
contact_email = pd.read_sql("""
select * from people_emails
""", engine_sqlite)
contact_email1 = pd.pivot_table(contact_email, values='email',columns='email_type',aggfunc='first', index = 'people_id')
contact_email1.loc[contact_email1['people_id']==24636540]
contact_email1.to_csv('contact_email1.csv')
# contact_email1.info()
contact['owner'] = contact[['ownedBy', 'createdBy']].apply(lambda x: ','.join(set([e for e in x if e])), axis=1)
# contact['email'] = contact['emails'].apply(lambda x: get_email(x) if x else x)

contact_info['rn'] = contact_info.groupby('id').cumcount()
contact_info = contact_info.loc[contact_info['rn']==0]
contact = contact.merge(contact_info[['id','company']],on='id',how='left')

com = company[['id','name']].rename(columns={'id':'company_id','name':'company'})
com['rn'] = com.groupby('company').cumcount()
com = com.loc[com['rn']==0]
contact = contact.merge(com,on='company',how='left')

contact['rn'] = contact.groupby('id').cumcount()

# contact_email['rn'] = contact_email.groupby('people_id').cumcount()
# contact_email = contact_email.loc[contact_email['rn']==0]
contact1 = contact.merge(contact_email1,left_on='id',right_on='people_id',how='left')
contact1 = contact1.where(contact1.notnull(),None)
contact1.to_csv('ct_contact.csv',index=False)

contact = pd.read_csv('ct_contact.csv')
contact = contact.where(contact.notnull(),None)
contact['company_id'] = contact['company_id'].apply(lambda x: str(x).split('.')[0] if x else x)
contact['id'] = contact['id'].astype(str)
contact['contact-firstName'] = contact['name'].apply(lambda x: x.split(' ')[0])
contact['contact-lastName'] = contact['name'].apply(lambda x: x.split(' ')[-1])
contact['email'] = contact['Main']
contact.loc[(contact['email'].isnull()), 'email'] = contact['Work']
# assert False

job = pd.read_sql("""
select id, title, company, contacts, owners from jobs
""", engine_sqlite)
job['owners'] = job['owners'].apply(lambda x: x.replace('\'','').replace(']','').replace('[','') if x else x)
job['owners'] = job['owners'].apply(lambda x: x.replace(', ',',') if x else x)
job['contacts'] = job['contacts'].apply(lambda x: x.split(',')[0] if x else x)
job['contacts'] = job['contacts'].apply(lambda x: x.replace(']','').replace('[','') if x else x)

job = job.merge(contact[['id','company_id']].rename(columns={'id':'contact_id'}), left_on=['company','contacts'], right_on=['company_id','contact_id'], how='left')
# job.to_csv('ct_job.csv',index=False)

candidate = pd.read_sql("""
select id, name, ownedBy, createdBy, emails from people
where types like '%Candidate%'
""", engine_sqlite) #59359
candidate['owner'] = candidate[['ownedBy', 'createdBy']].apply(lambda x: ','.join(set([e for e in x if e])), axis=1)
candidate['candidate-firstName'] = candidate['name'].apply(lambda x: x.split(' ')[0])
candidate['candidate-lastName'] = candidate['name'].apply(lambda x: x.split(' ')[-1])
candidate1 = candidate.merge(contact_email1,left_on='id',right_on='people_id',how='left')
candidate1 = candidate1.where(candidate1.notnull(),None)
candidate1['email'] = candidate1['Main']
candidate1.loc[(candidate1['email'].isnull()), 'email'] = candidate1['Personal']
assert False
# %% transpose
company.rename(columns={
    'id': 'company-externalId',
    'name': 'company-name',
    'owner': 'company-owners',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'id': 'contact-externalId',
    'company_id': 'contact-companyId',
    # 'LAST NAME': 'contact-lastName',
    # 'FIRST NAME': 'contact-firstName',
    # 'middlename': 'contact-middleName',
     'email': 'contact-email',
     'owner': 'contact-owners',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'id': 'position-externalId',
    'company': 'position-companyId',
    'contact_id': 'position-contactId',
    'title': 'position-title',
    'owners': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

candidate1.rename(columns={
    'id': 'candidate-externalId',
    # 'FIRST NAME': 'candidate-firstName',
    # 'LAST NAME': 'candidate-lastName',
    # 'MIDDLE NAME': 'candidate-middleName',
    'email': 'candidate-email',
    'owner': 'candidate-owners',
}, inplace=True)
candidate = vincere_standard_migration.process_vincere_cand(candidate1, mylog)

# %% to csv files
candidate.to_csv(os.path.join(standard_file_upload, '6_candidate.csv'), index=False)
job.to_csv(os.path.join(standard_file_upload, '5_job.csv'), index=False)
contact.to_csv(os.path.join(standard_file_upload, '4_contact.csv'), index=False)
company.to_csv(os.path.join(standard_file_upload, '2_company.csv'), index=False)
if len(default_contacts):
    default_contacts.to_csv(os.path.join(standard_file_upload, '3_default_contacts.csv'), index=False)

tem = default_contacts.loc[default_contacts['contact-companyId'] == 'DEFAULT_COMPANY']
if len(default_company):
    default_company.to_csv(os.path.join(standard_file_upload, '1_default_company.csv'), index=False)
elif len(tem):
    default_company = pd.DataFrame({'company-externalId': ['DEFAULT_COMPANY'], 'company-name': ['DEFAULT COMPANY']})
    default_company.to_csv(os.path.join(standard_file_upload, '1_default_company.csv'), index=False)
