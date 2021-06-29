# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
import numpy as np
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('pj_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)
# assert False
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% connect data
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

"""
"""
assert False
# %% company
company = pd.read_csv(os.path.join(standard_file_upload, 'company.csv'))
company = vincere_standard_migration.process_vincere_comp(company, mylog)

# %% contact
company_db = pd.read_sql("""select * from company;""", engine_postgre)
company = pd.read_csv(os.path.join(standard_file_upload, 'company.csv'))
company_id = company[['company-externalId', 'company-name']]
contact = pd.read_csv(os.path.join(standard_file_upload, 'contact.csv'))
contact = contact.merge(company_id, left_on='contact-companyId', right_on='company-name')

company_id_db = company_db[['id', 'external_id']]
company_id_db.info()
contact.info()
contact['company-externalId'] = contact['company-externalId'].astype(str)
contact = contact.merge(company_id_db, left_on='company-externalId', right_on='external_id')
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

# %% job
job = pd.read_csv(os.path.join(standard_file_upload, 'jobs_activities_3.csv'))
job = job[['ID', 'Name', 'contact-externalId', 'company-externalId']]
job = job.drop_duplicates()
job.rename(columns={
    'ID': 'position-externalId',
    'contact-externalId': 'position-contactId',
    'company-externalId': 'position-companyId',
    'Name': 'position-title'
}, inplace=True)
df1 = job.where((pd.notnull(job)), None)
df1.info()
df1 = df1.loc[df1['position-companyId'].notnull()]

df1['position-contactId'] = df1['position-contactId'].map(lambda x: str(x) if x else x)
df1['position-companyId'] = df1['position-companyId'].map(lambda x: str(x) if x else x)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(df1, mylog)

job['position-contactId'] = job['position-contactId'].apply(lambda x: x.split('.')[0].strip())
job['position-companyId'] = job['position-companyId'].apply(lambda x: x.split('.')[0].strip())

default_contacts['contact-externalId'] = default_contacts['contact-externalId'].apply(lambda x: x.split('.')[0].strip())
default_contacts['contact-companyId'] = default_contacts['contact-companyId'].apply(lambda x: x.split('.')[0].strip())

# %% candidate
candidate = pd.read_csv(os.path.join(standard_file_upload, 'candidates.csv'))
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)

# candidate['rn'] = candidate.groupby(candidate['candidate-email']).cumcount()
# cand_mail = candidate[['candidate-email', 'rn']]
# cand_mail = cand_mail.dropna()
# cand_mail.loc[cand_mail['rn'] > 0]

# %% csv
candidate.to_csv(os.path.join(standard_file_upload, 'candidate_2.csv'), index=False)
contact.to_csv(os.path.join(standard_file_upload, 'contact_2.csv'), index=False)


job.to_csv(os.path.join(standard_file_upload, 'job_2.csv'), index=False)

if len(default_contacts):
    default_contacts.to_csv(os.path.join(standard_file_upload, '3_default_contacts.csv'), index=False)
