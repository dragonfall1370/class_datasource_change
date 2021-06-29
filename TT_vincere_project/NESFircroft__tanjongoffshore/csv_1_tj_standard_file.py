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
cf.read('tj_config.ini')
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
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# assert False
# %%
company = pd.read_sql("""
select RMSCLIENTID, "COMPANY NAME"
from Company
""", engine_sqlite)

contact = pd.read_sql("""
select c2.RMSCLIENTID,
       RMSCONTACTID,
       "FIRST NAME",
       "LAST NAME",
       "WORK EMAIL"
from Contact c1
left join Company c2 on c1.RMSCLIENTID = c2.RMSCLIENTID
""", engine_sqlite)

job = pd.read_sql("""
select RMSPLACEMENTID,
       "JOB TITLE(S)",
       p.RMSCLIENTID,
       c.RMSCONTACTID
from Placement p
left join Contact c on c.RMSCLIENTID = p.RMSCLIENTID and c.RMSCONTACTID = p.RMSCONTACTID
""", engine_sqlite)

candidate = pd.read_sql("""
select RMSCANDIDATEID,
       "FIRST NAME",
       "LAST NAME",
       "MIDDLE NAME",
       "HOME EMAIL"
from Candidate
""", engine_sqlite) #59359
assert False
# %% transpose
company.rename(columns={
    'RMSCLIENTID': 'company-externalId',
    'COMPANY NAME': 'company-name',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'RMSCONTACTID': 'contact-externalId',
    'RMSCLIENTID': 'contact-companyId',
    'LAST NAME': 'contact-lastName',
    'FIRST NAME': 'contact-firstName',
    # 'middlename': 'contact-middleName',
     'WORK EMAIL': 'contact-email',
     # 'EMC_ACC_EMAILS': 'contact-owners',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'RMSPLACEMENTID': 'position-externalId',
    'RMSCLIENTID': 'position-companyId',
    'RMSCONTACTID': 'position-contactId',
    'JOB TITLE(S)': 'position-title',
    # 'Email_Address': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

candidate.rename(columns={
    'RMSCANDIDATEID': 'candidate-externalId',
    'FIRST NAME': 'candidate-firstName',
    'LAST NAME': 'candidate-lastName',
    'MIDDLE NAME': 'candidate-middleName',
    'HOME EMAIL': 'candidate-email',
    # 'owner': 'candidate-owners',
}, inplace=True)
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)

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
