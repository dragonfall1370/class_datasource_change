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
cf.read('sh_config.ini')
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
assert False
# %% production
company = pd.read_sql("""
select cl.cli_no, cli_name
from dbo.client cl
""", engine_mssql)

company_owner = pd.read_sql("""
select clo.cli_no, lower(c.con_email) as con_email
from dbo.cli_owner clo
left join dbo.consultant c on c.con_initials=clo.cli_con
""", engine_mssql)
company_owner = company_owner.drop_duplicates()
company_owner = company_owner.groupby('cli_no')['con_email'].apply(','.join).reset_index()
company = company.merge(company_owner, on='cli_no')

contact = pd.read_sql("""
select peo_no
     , nullif(trim(peo_forename),'') as peo_forename
     , nullif(trim(peo_middlename),'') as peo_middlename
     , nullif(trim(peo_surname),'') as peo_surname
     , nullif(trim(peo_cnt_email),'') as peo_cnt_email
     , c2.cli_no, con_email
from people pe
left join dbo.consultant c on c.con_initials=pe.peo_con
left join client c2 on pe.cli_no = c2.cli_no
where peo_flag in (2,3,6)
""", engine_mssql)
contact = contact.where(contact.notnull(),None)
contact['cli_no'] = contact['cli_no'].apply(lambda x: str(x).split('.')[0] if x else x)

job = pd.read_sql("""
select job_no, j.cli_no, peo_no, job_title, con_email
from jobs j
left join (select peo_no,
     , c2.cli_no
from people pe
left join client c2 on pe.cli_no = c2.cli_no
where peo_flag in (2,3,6)) cont on cont.peo_no = j.job_con_cnt_peo_no and cont.cli_no = j.cli_no
left join consultant c on c.con_initials=j.job_con
""", engine_mssql)
job = job.where(job.notnull(),None)
job['peo_no'] = job['peo_no'].apply(lambda x: str(x).split('.')[0] if x else x)
job['cli_no'] = job['cli_no'].apply(lambda x: str(x) if x else x)

candidate = pd.read_sql("""
select peo_no
     , nullif(trim(peo_forename),'') as peo_forename
     , nullif(trim(peo_middlename),'') as peo_middlename
     , nullif(trim(peo_surname),'') as peo_surname
    , nullif(trim(peo_email),'') as peo_email, con_email
from people pe
left join dbo.consultant c on c.con_initials=pe.peo_con
where peo_flag = 1
""", engine_mssql) #59359
assert False
# %% transpose
company.rename(columns={
    'cli_no': 'company-externalId',
    'cli_name': 'company-name',
    'con_email': 'company-owners',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'peo_no': 'contact-externalId',
    'cli_no': 'contact-companyId',
    'peo_surname': 'contact-lastName',
    'peo_forename': 'contact-firstName',
    'peo_middlename': 'contact-middleName',
     'peo_cnt_email': 'contact-email',
     'con_email': 'contact-owners',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'job_no': 'position-externalId',
    'cli_no': 'position-companyId',
    'peo_no': 'position-contactId',
    'job_title': 'position-title',
    'con_email': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

candidate.rename(columns={
    'peo_no': 'candidate-externalId',
    'peo_forename': 'candidate-firstName',
    'peo_surname': 'candidate-lastName',
    'peo_middlename': 'candidate-middleName',
    'peo_email': 'candidate-email',
    'con_email': 'candidate-owners',
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