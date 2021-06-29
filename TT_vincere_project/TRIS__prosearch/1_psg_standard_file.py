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
cf.read('psg_config.ini')
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

# %% company
company = pd.read_sql("""
select ClientID
, ClientName
, a.AMEMail as owner
from Client
left join AM A on Client.AMID = A.AMID
""", engine_mssql)
company = company.where(company.notnull(),None)
company['ClientID'] =company['ClientID'].apply(lambda x: str(x) if x else x)
company['owner'] = company['owner'].apply(lambda x: x.replace('sdf','') if x else x)

contact = pd.read_sql("""
select PersonID
, PersonFirstName
, PersonSurname
, nullif(PersonWorkEMail,'') as PersonWorkEMail
, ClientID
, o.AMEMail as owner
 from Contact c
left join (select ContactID, a.AMEMail from ContactAM ca
left join AM A on ca.AMID = A.AMID) o on o.ContactID = c.PersonID
""", engine_mssql)
contact = contact.where(contact.notnull(),None)
tem = contact[['PersonID','owner']].dropna()
tem = tem.groupby('PersonID')['owner'].apply(lambda x: ','.join(x)).reset_index()
contact = contact.drop('owner', axis=1).drop_duplicates().merge(tem, on='PersonID', how='left')
contact = contact.where(contact.notnull(),None)
contact['ClientID'] =contact['ClientID'].apply(lambda x: str(x).split('.')[0] if x else x)
contact['PersonID'] =contact['PersonID'].apply(lambda x: str(x) if x else x)
contact['owner'] = contact['owner'].apply(lambda x: x.replace('sdf','') if x else x)

job = pd.read_sql("""
select JobID
, JobTitle
, j.ClientID
, c.PersonID
, a.AMEMail as owner
from Job j
left join Contact c on c.PersonID = j.PersonID and c.ClientId = j.ClientID
left join AM A on j.AMID = A.AMID
""", engine_mssql)
job = job.where(job.notnull(),None)
job['ClientID'] =job['ClientID'].apply(lambda x: str(x).split('.')[0] if x else x)
job['PersonID'] =job['PersonID'].apply(lambda x: str(x).split('.')[0] if x else x)
job['JobID'] =job['JobID'].apply(lambda x: str(x) if x else x)
job['owner'] = job['owner'].apply(lambda x: x.replace('sdf','') if x else x)

candidate = pd.read_sql("""
select PersonID
, PersonFirstName
, PersonSurname
, nullif(PersonHomeEMail,'') as PersonHomeEMail
, a.AMEMail as owner
 from Candidate
left join AM A on Candidate.AMID = A.AMID
""", engine_mssql)
candidate = candidate.where(candidate.notnull(),None)
candidate['PersonID'] =candidate['PersonID'].apply(lambda x: str(x) if x else x)
candidate['owner'] = candidate['owner'].apply(lambda x: x.replace('sdf','') if x else x)
# assert False
# %% transpose
company.rename(columns={
    'ClientID': 'company-externalId',
    'ClientName': 'company-name',
    'owner': 'company-owners',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'PersonID': 'contact-externalId',
    'ClientID': 'contact-companyId',
    'PersonFirstName': 'contact-firstName',
    'PersonSurname': 'contact-lastName',
    'owner': 'contact-owners',
    'PersonWorkEMail': 'contact-email',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'JobID': 'position-externalId',
    'ClientID': 'position-companyId',
    'PersonID': 'position-contactId',
    'JobTitle': 'position-title',
    'owner': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

candidate.rename(columns={
    'PersonID': 'candidate-externalId',
    'PersonFirstName': 'candidate-firstName',
    'PersonSurname': 'candidate-lastName',
    'PersonHomeEMail': 'candidate-email',
    'owner': 'candidate-owners',
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