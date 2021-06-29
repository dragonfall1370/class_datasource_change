# -*- coding: UTF-8 -*-
# import sys
# sys.path.append('D:\Tony\Working\DMvincere')
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
cf.read('saleexpert.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
db_source = cf[cf['default'].get('src_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% data connections
connection_str = "mysql+pymysql://{user}:{password}@{server}/{db}" \
    .format(user=db_source.get('user')
            , password=db_source.get('password')
            , server=db_source.get('server')
            , db=db_source.get('database'))
engine = sqlalchemy.create_engine(connection_str)
# assert False
# %% company
sql = """
select concat('SE',firma_id) as firma_id
     , nullif(firma_name1,'') as firma_name1
     , nullif(u.reg_mail,'') as reg_mail
from mand_firma c
left join user_login u on u.user_id = c.firma_zustaendig"""
company = pd.read_sql(sql, engine)
company = company.where(company.notnull(), None)
company['firma_id'] = company['firma_id'].apply(lambda x: str(x) if x else x)

contact = pd.read_sql("""
select concat('SE',mp.person_id) as person_id
     , nullif(mp.person_vorname,'') as person_vorname
     , nullif(mp.person_nachname,'') as person_nachname
     , nullif(mfp.person_email,'') as person_email
     , concat('SE',mfp.firma_id) as firma_id
from mand_person mp
left join (select person_id, max(firma_id) as firma_id, person_email from mand_firma_person group by person_id) mfp on mp.person_id = mfp.person_id
""", engine)
contact = contact.where(contact.notnull(), None)
contact['firma_id'] = contact['firma_id'].apply(lambda x: str(x).split('.')[0] if x else x)
contact['person_id'] = contact['person_id'].apply(lambda x: str(x) if x else x)

job = pd.read_sql("""
select concat('SE',id) as id
     , nullif(titel,'') as titel
     , l.reg_mail
     , concat('SE',p.mandant) as mandant
     , concat('SE',cont.person_id) as person_id
from projekte p
left join (select max(person_id) as person_id, firma_id
from mand_firma_person
group by firma_id) cont on cont.firma_id = p.mandant
left join user_login l on p.berater = l.user_id
""", engine)
job = job.where(job.notnull(), None)
job['id'] = job['id'].apply(lambda x: str(x) if x else x)
job['person_id'] = job['person_id'].apply(lambda x: str(x).split('.')[0] if x else x)
job['mandant'] = job['mandant'].apply(lambda x: str(x) if x else x)


# sql = """
# select concat('SE',u.user_id) as user_id
#      ,u.vorname
#      ,u.nachname
#      ,l.reg_mail
# from user_data u
# left join user_login l on u.user_id = l.user_id
# where u.user_id in (select user_profil_stellensuchender.user_id from user_profil_stellensuchender)
# """
# candidate = pd.read_sql(sql, engine)
# candidate = candidate.where(candidate.notnull(), None)
# candidate['user_id'] = candidate['user_id'].apply(lambda x: str(x) if x else x)
sql = """
select cand.*
      , concat('SE',cand.user_id) as candidate_externalid
     , u_mail.reg_mail as owner
from (select u.user_id
     ,nullif(u.vorname,'') as vorname
     ,nullif(u.nachname,'') as nachname
     ,nullif(l.reg_mail,'') as reg_mail
     ,p.zuordnung_intern
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
left join user_login l on u.user_id = l.user_id
where p.profil_id is not null) cand
left join user_login u_mail on cand.zuordnung_intern = u_mail.user_id
where cand.user_id in (select user_profil_stellensuchender.user_id from user_profil_stellensuchender)
"""
candidate = pd.read_sql(sql, engine)

# %% transform
company.rename(columns={
    'firma_id': 'company-externalId',
    'firma_name1': 'company-name',
    'reg_mail': 'company-owners',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'person_id': 'contact-externalId',
    'firma_id': 'contact-companyId',
    'person_vorname': 'contact-firstName',
    'person_nachname': 'contact-lastName',
    'person_email': 'contact-email',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'id': 'position-externalId',
    'mandant': 'position-companyId',
    'person_id': 'position-contactId',
    'titel': 'position-title',
    'reg_mail': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

# candidate.rename(columns={
#     'user_id': 'candidate-externalId',
#     'vorname': 'candidate-firstName',
#     'nachname': 'candidate-lastName',
#     'reg_mail': 'candidate-email',
# }, inplace=True)
# candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)

candidate.rename(columns={
    'candidate_externalid': 'candidate-externalId',
    'vorname': 'candidate-firstName',
    'nachname': 'candidate-lastName',
    'reg_mail': 'candidate-email',
    'owner': 'candidate-owners',
}, inplace=True)
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)

# %% csv
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
