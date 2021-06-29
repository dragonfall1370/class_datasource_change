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
cf.read('rt_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% dest db
# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

# conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
# engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# ddbconn = engine_postgre.raw_connection()

job = pd.read_sql("""
select j.Id
     , Title
     , cont.CompanyID as company_extranlid
     , cont.ID as cont_externalid
     , p.WorkEMail as owner
from Job j
left join Person p on j.ConsultantID = p.ID
left join (select p.ID
     , p.FirstName
     , p.Surname
     , p.WorkEMail
     , o.WorkEMail as owner
     , com.CompanyID
     , com.DateFrom
from Person p
join Candidate c on p.ID = c.ID
left join Person o on c.OwnerPersonID = o.ID
left join (select * from CandidateEmployment where DateTo is null and YearTo = 999) com on p.ID = com.CandidateID
where c.IsActivated = 0
and p.IsActivated = 1
and p.IsArchived = 0
UNION
select p.ID
     , p.FirstName
     , p.Surname
     , p.WorkEMail
     , o.WorkEMail as owner
     , com.CompanyID
     , com.DateFrom
from Person p
join Candidate c on p.ID = c.ID
left join Person o on c.OwnerPersonID = o.ID
left join (select * from CandidateEmployment where DateTo is null and YearTo = 999) com on p.ID = com.CandidateID
where c.IsActivated = 1
and p.IsActivated = 1
and p.IsArchived = 0) cont on cont.ID = j.ContactID and cont.CompanyID = j.CompanyID
where j.ID not in (26
,42
,47
,57
,118
,145
,175
,176
,177
,199
,201
,202
,203
,221
,222
,226
,227
,230
,232
,233
,234
,235
,239
,246
,285
,317
,337
,340
,516)
""", engine_sqlite)


job2 = pd.read_sql("""
select j.Id
     , Title
     , j.CompanyID as company_extranlid
     , cont.ID as cont_externalid
     , p.WorkEMail as owner
from Job j
left join Person p on j.ConsultantID = p.ID
left join (select p.ID
     , p.FirstName
     , p.Surname
     , p.WorkEMail
     , o.WorkEMail as owner
     , com.CompanyID
     , com.DateFrom
from Person p
join Candidate c on p.ID = c.ID
left join Person o on c.OwnerPersonID = o.ID
left join (select * from CandidateEmployment where DateTo is null and YearTo = 999) com on p.ID = com.CandidateID
where c.IsActivated = 0
and p.IsActivated = 1
and p.IsArchived = 0
UNION
select p.ID
     , p.FirstName
     , p.Surname
     , p.WorkEMail
     , o.WorkEMail as owner
     , com.CompanyID
     , com.DateFrom
from Person p
join Candidate c on p.ID = c.ID
left join Person o on c.OwnerPersonID = o.ID
left join (select * from CandidateEmployment where DateTo is null and YearTo = 999) com on p.ID = com.CandidateID
where c.IsActivated = 1
and p.IsActivated = 1
and p.IsArchived = 0) cont on cont.ID = j.ContactID and cont.CompanyID = j.CompanyID
where j.ID not in (26
,42
,47
,57
,118
,145
,175
,176
,177
,199
,201
,202
,203
,221
,222
,226
,227
,230
,232
,233
,234
,235
,239
,246
,285
,317
,337
,340
,516)
""", engine_sqlite)
job
job2

job_match = job.merge(job2[['ID','Title','company_extranlid','cont_externalid']], on=['ID','Title','company_extranlid','cont_externalid'])

job_not_match = job2.loc[~job2['ID'].isin(job_match['ID'])]

job_not_match.to_csv('job_add_more.csv')
job_not_match.rename(columns={
    'ID': 'position-externalId',
    'company_extranlid': 'position-companyId',
    'cont_externalid': 'position-contactId',
    'Title': 'position-title',
    'owner': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job_not_match, mylog)



job.to_csv(os.path.join(standard_file_upload, '5_job_more.csv'), index=False)
if len(default_contacts):
    default_contacts.to_csv(os.path.join(standard_file_upload, '3_default_contacts_more.csv'), index=False)
