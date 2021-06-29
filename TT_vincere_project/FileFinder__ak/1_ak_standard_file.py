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
cf.read('ak_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% data connections
conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# assert False
# %% company
company = pd.read_sql("""
select c.idcompany, c.companyname, u.useremail
from company c
left join "user" u on u.iduser = c.iduser
where c.isdeleted = '0'
""", engine_postgre_src)

contact = pd.read_sql("""
select p.idperson, p.firstname, p.lastname, p.emailprivate, cont.idcompany, u.useremail
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
left join "user" u on u.iduser = p.iduser
where cont.rn = 1
and p.isdeleted = '0'
""", engine_postgre_src)

job = pd.read_sql("""
select a.idassignment
     , a.assignmenttitle, contact.idperson, a.idcompany
     , u.useremail
from assignment a
left join (select * from
(select ac.idassignment
     , ac.idperson , comp_per.idcompany
     , ROW_NUMBER() OVER(PARTITION BY ac.idassignment ORDER BY ac.createdon DESC) rn
from assignmentcontact ac
join (select p.idperson, cont.idcompany
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
where cont.rn = 1
and p.isdeleted = '0') comp_per on ac.idperson = comp_per.idperson) cont
where cont.rn = 1) contact on contact.idassignment = a.idassignment and contact.idcompany = a.idcompany
left join "user" u on u.iduser = a.iduser
where a.isdeleted = '0'
""", engine_postgre_src)

candidate = pd.read_sql("""
select px.idperson
     , px.firstname
     , px.lastname
     , px.emailprivate
     , px.useremail
     , ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
from candidate c
join (select personx.idperson, personx.firstname, personx.lastname, emailprivate, personx.createdon, u.useremail from personx left join "user" u on u.iduser = personx.iduser where isdeleted = '0') px on c.idperson = px.idperson

""", engine_postgre_src)
# assert False
# %% transpose
company.rename(columns={
    'idcompany': 'company-externalId',
    'companyname': 'company-name',
    'useremail': 'company-owners',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'idperson': 'contact-externalId',
    'idcompany': 'contact-companyId',
    'firstname': 'contact-firstName',
    'lastname': 'contact-lastName',
    'useremail': 'contact-owners',
    'emailprivate': 'contact-email',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'idassignment': 'position-externalId',
    'idcompany': 'position-companyId',
    'idperson': 'position-contactId',
    'assignmenttitle': 'position-title',
    'useremail': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

candidate.rename(columns={
    'idperson': 'candidate-externalId',
    'firstname': 'candidate-firstName',
    'lastname': 'candidate-lastName',
    'emailprivate': 'candidate-email',
    'useremail': 'candidate-owners',
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