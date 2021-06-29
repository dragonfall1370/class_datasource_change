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
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('theo_config.ini')
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
# assert False
# %% company
company = pd.read_sql("""
select 
com.clientCorporationID as company_externalid
, com.name as company_name
, com.dateAdded
from bullhorn1.BH_ClientCorporation com
join bullhorn1.BH_Department de on com.departmentID = de.departmentID
where com.status != 'Archive' 
and de.name = 'Theo James Recruitment Limited'
;
""", engine_mssql)
company = company.where(company.notnull(), None)
company.company_externalid = company.company_externalid.astype(str)

contact = pd.read_sql("""
select 
cont.clientId as contact_externalid
, com.company_externalid
, cont.firstName as contact_firstname
, cont.lastName as contact_lastname
, cont.middleName as contact_middlename
, cont.email as contact_email
, cont.dateAdded
from bullhorn1.Client cont
join (
	select 
	com.clientCorporationID as company_externalid
	, com.name as company_name
	, com.dateAdded
	from bullhorn1.BH_ClientCorporation com
	join bullhorn1.BH_Department de on com.departmentID = de.departmentID
	where com.status != 'Archive' 
	and de.name = 'Theo James Recruitment Limited'
) com 
	on (cont.clientCorporationID = com.company_externalid)
where cont.isDeleted <>1 and cont.status != 'Archive'
;
""", engine_mssql)
contact.contact_externalid = contact.contact_externalid.astype(str)
contact.company_externalid = contact.company_externalid.astype(str)
contact.info()

job = pd.read_sql("""
select 
job.jobPostingID as job_externalid
, cont.clientID as contact_externalid
, com.company_externalid
, job.title as job_title
, job.dateAdded
from bullhorn1.BH_JobPosting job
join (
	select 
	com.clientCorporationID as company_externalid
	, com.name as company_name
	, com.dateAdded
	from bullhorn1.BH_ClientCorporation com
	join bullhorn1.BH_Department de on com.departmentID = de.departmentID
	where com.status != 'Archive' 
	and de.name = 'Theo James Recruitment Limited'
) com on  job.clientcorporationid = com.company_externalid
left join (
	select userID, clientcorporationid, isdeleted, max(clientID) as clientID 
	from bullhorn1.BH_Client where isdeleted <> 1 and status <> 'Archive' 
	group by userID, clientcorporationid, isdeleted) cont 
	on (job.clientUserID = cont.userID and job.clientcorporationid = cont.clientcorporationid)
where job.isDeleted <> 1 and job.status != 'Archive'
;
""", engine_mssql)
# assert False
job = job.where((pd.notnull(job)), None)
job.job_externalid = job.job_externalid.astype(str)
job.contact_externalid = job.contact_externalid.map(lambda x: str(x) if x else x)
job.contact_externalid = job.contact_externalid.apply(lambda x: x.split('.')[0] if x else x)
job.company_externalid = job.company_externalid.map(lambda x: str(x) if x else x)
job.info()

candidate = pd.read_sql("""
select 
cand.candidateID as candidate_externalid
, cand.firstName as candidate_firstname
, cand.lastName as candidate_lastname
, cand.middleName as candidate_middlename
, cand.email as candidate_email
from bullhorn1.Candidate cand
where cand.isDeleted != 1 and cand.status != 'Archive'
""", engine_mssql)
candidate.candidate_externalid = candidate.candidate_externalid.astype(str)

# %% owner of candidate
cand_owner = pd.read_sql("""
select 
ca.candidateid
, ca.recruiterUserID
, ca.owneruseridlist
from bullhorn1.Candidate ca
left join bullhorn1.BH_UserContact uc on ca.recruiterUserID = uc.userID
where ca.isdeleted <> 1 and ca.status <> 'Archive'
""", engine_mssql)
cand_owner['owners'] = cand_owner[['recruiterUserID', 'owneruseridlist']].apply(lambda x: ','.join([str(e) for e in x if e]), axis='columns')

tem = cand_owner.owners \
    .map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cand_owner[['candidateid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidateid'], value_name='owner') \
    .drop('variable', axis='columns') \
    .drop_duplicates() \
    .dropna()
tem.owner = tem.owner.astype(int)
tem = tem.merge(pd.read_sql("select userid as owner, email, name from bullhorn1.BH_UserContact", engine_mssql), on='owner')
tem.loc[tem.candidateid == 100311]
tem.name.unique()

cand_owner = tem
cand_owner.candidateid = cand_owner.candidateid.astype(str)
cand_owner.to_sql(con=engine_sqlite, name='cand_owner', if_exists='replace', index=False)

# %% owner of contact
cont_owner = pd.read_sql("""
select 
userID
, concat('', recruiterUserID ) as recruiterUserID
from bullhorn1.BH_Client
union all
select 
userID
, ownerUserIDList as recruiterUserID
from bullhorn1.BH_UserContact
""", engine_mssql)
cont_owner = cont_owner.recruiterUserID.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cont_owner[['userID']], left_index=True, right_index=True) \
    .melt(id_vars=['userID'], value_name='recruiterUserID') \
    .drop('variable', axis='columns') \
    .dropna() \
    .drop_duplicates()
cont_owner = cont_owner.merge(pd.read_sql("""
select 
cont.clientID as contact_externalid
, u.userID
, u.name, u.email 
from bullhorn1.BH_UserContact u
join bullhorn1.BH_Client cont on u.userID = cont.userID
""", engine_mssql), on='userID')
cont_owner.contact_externalid = cont_owner.contact_externalid.astype(str)
cont_owner = cont_owner.loc[cont_owner.contact_externalid.isin(contact.contact_externalid)] # only get owners of selected contact
cont_owner.to_sql(con=engine_sqlite, name='cont_owner', if_exists='replace', index=False)

# %% only include some candidate whose consultants are valid
candidate = candidate.merge(cand_owner, left_on='candidate_externalid', right_on='candidateid')

# %% job owner
job_owner = pd.read_sql("""
select
j.jobPostingID as job_externalid
, cont.email
, cont.email2
, cont.email3
, cont2.email as second_owner_email 
, cont2.email2 as second_owner_email2
, cont2.email3 as second_owner_email3
from bullhorn1.BH_JobPosting j
left join bullhorn1.BH_UserContact cont on j.userID = cont.userID
left join bullhorn1.BH_UserContact cont2 on j.reportToUserID = cont2.userID
""", engine_mssql)
job_owner = job_owner.melt(id_vars=['job_externalid'], value_name='email').drop('variable', axis='columns').dropna().drop_duplicates()
job_owner = job_owner.loc[job_owner.email.str.strip() != '']
job_owner = job_owner.loc[job_owner.job_externalid.isin(job.job_externalid)]
job_owner.to_sql(con=engine_sqlite, name='job_owner', if_exists='replace', index=False)

# %% transpose
company.rename(columns={
    'company_externalid': 'company-externalId',
    'company_name': 'company-name',
    'company_owner': 'company-owners',
}, inplace=True)
company.loc[company['company-externalId'] == '4122']
company['company-name'] = company['company-name'].apply(lambda x: x.strip()).replace('','DEFAULT NAME')
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'contact_externalid': 'contact-externalId',
    'company_externalid': 'contact-companyId',
    'contact_firstname': 'contact-firstName',
    'contact_middlename': 'contact-middleName',
    'contact_lastname': 'contact-lastName',
    'contact_owner': 'contact-owners',
    'contact_email': 'contact-email',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'job_externalid': 'position-externalId',
    'company_externalid': 'position-companyId',
    'contact_externalid': 'position-contactId',
    'job_title': 'position-title',
    'job_owner': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

candidate.rename(columns={
    'candidate_externalid': 'candidate-externalId',
    'candidate_firstname': 'candidate-firstName',
    'candidate_middlename': 'candidate-middleName',
    'candidate_lastname': 'candidate-lastName',
    'candidate_email': 'candidate-email',
    'candidate_owner': 'candidate-owners',
}, inplace=True)
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)
candidate = candidate.drop_duplicates()

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


