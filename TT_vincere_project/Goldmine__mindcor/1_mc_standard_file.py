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
cf.read('mc_config.ini')
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

job = pd.read_sql("""
select OPID, j.NAME, j.COMPANY, j.ACCOUNTNO, j.CONTACT, cont.*, u.EMC_ACC_EMAILS, com.ID from OPMGR j
join company com on com.COMPANY = j.COMPANY
left join (select c1.ACCOUNTNO as ACCOUNTNO_2, c1.CONTACT as CONTACT_2, c.COMPANY as COMPANY_2 from CONTACT1 c1
left join company c on c.COMPANY = c1.COMPANY
where KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'BANKING'
,'CANDIDATE / CLIENT'
,'CANDIDATE/CLIENT'
,'CLIENT'
,'CLIENT /'
,'CLIENT/CANDIDATE'
,'CLIENT/EXTRAORDINARY'
,'COMPETITOR'
,'CONSULTING CLIENT'
,'CONTRACTOR'
,'EXTRAORDINARY CLIEN'
,'EXTRAORDINARY CLIENT'
,'FINANCE'
,'HOTEL'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'POSSIBLE BUSINESS PA'
,'POTENTIAL AQUISITION'
,'POTENTIAL BUSINESS P'
,'POTENTIAL CLIENT'
,'SOURCE'
,'SOURCING')) cont on cont.ACCOUNTNO_2 = j.ACCOUNTNO and cont.COMPANY_2 = j.COMPANY
left join USERS u on u.USERNAME = j.USERID
where RECTYPE = 'O  '
and com.COMPANY is not null
""", engine_mssql)
job1 = job.loc[job['ACCOUNTNO_2'].notnull()]
job2 = job.loc[job['ACCOUNTNO_2'].isnull()]
job2['ACCOUNTNO'] = job2['ACCOUNTNO']+'_Mindcor_'+job2['ID'].astype(str)
job2.loc[job2['ACCOUNTNO']=='B2112753882(RGPC8Ali_Mindcor_9037a890-6515-4ffd-b217-9c2eadf186fe']
cont_dup = job2[['ACCOUNTNO','CONTACT','ID']]
cont_dup = cont_dup.drop_duplicates()
cont_dup['CONTACT'] = cont_dup['CONTACT'].apply(lambda x: x.strip())
cont_dup['contact-firstName'] = cont_dup['CONTACT'].apply(lambda x: x.split(' ')[0])
cont_dup['LASTNAME'] = [a.replace(b, '').strip() for a, b in zip(cont_dup['CONTACT'], cont_dup['contact-firstName'])]
# cont_dup['ACCOUNTNO'] = cont_dup['ACCOUNTNO']+'_Mindcor_'+cont_dup['ID'].astype(str)
cont_dup['EMC_ACC_EMAILS'] = None #773
cont_dup.loc[cont_dup['ACCOUNTNO']=='B3022559731)#&=85Dan_Mindcor_922936b5-9949-48ad-8b51-d33f128e7dc0']

job = pd.concat([job1[['OPID','NAME','ACCOUNTNO','ID','EMC_ACC_EMAILS']],job2[['OPID','NAME','ACCOUNTNO','ID','EMC_ACC_EMAILS']]])


contact = pd.read_sql("""
select c1.ACCOUNTNO, c1.CONTACT, trim(LASTNAME) as LASTNAME,c.ID, u.EMC_ACC_EMAILS from CONTACT1 c1
left join company c on c.COMPANY = c1.COMPANY
left join USERS u on u.USERNAME = c1.CREATEBY
where KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'BANKING'
,'CANDIDATE / CLIENT'
,'CANDIDATE/CLIENT'
,'CLIENT'
,'CLIENT /'
,'CLIENT/CANDIDATE'
,'CLIENT/EXTRAORDINARY'
,'COMPETITOR'
,'CONSULTING CLIENT'
,'CONTRACTOR'
,'EXTRAORDINARY CLIEN'
,'EXTRAORDINARY CLIENT'
,'FINANCE'
,'HOTEL'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'POSSIBLE BUSINESS PA'
,'POTENTIAL AQUISITION'
,'POTENTIAL BUSINESS P'
,'POTENTIAL CLIENT'
,'SOURCE'
,'SOURCING')
""", engine_mssql)
tem = contact.loc[contact['CONTACT'].notnull()]

tem1 = contact.loc[contact['CONTACT'].isnull()]
tem1['contact-firstName'] = None

tem2 = tem.loc[tem['LASTNAME'].isnull()]
tem2['contact-firstName'] = None

tem3 = tem.loc[tem['LASTNAME'].notnull()]
tem3['contact-firstName'] = [a.replace(b, '').strip() for a, b in zip(tem3['CONTACT'], tem3['LASTNAME'])]

contact = pd.concat([tem1, tem2, tem3,cont_dup]) #10516+773 11289

candidate = pd.read_sql("""
select c1.ACCOUNTNO, c1.CONTACT, LASTNAME, u.EMC_ACC_EMAILS from CONTACT1 c1
left join USERS u on u.USERNAME = c1.CREATEBY
where KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'CAN'
,'CAND'
,'CANDIADATE'
,'CANDIADTE'
,'CANDIDATE'
,'CANDIDATE -'
,'CANDIDATE / CLIENT'
,'CANDIDATE FOR MINDCO'
,'CANDIDATE/CLIENT'
,'CANDIDATES'
,'CANDIDIDATE'
,'CLIENT/CANDIDATE'
,'CONTRACTOR'
,'EXTRAORDINARY CANDI'
,'EXTRAORDINARY CANDID'
,'FINANCE'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'NOT ON THE MARKET'
,'OUTSOURCED RESEARCHE'
,'P'
,'PLACED CANDIDATE'
,'POTENTIAL CANDIDATE'
,'PRIVATE'
,'REFEREE'
,'SOURCE'
,'SOURCING'
,'USEFUL NUMBERS'
,'VERONICA'
,'YCANDIDATE')
""", engine_mssql) #59359
tem = candidate.loc[candidate['CONTACT'].notnull()]

tem1 = candidate.loc[candidate['CONTACT'].isnull()]
tem1['candidate-firstName'] = None

tem2 = tem.loc[tem['LASTNAME'].isnull()]
tem2['candidate-firstName'] = None

tem3 = tem.loc[tem['LASTNAME'].notnull()]
tem3['candidate-firstName'] = [a.replace(b, '').strip() for a, b in zip(tem3['CONTACT'], tem3['LASTNAME'])]

candidate = pd.concat([tem1, tem2, tem3])
assert False
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

job.rename(columns={
    'OPID': 'position-externalId',
    'ID': 'position-companyId',
    'ACCOUNTNO': 'position-contactId',
    'NAME': 'position-title',
    'EMC_ACC_EMAILS': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

candidate.rename(columns={
    'ACCOUNTNO': 'candidate-externalId',
    # 'Forename': 'candidate-firstName',
    'LASTNAME': 'candidate-lastName',
    # 'Middle': 'candidate-middleName',
    # 'Email_Address': 'candidate-email',
    'EMC_ACC_EMAILS': 'candidate-owners',
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

# %% to csv files
# cand = pd.read_csv(os.path.join(standard_file_upload, '6_candidate.csv'))
# cand['candidate-email'] = 'default_email_'+cand['candidate-externalId']+'@noemail.com'
# cand.to_csv(os.path.join(standard_file_upload, '6_candidate.csv'), index=False)