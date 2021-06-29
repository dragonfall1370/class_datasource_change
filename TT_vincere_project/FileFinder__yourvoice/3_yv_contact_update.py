# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import sqlalchemy
import datetime
from functools import reduce
from common import vincere_job_application
import pandas as pd

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('yv_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
# src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
# conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_contact
vcont = vincere_contact.Contact(connection)

from common import vincere_company
vcom = vincere_company.Company(connection)
# %% info
contact = pd.read_sql("""
select p.idperson as contact_externalid
      , p.firstname
      , p.lastname
      , p.middlename
      , p.knownas
      , p.idtitle_string
      , p.emailother
      , p.emailwork
      , p.phonehome, p.phonehome2, p.phoneother, DefaultFax
      , p.defaultphone
      , p.idlocation_string
      , p.dateofbirth
      , p.idLanguage_String_List
      , p.directlinephone
      , p.mobileprivate
      , p.urlprivate
      , p.defaulturl
      , p.jobtitle, p.EmailPrivate, p.EmailPrivate2
from personx p
left join title t on t.idtitle = p.idtitle_string
""", engine_sqlite)

contact1 = pd.read_sql("""
select idPerson as contact_externalid, ea.CommValue as value, cct.Value as type
from Person_EAddress pe
left join PersonCommunicationType cct on cct.idPersonCommunicationType = pe.idPersonCommunicationType
left join EAddress ea on ea.idEAddress = pe.idEAddress
""", engine_sqlite)
contact1['type'].unique()
assert False
# %% location name/address
location = pd.read_sql("""
select p.idperson as contact_externalid
, cont.idcompany as company_externalid
, l.Value as location
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
left join "user" u on u.iduser = p.iduser
left join personstatus ps on ps.idpersonstatus = p.idpersonstatus_string
left join Location l on p.idLocation_String = l.idLocation
where cont.rn = 1
and p.isdeleted = '0'
and ps.Value in ('CLIENT - CLIENT','Prospect - commercial')
""", engine_sqlite)
location = location.dropna()
location['address'] = location['location']
location['location_name'] = location['address']
cp1 = vcom.insert_company_location(location, mylog)
cp2 = vcont.insert_contact_work_location(location, mylog)

# %% location name/address mailing
location = pd.read_sql("""
select idPerson as contact_externalid, AddressDefaultFull
from personx p
where AddressDefaultFull is not null
""", engine_sqlite)
location['AddressDefaultFull'] = location['AddressDefaultFull'].apply(lambda x: x.replace('\\x0d\\x0a',', '))
location['address'] = location['AddressDefaultFull']
vcont.update_personal_location_address_2(location, dest_db, mylog)

# %% location name/address
# vcont.set_work_location_by_company_location(mylog)

# %% job title
jobtitle = contact[['contact_externalid', 'JobTitle']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['JobTitle']
vcont.update_job_title2(jobtitle, dest_db, mylog)

# %% primary phone
tem = contact1.loc[contact1['type'] == 'Business']
tem = tem.dropna()
tem['primary_phone'] = tem['value']
vcont.update_primary_phone(tem, mylog)

# %% switchboard phone
tem = contact1.loc[contact1['type'] == 'Switchboard']
tem = tem.dropna()
tem['switchboard_phone'] = tem['value']
vcont.update_switchboard_phone(tem, mylog)

# %% switchboard phone
# tem = contact1.loc[contact1['type'] == 'Direct Line']
# tem = tem.dropna()
# tem['switchboard_phone'] = tem['value']
# vcont.update_switchboard_phone(tem, mylog)

# %% mobile phone
mobile_phone = contact[['contact_externalid', 'MobilePrivate']].dropna().drop_duplicates()
mobile_phone['mobile_phone'] = mobile_phone['MobilePrivate']
vcont.update_mobile_phone(mobile_phone, mylog)

# %% home phone
home_phone = contact[['contact_externalid', 'PhoneHome', 'PhoneHome2']].drop_duplicates()
home_phone['home_phone'] = home_phone[['PhoneHome', 'PhoneHome2']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
home_phone = home_phone.loc[home_phone['home_phone'] != '']
vcont.update_home_phone(home_phone, mylog)

# %% work email
pemail = contact[['contact_externalid', 'EmailPrivate','EmailPrivate2']].drop_duplicates()
pemail['personal_email'] = pemail[['EmailPrivate','EmailPrivate2']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
pemail = pemail.loc[pemail['personal_email'] != '']
vcont.update_personal_email(pemail, mylog)

# %% primary email
email = contact[['contact_externalid', 'EmailWork']].dropna().drop_duplicates()
email['email'] = email[['EmailWork']]
vcont.update_email(email, mylog)

# %% social
tem = contact1.loc[contact1['type'] == 'LinkedIn']
tem['linkedin'] = tem['value']
vcont.update_linkedin(tem, mylog)

# %% social
st = pd.read_sql("""
select idPerson as contact_externalid, FromDate as start_date
from personx p
where FromDate is not null
""", engine_sqlite)
st['start_date'] = pd.to_datetime(st['start_date'])
vcont.update_start_date(st, mylog)

# %% reg date
# tem = contact[['contact_externalid', 'CreatedOn']].dropna().drop_duplicates()
# tem['reg_date'] = pd.to_datetime(tem['CreatedOn'])
# vcont.update_reg_date(tem, mylog)

# %% dob
# tem = contact[['contact_externalid', 'DateOfBirth']].dropna().drop_duplicates()
# # tem.to_csv('dob.csv')
# tem = tem.loc[tem['DateOfBirth'] != '0001-01-01']
# tem['date_of_birth'] = pd.to_datetime(tem['DateOfBirth'])
# vcont.update_dob(tem, mylog)

# %% middle name
# tem = contact[['contact_externalid', 'MiddleName']].dropna().drop_duplicates()
# tem['middle_name'] = tem['MiddleName']
# vcont.update_middle_name(tem, mylog)

# %% preferred name
# tem = contact[['contact_externalid', 'KnownAs']].dropna().drop_duplicates()
# tem['preferred_name'] = tem['KnownAs']
# vcont.update_preferred_name(tem, mylog)

# %% note
cont_info = pd.read_sql("""
select p.idperson as contact_externalid
      , p.firstname
      , p.lastname
      , p.PersonId
      , AlertText
      , Note
      , PersonComment
      , t.Value as title
      , EmailOther
      , MobileOther
      , PhoneOther
      , ToDate
from personx p
left join title t on t.idtitle = p.idtitle_string
""", engine_sqlite)
cont_info['PersonId'] = cont_info['PersonId'].astype(str)
cont_info['PersonComment'] = cont_info['PersonComment'].apply(lambda x: x.replace('\\x0d','\r') if x else x)
cont_info['PersonComment'] = cont_info['PersonComment'].apply(lambda x: x.replace('\\x0a','\n') if x else x)
cont_info['Note'] = cont_info['Note'].apply(lambda x: x.replace('\\x0d','\r') if x else x)
cont_info['Note'] = cont_info['Note'].apply(lambda x: x.replace('\\x0a','\n') if x else x)
cont_info['AlertText'] = cont_info['AlertText'].apply(lambda x: x.replace('\\x0d','\r') if x else x)
cont_info['AlertText'] = cont_info['AlertText'].apply(lambda x: x.replace('\\x0a','\n') if x else x)

rela = pd.read_sql("""
select pr.idPerson as contact_externalid
, p1.FullName
, p2.FullName as related
, RelationDescription
, Notes
from PersonRelation pr
left join PersonX p1 on pr.idPerson = p1.idPerson
left join PersonX p2 on pr.idPerson1 = p2.idPerson
""", engine_sqlite)
rela['Notes'] = rela['Notes'].apply(lambda x: x.replace('\\x0d','\r') if x else x)
rela['Notes'] = rela['Notes'].apply(lambda x: x.replace('\\x0a','\n') if x else x)
rela['rela'] = rela[['FullName', 'RelationDescription', 'related', 'Notes']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Full Name', 'Relationship', 'Related Person\'s Name', 'Note'], x) if e[1]]), axis=1)
rela1 = rela[['contact_externalid','rela']]
rela = rela1.groupby('contact_externalid')['rela'].apply('\n\n'.join).reset_index()
rela['rela'] = '---Relationships/Groups---\n\n'+rela['rela']

compl = pd.read_sql("""
select idPerson as contact_externalid
, pr.Value as ProcessingReason
, ProcessingReasonValue
, ReasonLog
, Email
, EmailTemplate
, Result
, ErrorCode
, ErrorDescription
from ComplianceLog cl
left join ProcessingReason pr on cl.idProcessingReason = pr.idProcessingReason
""", engine_sqlite)
compl['ReasonLog'] = compl['ReasonLog'].apply(lambda x: x.replace('\\x0d','\r') if x else x)
compl['ReasonLog'] = compl['ReasonLog'].apply(lambda x: x.replace('\\x0a','\n') if x else x)
compl['compl'] = compl[['ProcessingReason', 'ProcessingReasonValue', 'ReasonLog', 'Email','EmailTemplate','Result','ErrorCode','ErrorDescription']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['ProcessingReason', 'ProcessingReasonValue', 'ReasonLog', 'Email','EmailTemplate','Result','ErrorCode','ErrorDescription'], x) if e[1]]), axis=1)
compl1 = compl[['contact_externalid','compl']]
# compl1['rn'] = compl1.groupby('contact_externalid').cumcount()
# compl1.loc[compl1['rn']>0]
compl = compl1.groupby('contact_externalid')['compl'].apply('\n\n'.join).reset_index()
compl['compl'] = '---Compliance---\n\n'+compl['compl']

cont_info = cont_info.merge(rela, on='contact_externalid',how ='left')
cont_info = cont_info.merge(compl, on='contact_externalid',how ='left')
cont_info = cont_info.where(cont_info.notnull(),None)

cont_info['note'] = cont_info[['PersonId'
    , 'AlertText', 'Note', 'PersonComment'
    , 'title', 'EmailOther', 'MobileOther'
    , 'PhoneOther', 'ToDate', 'rela', 'compl']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Person Id'
    , 'Memo Text', 'Notes', 'Internal Comment'
    , 'Title', 'Email Other', 'Mobile Other'
    , 'Phone Other', 'Date To', '', ''], x) if e[1]]), axis=1)

cp7 = vcont.update_note_2(cont_info, dest_db, mylog)

# %% industry
sql = """
select P.idperson as contact_externalid, idIndustry_String_List
               from personx P
where isdeleted = '0' and idindustry_string_list is not null
"""
contact_industries = pd.read_sql(sql, engine_sqlite)
contact_industries = contact_industries.dropna()

industry = contact_industries.idIndustry_String_List.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(contact_industries[['contact_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['contact_externalid'], value_name='idIndustry') \
    .drop('variable', axis='columns') \
    .dropna()
industry['idIndustry'] = industry['idIndustry'].str.lower()

industries = pd.read_sql("""
select i1.idIndustry, i2.Value as ind, i1.Value as sind
from Industry i1
left join Industry i2 on i1.ParentId = i2.idIndustry
""", engine_sqlite)
industries['idIndustry'] = industries['idIndustry'].str.lower()

contact_industries = industry.merge(industries, on='idIndustry')
contact_industries.loc[contact_industries['contact_externalid']=='7bdb6e33-390d-4e94-ab3f-072f612e95ef']
tem1 = contact_industries[['contact_externalid','ind']].drop_duplicates().dropna()
tem1['name'] = tem1['ind']
cp10 = vcont.insert_contact_industry_subindustry(tem1, mylog)

tem2 = contact_industries[['contact_externalid','sind']].drop_duplicates().dropna()
tem2['name'] = tem2['sind']
cp10 = vcont.insert_contact_industry_subindustry(tem2, mylog)

# %% update board
board = pd.read_sql("""select p.idperson as contact_externalid
      , ps.Value as pstatus
from personx p
join personstatus ps on ps.idpersonstatus = p.idpersonstatus_string
and ps.Value in ('CLIENT - CLIENT','Prospect - commercial')""", engine_sqlite)
board.loc[board['pstatus'] == 'CLIENT - CLIENT', 'board'] = 4
board.loc[board['pstatus'] == 'Prospect - commercial', 'board'] = 2
board['status'] = 1
vcont.update_status_board(board, mylog)

# %% distribution list
dlist = pd.read_sql("""
select
       idPerson as contact_externalid
     , CampaignTitle as name 
     , u.useremail as owner
from CampaignContact cc
left join Campaign c on cc.idCampaign = c.idCampaign
left join "user" u on u.fullname = cc.createdby
""",engine_sqlite)
dlist = dlist.drop_duplicates()
dlist.loc[dlist['contact_externalid']=='cefb218f-fd0f-4fbe-94e8-8fc239c4c4d1']
tem = dlist[['name','owner']]
tem['owner'] = tem['owner'].fillna(' ')
tem = tem.groupby('name')['owner'].apply(','.join).reset_index()
tem['owner'] = tem['owner'].apply(lambda x: x.split(',')[0])
vcont.create_distribution_list(tem,mylog)
dlist['group_name'] = dlist['name']
vcont.add_contact_distribution_list(dlist,mylog)