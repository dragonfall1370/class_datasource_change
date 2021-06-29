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
cf.read('ls_config.ini')
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
      , p.createdon, t.value
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
contact1.loc[contact1['type'] == 'Other']
assert False
# %% location name/address
vcont.set_work_location_by_company_location(mylog)

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

# %% mobile phone
mobile_phone = contact[['contact_externalid', 'MobilePrivate']].dropna().drop_duplicates()
mobile_phone['mobile_phone'] = mobile_phone['MobilePrivate']
vcont.update_mobile_phone(mobile_phone, mylog)

# %% home phone
home_phone = contact[['contact_externalid', 'PhoneHome', 'PhoneHome2', 'PhoneOther']].drop_duplicates()
home_phone['home_phone'] = home_phone[['PhoneHome', 'PhoneHome2', 'PhoneOther']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
home_phone = home_phone.loc[home_phone['home_phone'] != '']
vcont.update_home_phone(home_phone, mylog)

# %% work email
pemail = contact[['contact_externalid', 'EmailPrivate','EmailPrivate2','EmailOther']].drop_duplicates()
pemail['personal_email'] = pemail[['EmailPrivate','EmailPrivate2','EmailOther']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
pemail = pemail.loc[pemail['personal_email'] != '']
vcont.update_personal_email(pemail, mylog)

# %% primary email
email = contact[['contact_externalid', 'EmailWork']].dropna().drop_duplicates()
email['email'] = email[['EmailWork']]
vcont.update_email(email, mylog)

# %% social
tem1 = contact1.loc[contact1['type'] == 'LinkedIn']
tem2 = contact1.loc[contact1['type'] == 'URL']
tem1 = tem1.dropna()
tem2 = tem2.dropna()
tem = pd.concat([tem1, tem2])

tem['linkedin'] = tem['value']
vcont.update_linkedin(tem, mylog)

# %% reg date
tem = contact[['contact_externalid', 'CreatedOn']].dropna().drop_duplicates()
tem['reg_date'] = pd.to_datetime(tem['CreatedOn'])
vcont.update_reg_date(tem, mylog)

# %% dob
tem = contact[['contact_externalid', 'DateOfBirth']].dropna().drop_duplicates()
# tem.to_csv('dob.csv')
tem = tem.loc[tem['DateOfBirth'] != '0001-01-01']
tem['date_of_birth'] = pd.to_datetime(tem['DateOfBirth'])
vcont.update_dob(tem, mylog)

# %% middle name
tem = contact[['contact_externalid', 'MiddleName']].dropna().drop_duplicates()
tem['middle_name'] = tem['MiddleName']
vcont.update_middle_name(tem, mylog)

# %% preferred name
tem = contact[['contact_externalid', 'KnownAs']].dropna().drop_duplicates()
tem['preferred_name'] = tem['KnownAs']
vcont.update_preferred_name(tem, mylog)

# %% note
cont_info = pd.read_sql("""
select p.idperson as contact_externalid
      , p.firstname
      , p.lastname
      , p.maidenname
     , ps.Value as status
      , l.Value as location
      , p.family
      , p.createdby
      , p.modifiedby
      , p.modifiedon
     , pc.value as previous_candidate
     , g.Value as gender
     , p.FromDate
     , p.ToDate
from personx p
left join previouscandidate pc on pc.idpreviouscandidate = p.idpreviouscandidate_string
left join personstatus ps on ps.idpersonstatus = p.idpersonstatus_string
left join Location l on l.idLocation = p.idLocation_String
left join Gender g on g.idGender = p.idGender_String""", engine_sqlite)

cont_salary = pd.read_sql("""
select idPerson as contact_externalid, salary.*
from (select p.idperson, cont.idCompany_Person
from personx p
join
(select cp.idperson, cp.idcompany, cp.idCompany_Person,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
left join "user" u on u.iduser = p.iduser
where cont.rn = 1
and p.isdeleted = '0') cp1
join (select r.idCompany_Person
     , r.RemunerationYear
     , r.Salary
     , r.Package
     , r.Bonus
     , r.PackageNote
     , rb.benefit
from Remuneration r
left join (SELECT idRemuneration ,group_concat(benefit) as benefit
FROM (select idRemuneration, Benefit.Value as benefit
from RemunerationBenefit
join Benefit on Benefit.idBenefit = RemunerationBenefit.idBenefit)
GROUP BY idRemuneration) rb on rb.idRemuneration = r.idRemuneration) salary on salary.idCompany_Person = cp1.idCompany_Person
""", engine_sqlite)

sql = """
select idPerson as contact_externalid, idNationality_String_List from PersonX
where idNationality_String_List is not null
"""
contact_nation = pd.read_sql(sql, engine_sqlite)

nation = contact_nation.idNationality_String_List.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(contact_nation[['contact_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['contact_externalid'], value_name='idNationality') \
    .drop('variable', axis='columns') \
    .dropna()
nation['idNationality'] = nation['idNationality'].str.lower()

nation_value = pd.read_sql("""
select idNationality, Value as nationnality from Nationality
""", engine_sqlite)
nation = nation.merge(nation_value, on='idNationality')
nation_1 = nation[['contact_externalid', 'nationnality']]
nation_1 = nation_1.groupby('contact_externalid')['nationnality'].apply(lambda x: ', '.join(x)).reset_index()

tem = contact1.loc[contact1['type'] == 'Switchboard']
tem = tem.dropna()

cont_info = cont_info.merge(cont_salary, on='contact_externalid',how ='left')
cont_info = cont_info.merge(nation_1, on='contact_externalid',how ='left')
cont_info = cont_info.merge(tem, on='contact_externalid',how ='left')
cont_info = cont_info.where(cont_info.notnull(),None)

cont_info['note'] = cont_info[['status'
    , 'MaidenName', 'nationnality', 'RemunerationYear'
    , 'Salary', 'Package', 'Bonus'
    , 'PackageNote', 'benefit', 'previous_candidate', 'gender'
    , 'Family', 'CreatedBy', 'ModifiedBy', 'ModifiedOn'
    , 'FromDate', 'ToDate', 'value']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Status', 'Maiden Name', 'Nationality', 'Salary  ▶  Year'
                                                            , 'Salary  ▶  Salary', 'Salary  ▶  Package', 'Salary  ▶  Bonus', 'Salary  ▶  Package Note', 'Salary  ▶  Benefits'
                                                            , 'Previous Candidate', 'Gender', 'Family', 'Created By', 'Modified By'
                                                            , 'Modified On', 'From Date', 'To Date', 'Switchboard Phone'], x) if e[1]]), axis=1)

cp7 = vcont.update_note_2(cont_info, dest_db, mylog)

# %% industry
sql = """
select P.idperson as contact_externalid, idIndustry_String_List
               from personx P
where isdeleted = '0'
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

industry_1 = industry.merge(industries, on='idIndustry')
industry_1['value'] = industry_1[['ind', 'sind']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

industries_csv = pd.read_csv('industries.csv')
industries_csv['matcher'] = industries_csv['Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
contact_industries = industry_1.merge(industries_csv, on='matcher')

contact_industries_2 = contact_industries[['contact_externalid','Vincere Industry','Sub Industry']].drop_duplicates()
contact_industries_2 = contact_industries_2.where(contact_industries_2.notnull(),None)
tem1 = contact_industries_2[['contact_externalid','Vincere Industry']].drop_duplicates().dropna()
tem1['name'] = tem1['Vincere Industry']
cp10 = vcont.insert_contact_industry_subindustry(tem1, mylog)

tem2 = contact_industries_2[['contact_externalid','Sub Industry']].drop_duplicates().dropna()
tem2['name'] = tem2['Sub Industry']
cp10 = vcont.insert_contact_industry_subindustry(tem2, mylog)


