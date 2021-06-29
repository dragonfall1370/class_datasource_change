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
cf.read('ak_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_contact
vcont = vincere_contact.Contact(connection)

from common import vincere_company
vcom = vincere_company.Company(connection)
# from common import parse_gender_title

#%% contact
contact = pd.read_sql("""
select cont_info.*, pe.*, t.value as title from
(select p.idperson as contact_externalid
      , cont.idcompany as company_externalid
      , p.firstname
      , p.lastname
      , p.middlename
      , p.knownas
      , p.idtitle_string
      , p.nationalityvalue_string
      , p.emailother
      , p.emailwork
      , p.phonehome, p.phonehome2, p.phoneother
      , p.defaultphone
      , p.idlocation_string
      , p.dateofbirth
      , p.qualificationvalue_string
      , p.directlinephone
      , p.mobileprivate
      , p.urlprivate
      , p.defaulturl
      , p.jobtitle
      , p.createdon
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
where cont.rn = 1) cont_info
left join (select idperson, addressline1, addressline2, addressline3, addressline4, city, postcode, country  from person_paddress pp
left join (select paddress.*, country.value as country
from paddress
join country on country.idcountry = paddress.idcountry) pa on pp.idpaddress = pa.idpaddress) pe on cont_info.contact_externalid = pe.idperson

left join title t on t.idtitle = cont_info.idtitle_string
""", engine_postgre_src)
assert False
# %% location name/address
contact['location_name'] = contact[['addressline1', 'addressline2', 'addressline3', 'addressline4', 'city','postcode','country']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
contact['address'] = contact.location_name

# %%
# assign contacts's addresses to their companies
comaddr = contact[['company_externalid', 'address']].drop_duplicates()
cp1 = vcom.insert_company_location(comaddr, mylog)

# %%
# assign the new addesses to contacts work location
tem2 = contact[['contact_externalid', 'company_externalid', 'address']]
cp2 = vcont.insert_contact_work_location(tem2, mylog)

# %% job title
jobtitle = contact[['contact_externalid', 'jobtitle']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['jobtitle']
vcont.update_job_title2(jobtitle, dest_db, mylog)

# %% primary phone
primary_phone = contact[['contact_externalid', 'defaultphone', 'directlinephone']].dropna().drop_duplicates()
primary_phone['primary_phone'] = primary_phone[['defaultphone', 'directlinephone']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
vcont.update_primary_phone(primary_phone, mylog)

# %% mobile phone
mobile_phone = contact[['contact_externalid', 'mobileprivate']].dropna().drop_duplicates()
mobile_phone['mobile_phone'] = mobile_phone['mobileprivate']
vcont.update_mobile_phone(mobile_phone, mylog)

# %% home phone
home_phone = contact[['contact_externalid', 'phonehome', 'phonehome2', 'phoneother']].drop_duplicates()
home_phone['home_phone'] = home_phone[['phonehome', 'phonehome2', 'phoneother']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
home_phone = home_phone.loc[home_phone['home_phone'] != '']
vcont.update_home_phone(home_phone, mylog)

# %% work email
pemail = contact[['contact_externalid', 'emailwork', 'emailother']].drop_duplicates()
pemail['personal_email'] = pemail[['emailwork', 'emailother']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
pemail = pemail.loc[pemail['personal_email'] != '']
vcont.update_personal_email(pemail, mylog)

# %% social
lk = contact[['contact_externalid', 'urlprivate']].dropna().drop_duplicates()
lk['linkedin'] = lk['urlprivate']
lk = lk.loc[lk['linkedin'].str.contains('linkedin')]
vcont.update_linkedin(lk, mylog)

# %% reg date
tem = contact[['contact_externalid', 'createdon']].dropna().drop_duplicates()
tem['reg_date'] = pd.to_datetime(tem['createdon'])
vcont.update_reg_date(tem, mylog)

# %% dob
tem = contact[['contact_externalid', 'dateofbirth']].dropna().drop_duplicates()
tem['date_of_birth'] = pd.to_datetime(tem['dateofbirth'])
vcont.update_dob(tem, mylog)

# %% middle name
tem = contact[['contact_externalid', 'middlename']].dropna().drop_duplicates()
tem['middle_name'] = tem['middlename']
vcont.update_middle_name(tem, mylog)

# %% preferred name
tem = contact[['contact_externalid', 'knownas']].dropna().drop_duplicates()
tem['preferred_name'] = tem['knownas']
vcont.update_preferred_name(tem, mylog)

# %% note
note = pd.read_sql("""
select cont_info.*
     , po.*
     , pc.value as previous_candidate
     , rl.value as relocate
     , pr.value as rating
     , ms.value as marital
     , ctr.*, ps.value as status
from
(select p.idperson as contact_externalid
      , cont.*
      , p.firstname
      , p.lastname
      , p.personreference
      , p.idpersonstatus_string
      , p.maidenname
      , p.isofflimit
      , p.idpreviouscandidate_string
      , p.idrelocate_string
      , p.idpersonrating_string
      , p.originaltraining
      , p.biography
      , p.personcomment
      , p.note as cont_note
      , p.isdateofbirthestimated
      , p.idmaritalstatus_string
      , p.family
      , p.createdby
      , p.modifiedby
      , p.modifiedon
      , p.internationalvalue_string
      , p.languagevalue_string
from personx p
join
(select cp.idperson as id_1, cp.idcompany, cp.employmentassistant, cp.checkedby, cp.checkedon,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.id_1 = p.idperson
where cont.rn = 1) cont_info
left join (select pol.idperson as id_3, pol.isactive, offlimitdatefrom, offlimitdateto, offlimitnote, olt.value as offlimittype
from personofflimit pol
left join offlimittype olt on pol.idofflimittype = olt.idofflimittype) po on po.id_3 = cont_info.contact_externalid
left join previouscandidate pc on pc.idpreviouscandidate = cont_info.idpreviouscandidate_string
left join relocate rl on rl.idrelocate = cont_info.idrelocate_string
left join personrating pr on pr.idpersonrating = cont_info.idpersonrating_string
left join maritalstatus ms on ms.idmaritalstatus = cont_info.idmaritalstatus_string
left join (select idperson as id_2
                , contractjobtitle
                , contractreference
                , createdon
                , contractstartdate
                , estimatedcontractenddate
                , contracthoursperday from contract) ctr on ctr.id_2 = cont_info.contact_externalid
left join personstatus ps on ps.idpersonstatus = cont_info.idpersonstatus_string""", engine_postgre_src)

note['biography'] = note['biography'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
note['personcomment'] = note['personcomment'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
note['cont_note'] = note['cont_note'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
note['offlimitnote'] = note['offlimitnote'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
note.loc[note['isofflimit'] == '0', 'isofflimit'] = 'No'
note.loc[note['isofflimit'] == '1', 'isofflimit'] = 'Yes'
note.loc[note['isactive'] == '0', 'isactive'] = 'No'
note.loc[note['isactive'] == '1', 'isactive'] = 'Yes'
note.loc[note['isdateofbirthestimated'] == '0', 'isdateofbirthestimated'] = 'No'
note.loc[note['isdateofbirthestimated'] == '1', 'isdateofbirthestimated'] = 'Yes'


note['note'] = note[['personreference'
    , 'status'
    , 'maidenname', 'isofflimit', 'isactive'
    , 'offlimittype', 'offlimitdatefrom', 'offlimitdateto'
    , 'offlimitnote', 'previous_candidate', 'relocate', 'rating'
    , 'originaltraining', 'personcomment', 'biography', 'cont_note'
    , 'isdateofbirthestimated', 'marital', 'family', 'createdby', 'modifiedby', 'modifiedon'
    , 'employmentassistant', 'checkedon', 'checkedby', 'internationalvalue_string', 'languagevalue_string'
    , 'contractjobtitle', 'contractreference', 'createdon', 'contractstartdate', 'estimatedcontractenddate', 'contracthoursperday']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Reference'
                                                            , 'Status', 'Maiden Name', 'Off Limits', ' Off Limits Active'
                                                            , 'Off Limits Type', 'Off Limits Date From', 'Off Limits Date To', 'Off Limits Note'
                                                            , 'Previous Candidate', 'Relocate', 'Rating', 'Original Training', 'Internal Comment'
                                                            , 'Biography', 'Notes', 'Is DOB Estimated', 'Marital Status', 'Family', 'Created By', 'Modified By'
                                                            , 'Modified On', 'Assistant', 'Checked On', 'Checked By', 'International', 'Language'
                                                            , 'Contract Job Title', 'Contract Reference', 'Contract Created On', 'Contract Start Date', 'Contract End Date', 'Contract Hours Per Day'], x) if e[1]]), axis=1)

cp7 = vcont.update_note_2(note, dest_db, mylog)

# %% reg date
reg_date = contact[['contact_externalid', 'createdon']]
reg_date['reg_date'] = pd.to_datetime(reg_date['createdon'])
vcont.update_reg_date(reg_date, mylog)

# %% industry
sql = """
select cont_info.*from
(select p.idperson as contact_externalid
      , p.idindustry_string_list
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
where cont.rn = 1) cont_info
"""
contact_industries = pd.read_sql(sql, engine_postgre_src)
contact_industries = contact_industries.dropna()
industry = contact_industries.idindustry_string_list.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(contact_industries[['contact_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['contact_externalid'], value_name='idindustry') \
    .drop('variable', axis='columns') \
    .dropna()
industry['idindustry'] = industry['idindustry'].str.lower()

industries_value = pd.read_sql("""
select idindustry, value from industry
""", engine_postgre_src)
industry_1 = industry.merge(industries_value, on='idindustry')
industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
industries_csv = pd.read_csv('industries.csv')
industries_csv['matcher'] = industries_csv['INDUSTRY'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
contact_industries = industry_1.merge(industries_csv, on='matcher')

contact_industries['name'] = contact_industries['INDUSTRY']
contact_industries = contact_industries.drop_duplicates().dropna()
cp10 = vcont.insert_contact_industry(contact_industries, mylog)



