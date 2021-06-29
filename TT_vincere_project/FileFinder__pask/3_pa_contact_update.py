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
cf.read('pa_config.ini')
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

#%% mailing address
contact = pd.read_sql("""
select cont_info.*, pe.* from
(select p.idperson as contact_externalid
      , cont.idcompany as company_externalid
     , p.firstname
     , p.lastname
     , p.emailother
     , p.emailwork
     , p.phonehome
     , p.defaultphone
     , p.directlinephone
     , p.mobileprivate
     , p.urlprivate
     , p.defaulturl
     , p.jobtitle, p.createdon
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company
where companyid in ('5714'
,'5745'
,'5841'
,'4138'
,'4990'
,'1611'
,'236'
,'1494'
,'379'
,'5038'
,'650'
,'3038'
,'1046'
,'4020'
,'4063'
,'107'
,'1030'
,'2159'
,'3982'
,'2179'
,'5508'
,'757'
,'1792'
,'4634'
,'281'
,'3535'
,'4348'
,'5846'
,'5855'
,'5869'
,'5940'
,'5941'
,'5982'
,'5991'
,'5996'
,'5999'
,'6002'
,'6004'
,'6005'
,'6006'
,'6007'
,'6008'
,'6009'
,'6010'
,'6011'
,'6012'
,'6013'
,'6014'
,'6015'
,'6016'
,'6017'
,'6018'
,'6019'
,'6021'
,'6025'
,'6026'
,'6027'
,'6028'
,'6029'
,'6031'
,'6034'
,'6035'
,'6037'
,'6038'
,'6039'
,'6040'
,'6042'
,'6043'
,'6044'
,'6045'
,'6046'
,'6047'
,'6048'
,'6049'
,'6050'
,'6051'
,'6052'
,'6053'
,'6054'
,'6055'
,'6056'
,'6057'
,'6058'
,'6060'
,'6078'
,'6079'
,'6080'
,'6081'
,'6082'
,'6083'
,'6096'
,'6097'
,'6098'
,'6099'
,'6100'
,'6176'
,'6237'
,'6241')) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
where cont.rn = 1) cont_info
left join (select idperson, addressline1, addressline2, addressline3, addressline4, city, postcode, country  from person_paddress pp
left join (select paddress.*, country.value as country
from paddress
join country on country.idcountry = paddress.idcountry) pa on pp.idpaddress = pa.idpaddress) pe on cont_info.contact_externalid = pe.idperson
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
primary_phone = contact[['contact_externalid', 'defaultphone']].dropna().drop_duplicates()
primary_phone['primary_phone'] = primary_phone['defaultphone']
vcont.update_primary_phone(primary_phone, mylog)

# %% mobile phone
mobile_phone = contact[['contact_externalid', 'mobileprivate']].dropna().drop_duplicates()
mobile_phone['mobile_phone'] = mobile_phone['mobileprivate']
vcont.update_mobile_phone(mobile_phone, mylog)

# %% home phone
home_phone = contact[['contact_externalid', 'phonehome']].dropna().drop_duplicates()
home_phone['home_phone'] = home_phone['phonehome']
vcont.update_home_phone(home_phone, mylog)

# %% work email
pemail = contact[['contact_externalid', 'emailwork', 'emailother']].drop_duplicates()
pemail['personal_email'] = pemail[['emailwork', 'emailother']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
vcont.update_personal_email(pemail, mylog)

# %% method
method = contact[['contact_externalid', 'directlinephone']].dropna().drop_duplicates()
method['method_of_contact'] = 'Direct Line: ' + method['directlinephone']
vcont.update_method_contact(method, mylog)

# %% social
lk = contact[['contact_externalid', 'urlprivate']].dropna().drop_duplicates()
lk['linkedin'] = lk['urlprivate']
lk = lk.loc[lk['linkedin'].str.contains('linkedin')]
vcont.update_linkedin(lk, mylog)

# %% note
note = pd.read_sql("""
select cont_info.*, c2.*, r.* from
(select p.idperson as contact_externalid, personid
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company
where companyid in ('5714'
,'5745'
,'5841'
,'4138'
,'4990'
,'1611'
,'236'
,'1494'
,'379'
,'5038'
,'650'
,'3038'
,'1046'
,'4020'
,'4063'
,'107'
,'1030'
,'2159'
,'3982'
,'2179'
,'5508'
,'757'
,'1792'
,'4634'
,'281'
,'3535'
,'4348'
,'5846'
,'5855'
,'5869'
,'5940'
,'5941'
,'5982'
,'5991'
,'5996'
,'5999'
,'6002'
,'6004'
,'6005'
,'6006'
,'6007'
,'6008'
,'6009'
,'6010'
,'6011'
,'6012'
,'6013'
,'6014'
,'6015'
,'6016'
,'6017'
,'6018'
,'6019'
,'6021'
,'6025'
,'6026'
,'6027'
,'6028'
,'6029'
,'6031'
,'6034'
,'6035'
,'6037'
,'6038'
,'6039'
,'6040'
,'6042'
,'6043'
,'6044'
,'6045'
,'6046'
,'6047'
,'6048'
,'6049'
,'6050'
,'6051'
,'6052'
,'6053'
,'6054'
,'6055'
,'6056'
,'6057'
,'6058'
,'6060'
,'6078'
,'6079'
,'6080'
,'6081'
,'6082'
,'6083'
,'6096'
,'6097'
,'6098'
,'6099'
,'6100'
,'6176'
,'6237'
,'6241')) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
where cont.rn = 1) cont_info
left join (select idperson, processingreasonvalue, createdon, email, emailtemplate, result, errorcode, errordescription from compliancelog) c2 on cont_info.contact_externalid = c2.idperson
left join remuneration r on r.idcompany_person = contact_externalid""", engine_postgre_src)

note['compliance'] = note[['processingreasonvalue', 'createdon', 'processingreasonvalue', 'email', 'emailtemplate', 'result', 'errorcode', 'errordescription']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Processing Reason - Current', 'Created on', 'Processing Reason Value',
                                                           'Email','Template','Result','GT Error Code','GT Error Description'], x) if e[1]]), axis=1)

note1 = note[['contact_externalid','personid', 'compliance']]
note2 = note1[['contact_externalid','compliance']]
note2 = note2.groupby('contact_externalid')['compliance'].apply('\n\n'.join).reset_index()
note1 = note1.merge(note2, on='contact_externalid')
note1 = note1.drop_duplicates()

note1['note1'] = note1[['personid']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Pask ID'], x) if e[1]]), axis=1)
note1['note'] = note1['note1'] + '\n\n\n:::::: Compliance ::::::\n' + note1['compliance_y']
cp7 = vcont.update_note_2(note1, dest_db, mylog)

# %% reg date
reg_date = contact[['contact_externalid', 'createdon']]
reg_date['reg_date'] = pd.to_datetime(reg_date['createdon'])
vcont.update_reg_date(reg_date, mylog)

# %% industry
sql = """
select cont_info.* from
(select p.idperson as contact_externalid
      , p.idindustry_string_list
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company
where companyid in ('5714'
,'5745'
,'5841'
,'4138'
,'4990'
,'1611'
,'236'
,'1494'
,'379'
,'5038'
,'650'
,'3038'
,'1046'
,'4020'
,'4063'
,'107'
,'1030'
,'2159'
,'3982'
,'2179'
,'5508'
,'757'
,'1792'
,'4634'
,'281'
,'3535'
,'4348'
,'5846'
,'5855'
,'5869'
,'5940'
,'5941'
,'5982'
,'5991'
,'5996'
,'5999'
,'6002'
,'6004'
,'6005'
,'6006'
,'6007'
,'6008'
,'6009'
,'6010'
,'6011'
,'6012'
,'6013'
,'6014'
,'6015'
,'6016'
,'6017'
,'6018'
,'6019'
,'6021'
,'6025'
,'6026'
,'6027'
,'6028'
,'6029'
,'6031'
,'6034'
,'6035'
,'6037'
,'6038'
,'6039'
,'6040'
,'6042'
,'6043'
,'6044'
,'6045'
,'6046'
,'6047'
,'6048'
,'6049'
,'6050'
,'6051'
,'6052'
,'6053'
,'6054'
,'6055'
,'6056'
,'6057'
,'6058'
,'6060'
,'6078'
,'6079'
,'6080'
,'6081'
,'6082'
,'6083'
,'6096'
,'6097'
,'6098'
,'6099'
,'6100'
,'6176'
,'6237'
,'6241')) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
where cont.rn = 1) cont_info
"""
contact_industries = pd.read_sql(sql, engine_postgre_src)
industry = contact_industries.idindustry_string_list.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(contact_industries[['contact_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['contact_externalid'], value_name='idindustry_string') \
    .drop('variable', axis='columns') \
    .dropna()
industries_value = pd.read_sql("""
select idindustry, value from industry
""", engine_postgre_src)
industry_1 = industry.merge(industries_value, left_on='idindustry_string', right_on='idindustry', how='left')

industry_1['name'] = industry_1['value']
industry_1 = industry_1.drop_duplicates().dropna()
cp10 = vcont.insert_contact_industry(industry_1, mylog)



