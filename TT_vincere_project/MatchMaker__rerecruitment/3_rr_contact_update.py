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
cf.read('rr_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_company
vcom = vincere_company.Company(connection)
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
# %% info
contact = pd.read_sql("""
select CONCAT('',peo_no) as contact_externalid
     , nullif(peo_title,'') as peo_title
     , nullif(peo_known,'') as peo_known
     , nullif(peo_cnt_position,'') as peo_cnt_position
     , nullif(peo_cnt_tel,'') as peo_cnt_tel
, nullif(peo_cnt_establish,'') as peo_cnt_establish
, nullif(peo_cnt_street,'') as peo_cnt_street
, nullif(peo_cnt_district,'') as peo_cnt_district
, nullif(peo_cnt_town,'') as peo_cnt_town
, nullif(peo_cnt_county,'') as peo_cnt_county
, nullif(peo_cnt_postcode,'') as peo_cnt_postcode
, nullif(peo_cnt_country,'') as peo_cnt_country
     , nullif(peo_branch,'') as peo_branch
     , nullif(peo_division,'') as peo_division
     , nullif(peo_cnt_comm,'') as peo_cnt_comm
     , CONCAT('',c2.cli_no) as company_externalid
from people pe
left join client c2 on pe.cli_no = c2.cli_no
where peo_flag in (2,3,6,7)
""", engine_mssql)
assert False
# %% location name/address
contact['address'] = contact[['peo_cnt_establish', 'peo_cnt_street','peo_cnt_district','peo_cnt_town','peo_cnt_county','peo_cnt_postcode','peo_cnt_country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
contact['location_name'] = contact['address']
# assign contacts's addresses to their companies
comaddr = contact[['company_externalid', 'address','location_name']].drop_duplicates()
comaddr = comaddr.loc[comaddr['company_externalid'].notnull()]
cp1 = vcom.insert_company_location(comaddr, mylog)

# %%
# assign the new addesses to contacts work location
tem2 = contact[['contact_externalid', 'company_externalid', 'address']]
cp2 = vcont.insert_contact_work_location(tem2, mylog)

# %% location
comaddr = contact[['company_externalid', 'address', 'peo_cnt_establish', 'peo_cnt_street','peo_cnt_district','peo_cnt_town','peo_cnt_county','peo_cnt_postcode','peo_cnt_country']].drop_duplicates()

# %% addr 1
tem = comaddr[['company_externalid', 'address', 'peo_cnt_establish']].dropna().drop_duplicates()
tem['address_line1'] = tem['peo_cnt_establish']
cp3 = vcom.update_location_address_line1(tem, dest_db, mylog)

# %% addr 2
tem = comaddr[['company_externalid', 'address', 'peo_cnt_street']].dropna().drop_duplicates()
tem['address_line2'] = tem['peo_cnt_street']
cp3 = vcom.update_location_address_line2(tem, dest_db, mylog)

# %% city
tem = comaddr[['company_externalid', 'address', 'peo_cnt_town']].dropna().drop_duplicates()
tem['city'] = tem['peo_cnt_town']
cp3 = vcom.update_location_city_2(tem, dest_db, mylog)

# %% postcode
tem = comaddr[['company_externalid', 'address', 'peo_cnt_postcode']].dropna().drop_duplicates()
tem['post_code'] = tem['peo_cnt_postcode']
cp4 = vcom.update_location_post_code_2(tem, dest_db, mylog)

# %% district
tem = comaddr[['company_externalid', 'address', 'peo_cnt_district']].dropna().drop_duplicates()
tem['district'] = tem['peo_cnt_district']
cp5 = vcom.update_location_district_2(tem, dest_db, mylog)

# %% state
tem = comaddr[['company_externalid', 'address', 'peo_cnt_county']].dropna().drop_duplicates()
tem['state'] = tem['peo_cnt_county']
cp5 = vcom.update_location_state_2(tem, dest_db, mylog)

# %% country
tem = comaddr[['company_externalid', 'address', 'peo_cnt_country']].dropna().drop_duplicates()
tem['country_code'] = tem['peo_cnt_country'].map(vcom.get_country_code)
tem['country'] = tem['peo_cnt_country']
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% job title
jobtitle = contact[['contact_externalid', 'peo_cnt_position']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['peo_cnt_position']
vcont.update_job_title2(jobtitle, dest_db, mylog)

# %% primary phone
tem = contact[['contact_externalid', 'peo_cnt_tel']].dropna().drop_duplicates()
tem['primary_phone'] = tem['peo_cnt_tel']
vcont.update_primary_phone(tem, mylog)

# %% mobile phone
tem = contact[['contact_externalid', 'peo_cnt_tel']].dropna().drop_duplicates()
tem['mobile_phone'] = tem['peo_cnt_tel']
vcont.update_mobile_phone(tem, mylog)

# %% preferred name
tem = contact[['contact_externalid', 'peo_known']].dropna().drop_duplicates()
tem['preferred_name'] = tem['peo_known']
vcont.update_preferred_name(tem, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = contact[['contact_externalid', 'peo_title']].dropna().drop_duplicates()
tem['gender_title'] = tem['peo_title']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
tem2 = tem[['contact_externalid','gender_title']].dropna().drop_duplicates()
cp = vcont.update_gender_title(tem2, mylog)

# %% note
note = contact[['contact_externalid','peo_branch','peo_division','peo_cnt_comm']]
note['note'] = note[['peo_branch','peo_division','peo_cnt_comm']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Office','Sector','Comments'], x) if e[1]]), axis=1)
cp7 = vcont.update_note_2(note, dest_db, mylog)

# %% unsub
# tem = pd.read_sql("""
# select cc.Client_Contact_Id, Email_Address as email from Client_Contact cc
# left join Contact_Info ci on cc.Contact_Info_Id = ci.Contact_Info_Id
# join (select Client_Contact_Id, Description
# from Client_Contact_Skill cs
# join Skill s on s.Skill_Id = cs.Skill_Id
# where Description =' 1. UNSUBSCRIBED') s on cc.Client_Contact_Id = s.Client_Contact_Id
# where Email_Address is not null
# and cc.Client_Contact_Id not in (
# 294
# ,3474
# ,5152
# ,7907
# ,10677
# ,10949
# ,15334
# ,16218
# ,18982
# ,20476
# ,21028
# ,22062
# )
# """, engine_mssql)
# tem = tem.loc[tem['email']!='UNSUBSCRIBED']
# tem = tem.loc[tem['email'].str.contains('@')]
# tem['subscribed']=0
# vcont.email_subscribe(tem, mylog)
#
# # %% delete
# tem = pd.read_sql("""
# select Client_Contact_Id from Client_Contact where Active_YN = 'N'
# and Client_Contact_Id not in (
# 294
# ,3474
# ,5152
# ,7907
# ,10677
# ,10949
# ,15334
# ,16218
# ,18982
# ,20476
# ,21028
# ,22062
# )
# """, engine_mssql)
# tem['contact_externalid'] = tem['Client_Contact_Id'].astype(str)
# tem = tem.merge(vcont.contact, on=['contact_externalid'])
# tem['deleted_timestamp'] = datetime.datetime.now()
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vcont.ddbconn, ['deleted_timestamp', ], ['id', ], 'contact', mylog)
#
# con_del = pd.read_sql("""select id, deleted_timestamp from contact c2 where company_id in (
# select id from company c1  where c1.deleted_timestamp is not null)
# and c2.deleted_timestamp is null""", connection)
# con_del['deleted_timestamp'] = datetime.datetime.now()
# vincere_custom_migration.psycopg2_bulk_update_tracking(con_del, vcont.ddbconn, ['deleted_timestamp', ], ['id', ], 'contact', mylog)
#
#
# job_del = pd.read_sql("""select id, deleted_timestamp from position_description pd where contact_id in (
#     select id from contact c where c.deleted_timestamp is not null)
# and pd.deleted_timestamp is null""", connection)
# job_del['deleted_timestamp'] = datetime.datetime.now()
# vincere_custom_migration.psycopg2_bulk_update_tracking(job_del, vcont.ddbconn, ['deleted_timestamp', ], ['id', ], 'position_description', mylog)