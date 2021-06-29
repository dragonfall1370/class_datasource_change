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
from datetime import datetime

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
# %%
company = pd.read_sql("""
select
 CONCAT('',cl.cli_no) as company_externalid
        ,cli_name
        ,CONCAT_WS(', '
                ,nullif(cl.cli_establish,'')
                ,nullif(cl.cli_street,'')
                ,nullif(cl.cli_district,'')
                ,nullif(cl.cli_town,'')
                ,nullif(cl.cli_county,'')
                ,nullif(cl.cli_postcode,'')
                ,nullif(cl.cli_country,'')
        ) as address
        ,CONCAT_WS(', '
                ,nullif(cl.cli_establish,'')
                ,nullif(cl.cli_street,'')
                ,nullif(cl.cli_district,'')
                ,nullif(cl.cli_town,'')
                ,nullif(cl.cli_county,'')
                ,nullif(cl.cli_postcode,'')
                ,nullif(cl.cli_country,'')
        ) as location_name
        ,nullif(cl.cli_establish,'') as address_line1
        ,nullif(cl.cli_street,'') as address_line2
        ,nullif(cl.cli_town,'') as city
        ,nullif(cl.cli_district,'') as district
        ,nullif(cl.cli_county,'') as state
        ,nullif(cl.cli_postcode,'') as post_code
        ,case
                when cl.cli_country='UK' then	'GB'
                when cl.cli_country='England' then	'GB'
                when cl.cli_country='United Kingdom' then	'GB'
                when cl.cli_country='Ireland' then	'IE'
                when cl.cli_country='South Africa' then	'ZA'
                when cl.cli_country='Cambodia' then	'KH'
                when cl.cli_country='Gabon' then	'GA'
                when cl.cli_country='Wales' then	'GB'
                when cl.cli_country='united kingdon' then	'GB'
                when cl.cli_country='Uganda' then	'UG'
                ELSE 'GB'
        end as country_code
        ,nullif(cl.cli_tel,'') as phone
        ,nullif(cl.cli_tel,'') as switch_board
        ,nullif(cl.cli_fax,'') as fax
        ,nullif(cl.cli_www,'') as website
     , nullif(cl.cli_last_contact,'') as cli_last_contact
     , nullif(cl.cli_reg_date,'') as cli_reg_date
     , nullif(cl.cli_status,'') as cli_status
     , nullif(cl.cli_company_reg_no,'') as cli_company_reg_no
     , nullif(cln.cli_nob,'') as cli_nob
     , nullif(cli_comments,'') as cli_comments

        ,CONCAT_WS(char(10)
                ,nullif(cl.cli_payment_terms,'')
                ,COALESCE('Credit limit: '+convert(varchar,nullif(cl.cli_credit_limit,'')),NULL)
                ,COALESCE('Currency: '+nullif(cl.cli_currency,''),NULL)
        ) as company_payment_term
from client cl
left join dbo.cli_nob cln on cln.cli_no=cl.cli_no
left join cli_com clc on clc.cli_no=cl.cli_no""", engine_mssql)
# company = company.drop_duplicates()
assert False
# %% location name/address
# company['address'] = company[['Line_1', 'Line_2','Line_3','town','county','Postcode','country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# company['location_name'] = company[['town','country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# %%
# assign contacts's addresses to their companies
comaddr = company[['company_externalid', 'address','location_name','address_line1', 'address_line2','city','district','state','post_code','country_code']].drop_duplicates()
comaddr = comaddr.loc[comaddr['address']!='']
cp1 = vcom.insert_company_location(comaddr, mylog)

# %% addr 1
tem = comaddr[['company_externalid', 'address', 'address_line1']].dropna().drop_duplicates()
cp3 = vcom.update_location_address_line1(tem, dest_db, mylog)

# %% addr 2
tem = comaddr[['company_externalid', 'address', 'address_line2']].dropna().drop_duplicates()
cp3 = vcom.update_location_address_line2(tem, dest_db, mylog)

# %% city
tem = comaddr[['company_externalid', 'address', 'city']].dropna().drop_duplicates()
cp3 = vcom.update_location_city_2(tem, dest_db, mylog)

# %% postcode
tem = comaddr[['company_externalid', 'address', 'post_code']].dropna().drop_duplicates()
cp4 = vcom.update_location_post_code_2(tem, dest_db, mylog)

# %% state
tem = comaddr[['company_externalid', 'address', 'state']].dropna().drop_duplicates()
cp5 = vcom.update_location_state_2(tem, dest_db, mylog)

# %% district
tem = comaddr[['company_externalid', 'address', 'district']].dropna().drop_duplicates()
cp5 = vcom.update_location_district_2(tem, dest_db, mylog)

# %% country
tem = comaddr[['company_externalid', 'address', 'country_code']].dropna().drop_duplicates()
tem['country'] = tem.country_code
tem.loc[tem['country_code']=='']
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% location type
# tem = comaddr[['company_externalid','address']]
# tem['location_type'] = 'HEADQUARTER'
# cp7 = vcom.update_location_types_array(tem, dest_db, mylog)

# %% location type
tem = comaddr[['company_externalid','address']]
tem['rn'] = tem.groupby('company_externalid').cumcount()
tem = tem.loc[tem['rn']==0]
tem['location_type'] = 'BILLING_ADDRESS'
cp7 = vcom.update_location_types_array(tem, dest_db, mylog)

# tem = company_info[['company_externalid','Company_Reg_No']].dropna()
# tem['company_number'] = tem['Company_Reg_No']
# tem2 = tem.merge(vcom.company, on=['company_externalid'])
# tem2['company_id'] = tem2['id']
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcom.ddbconn, ['company_number', ], ['company_id', ],'company_location', mylog)
# vcom.update_company_number_location(tem, mylog)

tem = company[['company_externalid','cli_company_reg_no']].dropna().drop_duplicates()
tem['business_number'] = tem['cli_company_reg_no']
tem2 = tem.merge(vcom.company, on=['company_externalid'])
tem2['company_id'] =  tem2['id']
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcom.ddbconn, ['business_number', ], ['company_id', ],'company_location', mylog)
# vcom.update_business_number_tax(tem, mylog)

tem = company[['company_externalid','company_payment_term']].dropna().drop_duplicates()
tem = tem.loc[tem['company_payment_term']!='']
tem2 = tem.merge(vcom.company, on=['company_externalid'])
tem2['company_id'] =  tem2['id']
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcom.ddbconn, ['company_payment_term', ], ['company_id', ],'company_location', mylog)
# vcom.update_payment_term(tem, mylog)

# %% phone / switchboard
tem = company[['company_externalid','phone','switch_board']].dropna().drop_duplicates()
vcom.update_switch_board(tem, mylog)
vcom.update_phone(tem, mylog)

#  website
tem = company[['company_externalid','website']].dropna().drop_duplicates()
tem['website'] = tem['website'].apply(lambda x: x[:100])
tem = tem.loc[tem['website']!='']
cp5 = vcom.update_website(tem, mylog)

# %% note
note = company[['company_externalid','cli_nob','cli_comments']]

tem = pd.read_sql("""
select
       CONCAT('',cli_no) as company_externalid
    , con_branch
    , con_division
from cli_owner
""", engine_mssql)
tem = tem.drop_duplicates()
tem1 = tem.groupby('company_externalid')['con_branch'].apply(', '.join).reset_index()
tem2 = tem.groupby('company_externalid')['con_division'].apply(', '.join).reset_index()
tem = tem1.merge(tem2,on='company_externalid')
note = note.merge(tem,on='company_externalid',how='left')
note['note'] = note[['company_externalid','cli_nob','cli_comments','con_branch','con_division']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Client ID','Nature of Business','Comments','Office','Sector'], x) if e[1]]), axis=1)
vcom.update_note_2(note, dest_db, mylog)

# %% reg_date
tem = company[['company_externalid','cli_reg_date']].dropna().drop_duplicates()
tem['cli_reg_date'] = tem['cli_reg_date'].astype(str)
tem['cli_reg_date_1'] = tem['cli_reg_date'].apply(lambda x: x[0:4] if x else x)
tem['cli_reg_date_2'] = tem['cli_reg_date'].apply(lambda x: x[4:6] if x else x)
tem['cli_reg_date_3'] = tem['cli_reg_date'].apply(lambda x: x[6:9] if x else x)
tem['cli_reg_date'] = tem['cli_reg_date_1'] +'-'+tem['cli_reg_date_2']+'-'+tem['cli_reg_date_3']
tem['reg_date'] = pd.to_datetime(tem['cli_reg_date'], format='%Y/%m/%d %H:%M:%S')
vcom.update_reg_date(tem, mylog)

# %% last activity date
tem = company[['company_externalid','cli_last_contact']].dropna().drop_duplicates()
tem['cli_last_contact'] = tem['cli_last_contact'].apply(lambda x: str(x).split('.')[0])
tem['cli_last_contact_1'] = tem['cli_last_contact'].apply(lambda x: x[0:4] if x else x)
tem['cli_last_contact_2'] = tem['cli_last_contact'].apply(lambda x: x[4:6] if x else x)
tem['cli_last_contact_3'] = tem['cli_last_contact'].apply(lambda x: x[6:9] if x else x)
tem['cli_last_contact'] = tem['cli_last_contact_1'] +'-'+tem['cli_last_contact_2']+'-'+tem['cli_last_contact_3']
tem['last_activity_date'] = pd.to_datetime(tem['cli_last_contact'], format='%Y/%m/%d %H:%M:%S')
vcom.update_last_activity_date(tem, mylog)

# %% status
tem = company[['company_externalid','cli_status']].dropna().drop_duplicates()
tem['name'] = tem['cli_status']
tem1 = tem[['name']].drop_duplicates()
tem1['owner']=''
vcom.create_status_list(tem1,mylog)
vcom.add_company_status(tem, mylog)

# %% parent company
tem = pd.read_sql("""select CONCAT('',cli_no) as company_externalid
     , CONCAT('',cli_parent_cli_no) as parent_externalid
from client
where cli_parent_cli_no != 0""",engine_mssql)
vcom.update_parent_company(tem,mylog)


