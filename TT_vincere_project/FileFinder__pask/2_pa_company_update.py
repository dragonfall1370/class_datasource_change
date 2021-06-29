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
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
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

from common import vincere_company
vcom = vincere_company.Company(connection)
# %%
sql = """
select c.idcompany as company_externalid
,c.companyname
, isdefault
, addressline1
, addressline2
, addressline3
, addressline4
, city
, postcode ,createdon
, pa.country
from company_paddress cp
join (select paddress.*, country.value as country
from paddress
join country on country.idcountry = paddress.idcountry) pa on cp.idpaddress = pa.idpaddress
right join (select * from company where companyid in ('5714'
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
,'6241')) c on c.idcompany = cp.idcompany
"""
company = pd.read_sql(sql, engine_postgre_src)
assert False

# %% billing address
company['address'] = company[['addressline1', 'addressline2', 'addressline3', 'addressline4', 'city','postcode','country']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
cp2 = vcom.insert_company_location_2(company, dest_db, mylog)

# %% city
cp3 = vcom.update_location_city_2(company, dest_db, mylog)

# %% postcode
company['post_code'] = company['postcode']
cp4 = vcom.update_location_post_code_2(company, dest_db, mylog)

# %% country
tem = company[['company_externalid', 'country', 'address']].dropna()
tem['country_code'] = tem.country.map(vcom.get_country_code)
tem['country'] = tem.country
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% company info
sql = """
select idcompany as company_externalid
, companyurl1
, companyswitchboard from companyx where companyid in ('5714'
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
,'6241')
"""
company = pd.read_sql(sql, engine_postgre_src)

# %% phone / switchboard
company['switch_board'] = company['companyswitchboard']
company['phone'] = company['companyswitchboard']
vcom.update_switch_board(company, mylog)
vcom.update_phone(company, mylog)

# %% website
tem = company[['company_externalid', 'companyurl1']].dropna()
tem['website'] = tem['companyurl1']
cp5 = vcom.update_website(tem, mylog)

# %% note
sql = """
select c.idcompany as company_externalid, c.companyname, cs.value as status, l.value as location, c.companyid
from company c
left join location l on l.idlocation = c.idlocation
left join companystatus cs on cs.idcompanystatus = c.idcompanystatus
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
,'6241')
"""
company = pd.read_sql(sql, engine_postgre_src)
company['note'] = company[['companyid', 'status', 'location']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Pask ID', 'Status', 'Location'], x) if e[1]]), axis=1)
# company.to_csv(os.path.join(standard_file_upload, 'dt_out.csv'), index=False)
vcom.update_note_2(company, dest_db, mylog)

# %% industry
industries = pd.read_csv('industries.csv')
from common import vincere_candidate
import datetime
vcand = vincere_candidate.Candidate(connection)
tem = industries[['Vincere Industry']].drop_duplicates()
tem['insert_timestamp'] = datetime.datetime.now()
tem['name'] = tem['Vincere Industry']
tem = tem.loc[tem.name!='']
vcand.insert_industry(tem, mylog)

sql = """
select c.idcompany as company_externalid
,i.value
from companyx c
left join industry i on i.idindustry = c.idindustry_string_list
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
,'6241')
"""
company_industries = pd.read_sql(sql, engine_postgre_src)

company_industries['name'] = company_industries['value']
company_industries = company_industries.drop_duplicates().dropna()
cp10 = vcom.insert_company_industry(company_industries, mylog)

# %% reg date
tem = company[['company_externalid', 'createdon']]
tem['reg_date'] = pd.to_datetime(tem['createdon'])
vcom.update_reg_date(tem, mylog)