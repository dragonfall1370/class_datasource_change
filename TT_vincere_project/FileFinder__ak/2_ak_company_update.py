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
right join (select * from company) c on c.idcompany = cp.idcompany
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
, companyswitchboard
, defaultfax
, defaultemail
, noofemployees
from companyx
"""
company = pd.read_sql(sql, engine_postgre_src)

# %% phone / switchboard
tem = company[['company_externalid', 'companyswitchboard']].dropna()
tem['switch_board'] = tem['companyswitchboard']
tem['phone'] = tem['companyswitchboard']
vcom.update_switch_board(tem, mylog)
vcom.update_phone(tem, mylog)

# %% website
tem = company[['company_externalid', 'companyurl1']].dropna()
tem['website'] = tem['companyurl1']
cp5 = vcom.update_website(tem, mylog)

# %% email
tem = company[['company_externalid', 'defaultfax']].dropna()
tem['fax'] = tem['defaultfax']
cp5 = vcom.update_fax(tem, mylog)

# %% note
sql = """
select c.idcompany as company_externalid
, cx.researchedby
, c.companyreference
, cs.value as status
, c.branch
, col.isactive, olt.value as offlimit_type
, longname.aliasname
, co.value as origin
, cx.companycomment
, cx.companynote
from company c
left join companyx cx on c.idcompany = cx.idcompany
left join companyofflimit col on col.idcompany = c.idcompany
left join offlimittype olt on olt.idofflimittype = col.idofflimittype
left join (with company_longname as (SELECT idcompany
		, idalias
		, createdon
		, row_number() over (partition by idcompany order by createdon::date desc) rn
		FROM "company_alias")

select cl.idcompany
, cl.idalias
, a.aliasname
from company_longname cl
left join "alias" a on a.idalias = cl.idalias
where cl.rn = 1) longname on longname.idcompany = c.idcompany
left join companystatus cs on cs.idcompanystatus = c.idcompanystatus
left join companyorigin co on co.idcompanyorigin = c.idcompanyorigin
"""
company = pd.read_sql(sql, engine_postgre_src)
company['companycomment'] = company['companycomment'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
company['companynote'] = company['companynote'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
company.loc[company['isactive'] == '0', 'isactive'] = 'No'
company.loc[company['isactive'] == '1', 'isactive'] = 'Yes'
company['note'] = company[['researchedby', 'companyreference', 'status', 'branch', 'isactive', 'offlimit_type', 'aliasname', 'origin', 'companycomment', 'companynote']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Researched by', 'Reference', 'Status', 'Branch', 'Active', 'Offlimit type', 'Long name', 'Origin', 'Comment', 'Note'], x) if e[1]]), axis=1)
# company.to_csv(os.path.join(standard_file_upload, 'dt_out.csv'), index=False)
vcom.update_note_2(company, dest_db, mylog)

# %% industry
industries = pd.read_csv('industries.csv')
from common import vincere_candidate
import datetime
vcand = vincere_candidate.Candidate(connection)
tem = industries[['INDUSTRY']].drop_duplicates()
tem['insert_timestamp'] = datetime.datetime.now()
tem['name'] = tem['INDUSTRY']
tem = tem.loc[tem.name!='']
vcand.insert_industry(tem, mylog)

company_industries = pd.read_sql("""
select idcompany as company_externalid, idindustry_string_list from companyx
""", engine_postgre_src)
company_industries = company_industries.dropna()
industry = company_industries.idindustry_string_list.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(company_industries[['company_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['company_externalid'], value_name='idindustry') \
    .drop('variable', axis='columns') \
    .dropna()

industries = pd.read_sql("""
select idindustry, value from industry
""", engine_postgre_src)
company_industries = industry.merge(industries, on='idindustry')

company_industries['matcher'] = company_industries['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
industries_csv = pd.read_csv('industries.csv')
industries_csv['matcher'] = industries_csv['INDUSTRY'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
company_industries = company_industries.merge(industries_csv, on='matcher')

company_industries['name'] = company_industries['INDUSTRY']
company_industries = company_industries.drop_duplicates().dropna()
cp10 = vcom.insert_company_industry(company_industries, mylog)

# %% reg date
tem = company[['company_externalid', 'createdon']]
tem['reg_date'] = pd.to_datetime(tem['createdon'])
vcom.update_reg_date(tem, mylog)