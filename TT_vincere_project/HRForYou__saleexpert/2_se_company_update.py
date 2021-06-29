# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
# from edenscott._edenscott_dtypes import *
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
cf.read('saleexpert.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
db_source = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('review_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
connection_str = "mysql+pymysql://{user}:{password}@{server}/{db}" \
    .format(user=db_source.get('user')
            , password=db_source.get('password')
            , server=db_source.get('server')
            , db=db_source.get('database'))
engine = sqlalchemy.create_engine(connection_str)

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

from common import vincere_company
vcom = vincere_company.Company(connection)
# %%
sql = """
select firma_id as company_externalid
     , firma_name1
     , firma_strasse
     , firma_plz
     , firma_ort
     , firma_land
     , firma_telefon
     , firma_telefax
     , firma_homepage
     , firma_jobsite
     , parent_id
     , firma_email
     , cm.text_de as mitarbeiter
     , firma_bemerkung
     , country_code.countries_iso2 as location_country_code
     , countries.names_text as country_name
from mand_firma c
left join cat_countries_names countries on c.firma_land = countries.countries_id and countries.names_sprache = 'de'
left join cat_countries country_code on c.firma_land = country_code.countries_id
left join cat_mitarbeiter cm on c.firma_mitarbeiter = cm.id
"""
company = pd.read_sql(sql, engine)
company['company_externalid'] = company['company_externalid'].apply(lambda x: str(x) if x else x)
assert False
# %% billing address
company['firma_plz'] = company['firma_plz'].apply(lambda x: str(x) if x else x)
company['address'] = company[['firma_strasse', 'firma_ort', 'firma_plz', 'country_name']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
cp2 = vcom.insert_company_location_2(company, dest_db, mylog)

# %% city
company['city'] = company['firma_ort']
cp3 = vcom.update_location_city_2(company, dest_db, mylog)

# %% postcode
company['post_code'] = company['firma_plz']
cp4 = vcom.update_location_post_code_2(company, dest_db, mylog)

# %% country
tem = company[['company_externalid', 'country_name', 'address']].dropna()
tem['country_code'] = tem.country_name.map(vcom.get_country_code)
tem['country'] = tem.country_name
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% phone / switchboard
tem = company[['company_externalid', 'firma_telefon']].dropna()
tem['switch_board'] = tem['firma_telefon']
tem['phone'] = tem['firma_telefon']
vcom.update_switch_board(tem, mylog)
vcom.update_phone(tem, mylog)

# %% parant
tem = company[['company_externalid', 'parent_id']].dropna()
tem['parent_externalid'] = tem['parent_id']
tem['parent_externalid'] = tem['parent_externalid'].apply(lambda x: str(x) if x else x)
cp2 = vcom.update_parent_company(tem, mylog)

# %% fax
tem = company[['company_externalid', 'firma_telefax']].dropna()
tem['fax'] = tem['firma_telefax']
vcom.update_fax(tem, mylog)

# %% website
tem = company[['company_externalid', 'firma_homepage']].dropna()
tem['website'] = tem['firma_homepage']
tem.to_csv('web.csv')
cp5 = vcom.update_website(tem, mylog)

# %% note
tem = company[['company_externalid', 'firma_email', 'mitarbeiter', 'firma_bemerkung']]
company['note'] = company[['company_externalid', 'firma_email', 'mitarbeiter', 'firma_bemerkung']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['HR4YOU ID', 'E-Mail', 'Mitarbeiter', 'Bemerkungen'], x) if e[1]]), axis=1)
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
tem = company[['company_externalid', 'CreatedOn']]
tem['reg_date'] = pd.to_datetime(tem['CreatedOn'])
vcom.update_reg_date(tem, mylog)


# %%
sql = """
select c.idcompany as company_externalid
,c.companyname, addr.*
from Company c
join (select idCompany, isdefault
, addressline1
, addressline2
, addressline3
, addressline4
, city
, postcode
, pa.country
from Company_PAddress cp
left join (select paddress.*, country.value as country
from paddress
join country on country.idcountry = paddress.idcountry) pa on cp.idpaddress = pa.idpaddress) addr on addr.idCompany = c.idCompany
"""
company = pd.read_sql(sql, engine_sqlite)
company['address'] = company[['addressline1', 'addressline2', 'addressline3', 'addressline4', 'city','postcode','country']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
company = company.loc[company['address'] != '']
sql = """
select c.idcompany as company_externalid
, l.Value as location
from company c
left join companyx cx on c.idcompany = cx.idcompany
left join Location l on l.idLocation = c.idLocation
"""
company_location = pd.read_sql(sql, engine_sqlite)
company_location = company_location.loc[~company_location['company_externalid'].isin(company['company_externalid'])]
company_location = company_location.dropna()
assert False

# %% billing address
company_location['address'] = company_location['location']
cp2 = vcom.insert_company_location_2(company_location, dest_db, mylog)

# %% country
company_location['country_code'] = company_location.location.map(vcom.get_country_code)
company_location = company_location.loc[company_location['country_code'] != '']
company_location['country'] = company_location.location
cp6 = vcom.update_location_country_2(company_location, dest_db, mylog)