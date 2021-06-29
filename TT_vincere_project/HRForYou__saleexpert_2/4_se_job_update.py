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

from common import vincere_job
vjob = vincere_job.Job(connection)
# %%
sql = """
select concat('SE',p.id) as job_externalid
     , nullif(titel,'') as titel
     , nullif(l.reg_mail,'') as reg_mail
     , nullif(p.mandant,'') as mandant
     , nullif(cont.person_id,'') as person_id
     , nullif(cp.beschreibung_de,'') as phase
     , nullif(cpa.text_de,'') as abgeschlossen_weil
     , nullif(cps.text_de,'') as status
     , nullif(cb.text_de,'')  as beruf
     , nullif(abschluss_am,'') as abschluss_am
     , nullif(p.beschreibung,'') as beschreibung
     , nullif(vertrag_datum,'') as vertrag_datum
from projekte p
left join (select max(person_id) as person_id, firma_id
from mand_firma_person
group by firma_id) cont on cont.firma_id = p.mandant
left join user_login l on p.berater = l.user_id
left join cat_phase cp on cp.id = projektphase
left join cat_berufsbereich cb on cb.id = berufskategorie
left join cat_projektstatus cps on cps.id = status
left join cat_projekt_abschluss cpa on cpa.id = abgeschlossen_weil
"""
job = pd.read_sql(sql, engine)
job['job_externalid'] = job ['job_externalid'].apply(lambda x: str(x) if x else x)
assert False
vjob.update_default_currency('euro',mylog)
# %% set location
vjob.set_job_location_by_company_location(mylog)

# %% inter job des
tem = job[['job_externalid', 'beschreibung']].dropna()
tem['internal_description'] = tem['beschreibung']
cp2 = vjob.update_internal_description(tem, mylog)

# %% note
tem = job[['job_externalid', 'phase', 'abgeschlossen_weil', 'beruf', 'abschluss_am','vertrag_datum']]
tem.loc[tem['abschluss_am']=='0000-00-00', 'abschluss_am'] = None
tem.loc[tem['vertrag_datum']=='0000-00-00', 'vertrag_datum'] = None
tem['abschluss_am'] = tem['abschluss_am'].apply(lambda x: str(x) if x else x)
tem['vertrag_datum'] = tem['vertrag_datum'].apply(lambda x: str(x) if x else x)
tem['note'] = tem[['job_externalid', 'phase', 'abgeschlossen_weil', 'beruf', 'abschluss_am','vertrag_datum']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['HR4YOU ID', 'Phase','Abgeschlossen Weil','Berufskategorie','Abschluss am','Vertrag Datum'], x) if e[1]]), axis=1)
vjob.update_note(tem, mylog)

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