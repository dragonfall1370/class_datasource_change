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

from common import vincere_contact
vcont = vincere_contact.Contact(connection)

from common import vincere_company
vcom = vincere_company.Company(connection)
# %%
sql = """
select concat('SE',mp.person_id) as contact_externalid
     , nullif(mp.person_vorname,'') as person_vorname
     , nullif(person_info,'') as person_info
     , nullif(mp.person_nachname,'') as person_nachname
     , nullif(person_geschlecht,'') as person_geschlecht
     , nullif(person_titel,'') as person_titel
     , nullif(person_geburtstag,'') as person_geburtstag
     , mfp.*
from mand_person mp
left join (select person_id
                , concat('SE',max(firma_id)) as company_externalid
                , nullif(person_email,'') as person_email
                , nullif(ca.abteilung_de,'') as abteilung_de
                , nullif(person_funktion,'') as person_funktion
                , nullif(person_telefon,'') as person_telefon
                , nullif(person_mobil,'') as person_mobil
                , nullif(person_skype,'') as person_skype
from mand_firma_person
left join cat_abteilung ca on ca.id = mand_firma_person.person_abteilung
group by person_id) mfp on mp.person_id = mfp.person_id
"""
contact = pd.read_sql(sql, engine)
contact['contact_externalid'] = contact['contact_externalid'].apply(lambda x: str(x) if x else x)
assert False
# %% insert department company
tem = contact[['company_externalid', 'abteilung_de']].drop_duplicates().dropna()
tem['department_name'] = tem['abteilung_de']
cp7 = vcom.insert_department(tem, mylog)

# %% insert department contact
# assign the new addesses to contacts work location
tem2 = contact[['contact_externalid', 'company_externalid', 'abteilung_de']].drop_duplicates().dropna()
tem2['department_name'] = tem2['abteilung_de']
cp8 = vcont.insert_contact_department(tem2, mylog)

# %% job title
tem = contact[['contact_externalid', 'person_funktion']].dropna()
tem['job_title'] = tem['person_funktion']
cp3 = vcont.update_job_title2(tem, dest_db, mylog)

# %% dob
tem = contact[['contact_externalid', 'person_geburtstag']].dropna()
tem = tem.loc[tem['person_geburtstag'] != '0000-00-00']
tem['date_of_birth'] = pd.to_datetime(tem['person_geburtstag'])
cp4 = vcont.update_dob(tem, mylog)

# %% primary phone
tem = contact[['contact_externalid', 'person_telefon']].dropna()
tem['primary_phone'] = tem['person_telefon']
cp6 = vcont.update_primary_phone(tem, mylog)

# %% mobile phone
tem = contact[['contact_externalid', 'person_mobil']].dropna()
tem['mobile_phone'] = tem['person_mobil']
cp7 = vcont.update_mobile_phone(tem, mylog)

# %% skype
tem = contact[['contact_externalid', 'person_skype']].dropna()
tem['skype'] = tem['person_skype']
cp8 = vcont.update_skype(tem, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
salutation = contact[['contact_externalid', 'person_geschlecht']].dropna()
salutation.loc[salutation['person_geschlecht']=='m', 'Salutation'] = 'Mr'
salutation.loc[salutation['person_geschlecht']=='w', 'Salutation'] = 'Ms'
tem = salutation[['contact_externalid', 'Salutation']].dropna().drop_duplicates()
tem['gender_title'] = tem['Salutation']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
vcont.update_gender_title(tem, mylog)

# %% note
tem = contact[['contact_externalid', 'person_info']]
tem['note'] = tem[['contact_externalid', 'person_info']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['HR4YOU ID', 'Notizen'], x) if e[1]]), axis=1)
vcont.update_note_2(tem, dest_db, mylog)

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

# %% owner
company_owner = pd.read_sql("""
select concat('SE',firma_id) as firma_id
     , nullif(firma_name1,'') as firma_name1
     , nullif(u.reg_mail,'') as reg_mail
from mand_firma c
left join user_login u on u.user_id = c.firma_zustaendig""", engine)


cont = pd.read_sql("""
select concat('SE',mp.person_id) as person_id
     , nullif(mp.person_vorname,'') as person_vorname
     , nullif(mp.person_nachname,'') as person_nachname
     , nullif(mfp.person_email,'') as person_email
     , concat('SE',mfp.firma_id) as firma_id
from mand_person mp
left join (select person_id, max(firma_id) as firma_id, person_email from mand_firma_person group by person_id) mfp on mp.person_id = mfp.person_id""", engine)

tem = cont.merge(company_owner, on='firma_id')
tem2 = tem[['person_id','reg_mail']].dropna().drop_duplicates()
tem2.rename(columns={'person_id': 'contact_externalid','reg_mail': 'email',}, inplace=True)
vcont.update_owner(tem2,mylog)
