# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('theo_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)
# assert False
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% connect data
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

from common import vincere_company
vcom = vincere_company.Company(engine_postgre.raw_connection())

# %% company
company = pd.read_sql("""
select 
com.clientCorporationID as company_externalid
, com.name as company_name
, com.dateAdded
, com.address1
, com.address2
, com.city
, com.state
, com.phone
, com.zip
, c.COUNTRY
, com.billingContact
, com.billingPhone
, com.companyDescription
, com.competitors
, com.customText1
, com.feeArrangement
, com.numOffices
, com.revenue
, com.status
, com.twitterHandle

, com.billingAddress1
, com.billingAddress2
, com.billingCity
, com.billingState
, com.billingZip
, c1.COUNTRY as billingCountry

, com.businessSectorList
, com.notes
, com.companyURL as website
, com.facebookProfileName
, com.linkedinProfileName
, com.numEmployees as employees_number
, com.parentClientCorporationID as parent_externalid

from bullhorn1.BH_ClientCorporation com
join bullhorn1.BH_Department de on com.departmentID = de.departmentID
left join tmp_country c on com.countryID = c.CODE
left join tmp_country c1 on com.billingCountryID = c1.CODE
where com.status != 'Archive' 
and de.name = 'Theo James Recruitment Limited'
;
""", engine_mssql)
company = company.where(company.notnull(), None)
company.company_externalid = company.company_externalid.astype(str)

# %% main
assert False

# %% website
vcom.update_website(company, mylog)

# %% employees_number
cp12 = vcom.update_employees_number(company, mylog)

# %%
cp11 = vcom.update_parent_company(company, mylog)

# %% location name/address
company['location_name'] = company[['address1', 'address2', 'city', 'state', 'zip', 'COUNTRY']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
company['address'] = company.location_name
company.info()
cp2 = vcom.insert_company_location_2(company, dest_db, mylog)

# %% billing address
tem = company[['company_externalid', 'billingAddress1', 'billingAddress2', 'billingCity', 'billingState', 'billingZip', 'billingCountry']]
tem['address'] = tem.drop('company_externalid', axis='columns').apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem = tem.loc[tem.address.str.strip() != '']
cp9 = vcom.insert_company_billing_address(tem, mylog)

# %% city
cp3 = vcom.update_location_city_2(company, dest_db, mylog)

# %% postcode
company['post_code'] = company.zip
cp4 = vcom.update_location_post_code_2(company, dest_db, mylog)

# %% state
cp5 = vcom.update_location_state_2(company, dest_db, mylog)

# %% country
company['country_code'] = company.COUNTRY.map(vcom.get_country_code)
company['country'] = company.COUNTRY
cp6 = vcom.update_location_country_2(company, dest_db, mylog)

# %% phone
company['switch_board'] = company.phone
cp7 = vcom.update_phone(company, mylog)
cp8 = vcom.update_switch_board(company, mylog)

# %% reg date
company.rename(columns={'dateAdded': 'reg_date'}, inplace=True)
cp1 = vcom.update_reg_date(company, mylog)

# %% note
def html_to_text(html):
    from bs4 import BeautifulSoup
    # url = "http://news.bbc.co.uk/2/hi/health/2284783.stm"
    # html = urllib.urlopen(url).read()
    soup = BeautifulSoup(html)

    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()  # rip it out

    # get text
    text = soup.get_text()

    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)
    return text

note = company[[
    'company_externalid',
    'companyDescription',
    'notes'
                ]]

prefixes = [
'BH ID',
'Company Description',
'Notes',
]
note.companyDescription = note.companyDescription.map(lambda x: html_to_text(x) if x else x)
note['note'] = note.apply(lambda x: '\n■ '.join([': '.join(e) for e in zip(prefixes, x) if e[1]]), axis='columns')
vcom.update_note_2(note, dest_db, mylog)

# %% industry
industry = company[['company_externalid', 'businessSectorList']].dropna()
industry = industry.businessSectorList.map(lambda x: x.split(';')) \
    .apply(pd.Series) \
    .merge(industry[['company_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['company_externalid'], value_name='name') \
    .drop('variable', axis='columns') \
    .dropna()

from common import vincere_candidate
import datetime
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())
tem = industry[['name']].drop_duplicates()
tem['insert_timestamp'] = datetime.datetime.now()
tem = tem.loc[tem.name!='']
vcand.insert_industry(tem, mylog)

cp10 = vcom.insert_company_industry(industry, mylog)

cp10['rn'] = cp10.groupby('company_id').cumcount()