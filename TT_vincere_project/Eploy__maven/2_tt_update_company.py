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
cf.read('maven_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]

mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% clean data
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///thetalent.db', encoding='utf8')

from common import vincere_company
vcom = vincere_company.Company(engine_postgre.raw_connection())

# %% funs

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

# %% company location, address, postcode, switch board, phone, fax
company = pd.read_sql("""
select
c.company_id as company_externalid
, c.name as company_name
, c.address as address1
, c.city
, c.state
, c.zip
, c.country_id
, c2.name as country_name
, c.phone1
, c.phone2
, c.url
, c.billing_contact
, c.is_hot
, c.key_technologies
, c.notes
, d.name as department_name
, c.date_created
from company c
left join country c2 on c.country_id = c2.country_id
left join company_department d on c.company_id = d.company_id;
""", engine_sqlite)
assert False
# %% address
company['address'] = company[['address1', 'city', 'state', 'zip', 'country_name']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
cp2 = vcom.insert_company_location_2(company, dest_db, mylog)

# %% city
cp3 = vcom.update_location_city_2(company, dest_db, mylog)

# %% postcode
company['post_code'] = company.zip
cp4 = vcom.update_location_post_code_2(company, dest_db, mylog)

# %% state
cp5 = vcom.update_location_state_2(company, dest_db, mylog)

# %% country
company['country_code'] = company.country_name.map(vcom.get_country_code)
company['country'] = company.country_name
cp6 = vcom.update_location_country_2(company, dest_db, mylog)

# %% phone
company['phone'] = company[['phone1', 'phone2']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
cp7 = vcom.update_phone(company, mylog)

# %% reg date
comp_date = company.loc[company['date_created']!='0000-00-00 00:00:00']
comp_date['date_created'] = pd.to_datetime(comp_date['date_created'])
comp_date.rename(columns={'date_created': 'reg_date'}, inplace=True)
cp1 = vcom.update_reg_date(comp_date, mylog)

# %% web site
company.rename(columns={'url': 'website'}, inplace=True)
vcom.update_website(company, mylog)

# %% note
company.info()
company.loc[company['is_hot']=='0', 'is_hot'] = 'No'
company.loc[company['is_hot']=='1', 'is_hot'] = 'Yes'
note = company[[
    'company_externalid',
    'billing_contact',
    'key_technologies',
    'department_name',
    'is_hot',
    'notes'
                ]]

prefixes = [
'CATS ID',
'Billing Contact',
'Key Technologies',
'Departments',
'Hot',
'Notes',
]

note['note'] = note.apply(lambda x: '\n■ '.join([': '.join(e) for e in zip(prefixes, x) if e[1]]), axis='columns')
vcom.update_note_2(note, dest_db, mylog)
