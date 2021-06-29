# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
import os
import datetime
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

# %% data connection
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

engine_sqlite = sqlalchemy.create_engine('sqlite:///thetalent.db', encoding='utf8')

from common import vincere_contact
vcont = vincere_contact.Contact(engine_postgre.raw_connection())
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

# %% reg date
contact = pd.read_sql("""
select c2.company_id as company_externalid
, c.contact_id as contact_externalid
, c.title as job_title
, c.email2 as personal_email
, c.phone_work as primary_phone
, c.phone_cell as mobile_phone
, c.address as address1
, c.city
, c.state
, c.zip
, c.country_id
, c3.name as country_name
, c.is_hot
, c.left_company
, c.company_department_id
, c.reports_to
, c.notes
, c.date_created
from contact c
left join company c2 on c2.company_id = c.company_id
left join country c3 on c.country_id = c3.country_id
;
""", engine_sqlite)
assert False
# %% location name/address
contact['location_name'] = contact[['address1', 'city', 'state', 'zip', 'country_name']] \
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

# %% city
comaddr = contact[['company_externalid', 'address', 'city', 'zip', 'state', 'country_name']].drop_duplicates()
cp3 = vcom.update_location_city_2(comaddr, dest_db, mylog)

# %% postcode
comaddr['post_code'] = comaddr.zip
cp4 = vcom.update_location_post_code_2(comaddr, dest_db, mylog)

# %% state
cp5 = vcom.update_location_state_2(comaddr, dest_db, mylog)

# %% country
comaddr['country_code'] = comaddr.country_name.map(vcom.get_country_code)
comaddr['country'] = comaddr.country_name
cp6 = vcom.update_location_country_2(comaddr, dest_db, mylog)

# %% note 2
contact.loc[contact['left_company']=='0', 'left_company'] = 'No'
contact.loc[contact['left_company']=='1', 'left_company'] = 'Yes'
note = contact[[
    'contact_externalid',
    'left_company',
    'notes'
                ]]

prefixes = [
'CATS ID',
'Left Company',
'Notes',
]

note = note.where(note.notnull(), None)
note['note'] = note.apply(lambda x: '\n'.join([': '.join(e) for e in zip(prefixes, x) if e[1]]), axis='columns')
cp7 = vcont.update_note_2(note, dest_db, mylog)

# %% mobile
vcont.update_mobile_phone(contact, mylog)
vcont.update_primary_phone(contact, mylog)
# %% nick name
vcont.update_job_title(contact, mylog)
vcont.update_personal_email(contact, mylog)

# %% reg date
cont_date = contact.loc[contact['date_created']!='0000-00-00 00:00:00']
cont_date['date_created'] = pd.to_datetime(cont_date['date_created'])
cont_date.rename(columns={'date_created': 'reg_date'}, inplace=True)
vcont.update_reg_date(cont_date, mylog)

contact.loc[contact['address']=='']