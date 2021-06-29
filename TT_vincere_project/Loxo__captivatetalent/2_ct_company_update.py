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
cf.read('ct_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
# src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

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

def get_phone(exp):
    exp = exp.replace('\'', '"')
    df = pd.read_json(exp)
    return df[['phone']].iloc[0,0]

from common import vincere_company
vcom = vincere_company.Company(connection)
# %%
company = pd.read_sql("""select id, url, desc, created, phones, documents, logo from company""", engine_sqlite)
company['phone'] = company['phones'].apply(lambda x: get_phone(x) if x else x)
company['company_externalid'] = company['id'].astype(str)
assert False
# %% location name/address
company_address = pd.read_sql("""select * from company_address""", engine_sqlite)
company_address['company_externalid'] = company_address['id'].astype(str)
company_address['address'] = company_address['address'].apply(lambda x: x.replace('"','').replace(']','').replace('[','') if x else x)
company_address['address_line1'] = company_address['address']
company_address['address'] = company_address[['address_line1', 'city','zip','state','country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
company_address['location_name'] = company_address['address']
# %%
# assign contacts's addresses to their companies
comaddr = company_address[['company_externalid', 'address','location_name','address_line1', 'city','zip','state','country','type']].drop_duplicates()
comaddr = comaddr.loc[comaddr['address']!='']
cp1 = vcom.insert_company_location(comaddr, mylog)

# %% addr 1
tem = comaddr[['company_externalid', 'address', 'address_line1']].dropna().drop_duplicates()
tem['len'] = tem['address_line1'].apply(lambda x: len(x))
tem = tem.loc[tem['len']<100]
cp3 = vcom.update_location_address_line1(tem, dest_db, mylog)

# %% city
tem = comaddr[['company_externalid', 'address', 'city']].dropna().drop_duplicates()
cp3 = vcom.update_location_city_2(tem, dest_db, mylog)

# %% postcode
tem = comaddr[['company_externalid', 'address', 'zip']].dropna().drop_duplicates()
tem['post_code'] = tem['zip']
cp4 = vcom.update_location_post_code_2(tem, dest_db, mylog)

# %% state
tem = comaddr[['company_externalid', 'address', 'state']].dropna().drop_duplicates()
cp5 = vcom.update_location_state_2(tem, dest_db, mylog)

# %% country
tem = comaddr[['company_externalid', 'address', 'country']].dropna().drop_duplicates()
tem['country_code'] = tem.country
tem.loc[tem['country_code']=='']
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)
u
# %% location type
tem = comaddr[['company_externalid', 'address', 'type']].dropna().drop_duplicates()
tem['type'].unique()
tem.loc[tem['type']=='Main', 'location_type'] = 'HEADQUARTER'
tem.loc[tem['type']=='Alternate', 'location_type'] = 'WORKPLACE'
cp7 = vcom.update_location_types_array(tem, dest_db, mylog)

# %% phone / switchboard
company_phone = pd.read_sql("""select * from companies_phones""", engine_sqlite)
company_phone['company_externalid'] = company_phone['id'].astype(str)
company_phone['phone'] = company_phone['phone'].astype(str)
company_phone['switch_board'] = company_phone['phone']
# tem['phone'] = tem['switch_board']
vcom.update_switch_board(company_phone, mylog)
vcom.update_phone(company_phone, mylog)

# %%  website
tem = company[['company_externalid', 'url']].dropna()
tem['website'] = tem['url']
tem['website'] = tem['website'].apply(lambda x: x[:100])
tem = tem.loc[tem['website']!='']
cp5 = vcom.update_website(tem, mylog)

# %% note
tem = company[['company_externalid', 'desc']].dropna()
tem['note'] = tem['desc'].apply(lambda x: html_to_text(x))
vcom.update_note_2(tem, dest_db, mylog)

# %% reg date
tem = company[['company_externalid', 'created']].dropna()
tem['reg_date'] = pd.to_datetime(tem['created'])
vcom.update_reg_date(tem, mylog)

# %% docs
# tem = company[['company_externalid', 'documents']].dropna()
# for index, row in tem.iterrows():
#     print(row['company_externalid'], row['documents'])
#     get_docs(row['documents'], row['company_externalid'])
#
# df1 = pd.concat(arr_df)

# %% created by
createdby = pd.read_sql("""select id, createdBy as email from company where createdBy is not null""", engine_sqlite)
createdby['company_externalid'] = createdby['id'].astype(str)
vcom.update_created_by(createdby,mylog)