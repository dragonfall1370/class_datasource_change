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
cf.read('ac_config.ini')
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

engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

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

assert False

# %% company location, address, postcode, switch board, phone, fax
company = pd.read_sql("""
select * from CompanyAddress;
""", engine_mssql)
company['address'] = company[['Address', 'LocationText', ]].apply(lambda x: ', '.join([e for e in x if e]), axis=1) \
    .map(lambda x: html_to_text(x)).map(lambda x: x.replace('\n', ', ').replace(',,', ',').replace(', ,', ','))
company.rename(columns={'CompanyId': 'company_externalid', 'PostCode': 'post_code', 'FaxTelephone': 'fax'}, inplace=True)
vcom.insert_company_location_2(company, dest_db, mylog, allow_dup=False)
vcom.update_location_post_code_2(company, dest_db, mylog)

company['phone'] = company['Telephone']
company['switch_board'] = company['Telephone']
vcom.update_phone(company, mylog)
vcom.update_switch_board_2(company, dest_db, mylog)
vcom.update_fax(company, mylog)

# %% web site
company = pd.read_sql("""
select
Id as company_externalid
, TypeName
,CreatedUsersName
,CreatedDateTime
,ModifiedUsersName
,ModifiedDateTime
, WebAddress as website
from Company;""", engine_mssql)
company['CreatedDateTime'] = company['CreatedDateTime'].astype(str)
company['ModifiedDateTime'] = company['ModifiedDateTime'].astype(str)
company['note'] = company[['company_externalid', 'TypeName', 'CreatedUsersName', 'CreatedDateTime', 'ModifiedUsersName', 'ModifiedDateTime']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Itris Company ID', 'Type', 'Created By', 'Created On', 'Modified By', 'Modified On'], x) if e[1]]), axis=1)
vcom.update_website(company, mylog)
vcom.update_note(company, mylog)

company['reg_date'] = pd.to_datetime(company['CreatedDateTime'])
vcom.update_reg_date(company, mylog)
