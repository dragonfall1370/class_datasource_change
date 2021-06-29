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
cf.read('exede_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

from common import vincere_candidate
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())

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

# %% media to source
candidate = pd.read_sql("""
SELECT 
a.ApplicantId as candidate_externalid, m.NAME as source
FROM tblvwApplicantMedia a
join Media m on a.MediaId = m.ID
where a.ApplicantId > 'HQ00004169'
""", engine_mssql)
vcand.insert_source(candidate)
assert False

# %% compliance
candidate = pd.read_sql("""
select * from ztungtem_clienttable_candidate_personal_detail_compliance
""", engine_postgre)

# %% National Insurance number
candidate['country_code'] = 'GB'
cp4 = vcand.update_onboarding_choose_country(candidate, mylog)
cp5 = vcand.update_national_insurance_number(candidate)
cp6 = vcand.insert_onboarding_company_details__company_name(candidate, mylog)
cp6 = vcand.insert_onboarding_company_details__date_of_incorporation(candidate, mylog)
cp6 = vcand.insert_onboarding_company_details__vat_registered(candidate, mylog)
cp6 = vcand.insert_onboarding_company_details__vat_number(candidate, mylog)
cp6 = vcand.insert_onboarding_company_details__company_number(candidate, mylog)
cp6 = vcand.insert_onboarding_company_details__address(candidate, mylog)



