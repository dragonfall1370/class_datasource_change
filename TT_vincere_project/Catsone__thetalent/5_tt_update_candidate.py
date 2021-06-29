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
cf.read('tt_config.ini')
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
engine_sqlite = sqlalchemy.create_engine('sqlite:///thetalent.db', encoding='utf8')

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

candidate = pd.read_sql("""
select c.candidate_id as candidate_externalid
, c.title
, c.phone_home
, c.phone_cell
, c.phone_work
, c.email2
, c.address
, c.city
, c.state
, c.zip
, c.country_id
, c2.name as country_name
, c.is_hot
, c.is_active
, c.desired_pay
, c.current_employer
, c.date_available
, c.source
, c.key_skills
, c.notes
from candidate c
left join user u on c.owner = u.user_id
left join country c2 on c2.country_id = c.country_id;
""", engine_sqlite)
candidate['candidate_externalid'] = candidate['candidate_externalid'].astype(str)
assert False

# %% address
# tem = candidate[['candidate_externalid', 'address', 'city', 'state', 'zip', 'country_name']]
# tem['candidate_externalid'] = tem['candidate_externalid'].astype(str)
# tem['address'] = tem[['city', 'state', 'zip', 'country_name']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# tem = tem.loc[tem.address != '']
# tem['location_name'] = tem['address']
# cp = vcand.insert_common_location(tem, mylog)

tem1 = candidate[['candidate_externalid', 'city']].dropna()
cp1 = vcand.update_location_city(tem1, mylog)

tem2 = candidate[['candidate_externalid', 'state']].dropna()
cp2 = vcand.update_location_state(tem2, mylog)

tem3 = candidate[['candidate_externalid', 'zip']].dropna()
tem3['post_code'] = tem3['zip']
cp3 = vcand.update_location_post_code(tem3, mylog)

tem4 = candidate[['candidate_externalid','country_name']].dropna()
tem4['country_code'] = tem4.country_name.map(vcand.get_country_code)
vcand.update_location_country_code(tem4, mylog)

# %% phones
tem = candidate[['candidate_externalid', 'phone_home', 'phone_cell', 'phone_work']] \
    .rename(columns={'phone_home': 'home_phone', 'phone_cell': 'mobile_phone', 'phone_work': 'work_phone'})

vcand.update_home_phone(tem, mylog)
vcand.update_mobile_phone(tem, mylog)
tem['primary_phone'] = tem['mobile_phone']
vcand.update_primary_phone(tem, mylog)
vcand.update_work_phone(tem, mylog)

# %% current position
tem = candidate[['candidate_externalid', 'title', 'current_employer']].rename(columns={'title': 'current_job_title'})
tem['current_employer'].value_counts()
vcand.update_candidate_current_employer_title(tem, mylog)

# # %% currency
# tem = candidate[['candidate_externalid', 'CURRENCY']]
# tem['currency_of_salary'] = tem.CURRENCY.map(vcand.map_currency_code)
# tem['currency_of_salary'] = 'zar'
# vcand.update_currency_of_salary(tem, mylog)

# %% desired salary
tem = candidate[['candidate_externalid', 'desired_pay']].rename(columns={'desired_pay': 'desire_salary'})
vcand.update_desire_salary(tem, mylog)

# %% email
email = candidate[['candidate_externalid', 'email2']].dropna().rename(columns={'email2': 'work_email'})
vcand.update_work_email(email, mylog)

# %% skills
skills = candidate[['candidate_externalid', 'key_skills']].dropna().rename(columns={'key_skills': 'skills'})
vcand.update_skills(skills, mylog)

# %% note
note = pd.read_sql("""
select a.APP_ID as candidate_externalid, a.OTHER_TEL, a.LAST_CONTACTED, a.MODIFY_ON, a.MODIFY_USER, a.CREATED_ON, a.CREATE_USER, a.APP_TYPE
from Applicants a 
where a.DELETED = 0;
""", engine_mssql)

note.LAST_CONTACTED = note.LAST_CONTACTED.astype(object).where(note.LAST_CONTACTED.notnull(), None)
note.LAST_CONTACTED = note.LAST_CONTACTED.map(lambda x: datetime.datetime.strftime(x, '%d-%b-%Y %H:%M') if x else x)
note.MODIFY_ON = note.MODIFY_ON.dt.strftime('%d-%b-%Y %H:%M')
note.CREATED_ON = note.CREATED_ON.dt.strftime('%d-%b-%Y %H:%M')

note['note'] = note[['candidate_externalid', 'LAST_CONTACTED', 'MODIFY_ON', 'MODIFY_USER', 'CREATED_ON', 'CREATE_USER', 'APP_TYPE']] \
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Itris Candidate ID', 'Last Contacted', 'Modified On', 'Modified By',
                                                             'Created On', 'Created By', 'Type'], x) if e[1] and str(e[1]).strip() != '']), axis=1)
note = note[['candidate_externalid', 'note']]
vcand.update_note(note, mylog)

# %% reg date
vcand.update_reg_date(candidate, mylog)



