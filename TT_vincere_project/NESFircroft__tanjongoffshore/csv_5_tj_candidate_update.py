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
cf.read('tj_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

# %%
candidate = pd.read_sql("""
select *,COALESCE("COMPANY NAME", "LIMITED COMPANY") as current_employer from Candidate
""", engine_sqlite)
candidate['candidate_externalid'] = candidate['RMSCANDIDATEID'].astype(str)
assert False
# %% location name/address
c_location = candidate[['candidate_externalid','ADDRESS1', 'ADDRESS2','ADDRESS3','TOWN','COUNTY','POSTCODE','COUNTRY']]
c_location['address'] = c_location[['ADDRESS1', 'ADDRESS2','ADDRESS3','TOWN','COUNTY','POSTCODE','COUNTRY']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
c_location['location_name'] = c_location['address']
c_location = c_location.loc[c_location['address']!='']
cp2 = vcand.insert_common_location_v2(c_location, dest_db, mylog)
# update city
vcand = vincere_candidate.Candidate(engine_postgre_review.raw_connection())
comaddr = c_location[['candidate_externalid','ADDRESS1', 'ADDRESS2','ADDRESS3','TOWN','COUNTY','POSTCODE','COUNTRY']].drop_duplicates()\
    .rename(columns={'TOWN': 'city', 'COUNTY': 'state', 'POSTCODE': 'post_code'})

tem = comaddr[['candidate_externalid', 'ADDRESS1']].dropna()
tem['address_line1'] = tem['ADDRESS1']
tem['count'] = tem['address_line1'].apply(lambda x: len(x))
tem = tem.loc[tem['count']<100]
cp3 = vcand.update_address_line1(tem, dest_db, mylog)

tem = comaddr[['candidate_externalid', 'ADDRESS2','ADDRESS3']].dropna()
tem['address_line2'] = tem[['ADDRESS2','ADDRESS3']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
cp3 = vcand.update_address_line2(tem, dest_db, mylog)

tem = comaddr[['candidate_externalid', 'city']].dropna()
cp3 = vcand.update_location_city2(tem, dest_db, mylog)
# update state
tem = comaddr[['candidate_externalid', 'state']].dropna()
cp4 = vcand.update_location_state2(tem, dest_db, mylog)
# update postcode
tem = comaddr[['candidate_externalid', 'post_code']].dropna()
cp5 = vcand.update_location_post_code2(tem, dest_db, mylog)
#  update country
from common import vincere_company
vcom = vincere_company.Company(connection)
tem = comaddr[['candidate_externalid','COUNTRY']].dropna()
tem['country_code'] = tem.COUNTRY.map(vcom.get_country_code)
# tem['CandidateCountry'].unique()
cp6 = vcand.update_location_country_code(tem, mylog)

# %% home phones
home_phone = candidate[['candidate_externalid', 'HOME TELEPHONE']].dropna()
home_phone['home_phone'] = home_phone['HOME TELEPHONE']
cp = vcand.update_home_phone2(home_phone, dest_db, mylog)

# %% work phones
wphone = candidate[['candidate_externalid', 'WORK TELEPHONE']].dropna()
wphone['work_phone'] = wphone['WORK TELEPHONE']
cp = vcand.update_work_phone(wphone, mylog)

# %% mobile/primary phones
mphone = candidate[['candidate_externalid', 'MOBILE TELEPHONE']].dropna()
mphone['mobile_phone'] = mphone['MOBILE TELEPHONE']
mphone['primary_phone'] = mphone['MOBILE TELEPHONE']
cp = vcand.update_mobile_phone(mphone, mylog)
cp = vcand.update_primary_phone(mphone, mylog)

# %% work phones
wphone = candidate[['candidate_externalid', 'WORK EMAIL']].dropna()
wphone['work_email'] = wphone['WORK EMAIL']
cp = vcand.update_work_email(wphone, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = candidate[['candidate_externalid', 'TITLE']].dropna().drop_duplicates()
tem['gender_title'] = tem['TITLE']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
tem2 = tem[['candidate_externalid','gender_title']].dropna().drop_duplicates()
cp = vcand.update_gender_title(tem2, mylog)

# %% preferred name
tem = candidate[['candidate_externalid', 'KNOWN AS']].dropna().drop_duplicates()
tem['preferred_name'] = tem['KNOWN AS']
vcand.update_preferred_name(tem, mylog)

# %% dob
tem = candidate[['candidate_externalid', 'DATE OF BIRTH']].dropna().drop_duplicates()
tem['date_of_birth'] = pd.to_datetime(tem['DATE OF BIRTH'])
vcand.update_dob(tem, mylog)

# %% source
tem = candidate[['candidate_externalid', 'CV SOURCE']].dropna().drop_duplicates()
tem['source'] = tem['CV SOURCE']
cp = vcand.insert_source(tem)

# %% reg date
tem = candidate[['candidate_externalid', 'CV IN DATE']].dropna().drop_duplicates()
tem['reg_date'] = pd.to_datetime(tem['CV IN DATE'])
vcand.update_reg_date(tem, mylog)

# %% current employer
cur_emp = candidate[['candidate_externalid', 'current_employer', 'JOB TITLE']]
cur_emp['current_job_title'] = cur_emp['JOB TITLE'].str.strip()
vcand.update_candidate_current_employer_v3(cur_emp, dest_db, mylog)

# %% linkedin
tem = candidate[['candidate_externalid', 'LINKEDIN URL']].dropna().drop_duplicates()
tem['linkedin'] = tem['LINKEDIN URL']
vcand.update_linkedin(tem, mylog)

# %% website
tem = candidate[['candidate_externalid', 'WEBPAGE']].dropna().drop_duplicates()
tem['website'] = tem['WEBPAGE']
vcand.update_website(tem, mylog)

# %% note
note = candidate[['candidate_externalid', 'NATIONALITY', 'NATIONALITY2','COUNTRY OF RESIDENCE','FIRCROFT PLACEMENT']]
note['note'] = note[['candidate_externalid', 'NATIONALITY', 'NATIONALITY2','COUNTRY OF RESIDENCE','FIRCROFT PLACEMENT']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['RMSCANDIDATEID', 'NATIONALITY', 'NATIONALITY2','COUNTRY OF RESIDENCE','FIRCROFT PLACEMENT'], x) if e[1]]), axis=1)
cp7 = vcand.update_note2(note, dest_db, mylog)

# %% status
tem = candidate[['candidate_externalid', 'STATUS']].dropna().drop_duplicates()
tem['name'] = tem['STATUS']
tem1 = tem[['name']]
tem1['owner'] = ''
vcand.create_status_list(tem1, mylog)
vcand.add_candidate_status(tem, mylog)
