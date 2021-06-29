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
cf.read('dn_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
cand = pd.read_sql("""
select c.*, cs.name as source, a.email, current_job_title, ce.current_employer, pc.status, cl.address, country_code, a1.email as user_account
from candidate c
left join candidate_source cs on c.candidate_source_id = cs.id
left join user_account a on c.note_by = a.id
left join candidate_extension ce on c.id = ce.candidate_id
left join position_candidate pc on c.highest_pcid = pc.id
left join common_location cl on c.current_location_id = cl.id
left join user_account a1 on c.user_account_id = a1.id
where c.id in (
select candidate_id from candidate_extension where last_activity_date is not null order by last_activity_date desc limit 2000)
""",connection)

owner = pd.read_sql("""
select can.id, ua.email as owner_email
-- select count(*)
from candidate can
left join (select id as candidateid, (regexp_matches(candidate_owner_json, '[0-9]+\.?[0-9]*', 'g'))[1]::numeric as ownerid from candidate) ownerid on ownerid.candidateid = can.id
left join user_account ua on ua.id = ownerid.ownerid
where (ownerid.ownerid <> 100 and ownerid.ownerid <> 0)
""",connection)
owner = owner.dropna()
owner = owner.groupby('id')['owner_email'].apply(','.join).reset_index()
cand = cand.merge(owner,on='id')
assert False
cand['salary_type'].unique()
cand.loc[cand['male']==1000., 'gender'] = 'Other'
cand.loc[cand['male']==1., 'gender'] = 'Male'
cand.loc[cand['male']==0., 'gender'] = 'Female'

cand.loc[cand['status']==1, 'met'] = 'met'
cand.loc[cand['status']==2, 'met'] = 'not met'

cand.loc[cand['salary_type']==1., 'type_of_salary'] = 'annual'
cand.loc[cand['salary_type']==2., 'type_of_salary'] = 'monthly'

cand.to_csv('afr_candidate_export.csv',index=False)