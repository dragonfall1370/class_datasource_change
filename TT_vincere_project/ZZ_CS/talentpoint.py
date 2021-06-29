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
assert False
# %% work history
cand_wh_notnull = pd.read_sql("""
with workhistory as (
select
        id
        ,candidate.first_name
        ,last_name
        ,email
        ,insert_timestamp
        ,experience_details_json
        ,json_array_elements(experience_details_json::json)::json->>'company' as company
        ,json_array_elements(experience_details_json::json)::json->>'jobTitle' as jobTitle
from candidate
--where candidate.experience_details_json is not null
)
select id,first_name,last_name,email,insert_timestamp as creation_date
from workhistory
where COALESCE(company,jobTitle) is not null
""", connection)
cand_wh_notnull = cand_wh_notnull.drop_duplicates()
cand_wh_notnull['Missing Work History'] = 'N'

cand = pd.read_sql("""
select
        id
        ,first_name
        ,last_name
        ,email
        ,insert_timestamp as creation_date
from candidate
""", connection)
tem = cand.loc[~cand['id'].isin(cand_wh_notnull['id'])]
tem['Missing Work History'] = 'Y'
cand_list_wh = pd.concat([cand_wh_notnull, tem])


# %% education
cand_edu_notnull = pd.read_sql("""
with workhistory as (
select 
        id
        ,candidate.first_name
        ,last_name
        ,email
        ,insert_timestamp
        ,candidate.edu_details_json
        ,json_array_elements(edu_details_json::json)::json->>'schoolName' as schoolName
        ,json_array_elements(edu_details_json::json)::json->>'qualification' as qualification
        ,json_array_elements(edu_details_json::json)::json->>'degreeName' as degreeName
        ,json_array_elements(edu_details_json::json)::json->>'startDate' as startDate
        ,json_array_elements(edu_details_json::json)::json->>'graduationDate' as graduationDate
        ,json_array_elements(edu_details_json::json)::json->>'institutionName' as institutionName
        ,json_array_elements(edu_details_json::json)::json->>'course' as course
        ,json_array_elements(edu_details_json::json)::json->>'training' as training
        ,json_array_elements(edu_details_json::json)::json->>'educationId' as educationId
from candidate
)
select id,first_name,last_name,email,insert_timestamp as creation_date
from workhistory
where COALESCE(schoolName,qualification,degreeName,startDate,graduationDate,institutionName,course,training,educationId) is not null
;
""", connection)
cand_edu_notnull = cand_edu_notnull.drop_duplicates()

cand_edu_notnull['Missing Education'] = 'N'

tem2 = cand.loc[~cand['id'].isin(cand_edu_notnull['id'])]
tem2['Missing Education'] = 'Y'
cand_list_edu = pd.concat([cand_edu_notnull, tem2])
candidate = cand_list_wh.merge(cand_list_edu, on=['id','first_name','last_name','email','creation_date'])
candidate.to_csv('talenpoint_candidate_list_education_workhistory.csv',index=False)