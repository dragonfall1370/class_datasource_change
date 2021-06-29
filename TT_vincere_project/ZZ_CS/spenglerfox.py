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
from pandas.io.json import json_normalize
import json

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


def remove_dup_array(text):
    arr = []
    for item in text.split(','):
        item = item.strip()
        arr.append(item)
    mylist = list(dict.fromkeys(arr))
    return ', '.join(map(str, mylist))

candidate = pd.read_sql("""
select id
     , first_name
     , last_name
     , i.industry
     , parent_industry
     , functional_expertise
     , sub_functional_expertise
     , skills, 'Data Quality (CV+Tags)' as talent_pool, uploaded_filename, saved_filename
from candidate c
left join
(select candidate_id, string_agg(parent,', ') as parent_industry, string_agg(industry,', ') as industry
from candidate_industry ci
left join (select v1.id, v1.name as industry, v2.name as parent from vertical v1
left join vertical v2 on v1.parent_id = v2.id) v on ci.vertical_id = v.id
group by candidate_id) i on c.id = i.candidate_id
left join
(select candidate_id, string_agg(fe.name,', ') as functional_expertise, string_agg(sfe.name,', ') as sub_functional_expertise
from candidate_functional_expertise cfe
left join functional_expertise fe on cfe.functional_expertise_id = fe.id
left join sub_functional_expertise sfe on cfe.sub_functional_expertise_id = sfe.id
group by candidate_id) func on c.id = func.candidate_id
left join (select candidate_id, uploaded_filename, saved_filename from candidate_document where candidate_id > 0 and primary_document = 1) f on c.id = f.candidate_id
where c.id in (select candidate_id from candidate_group_candidate where candidate_group_id = 506)""",engine_postgre_review)
candidate['functional_expertise'] = candidate['functional_expertise'].apply(lambda x: remove_dup_array(x) if x else x)
candidate.to_csv('spengler_fox_candidate_export.csv', index=False)
candidate[['saved_filename']].dropna().to_csv('spengler_download_file.csv', index=False)

assert False
tem = candidate[['first_name','last_name','saved_filename','uploaded_filename']]
document_path = r'D:\Tony\project\vincere_project\ZZ_CS\documents\documents'
temp_msg_metadata = vincere_common.get_folder_structure(document_path)
tem2 = tem.merge(temp_msg_metadata, left_on='saved_filename', right_on='file')
tem2['rn'] = tem2.groupby('uploaded_filename').cumcount()
tem2.loc[tem2['uploaded_filename']=='CV.pdf','uploaded_filename'] = tem2['first_name']+'_'+tem2['last_name']+'CV.pdf'

import os
for index, row in tem2.iterrows():
    # print(row['uploaded_filename'], row['root'],row['file_fullpath'])
    new_name = row['root']+'\\'+row['uploaded_filename']
    print(new_name)
    os.rename(row['file_fullpath'], new_name)