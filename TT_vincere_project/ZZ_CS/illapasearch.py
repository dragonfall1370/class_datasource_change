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
cand_cs = pd.read_sql("""select id, company, job_title, experience_details_json from
(select c.additional_id, company, job_title from
(select additional_id, translate as company
from additional_form_values afv
join (select a.translate, b.field_value
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id = 11275
        and form_id = 1005) v on v.field_value = afv.field_value
where field_id = 11275 and form_id = 1005) c
left join (
select additional_id, translate as job_title
from additional_form_values afv
join (select a.translate, b.field_value
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id = 11276
        and form_id = 1005) v on v.field_value = afv.field_value
where field_id = 11276 and form_id = 1005) j on c.additional_id = j.additional_id) cs
join candidate c2 on c2.id = cs.additional_id""",engine_postgre_review)
cand_cs['experience_details_json'] = cand_cs['experience_details_json'].apply(lambda x: x.replace('"cbEmployer":"1"','"cbEmployer":""'))
cand_json = pd.read_sql("""with workhistory as (
select
        id
        ,experience_details_json
        ,json_array_elements(experience_details_json::json)::json->>'company' as company_json
        ,json_array_elements(experience_details_json::json)::json->>'jobTitle' as jobTitle_json
from candidate
--where candidate.experience_details_json is not null
)
select id, company_json,jobTitle_json
from workhistory
where nullif(trim(jobTitle_json),'') is not null
--where COALESCE(company,jobTitle) is null""",engine_postgre_review)
tem1 = cand_cs.loc[cand_cs['id'].isin(cand_json['id'])]
tem2 = cand_cs.loc[~cand_cs['id'].isin(cand_json['id'])]
tem1 = tem1.merge(cand_json,on='id')
tem1 = tem1.drop_duplicates()
tem1['rn'] = tem1.groupby('id').cumcount()

grouped_df = tem1.groupby("id")
maximums = grouped_df.max()
maximums = maximums.reset_index()
maximums['job_title'] = maximums['job_title'] +' '+maximums['jobtitle_json'] #,{"company":"","jobTitle":"",
maximums['company'] ='{"currentEmployer":"'+maximums['company']+'"'
maximums['job_title'] ='"jobTitle":"'+maximums['job_title']+'"'
maximums['cbEmployer'] ='"cbEmployer":"1"}'
maximums['new_json'] = maximums['company']+','+maximums['job_title']+','+maximums['cbEmployer']
maximums['experience_details_json'] = maximums['experience_details_json'].apply(lambda x: x.replace('[','').replace(']',''))
maximums['experience_details_json_2'] = maximums[['new_json','experience_details_json']].apply(lambda x: "{},{}".format(x[0],x[1]), axis=1)
maximums['experience_details_json_2'] = maximums['experience_details_json_2'].apply(lambda x: '['+x+']')
maximums['experience_details_json_2'] = maximums['experience_details_json_2'].apply(lambda x: x.replace('},]','}]'))
maximums['experience_details_json'] = maximums['experience_details_json_2']

tem2['company'] ='{"currentEmployer":"'+tem2['company']+'"'
tem2['job_title'] ='"jobTitle":"'+tem2['job_title']+'"'
tem2['cbEmployer'] ='"cbEmployer":"1"}'
tem2['new_json'] = tem2['company']+','+tem2['job_title']+','+tem2['cbEmployer']
tem2['experience_details_json'] = tem2['experience_details_json'].apply(lambda x: x.replace('[','').replace(']',''))
tem2['experience_details_json_2'] = tem2[['new_json','experience_details_json']].apply(lambda x: "{},{}".format(x[0],x[1]), axis=1)
tem2['experience_details_json_2'] = tem2['experience_details_json_2'].apply(lambda x: '['+x+']')
tem2['experience_details_json_2'] = tem2['experience_details_json_2'].apply(lambda x: x.replace('},]','}]'))
tem2['experience_details_json'] = tem2['experience_details_json_2']

cand = pd.concat([maximums[['id','experience_details_json']],tem2[['id','experience_details_json']]])
vincere_custom_migration.load_data_to_vincere(cand, dest_db, 'update', 'candidate', ['experience_details_json', ], ['id', ], mylog)
assert False

# def mark_current_employer(df):
#     if len(df):
#         df['cbEmployer'] = None
#         df.loc[df.index[0], 'cbEmployer'] = '1'
#     return df
#
# assert False
# cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: x.replace('null','""'))
# cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: x.replace('""""','""'))
# cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: eval(x))
# cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: json_normalize(x))
# cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: mark_current_employer(x))
# cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: x.to_json(orient='records')[1:-1].replace('},{', '} {'))
# cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: '['+x+']')
# cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: x.replace('} {','},{'))
# vincere_custom_migration.load_data_to_vincere(cand, dest_db, 'update', 'candidate', ['experience_details_json', ], ['id', ], mylog)



# str = '[{"company":null,"jobTitle":null,"currentEmployer":null,"yearOfExperience":null,"industry":null,"functionalExpertiseId":null,"subFunctionId":null,"cbEmployer":null,"currentEmployerId":null,"dateRangeFrom":null,"dateRangeTo":null}]'
# str = str.replace('null','""')
# str = str.replace('""""','""')
# a = eval(str)
# b = json_normalize(a)
# x = mark_current_employer(b)
# x.to_json(orient='records')[1:-1].replace('},{', '} {')