# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
# from edenscott._edenscott_dtypes import *
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
cf.read('ak_config.ini')
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
conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
# %%
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

from common import vincere_contact
vcont = vincere_contact.Contact(connection)
assert False
# %% media
api = '26bbea4bb44ab0ce50da1324aa4d8e3d'
# cand = pd.read_sql("""
# select px.*
# from candidate c
# join (select P.idperson as candidate_externalid, p.idjobfunction_string_list
#                from personx P
# where isdeleted = '0') px on c.idperson = px.candidate_externalid
# """, engine_postgre_src)
# cand = cand.dropna()
# func = cand.idjobfunction_string_list.map(lambda x: x.split(',') if x else x) \
#     .apply(pd.Series) \
#     .merge(cand[['candidate_externalid']], left_index=True, right_index=True) \
#     .melt(id_vars=['candidate_externalid'], value_name='idjobfunction') \
#     .drop('variable', axis='columns') \
#     .dropna()
# func['idjobfunction'] = func['idjobfunction'].str.lower()
#
# jobfunction = pd.read_sql("""
# select idjobfunction, value from jobfunction
# """, engine_postgre_src)
#
# jobfunction_1 = func.merge(jobfunction, on='idjobfunction')
# jobfunction_1['matcher'] = jobfunction_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# cs_csv = pd.read_csv('cs_mapping.csv')
# cs_csv_1 = cs_csv[['File Finder Job Function (ROLE)','MEDIA']]
# cs_csv_1 = cs_csv_1.loc[cs_csv_1['MEDIA'].notnull()]
# cs_csv_1['matcher'] = cs_csv_1['File Finder Job Function (ROLE)'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# cand_cs = jobfunction_1.merge(cs_csv_1, on='matcher')
# cand_cs_1 = cand_cs[['candidate_externalid', 'FUNCTIONAL EXPERTISE']]

sql = """
select px.*
from candidate c
join (select P.idperson as candidate_externalid
           , P.idindustry_string_list
from personx P
left join "user" u on u.iduser = P.iduser
left join preferredemploymenttype emt on emt.idpreferredemploymenttype = P.idpreferredemploymenttype_string
left join title t on t.idtitle = p.idtitle_string
where isdeleted = '0') px on c.idperson = px.candidate_externalid
"""
candidate_industries = pd.read_sql(sql, engine_postgre_src)
candidate_industries = candidate_industries.dropna()
industry = candidate_industries.idindustry_string_list.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(candidate_industries[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='idindustry') \
    .drop('variable', axis='columns') \
    .dropna()
industry['idindustry'] = industry['idindustry'].str.lower()

industries_value = pd.read_sql("""
select idindustry, value from industry
""", engine_postgre_src)
industry_1 = industry.merge(industries_value, on='idindustry')
industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
industries_csv = pd.read_csv('cs_mapping.csv')
industries_csv_1 = industries_csv[['File Finder Industry','MEDIA']].dropna().drop_duplicates()
industries_csv_1['matcher'] = industries_csv_1['File Finder Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_industries = industry_1.merge(industries_csv_1, on='matcher')
cand_cs_2 = cand_industries[['candidate_externalid', 'MEDIA']]
cand_cs_2.loc[cand_cs_2['MEDIA']=='Digital']
cand_cs = cand_cs_2
cand_cs = cand_cs.drop_duplicates()

vincere_custom_migration.insert_candidate_muti_selection_checkbox(cand_cs, 'candidate_externalid', 'MEDIA', api, connection)

# %% job role
api = 'b4c62884330eed1a4d9531926b38ae16'
cand = pd.read_sql("""
select px.*
from candidate c
join (select P.idperson as candidate_externalid, p.idjobfunction_string_list
               from personx P
where isdeleted = '0') px on c.idperson = px.candidate_externalid
""", engine_postgre_src)
cand = cand.dropna()
func = cand.idjobfunction_string_list.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cand[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='idjobfunction') \
    .drop('variable', axis='columns') \
    .dropna()
func['idjobfunction'] = func['idjobfunction'].str.lower()

jobfunction = pd.read_sql("""
select idjobfunction, value from jobfunction
""", engine_postgre_src)

jobfunction_1 = func.merge(jobfunction, on='idjobfunction')
jobfunction_1['matcher'] = jobfunction_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cs_csv = pd.read_csv('cs_mapping.csv')
cs_csv_1 = cs_csv[['File Finder Job Function (ROLE)','JOB ROLE']].dropna().drop_duplicates()
cs_csv_1['matcher'] = cs_csv_1['File Finder Job Function (ROLE)'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_cs = jobfunction_1.merge(cs_csv_1, on='matcher')
cand_cs_1 = cand_cs[['candidate_externalid', 'JOB ROLE']]
cand_cs_1.loc[cand_cs_1['JOB ROLE'] == 'Art Director']

sql = """
select px.*
from candidate c
join (select P.idperson as candidate_externalid
           , P.idindustry_string_list
from personx P
left join "user" u on u.iduser = P.iduser
left join preferredemploymenttype emt on emt.idpreferredemploymenttype = P.idpreferredemploymenttype_string
left join title t on t.idtitle = p.idtitle_string
where isdeleted = '0') px on c.idperson = px.candidate_externalid
"""
candidate_industries = pd.read_sql(sql, engine_postgre_src)
candidate_industries = candidate_industries.dropna()
industry = candidate_industries.idindustry_string_list.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(candidate_industries[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='idindustry') \
    .drop('variable', axis='columns') \
    .dropna()
industry['idindustry'] = industry['idindustry'].str.lower()

industries_value = pd.read_sql("""
select idindustry, value from industry
""", engine_postgre_src)
industry_1 = industry.merge(industries_value, on='idindustry')
industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
industries_csv = pd.read_csv('cs_mapping.csv')
industries_csv_1 = industries_csv[['File Finder Industry','JOB ROLE']].dropna().drop_duplicates()
industries_csv_1['matcher'] = industries_csv_1['File Finder Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_industries = industry_1.merge(industries_csv_1, on='matcher')
cand_cs_2 = cand_industries[['candidate_externalid', 'JOB ROLE']]
cand_cs = pd.concat([cand_cs_2, cand_cs_1])
cand_cs = cand_cs.drop_duplicates()

vincere_custom_migration.insert_candidate_muti_selection_checkbox(cand_cs, 'candidate_externalid', 'JOB ROLE', api, connection)

# %% level
api = '99bfd81cd16aab7e2796773fdba31ada'
cand = pd.read_sql("""
select px.*
from candidate c
join (select P.idperson as candidate_externalid, p.idjobfunction_string_list
               from personx P
where isdeleted = '0') px on c.idperson = px.candidate_externalid
""", engine_postgre_src)
cand = cand.dropna()
func = cand.idjobfunction_string_list.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cand[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='idjobfunction') \
    .drop('variable', axis='columns') \
    .dropna()
func['idjobfunction'] = func['idjobfunction'].str.lower()

jobfunction = pd.read_sql("""
select idjobfunction, value from jobfunction
""", engine_postgre_src)

jobfunction_1 = func.merge(jobfunction, on='idjobfunction')
jobfunction_1['matcher'] = jobfunction_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cs_csv = pd.read_csv('cs_mapping.csv')
cs_csv_1 = cs_csv[['File Finder Job Function (ROLE)','LEVEL']].dropna().drop_duplicates()
cs_csv_1['matcher'] = cs_csv_1['File Finder Job Function (ROLE)'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_cs = jobfunction_1.merge(cs_csv_1, on='matcher')
cand_cs_1 = cand_cs[['candidate_externalid', 'LEVEL']]

sql = """
select px.*
from candidate c
join (select P.idperson as candidate_externalid
           , P.idindustry_string_list
from personx P
left join "user" u on u.iduser = P.iduser
left join preferredemploymenttype emt on emt.idpreferredemploymenttype = P.idpreferredemploymenttype_string
left join title t on t.idtitle = p.idtitle_string
where isdeleted = '0') px on c.idperson = px.candidate_externalid
"""
candidate_industries = pd.read_sql(sql, engine_postgre_src)
candidate_industries = candidate_industries.dropna()
industry = candidate_industries.idindustry_string_list.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(candidate_industries[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='idindustry') \
    .drop('variable', axis='columns') \
    .dropna()
industry['idindustry'] = industry['idindustry'].str.lower()

industries_value = pd.read_sql("""
select idindustry, value from industry
""", engine_postgre_src)
industry_1 = industry.merge(industries_value, on='idindustry')
industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
industries_csv = pd.read_csv('cs_mapping.csv')
industries_csv_1 = industries_csv[['File Finder Industry','LEVEL']].dropna().drop_duplicates()
industries_csv_1['matcher'] = industries_csv_1['File Finder Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_industries = industry_1.merge(industries_csv_1, on='matcher')
cand_cs_2 = cand_industries[['candidate_externalid', 'LEVEL']]
cand_cs = pd.concat([cand_cs_2, cand_cs_1])
cand_cs = cand_cs.drop_duplicates()

vincere_custom_migration.insert_candidate_muti_selection_checkbox(cand_cs, 'candidate_externalid', 'LEVEL', api, connection)


# %% job level
api = 'eccddef34844d7948adf2e0094559c29'
jobfunction_1 = pd.read_sql("""
select a.idassignment as job_externalid, job_func.value
from assignment a
join (select ac.idassignment, jf.value
from assignmentcode ac
left join jobfunction jf on jf.idjobfunction = ac.codeid
where idtablemd = '6051cf96-6d44-4aeb-925e-175726d0f97b') job_func on a.idassignment = job_func.idassignment
""", engine_postgre_src)
jobfunction_1['matcher'] = jobfunction_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cs_csv = pd.read_csv('cs_mapping.csv')
cs_csv_1 = cs_csv[['File Finder Job Function (ROLE)','LEVEL']].dropna().drop_duplicates()
cs_csv_1['matcher'] = cs_csv_1['File Finder Job Function (ROLE)'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
job_cs = jobfunction_1.merge(cs_csv_1, on='matcher')
job_cs_1 = job_cs[['job_externalid', 'LEVEL']]
job_cs_1 = job_cs_1.drop_duplicates()
job_cs_1['job_externalid'].value_counts()
job_cs_1.loc[job_cs_1['job_externalid'] == 'f79185fd-8d08-40d4-9694-177764a5e10c']

vincere_custom_migration.insert_job_muti_selection_checkbox(job_cs_1, 'job_externalid', 'LEVEL', api, connection)