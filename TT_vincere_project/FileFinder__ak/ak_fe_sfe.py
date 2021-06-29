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
cf.read('ak_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
assert False

# %% contact
cont = pd.read_sql("""
select cont_info.* from
(select p.idperson as contact_externalid
      , p.idjobfunction_string_list
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
where cont.rn = 1) cont_info
""", engine_postgre_src)
cont = cont.dropna()
func = cont.idjobfunction_string_list.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cont[['contact_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['contact_externalid'], value_name='idjobfunction') \
    .drop('variable', axis='columns') \
    .dropna()
func['idjobfunction'] = func['idjobfunction'].str.lower()

jobfunction = pd.read_sql("""
select idjobfunction, value from jobfunction
""", engine_postgre_src)

jobfunction_1 = func.merge(jobfunction, on='idjobfunction')
jobfunction_1['matcher'] = jobfunction_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
jobfunction_csv = pd.read_csv('fe-sfe.csv')
jobfunction_csv_1 = jobfunction_csv[['File Finder Job Function (ROLE)','FUNCTIONAL EXPERTISE']].dropna().drop_duplicates()
jobfunction_csv_1['matcher'] = jobfunction_csv_1['File Finder Job Function (ROLE)'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
contact_jobfunction = jobfunction_1.merge(jobfunction_csv_1, on='matcher')
contact_jobfunction_1 = contact_jobfunction[['contact_externalid', 'FUNCTIONAL EXPERTISE']]

sql = """
select cont_info.*from
(select p.idperson as contact_externalid
      , p.idindustry_string_list
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
where cont.rn = 1) cont_info
"""
contact_industries = pd.read_sql(sql, engine_postgre_src)
contact_industries = contact_industries.dropna()
industry = contact_industries.idindustry_string_list.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(contact_industries[['contact_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['contact_externalid'], value_name='idindustry') \
    .drop('variable', axis='columns') \
    .dropna()
industry['idindustry'] = industry['idindustry'].str.lower()

industries_value = pd.read_sql("""
select idindustry, value from industry
""", engine_postgre_src)
industry_1 = industry.merge(industries_value, on='idindustry')
industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
industries_csv = pd.read_csv('fe-sfe.csv')
industries_csv_1 = industries_csv[['File Finder Industry','FUNCTIONAL EXPERTISE']].dropna().drop_duplicates()
industries_csv_1['matcher'] = industries_csv_1['File Finder Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
contact_industries = industry_1.merge(industries_csv_1, on='matcher')
contact_jobfunction_2 = contact_industries[['contact_externalid', 'FUNCTIONAL EXPERTISE']]
contact_jobfunction = pd.concat([contact_jobfunction_1, contact_jobfunction_2])
contact_jobfunction = contact_jobfunction.drop_duplicates()

contact_jobfunction['fe'] = contact_jobfunction['FUNCTIONAL EXPERTISE']
contact_jobfunction['sfe'] = None
# assert False
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
vcont.insert_fe_sfe2(contact_jobfunction, mylog)

# %% candidate
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
jobfunction_csv = pd.read_csv('fe-sfe.csv')
jobfunction_csv_1 = jobfunction_csv[['File Finder Job Function (ROLE)','FUNCTIONAL EXPERTISE']].dropna().drop_duplicates()
jobfunction_csv_1['matcher'] = jobfunction_csv_1['File Finder Job Function (ROLE)'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_jobfunction = jobfunction_1.merge(jobfunction_csv_1, on='matcher')
cand_jobfunction_1 = cand_jobfunction[['candidate_externalid', 'FUNCTIONAL EXPERTISE']]

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
industries_csv = pd.read_csv('fe-sfe.csv')
industries_csv_1 = industries_csv[['File Finder Industry','FUNCTIONAL EXPERTISE']].dropna().drop_duplicates()
industries_csv_1['matcher'] = industries_csv_1['File Finder Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_industries = industry_1.merge(industries_csv_1, on='matcher')
cand_jobfunction_2 = cand_industries[['candidate_externalid', 'FUNCTIONAL EXPERTISE']]
cand_jobfunction_2.loc[cand_jobfunction_2['FUNCTIONAL EXPERTISE'] == 'Design - Branding']
cand_jobfunction = pd.concat([cand_jobfunction_1, cand_jobfunction_2])
cand_jobfunction = cand_jobfunction.drop_duplicates()

cand_jobfunction['fe'] = cand_jobfunction['FUNCTIONAL EXPERTISE']
cand_jobfunction['sfe'] = None
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
vcand.insert_fe_sfe2(cand_jobfunction, mylog)

# %% Software skill
sw_skill = pd.read_sql("""
select cand.*, sk.sw_skill from
(select px.*
from candidate c
join (select P.idperson as candidate_externalid
               from personx P
where isdeleted = '0') px on c.idperson = px.candidate_externalid) cand
join(
select pc.idperson, u.value as sw_skill
from personcode pc
left join udskill1 u on u.idudskill1 = pc.codeid
where idtablemd = '50b5ef7e-1343-449a-8dcc-d119a0aa2b9e') sk on sk.idperson = cand.candidate_externalid
""", engine_postgre_src)
sw_skill['fe'] = sw_skill['sw_skill']
sw_skill['sfe'] = None
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
vcand.insert_fe_sfe2(sw_skill, mylog)


# %% Job Function
jobfunction_1 = pd.read_sql("""
select ac.idassignment as job_externalid, jf.value as job_func
from assignmentcode ac
left join jobfunction jf on jf.idjobfunction = ac.codeid
where idtablemd = '6051cf96-6d44-4aeb-925e-175726d0f97b'
""", engine_postgre_src)

jobfunction_1['matcher'] = jobfunction_1['job_func'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
jobfunction_csv = pd.read_csv('fe-sfe.csv')
jobfunction_csv_1 = jobfunction_csv[['File Finder Job Function (ROLE)','FUNCTIONAL EXPERTISE']].dropna().drop_duplicates()
jobfunction_csv_1['matcher'] = jobfunction_csv_1['File Finder Job Function (ROLE)'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
job_func = jobfunction_1.merge(jobfunction_csv_1, on='matcher')
tem = job_func[['job_externalid', 'FUNCTIONAL EXPERTISE']]

tem['fe'] = tem['FUNCTIONAL EXPERTISE']
tem['sfe'] = None
from common import vincere_job
vjob = vincere_job.Job(connection)
vjob.insert_fe_sfe2(tem, mylog)
