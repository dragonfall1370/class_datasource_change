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
cf.read('ca_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
role = pd.read_csv('role.csv', encoding = "ISO-8859-1")
role['matcher'] = role['Gel Role'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
skill = pd.read_csv('skill.csv')
skill['matcher'] = skill['Gel Skill'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
assert False
# %% candidate
cand_role = pd.read_sql("""
select Candidate_Id, Description
from Candidate_Role cr
left join Agency_Role ar on ar.Role_Id = cr.Role_Id
where Description is not null
and Candidate_Id not in (
39367
,2896
,37207
,22106
,25798
,35819
,50227
,48100
,46300
,47257)
""", engine_mssql)
cand_role['matcher'] = cand_role['Description'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_role = cand_role.merge(role, on='matcher')
cand_role['fe'] = cand_role['Functional Skill']
cand_role['sfe'] = cand_role['Sub Functional Skill']

cand_skill = pd.read_sql("""
select Candidate_Id, Description
from Candidate_Skill cs
join Skill s on s.Skill_Id = cs.Skill_Id
where Description is not null
and Candidate_Id not in (
39367
,2896
,37207
,22106
,25798
,35819
,50227
,48100
,46300
,47257)
""", engine_mssql)
cand_skill['matcher'] = cand_skill['Description'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_skill = cand_skill.merge(skill, on='matcher')
cand_skill['fe'] = cand_skill['Functional Skill']
cand_skill['sfe'] = cand_skill['Sub Functional Skill']

cand_trans = pd.read_sql("""
select Reference_Id as Candidate_Id from UDF_Record ur
left join UDF_Table ut on ur.Table_Id = ut.Table_Id
where Table_Name = ' COMPLIANCE'
and Reference_Id not in (
39367
,2896
,37207
,22106
,25798
,35819
,50227
,48100
,46300
,47257)
and Field_Value_15 in('Own Car','Own Transport','Yes')
""", engine_mssql)
cand_trans['fe'] = 'Transport'
cand_trans['sfe'] = 'Own Transport'

cand_fe = pd.concat([cand_role[['Candidate_Id','fe','sfe']], cand_skill[['Candidate_Id','fe','sfe']], cand_trans[['Candidate_Id','fe','sfe']]])
cand_fe = cand_fe.drop_duplicates()
cand_fe['candidate_externalid'] = cand_fe['Candidate_Id'].apply(lambda x: str(x) if x else x)
cand_fe = cand_fe.fillna('')
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
cp1 = vcand.insert_fe_sfe2(cand_fe, mylog)

# %% contact
cont_role = pd.read_sql("""
select Client_Contact_Id, Description
from Client_Contact_Role cr
left join Agency_Role ar on ar.Role_Id = cr.Role_Id
where Description is not null
and Client_Contact_Id not in (
294
,3474
,5152
,7907
,10677
,10949
,15334
,16218
,18982
,20476
,21028
,22062
)
""", engine_mssql)
cont_role.loc[cont_role['Description']=='Financial Controller']
role.loc[role['Gel Role']=='Financial Controller']
cont_role['matcher'] = cont_role['Description'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cont_role = cont_role.merge(role, on='matcher')
cont_role['fe'] = cont_role['Functional Skill']
cont_role['sfe'] = cont_role['Sub Functional Skill']

cont_skill = pd.read_sql("""
select Client_Contact_Id, Description
from Client_Contact_Skill cs
join Skill s on s.Skill_Id = cs.Skill_Id
where Description is not null
and Client_Contact_Id not in (
294
,3474
,5152
,7907
,10677
,10949
,15334
,16218
,18982
,20476
,21028
,22062
)
""", engine_mssql)
cont_skill['matcher'] = cont_skill['Description'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cont_skill = cont_skill.merge(skill, on='matcher')
cont_skill['fe'] = cont_skill['Functional Skill']
cont_skill['sfe'] = cont_skill['Sub Functional Skill']

cont_fe = pd.concat([cont_role[['Client_Contact_Id','fe','sfe']], cont_skill[['Client_Contact_Id','fe','sfe']]])
cont_fe = cont_fe.drop_duplicates()
cont_fe['contact_externalid'] = cont_fe['Client_Contact_Id'].apply(lambda x: str(x) if x else x)
cont_fe = cont_fe.fillna('')
# assert False
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
cp2 = vcont.insert_fe_sfe2(cont_fe, mylog)

# %% job
b_role = pd.read_sql("""
select br.Role_Id, ar.Description
from Booking_Role br
left join Agency_Role ar on ar.Role_Id = br.Agency_Role_Id
where ar.Description is not null
and br.Role_Id not in (
8547
,8467
,8696
,8473
,8490
,8678
,8710
,8849
,8508
,8626
,8627
,8655
,8656
,8669
,8708
,8514
,8544
)
""", engine_mssql)
b_role['job_externalid'] = 'BK'+b_role['Role_Id']
b_role['matcher'] = b_role['Description'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
b_role = b_role.merge(role, on='matcher')
b_role['fe'] = b_role['Functional Skill']
b_role['sfe'] = b_role['Sub Functional Skill']

v_role = pd.read_sql("""
select vr.Role_Id, ar.Description
from Vacancy_Role vr
left join Agency_Role ar on ar.Role_Id = vr.Agency_Role_Id
where ar.Description is not null
""", engine_mssql)
v_role['job_externalid'] = 'VC'+v_role['Role_Id']
v_role['matcher'] = v_role['Description'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
v_role = v_role.merge(role, on='matcher')
v_role['fe'] = v_role['Functional Skill']
v_role['sfe'] = v_role['Sub Functional Skill']

job_fe = pd.concat([b_role[['job_externalid','fe','sfe']], v_role[['job_externalid','fe','sfe']]])
job_fe = job_fe.drop_duplicates()
job_fe = job_fe.fillna('')
# assert False
from common import vincere_job
vjob = vincere_job.Job(connection)
cp3 = vjob.insert_fe_sfe2(job_fe, mylog)

# %% contact upsell
cont_role = pd.read_sql("""
select Client_Contact_Id, Description
from Client_Contact_Role cr
left join Agency_Role ar on ar.Role_Id = cr.Role_Id
where Description is not null
""", engine_mssql)
cont_role = cont_role.loc[cont_role['Description']=='Financial Controller']
cont_role['fe'] = 'A&F Roles'
cont_role['sfe'] = 'Financial Controller'
cont_role['contact_externalid'] = cont_role['Client_Contact_Id'].apply(lambda x: str(x) if x else x)
cont_role = cont_role.fillna('')
# assert False
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
cp2 = vcont.insert_fe_sfe2(cont_role, mylog)

# %% candidate upsell
cand_role = pd.read_sql("""
select Candidate_Id, Description
from Candidate_Role cr
left join Agency_Role ar on ar.Role_Id = cr.Role_Id
where Description is not null
""", engine_mssql)
cand_role = cand_role.loc[cand_role['Description']=='Financial Controller']
cand_role['fe'] = 'A&F Roles'
cand_role['sfe'] = 'Financial Controller'
cand_role['candidate_externalid'] = cand_role['Candidate_Id'].apply(lambda x: str(x) if x else x)
cand_role = cand_role.fillna('')
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
cp1 = vcand.insert_fe_sfe2(cand_role, mylog)