# %% package config
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
pd.set_option('show_dimensions', True)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('pt_config.ini')
# file storage config
data_folder = cf['default'].get('data_folder')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
# db config
review_db = cf['review_db']
sqlite_url = cf['default'].get('sqlite_url')
# log config
log_file = cf['default'].get('log_file')
mylog = log.get_info_logger(log_file)


# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
# pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_url, encoding='utf8')
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(review_db.get('user'), review_db.get('password'), review_db.get('server'), review_db.get('port'), review_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# %% Init the Vincere Candidate Object
import importlib
importlib.reload(vincere_candidate)
importlib.reload(vincere_custom_migration)
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
assert False
# %% 1. address
# sql
sql = """
select "Personnel ID
[link to folder]" as "candidate_externalid",
"Location
[free text]" as "city",
"State / Territory
[free text]" as "state"
from CRM_v05___2020_04_06_12PM___Main_List
"""
indt = pd.read_sql(sql, engine_sqlite)
indt['location_name'] = indt[['city', 'state']].apply(lambda x: ", ".join(str(e) for e in x if e) , axis = 1)
indt['address'] = indt['location_name']
cp = vcand.insert_common_location(indt[['candidate_externalid', 'location_name', 'address']], mylog)
cp = vcand.update_location_city(indt[['candidate_externalid', 'city'], mylog)
cp = vcand.update_location_state(indt['candidate_externalid', 'state'], mylog)

# %% 2. phones
# sql
sql = """
select 
"Personnel ID
[link to folder]" as "candidate_externalid",
"Mobile - Australia" as "primary_phone",
"Mobile - Overseas" as "mobile_phone",
"Other Contact Number" as "work_phone"
from CRM_v05___2020_04_06_12PM___Main_List
"""
indt = pd.read_sql(sql, engine_sqlite)
primary phone
indt['mobile_phone'] = indt['primary_phone'].where(pd.notnull, indt['mobile_phone'])
cp = vcand.update_primary_phone(indt[['candidate_externalid', 'primary_phone']], mylog)
cp = vcand.update_mobile_phone(indt[['candidate_externalid', 'mobile_phone']], mylog)
cp = vcand.update_work_phone(indt[['candidate_externalid', 'work_phone']], mylog)

# %% 3. Education & qualification
sql = """
select "Personnel ID
[link to folder]" as "candidate_externalid",
"Highest Qualification
[list]" as "Highest Qualification",
"Other Qualifications
[free text]" as "Other Qualifications",
"Accreditation / Certification
[free text]" as "Accreditation / Certification"
from CRM_v05___2020_04_06_12PM___Main_List
"""
indt = pd.read_sql(sql, engine_sqlite)
indt['education_summary'] = indt[['Highest Qualification', 'Other Qualifications', 'Accreditation / Certification']]\
    .apply(lambda x: "\n\n".join("[%s]: %s" % (n, e.strip().strip(";")) for n, e in x.iteritems() if e) , axis = 1)
vcand.update_education_summary(indt[['candidate_externalid', 'education_summary']], mylog)
indt['qualification'] = indt[['Other Qualifications', 'Accreditation / Certification']].apply(lambda x: ", ".join(str(e).strip().strip(";") for e in x if e) , axis = 1)
indt = indt.rename(columns={"Highest Qualification": "degreeName"})
vcand.update_education(indt[['candidate_externalid', 'degreeName', 'qualification']], mylog)

# %% 4. relocation & desired_job
sql = """
select "Personnel ID
[link to folder]" as "candidate_externalid",
"Willing To Relocate
[list]"  as "relocation",
"Availability
[list]" as "desired_job_type"
from CRM_v05___2020_04_06_12PM___Main_List
"""
indt = pd.read_sql(sql, engine_sqlite)
indt['desired_job_type'] = indt['desired_job_type'].apply(lambda x: 'permanent' if x == 'FT' else ('contract' if x == 'PT' else x))
vcand.update_candidate_current_employer_title(indt[['candidate_externalid', 'desired_job_type']], mylog)
indt['relocate'] = indt['relocate'].apply(lambda x: 1 if x == 'Yes' else 0)
vcand.update_candidate_current_employer_title(indt[['candidate_externalid', 'relocate']], mylog)

# %% 5. note
sql = """select "Personnel ID
[link to folder]" as "candidate_externalid",
"Alerts for Key Dates
[auto]" as "Alerts for Key Dates",
"Last Contacted
By
[free text]" as "Last Contacted By",
"Last Contacted
On
[mm/dd/yyyy]" as "Last Contacted On",
"Comments / History / Contact Notes
[free text]" as "Comments / History / Contact Notes",
"Internal Rating
[list]" as "Internal Rating",
"Potential for Executive Search
[list]" as "Potential for Executive Search",
"Identification Type
[list]" as "Identification Type",
"Job Start Date
[mm/dd/yyyy]" as "Job Start Date",
"1 Week BEFORE Start Date
[auto]"  as "1 Week BEFORE Start Date",
"Days to go" as "Days to go",
"FLAG
[auto]" as "FLAG",
"FOLLOW UP" as "FOLLOW UP",
"1 Month
[auto]" as "1 Month",
"Days to go.1" as "Days to go 1",
"FLAG
[auto].1" as "FLAG 1",
"FOLLOW UP.1" as "FOLLOW UP.1",
"2 Months
[auto]" as "2 Months",
"Days to go.2" as "Days to go 2",
"FOLLOW UP.2" as "FOLLOW UP 2",
"100 Days
[auto]" as "100 Days",
"Days to go.3" as "Days to go 3",
"FOLLOW UP.3" as "FOLLOW UP 3",
"6 Months
[auto]" as "6 Months",
"Days to go.4" as "Days to go 4",
"FOLLOW UP.4" as "FOLLOW UP 4",
"1 Year
[auto]" as "1 Year",
"Days to go.5" as "Days to go 5",
"FLAG
[auto].5" as "FLAG 5",
"FOLLOW UP.5" as "FOLLOW UP 5",
"Professional Achievements
[free text]" as "Professional Achievements"
from CRM_v05___2020_04_06_12PM___Main_List
"""
note = pd.read_sql(sql, engine_sqlite)
note = note.where(note.notnull(), None)
columns = note.columns
columns = ['ptId' if x == 'candidate_externalid' else x for x in columns]
note['note'] = note.apply(lambda x: '\n'.join([': '.join(e) for e in zip(['[%s]' % (f.upper()) for f in columns], x) if e[1] and e[1] != '#REF!']), axis='columns')
vcand.update_note(note, mylog)
# %% 6. skill codes
sql = """
select 
"Personnel ID
[link to folder]" as "candidate_externalid",
"Technical Skills
[free text]" as "skills"
from CRM_v05___2020_04_06_12PM___Main_List
"""
skills = pd.read_sql(sql, engine_sqlite)
skills = skills.dropna()
skills_new = vincere_common.splitDataFrameList_1(skills, 'skills', ';')
skills_new = pd.DataFrame([[id] + [z] for id, skills in skills_new.values for z in re.split(r',\s*(?![^()]*\))', skills)], columns=skills_new.columns)
skills_new['skills'] = skills_new['skills'].apply(lambda x: x.strip().rstrip('.'))
skills_new = skills_new.replace('', None).dropna()
vcand.update_skills(skills_new, mylog)


# %% 7. language skills
sql = """
select "Personnel ID
[link to folder]" as "candidate_externalid",
"Additional Languages
[free text]" as "language"
from CRM_v05___2020_04_06_12PM___Main_List
"""
skills = pd.read_sql(sql, engine_sqlite)
skills.dropna(inplace=True)
skills = pd.DataFrame([[id] + [z] for id, lang in skills.values for z in re.split(r';|,| and| And|&', lang)], columns=skills.columns)
skills = skills[skills['language'] != ""]
skills['language'] = skills['language'].apply(lambda x: x.strip().rstrip('.').rstrip(';'))
skills[['language', 'level']] = skills['language'].str.split("(", expand=True)
skills['language'] = skills['language'].str.strip()
skills['level'] = skills['level'].str.strip(")")
vcand.update_skill_languages(skills, mylog)

#%% 8. Work history & Current_employer
sql = """
select "Personnel ID
[link to folder]" as "candidate_externalid",
"Current Position
[free text]" as "experience"
from CRM_v05___2020_04_06_12PM___Main_List
"""
work = pd.read_sql(sql, engine_sqlite)
work.dropna(inplace=True)
work[work['experience'].notnull()].count()
work = pd.DataFrame([[id] + [z.strip().rstrip(',').rstrip(';')] for id, exp in work.values for z in re.split(r',|;', exp)], columns=work.columns)
tmp = work.dropna(subset=['experience'])
vcand.update_exprerience_work_history(work, mylog)
work = work.drop_duplicates(subset=['candidate_externalid'], keep='first')\
    .rename(columns = {'experience': 'current_job_title'})
work['current_employer'] = None
vcand.update_candidate_current_employer_title(work,mylog)

#%% 9. Salary
sql = """
select "Personnel ID
[link to folder]" as "candidate_externalid" ,
"Salary - 
Preferred
[list]" as "expected_salary_from",
"Salary - 
Minimum
[list]" as "current_salary",
"Salary - 
Maximum
[list]" as "desire_salary"
from CRM_v05___2020_04_06_12PM___Main_List
"""
salary = pd.read_sql(sql, engine_sqlite)
salary.dropna(inplace=True, how='all', subset=['expected_salary_from', 'current_salary', 'desire_salary'])
salary['SalaryType'] = 'peryear'
# TYPE
vcand.update_salary_type(salary[['candidate_externalid', 'SalaryType']], mylog)

tem = salary.dropna(subset=['current_salary'])
tem['current_salary'] = tem['current_salary'].apply(lambda x: x.replace('$', '').replace('k', '000').replace('+', '').split('-')[0])
tem['current_salary'] = tem['current_salary'].astype(int, errors='ignore')
vcand.update_current_salary(tem[['candidate_externalid', 'current_salary']], mylog)

tem = salary.dropna(subset=['desire_salary'])
tem['desire_salary'] = tem['desire_salary'].apply(lambda x: x.replace('$', '').replace('k', '000').replace('+', '').split('-')[1] if len(x.split('-')) > 1 else x.replace('$', '').replace('k', '000').replace('+', ''))
tem['desire_salary'] = tem['desire_salary'].astype(int, errors='ignore')
vcand.update_desire_salary(tem[['candidate_externalid', 'desire_salary']], mylog)

salary.dropna(subset=['expected_salary_from'], inplace=True)
salary[['expected_salary_from', 'expected_salary_to']] = salary['expected_salary_from'].str.replace('$', '').str.replace('k', '000').str.replace('+', '').str.split('-', expand=True)

tem = salary.dropna(subset=['expected_salary_from'])
tem['expected_salary_from'] = tem['expected_salary_from'].astype(int, errors='ignore')
vcand.update_expected_salary_from(tem[['candidate_externalid', 'expected_salary_from']], mylog)

tem = salary.dropna(subset=['expected_salary_to'])
tem['expected_salary_to'] = tem['expected_salary_to'].astype(int, errors='ignore')
vcand.update_expected_salary_to(tem[['candidate_externalid', 'expected_salary_to']], mylog)

# %% 10. industry
sql = """
select "Personnel ID
[link to folder]" as "candidate_externalid",
"Sector 1
[list]" as "sec1",
"Sector 2
[list]" as "sec2",
"Sector 3
[list]" as "sec3"
from CRM_v05___2020_04_06_12PM___Main_List
"""
industries = pd.read_sql(sql, engine_sqlite)
industries.dropna(inplace=True, how='all', subset=['sec1', 'sec2', 'sec3'])
vertical = pd.DataFrame(pd.unique(industries[['sec1', 'sec2', 'sec3']].values.ravel()), columns=['name'])
vertical['insert_timestamp'] = datetime.datetime.now()
vertical.dropna(subset=['name'], inplace=True)
vcand.insert_industry(vertical, mylog)
cand = industries['candidate_externalid'].tolist()*3
name = industries['sec1'].tolist() + industries['sec2'].tolist() + industries['sec3'].tolist()
cand_industry = [[cand[i], name[i]] for i in range(len(cand))]
cand_industry = pd.DataFrame(cand_industry, columns=["candidate_externalid", "name"])
cand_industry.dropna(subset=['name'], inplace=True)
vcand.insert_candidate_industry(cand_industry, mylog)


# %% 11. functional expertises
sql = """
select "Personnel ID
[link to folder]" as "candidate_externalid",
"Capability Area 1
[list]" as "cap1",
"Capability Area 2
[list]" as "cap2",
"Capability Area 3
[list]" as "cap3",
"Capability Area 4
[list]" as "cap4",
"Capability Area 5
[list]" as "cap5",
"Capability Area 6
[free text]" as "cap6"
from CRM_v05___2020_04_06_12PM___Main_List
"""
cand_fes = pd.read_sql(sql, engine_sqlite)
cand_fes.dropna(inplace=True, how='all', subset=['cap1', 'cap2', 'cap3', 'cap4', 'cap5', 'cap6'])
# # split column text to multiple rows
tem = cand_fes.dropna(subset=['cap6'])
cand_fes_new1 = pd.DataFrame([[id] + [z.strip().rstrip(".").rstrip(";")] for id, caps in tem[['candidate_externalid', 'cap6']].values for z in re.split(r'[,;]\s*(?![^()]*\))', caps)], columns=['candidate_externalid', 'fe'])
# concate and distinct all of fes
fes = pd.DataFrame(pd.unique(list(cand_fes[['cap1', 'cap2', 'cap3', 'cap4', 'cap5']].values.ravel()) + cand_fes_new1['fe'].tolist()), columns=['Functional Expertise'])
fes
vincere_custom_migration.inject_functional_expertise_subfunctional_expertise(fes, 'Functional Expertise', None, None, connection)
cand_fes_new2 = pd.DataFrame([[id] + [z.strip().rstrip(".").rstrip(";") if z else z] for id, *caps in cand_fes[['candidate_externalid', 'cap1', 'cap2', 'cap3', 'cap4', 'cap5']]\
    .values for z in caps], columns=['candidate_externalid', 'fe'])
cand_fes_total = cand_fes_new1.append(cand_fes_new2)
cand_fes_total['sfe'] = None
cand_fes_total.dropna(subset=['fe'], inplace=True)
cand_fes_total = cand_fes_total[cand_fes_total['fe'] != '']
vcand.insert_fe_sfe2(cand_fes_total, mylog)
