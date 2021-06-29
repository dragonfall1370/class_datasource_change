# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
# from edenscott._edenscott_dtypes import *
import os
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
cf.read('rt_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% dest db
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre_review.raw_connection()

# %% backup db
conn_str_ddb_bkup = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format('postgres', '123456', 'dmpfra.vinceredev.com', '5432', 'rytons_backup_restored')
engine_postgre_bkup = sqlalchemy.create_engine(conn_str_ddb_bkup, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn_bkup = engine_postgre_bkup.raw_connection()

cand = pd.read_csv(r'C:\Users\tony\Desktop\rytons\files\candidate.csv')
cand.drop(['Last activity date', 'Activity content'], axis=1, inplace=True)
cand = cand.drop_duplicates()
assert False
cand_bkup = pd.read_sql("""select * from candidate """, engine_postgre_bkup)
cand_bkup = cand_bkup.merge(cand[['ID','Email']], left_on='id',right_on='ID')
cand_bkup.drop(['ID','Email'], axis=1, inplace=True)
# col = ['id','first_name','email','candidate_source_id','user_account_id','date_of_birth','male','latest_status_change_date',
# 'latest_status','insert_timestamp','last_name','middle_name','current_salary','desire_salary','other_benefits','total_p_a','relocate',
# 'full_time','linked_in_profile','phone','eeo_require','phone2','contract','graduate','internship','language','keyword','active','graduation_year',
# 'part_time','is_employee_referal','work_phone','home_phone','country_specific','employee','graduate_high_school','contact_id','nickname','maritalstatus',
# 'skills','statements','education_summary','experience','edu_details_json','experience_details_json','trigger_index_update_timestamp','personal_statements',
# 'availability','dependents','working_state','desired_industry_json','desired_functional_expertise_json','candidate_owner_json','tagged_resume_id',
# 'employment_type','status','desired_job_type_json','currency_type','deleted_timestamp','deleted_reason','deleted_by_user_id','personal_location_id','external_id',
# 'notice_period','salary_type','months_per_year','present_salary_rate','expected_salary_from','expected_salary_to','no_of_dependents','note_by',
# 'note_on','total_jobs','payslip_email','highest_pcid','hot_end_date','current_bonus','desired_bonus','timezone','last_activity_date','note']

cand_bkup2 = pd.read_sql("""select * from candidate where external_id is null""", engine_postgre_bkup)
cand_to_add = pd.concat([cand_bkup,cand_bkup2])
# cand_to_add.drop(['external_map'], axis=1, inplace=True)
cand_to_add = cand_to_add.drop_duplicates()

# current_location_id
# cand_1= cand_bkup[['id','first_name','email','candidate_source_id','user_account_id','insert_timestamp','last_name','middle_name','payslip_email','candidate_owner_json']]
# cand_2= cand_bkup2[['id','first_name','email','candidate_source_id','user_account_id','insert_timestamp','last_name','middle_name','payslip_email','candidate_owner_json']]
# cand_to_add = pd.concat([cand_1,cand_2])
# cand_to_add = cand_to_add.drop_duplicates()
#
# cand_to_add['user_account_id'] = cand_to_add['user_account_id'].astype(int)
# # cand1 = cand_to_add.loc[cand_to_add['external_id'].notnull()]
# # cand_now = pd.read_sql("""select * from candidate """, engine_postgre_review)
# #
# # a = cand_now.merge(cand1, on='external_id')
# # # a.to_csv('cand_dup.csv')
# #
# a = pd.read_csv('cand_dup.csv')
# cand_to_add = cand_to_add.loc[~cand_to_add['id'].isin(a['id_y'])]
# # cand_to_add['current_salary']
#
# activity_candidate = pd.read_sql("""select * from activity_candidate""", engine_postgre_bkup)
# activity_candidate = activity_candidate.merge(cand_to_add[['id','email']], left_on='candidate_id', right_on='id')
#
# activity = pd.read_sql("""select * from activity""", engine_postgre_bkup)
# activity = activity.merge(activity_candidate[['activity_id']], left_on='id', right_on='activity_id')
# activity.to_csv('activity.csv')
# activity_candidate.to_csv('activity_candidate.csv')
# # vincere_custom_migration.load_data_to_vincere(cand_to_add, dest_db, 'insert', 'candidate',col, [], mylog)
# # vincere_custom_migration.psycopg2_bulk_insert_tracking(cand_to_add, ddbconn, ['id','first_name','email','candidate_source_id','user_account_id','insert_timestamp','last_name','middle_name','payslip_email','candidate_owner_json'], 'candidate', mylog)
# col_activity = list(activity.columns)
# col_activity.pop()
# col_activity.remove('company_id')
#
# col_activity_cand = list(activity_candidate.columns)
# col_activity_cand.pop()
#
# vincere_custom_migration.psycopg2_bulk_insert_tracking(activity, ddbconn,col_activity, 'activity', mylog)
# vincere_custom_migration.psycopg2_bulk_insert_tracking(activity_candidate, ddbconn,col_activity_cand, 'activity_candidate', mylog)
'id','first_name','email','candidate_source_id','user_account_id','insert_timestamp','last_name','middle_name','payslip_email','candidate_owner_json'

cols = [
'objective','skills','education_summary','experience','edu_details_json','skill_details_json','experience_details_json','linked_in_resume_content','payment_type','note'
]













# ,'contact_id' ,'highest_pcid',
# 'current_location_id'

for i in cols:
    print(i)
    tem = cand_to_add[['id', i]].dropna()
    if not tem.empty:
    # vincere_custom_migration.psycopg2_bulk_update_tracking(tem, ddbconn, [i], ['id'], 'candidate', mylog)
        vincere_custom_migration.load_data_to_vincere(tem, dest_db, 'update', 'candidate', [i ], ['id'], mylog)
col_to_update = 'contact_id'
tem = cand_to_add[['id',col_to_update]].dropna()
tem
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem, ddbconn, [col_to_update], ['id'],'candidate', mylog)









# 'id','first_name','email','candidate_source_id','user_account_id','date_of_birth','male','latest_status_change_date',
# 'latest_status','insert_timestamp','last_name','middle_name','current_salary','desire_salary','other_benefits','total_p_a','relocate',
# 'full_time','linked_in_profile','phone','eeo_require','phone2','contract','graduate','internship','language','keyword','active','graduation_year',
# 'part_time','is_employee_referal','work_phone','home_phone','country_specific','employee','graduate_high_school','contact_id','nickname','maritalstatus',
# 'skills','statements','education_summary','experience','edu_details_json','experience_details_json','trigger_index_update_timestamp','personal_statements',
# 'availability','dependents','working_state','desired_industry_json','desired_functional_expertise_json','candidate_owner_json','tagged_resume_id',
# 'employment_type','status','desired_job_type_json','currency_type','deleted_timestamp','deleted_reason','deleted_by_user_id','current_location_id','personal_location_id','external_id',
# 'notice_period','salary_type','months_per_year','present_salary_rate','expected_salary_from','expected_salary_to','no_of_dependents','note_by',
# 'note_on','total_jobs','payslip_email','highest_pcid','hot_end_date','current_bonus','desired_bonus','timezone','last_activity_date','note','external_map'
