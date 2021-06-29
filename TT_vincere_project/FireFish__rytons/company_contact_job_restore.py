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

# job_bkup = pd.read_sql("""select * from position_description where external_id is null""", engine_postgre_bkup)
# contact_bkup = pd.read_sql("""select * from contact where external_id is null""", engine_postgre_bkup)
# company_bkup = pd.read_sql("""select * from company""", engine_postgre_bkup)
# comp = company_bkup.merge(contact_bkup[['id', 'company_id', 'email', 'first_name', 'last_name']], right_on='company_id', left_on='id')
# comp['external_id'].values
# comp.loc[comp['external_id'] == '222']

# '222',
#        '1129', '1710', '291', '50', '334', '320', '341', '840','1155', '1432','1659',
#        '1681', '1713''1682', '1682','1371'
# col_comp = list(company_bkup.columns)
# col_comp.pop()
# vincere_custom_migration.psycopg2_bulk_insert_tracking(company_bkup, ddbconn,col_comp, 'company', mylog)
# vincere_custom_migration.psycopg2_bulk_insert_tracking(activity_candidate, ddbconn,col_activity_cand, 'activity_candidate', mylog)
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem, ddbconn, [col_to_update], ['id'],'candidate', mylog)

# contact_bkup = pd.read_sql("""select * from contact where id in (33084
# ,33085
# ,33090
# ,33091
# ,33095
# ,33100
# ,33105
# ,33110
# ,33113
# ,33119
# ,33120
# ,33121
# ,33122
# ,33123
# ,33124
# ,33125
# ,33127
# ,33128
# ,33129
# ,33131
# ,33134
# ,33135
# ,33136
# ,33139
# ,33146
# ,33154
# ,33155
# ,33159
# ,33160
# ,33164
# ,70788
# ,70795
# ,70797
# ,70801
# ,70803
# ,70806
# ,70816
# # )""", engine_postgre_bkup)
#
# col_cont = list(contact_bkup.columns)
# col_cont.pop()
# vincere_custom_migration.psycopg2_bulk_insert_tracking(contact_bkup, ddbconn,col_cont, 'contact', mylog)

# job_bkup = pd.read_sql("""select * from position_description where id in (32172
# ,32127
# ,32059
# ,32041
# ,32050
# ,32060
# ,32075
# ,32076
# ,32077
# ,32067
# ,32080
# ,32047
# ,32073
# ,32042
# ,32065
# ,32091
# ,32163
# ,32122
# ,32165
# ,32078
# ,32088
# ,32081
# ,32079
# ,32074
# ,32048
# ,32051
# ,32090
# ,32054
# ,32043
# ,32106
# ,32107
# ,32108
# ,32095
# ,32049
# ,32150
# ,32062
# ,32063
# ,32100
# ,32045
# ,32102
# ,32072
# ,32126
# ,32040
# ,32066
# ,32061
# ,32069
# ,32046
# ,32044
# ,32068
# ,32070
# ,32064
# ,32112
# ,32113
# ,32121
# ,32151
# ,32052
# ,32053
# ,32103
# ,32109
# ,32092
# )""", engine_postgre_bkup)
#
# col_job = list(job_bkup.columns)
# col_job.remove('company_location_id')
# # col_job.pop()
# vincere_custom_migration.psycopg2_bulk_insert_tracking(job_bkup, ddbconn,col_job, 'position_description', mylog)


job_bkup = pd.read_sql("""select * from position_description where id in (32172
,32127
,32059
,32041
,32050
,32060
,32075
,32076
,32077
,32067
,32080
,32047
,32073
,32042
,32065
,32091
,32163
,32122
,32165
,32078
,32088
,32081
,32079
,32074
,32048
,32051
,32090
,32054
,32043
,32106
,32107
,32108
,32095
,32049
,32150
,32062
,32063
,32100
,32045
,32102
,32072
,32126
,32040
,32066
,32061
,32069
,32046
,32044
,32068
,32070
,32064
,32112
,32113
,32121
,32151
,32052
,32053
,32103
,32109
,32092
)""", engine_postgre_bkup)
#
# col_job = list(job_bkup.columns)
# col_job.remove('company_location_id')
# # col_job.pop()
# vincere_custom_migration.psycopg2_bulk_insert_tracking(job_bkup, ddbconn,col_job, 'position_description', mylog)



# compensation_bkup = pd.read_sql("""select * from compensation where position_id in (32172
# ,32127
# ,32059
# ,32041
# ,32050
# ,32060
# ,32075
# ,32076
# ,32077
# ,32067
# ,32080
# ,32047
# ,32073
# ,32042
# ,32065
# ,32091
# ,32163
# ,32122
# ,32165
# ,32078
# ,32088
# ,32081
# ,32079
# ,32074
# ,32048
# ,32051
# ,32090
# ,32054
# ,32043
# ,32106
# ,32107
# ,32108
# ,32095
# ,32049
# ,32150
# ,32062
# ,32063
# ,32100
# ,32045
# ,32102
# ,32072
# ,32126
# ,32040
# ,32066
# ,32061
# ,32069
# ,32046
# ,32044
# ,32068
# ,32070
# ,32064
# ,32112
# ,32113
# ,32121
# ,32151
# ,32052
# ,32053
# ,32103
# ,32109
# ,32092
# )""", engine_postgre_bkup)
#
# col_compensation = list(compensation_bkup.columns)
# col_compensation.remove('company_location_id')
# # col_job.pop()
# vincere_custom_migration.psycopg2_bulk_insert_tracking(compensation_bkup, ddbconn,col_compensation, 'compensation', mylog)




# position_agency_consultant_bkup = pd.read_sql("""select * from position_agency_consultant where position_id in (32172
# ,32127
# ,32059
# ,32041
# ,32050
# ,32060
# ,32075
# ,32076
# ,32077
# ,32067
# ,32080
# ,32047
# ,32073
# ,32042
# ,32065
# ,32091
# ,32163
# ,32122
# ,32165
# ,32078
# ,32088
# ,32081
# ,32079
# ,32074
# ,32048
# ,32051
# ,32090
# ,32054
# ,32043
# ,32106
# ,32107
# ,32108
# ,32095
# ,32049
# ,32150
# ,32062
# ,32063
# ,32100
# ,32045
# ,32102
# ,32072
# ,32126
# ,32040
# ,32066
# ,32061
# ,32069
# ,32046
# ,32044
# ,32068
# ,32070
# ,32064
# ,32112
# ,32113
# ,32121
# ,32151
# ,32052
# ,32053
# ,32103
# ,32109
# ,32092
# )""", engine_postgre_bkup)
#
# col_position_agency_consultant = list(position_agency_consultant_bkup.columns)
# col_position_agency_consultant.remove('company_location_id')
# # col_job.pop()
# vincere_custom_migration.psycopg2_bulk_insert_tracking(position_agency_consultant_bkup, ddbconn,col_position_agency_consultant, 'position_agency_consultant', mylog)



# company = pd.read_sql("""select * from company where external_id is null""", engine_postgre_review)
#
# activity_compnay = pd.read_sql("""select * from activity_company""", engine_postgre_bkup)
# activity_compnay = activity_compnay.merge(company[['id']], left_on='company_id', right_on='id')
#
# activity = pd.read_sql("""select * from activity""", engine_postgre_bkup)
# activity = activity.merge(activity_compnay[['activity_id']], left_on='id', right_on='activity_id')
#
# activity_prod = pd.read_sql("""select * from activity""", engine_postgre_review)
# activity = activity.loc[~activity['id'].isin(activity_prod['id'])]
#
# col_activity = list(activity.columns)
# col_activity.pop()
# col_activity.remove('contact_id')
# col_activity.remove('candidate_id')
#
# col_activity_comp = list(activity_compnay.columns)
# col_activity_comp.pop()
#
# vincere_custom_migration.psycopg2_bulk_insert_tracking(activity, ddbconn,col_activity, 'activity', mylog)
# vincere_custom_migration.psycopg2_bulk_insert_tracking(activity_compnay, ddbconn,col_activity_comp, 'activity_company', mylog)

# activity_contact = pd.read_sql("""select * from activity_contact where contact_id in (33084
# ,33085
# ,33090
# ,33091
# ,33095
# ,33100
# ,33105
# ,33110
# ,33113
# ,33119
# ,33120
# ,33121
# ,33122
# ,33123
# ,33124
# ,33125
# ,33127
# ,33128
# ,33129
# ,33131
# ,33134
# ,33135
# ,33136
# ,33139
# ,33146
# ,33154
# ,33155
# ,33159
# ,33160
# ,33164
# ,70788
# ,70795
# ,70797
# ,70801
# ,70803
# ,70806
# ,70816)""", engine_postgre_bkup)
# # activity_compnay = activity_compnay.merge(company[['id']], left_on='company_id', right_on='id')
#
# activity = pd.read_sql("""select * from activity""", engine_postgre_bkup)
# activity = activity.merge(activity_contact[['activity_id']], left_on='id', right_on='activity_id')
#
# activity_prod = pd.read_sql("""select * from activity""", engine_postgre_review)
# activity = activity.loc[~activity['id'].isin(activity_prod['id'])]
#
# col_activity = list(activity.columns)
# col_activity.pop()
# col_activity.remove('contact_id')
# col_activity.remove('candidate_id')
#
# col_activity_cont = list(activity_contact.columns)
# col_activity_cont.pop()
#
# vincere_custom_migration.psycopg2_bulk_insert_tracking(activity, ddbconn,col_activity, 'activity', mylog)
# vincere_custom_migration.psycopg2_bulk_insert_tracking(activity_contact, ddbconn,col_activity_cont, 'activity_contact', mylog)


activity_job = pd.read_sql("""select * from activity_job where job_id in (32172
,32127
,32059
,32041
,32050
,32060
,32075
,32076
,32077
,32067
,32080
,32047
,32073
,32042
,32065
,32091
,32163
,32122
,32165
,32078
,32088
,32081
,32079
,32074
,32048
,32051
,32090
,32054
,32043
,32106
,32107
,32108
,32095
,32049
,32150
,32062
,32063
,32100
,32045
,32102
,32072
,32126
,32040
,32066
,32061
,32069
,32046
,32044
,32068
,32070
,32064
,32112
,32113
,32121
,32151
,32052
,32053
,32103
,32109
,32092
)""", engine_postgre_bkup)
# activity_compnay = activity_compnay.merge(company[['id']], left_on='company_id', right_on='id')
#
# activity = pd.read_sql("""select * from activity""", engine_postgre_bkup)
# activity = activity.merge(activity_job[['activity_id']], left_on='id', right_on='activity_id')
#
# activity_prod = pd.read_sql("""select * from activity""", engine_postgre_review)
# activity = activity.loc[~activity['id'].isin(activity_prod['id'])]
#
#
# activity_job_prod = pd.read_sql("""select * from activity_job""", engine_postgre_review)
# activity_job = activity_job.loc[(~activity_job['activity_id'].isin(activity_job_prod['activity_id'])) & (~activity_job['job_id'].isin(activity_job_prod['job_id']))]
#
# col_activity = list(activity.columns)
# col_activity.pop()
# col_activity.remove('contact_id')
# col_activity.remove('candidate_id')
#
# col_activity_job = list(activity_job.columns)
# col_activity_job.pop()
#
# activity_job.loc[(activity_job['activity_id'] == 381) & (activity_job['job_id'] == 32080)]
# vincere_custom_migration.psycopg2_bulk_insert_tracking(activity, ddbconn,col_activity, 'activity', mylog)
# vincere_custom_migration.psycopg2_bulk_insert_tracking(activity_job, ddbconn,col_activity_job, 'activity_job', mylog)


pos_cand = pd.read_sql("""select * from position_candidate where position_description_id in (32172
,32127
,32059
,32041
,32050
,32060
,32075
,32076
,32077
,32067
,32080
,32047
,32073
,32042
,32065
,32091
,32163
,32122
,32165
,32078
,32088
,32081
,32079
,32074
,32048
,32051
,32090
,32054
,32043
,32106
,32107
,32108
,32095
,32049
,32150
,32062
,32063
,32100
,32045
,32102
,32072
,32126
,32040
,32066
,32061
,32069
,32046
,32044
,32068
,32070
,32064
,32112
,32113
,32121
,32151
,32052
,32053
,32103
,32109
,32092)
""", engine_postgre_bkup)
candidate = pd.read_sql("""select id as candidate_id from candidate""", engine_postgre_review)
pos_cand = pos_cand.merge(candidate, on='candidate_id')

col_pos_cand = list(pos_cand.columns)
col_pos_cand.remove('company_location_id')
# col_job.pop()
vincere_custom_migration.psycopg2_bulk_insert_tracking(pos_cand, ddbconn, col_pos_cand, 'position_candidate', mylog)


offer = pd.read_sql("""select * from offer""", engine_postgre_bkup)
offer = offer.merge(pos_cand[['id']], left_on='position_candidate_id', right_on='id')
offer = offer.rename(columns={'id_x':'id'})
col_offer = list(offer.columns)
col_offer.pop()
vincere_custom_migration.psycopg2_bulk_insert_tracking(offer, ddbconn, col_offer, 'offer', mylog)

offer_pf = pd.read_sql("""select * from offer_personal_info""", engine_postgre_bkup)
offer_pf = offer_pf.merge(offer[['id']], left_on='offer_id', right_on='id')
offer_pf = offer_pf.rename(columns={'id_x':'id'})
col_offer_pf = list(offer_pf.columns)
col_offer_pf.pop()
col_offer_pf.remove('client_billing_location_id')
vincere_custom_migration.psycopg2_bulk_insert_tracking(offer_pf, ddbconn, col_offer_pf, 'offer_personal_info', mylog)


invoice = pd.read_sql("""select * from invoice""", engine_postgre_bkup)
invoice = invoice.merge(offer[['id']], left_on='offer_id', right_on='id')
invoice = invoice.rename(columns={'id_x':'id'})
col_invoice = list(invoice.columns)
col_invoice.pop()
col_offer_pf.remove('client_billing_location_id')
vincere_custom_migration.psycopg2_bulk_insert_tracking(invoice, ddbconn, col_invoice, 'invoice', mylog)