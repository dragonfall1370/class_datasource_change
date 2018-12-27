# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
from edenscott._edenscott_dtypes import *
import os
import psycopg2
import pymssql
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
from common import thread_pool as thread_pool
import pandas as pd
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)
#
# loading configuration
cf = configparser.RawConfigParser()
cf.read('_edenscott_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
locator = os.path.join(data_folder, 'locator')
fr = cf[cf['default'].get('src_db')]
to = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
#
# create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
pathlib.Path(locator).mkdir(parents=True, exist_ok=True)

def load_activities(act_csv_file):
    df_activities = pd.read_csv(os.path.join(data_input, act_csv_file), low_memory=False)
    df_activities['contact_external_id'] = df_activities['contact_external_id'].map(lambda x: str(x).strip())
    df_activities['candidate_external_id'] = df_activities['candidate_external_id'].map(lambda x: str(x).strip())
    df_activities['company_external_id'] = df_activities['company_external_id'].map(lambda x: str(x).strip())
    df_activities['position_external_id'] = df_activities['position_external_id'].map(lambda x: str(x).strip())
    df_activities['content'] = [x.replace('\r', '') for x in df_activities['content']]

    df_user_options = pd.read_csv(os.path.join(data_input, 'user_options.csv'))
    df_user_options_st = df_user_options[['UOemail', 'UOloginname', ]]

    df_vincere_contact = pd.read_sql("select id as contact_id, external_id from contact where deleted_timestamp is null", ddbconn)
    # df_vincere_contact.external_id = df_vincere_contact.external_id.astype(np.int64)
    df_vincere_candidate = pd.read_sql("select id as candidate_id, external_id from candidate where deleted_timestamp is null", ddbconn)
    # df_vincere_candidate.external_id = df_vincere_candidate.external_id.astype(np.int64)
    df_vincere_company = pd.read_sql("select id as company_id, external_id from company where deleted_timestamp is null", ddbconn)
    # df_vincere_company.external_id = df_vincere_company.external_id.astype(np.int64)
    df_vincere_position = pd.read_sql("select id as position_id, external_id from position_description where deleted_timestamp is null", ddbconn)
    # df_vincere_position.external_id = df_vincere_position.external_id.astype(np.int64)
    df_vincere_emails = pd.read_sql("select id as user_account_id, email from user_account", ddbconn)

    df_activities = df_activities.merge(df_user_options_st, left_on='userlogin', right_on='UOloginname', how='left')
    df_activities = df_activities.merge(df_vincere_emails, left_on='UOemail', right_on='email', how='left')
    df_activities = df_activities.merge(df_vincere_contact, left_on='contact_external_id', right_on='external_id', how='left')
    df_activities = df_activities.merge(df_vincere_candidate, left_on='candidate_external_id', right_on='external_id', how='left')
    df_activities = df_activities.merge(df_vincere_company, left_on='company_external_id', right_on='external_id', how='left')
    df_activities = df_activities.merge(df_vincere_position, left_on='position_external_id', right_on='external_id', how='left')
    df_activities = df_activities[df_activities.contact_id.notnull() | df_activities.candidate_id.notnull() | df_activities.company_id.notnull() | df_activities.position_id.notnull()]
    return df_activities

if __name__ == '__main__':
    # sdbconn = pymssql.connect(server=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), as_dict=True)

    ddbconn = psycopg2.connect(host=to.get('server'), user=to.get('user'), password=to.get('password'), database=to.get('database'), port=to.get('port'))
    ddbconn.set_client_encoding('UTF8')

if False:
    # df_activities = pd.concat([pd.read_csv(os.path.join(data_input, 'activities1.csv')), pd.read_csv(os.path.join(data_input, 'activities2.csv'))])
    df1 = load_activities('activities1.csv')
    df2 = load_activities('activities2.csv')
if False:
    act_inserted = pd.read_csv(os.path.join(data_input, 'activities_inserted.csv'), low_memory=False)

    df1['candidate_id'] = df1['candidate_id'].fillna(0)
    df1['candidate_id'] = df1['candidate_id'].astype(np.int64)
    df1['company_id'] = df1['company_id'].fillna(0)
    df1['company_id'] = df1['company_id'].astype(np.int64)
    df1['position_id'] = df1['position_id'].fillna(0)
    df1['position_id'] = df1['position_id'].astype(np.int64)
    df1['contact_id'] = df1['contact_id'].fillna(0)
    df1['contact_id'] = df1['contact_id'].astype(np.int64)

    df2['candidate_id'] = df2['candidate_id'].fillna(0)
    df2['candidate_id'] = df2['candidate_id'].astype(np.int64)
    df2['company_id'] = df2['company_id'].fillna(0)
    df2['company_id'] = df2['company_id'].astype(np.int64)
    df2['position_id'] = df2['position_id'].fillna(0)
    df2['position_id'] = df2['position_id'].astype(np.int64)
    df2['contact_id'] = df2['contact_id'].fillna(0)
    df2['contact_id'] = df2['contact_id'].astype(np.int64)

    act_inserted['candidate_id'] = act_inserted['candidate_id'].fillna(0)
    act_inserted['candidate_id'] = act_inserted['candidate_id'].astype(np.int64)
    act_inserted['company_id'] = act_inserted['company_id'].fillna(0)
    act_inserted['company_id'] = act_inserted['company_id'].astype(np.int64)
    act_inserted['position_id'] = act_inserted['position_id'].fillna(0)
    act_inserted['position_id'] = act_inserted['position_id'].astype(np.int64)
    act_inserted['contact_id'] = act_inserted['contact_id'].fillna(0)
    act_inserted['contact_id'] = act_inserted['contact_id'].astype(np.int64)

# %% load inserted activites. Run one time
if False:
    vincere_common.read_sql_to_csv1("""
    select company_id, contact_id, candidate_id, position_id, user_account_id, insert_timestamp, content, category, 'inserted' as checktype from activity
    order by id OFFSET %d ROWS FETCH NEXT %d ROWS ONLY
    """, ddbconn, os.path.join(data_input, 'activities_inserted.csv'), offset=0, chunk_size=10000)

# %% working
if False:
    # vincere_custom_migration.clean_activities(ddbconn, mylog)

    df1['content_test'] = df1['content'].map(lambda x: "".join(re.findall(r"[\w']+", x)) )
    df2['content_test'] = df2['content'].map(lambda x: "".join(re.findall(r"[\w']+", x)) )

    act_inserted = act_inserted[act_inserted['content'].notnull()]
    act_inserted['content_test'] = act_inserted['content'].map(lambda x: "".join(re.findall(r"[\w']+", x)) )

    # df1['test'] = df1.apply(lambda x: '%s%s%s%s' % (x['contact_id'], x['candidate_id'], x['company_id'], x['position_id']), axis=1)
    # act_inserted['test'] = act_inserted.apply(lambda x: '%s%s%s%s' % (x['contact_id'], x['candidate_id'], x['company_id'], x['position_id']), axis=1)
    # df1_test = df1.merge(act_inserted, on='test', how='outer')


    # merge inserted activities with client activities by outer, so that client activities needed to be inserted will be filterred out
    df1_test = df1.merge(act_inserted, on=['company_id', 'contact_id', 'candidate_id', 'position_id', 'content_test'], how='outer')
    df1_test_insert = df1_test[df1_test['checktype'].isnull()]

    df2_test = df2.merge(act_inserted, on=['company_id', 'contact_id', 'candidate_id', 'position_id', 'content_test'], how='left')
    df2_test_insert = df2_test[df2_test['checktype'].isnull()]

if False:

    # remove activities
    test = pd.read_sql("select count(*), min(id), max(id) from activity where insert_timestamp < '2018-12-06'", ddbconn)
    delfr = test.loc[0, 'min']
    for i in range(test.loc[0, 'min'], test.loc[0, 'max'], 5000):
        sql = 'delete from activity where id between %s and %s' % (delfr, i)
        print(sql)
        delfr = i
        cur = ddbconn.cursor()
        cur.execute(sql)
        ddbconn.commit()
    sql = 'delete from activity where id between %s and %s' % (i, test.loc[0, 'max'])
    cur = ddbconn.cursor()
    cur.execute(sql)
    ddbconn.commit()
if False:
    vincere_custom_migration.insert_activities(df1, ddbconn, mylog, False)
    vincere_custom_migration.insert_activities(df2, ddbconn, mylog, False)


if True:
    vincere_custom_migration.clean_reinsert_activity_mapping(ddbconn)

