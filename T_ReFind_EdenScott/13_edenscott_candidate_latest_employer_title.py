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

if __name__ == '__main__':
    sdbconn = pymssql.connect(server=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), as_dict=True)

    ddbconn = psycopg2.connect(host=to.get('server'), user=to.get('user'), password=to.get('password'), database=to.get('database'), port=to.get('port'))
    ddbconn.set_client_encoding('UTF8')

    df_candidate0 = pd.read_csv(os.path.join(standard_file_upload, 'edenscott_candidate_0_50000.csv'))
    df_candidate1 = pd.read_csv(os.path.join(standard_file_upload, 'edenscott_candidate_1_50000.csv'))
    df_candidate2 = pd.read_csv(os.path.join(standard_file_upload, 'edenscott_candidate_2_37825.csv'))
    df_candidate = pd.concat([df_candidate0, df_candidate1, df_candidate2])

    df_vincere_candidate = pd.read_sql("""select id, experience_details_json, external_id from candidate""",
                                       ddbconn)
    df_vincere_candidate['external_id'] = df_vincere_candidate['external_id'].astype(np.int64)
    df_candidate = df_candidate.merge(df_vincere_candidate, left_on='candidate-externalId', right_on='external_id')
    df_candidate = df_candidate[df_candidate['candidate-company1'].notnull() & (df_candidate['candidate-company1'].str.strip() !='') & df_candidate['candidate-jobTitle1'].notnull()]
    # replace job title
    df_candidate['experience_details_json'] = df_candidate.apply(lambda x: re.sub(r',\"jobTitle\":\".*?\",|,\"jobTitle\":null,', (',"jobTitle":"%s",' % x['candidate-jobTitle1']), x['experience_details_json']), axis=1)
    df_candidate['experience_details_json'] = df_candidate.apply(lambda x: re.sub(r',\"currentEmployer\":null,|,\"currentEmployer\":\".*?\",', (',"currentEmployer":"%s",' % x['candidate-company1']), x['experience_details_json']), axis=1)


    list_values = []
    for index, row in df_candidate.iterrows():
        a_record = list()
        a_record.append(row['id'])
        a_record.append(row['candidate-company1'])
        a_record.append(row['candidate-jobTitle1'])
        a_record.append(row['experience_details_json'])
        list_values.append(
            tuple(a_record)
        )

    cur = ddbconn.cursor()
    sql = """
        update candidate_extension 
        set 
            current_employer=data.v1
            ,current_job_title=data.v2
        from (values %s) as data(id, v1, v2, v3)
        where 
            candidate_extension.candidate_id=data.id 
          """
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    ddbconn.commit()

    sql = """
        update candidate_work_history 
        set 
            current_employer=data.v1
            ,job_title=data.v2
        from (values %s) as data(id, v1, v2, v3)
        where 
            candidate_work_history.candidate_id=data.id 
          """
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    ddbconn.commit()

    sql = """
        update candidate 
        set 
            experience_details_json=data.v3
        from (values %s) as data(id, v1, v2, v3)
        where 
            candidate.id=data.id 
          """
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    ddbconn.commit()

    cur.close()


