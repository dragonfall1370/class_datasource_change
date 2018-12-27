# -*- coding: UTF-8 -*-
import numpy as np
import pandas as pd
import vincere_custom_migration
import logger.logger
import connection_string
import pymssql
import psycopg2
import datetime

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

mylog = logger.logger.get_logger('hojona.log')

fr_mentalhealth = connection_string.client_hojona_mentalhealth
fr_socialcare = connection_string.client_hojona_socialcare
to_mentalhealth = connection_string.production_hojona_mentalhealth
to_socialworkers = connection_string.production_hojona_socialworkers
sdbconn_men = pymssql.connect(server=fr_mentalhealth.get('server'), user=fr_mentalhealth.get('user'), password=fr_mentalhealth.get('password'), database=fr_mentalhealth.get('database'), as_dict=True)
ddbconn_men = psycopg2.connect(host=to_mentalhealth.get('server'), user=to_mentalhealth.get('user'), password=to_mentalhealth.get('password'), database=to_mentalhealth.get('database'), port=to_mentalhealth.get('port'))
sdbconn_soc = pymssql.connect(server=fr_socialcare.get('server'), user=fr_socialcare.get('user'), password=fr_socialcare.get('password'), database=fr_socialcare.get('database'), as_dict=True)
ddbconn_soc = psycopg2.connect(host=to_socialworkers.get('server'), user=to_socialworkers.get('user'), password=to_socialworkers.get('password'), database=to_socialworkers.get('database'), port=to_socialworkers.get('port'))


df_men = pd.concat(
        [pd.read_csv('data_file/candidate_mental_health.csv'),
         pd.read_csv('data_file/contact_mental_health.csv')])

df_soc = pd.concat([pd.read_csv('data_file/candidate_socialcare.csv'),
                    pd.read_csv('data_file/contact_socialcare.csv')])

# Department: 22383eaa54ecaa86c5592883c422e8e8
len(df_men['DEPARTMENT'].unique())
df_soc_dept = df_soc['DEPARTMENT'].unique()
df_soc_dept = [x for x in df_soc_dept if str(x) != 'nan']
df_soc_dept.sort()


df_men_st = df_men[['CONTACTID','DEPARTMENT']]
df_men_st = df_men_st[df_men_st.DEPARTMENT.notnull()]
df_men_cand = df_men_st.merge(pd.read_sql("select id, external_id from candidate", ddbconn_men), left_on='CONTACTID', right_on='external_id')
df_men_cont = df_men_st.merge(pd.read_sql("select id, external_id from contact", ddbconn_men), left_on='CONTACTID', right_on='external_id')

df_soc_st = df_soc[['CONTACTID','DEPARTMENT']]
df_soc_st = df_soc_st[df_soc_st.DEPARTMENT.notnull()]

df_soc_cand = df_soc_st.merge(pd.read_sql("select id as additional_id, external_id from candidate", ddbconn_soc), left_on='CONTACTID', right_on='external_id')
df_soc_cont = df_soc_st.merge(pd.read_sql("select id as additional_id, external_id from contact", ddbconn_soc), left_on='CONTACTID', right_on='external_id')

if False:
    # soc cand: additional_form_values
    sql = """
    select a.translate, 'add_cand_info' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.form_id as constraint_id
    from configurable_form_language a
    join configurable_form_field_value b on a.language_code=b.title_language_code
    where field_id in (select id from configurable_form_field where  field_key ='c9b469ec51ce19c7a593cf4a70559bbb')
    and form_id in (select form_id from configurable_form_field where  field_key ='c9b469ec51ce19c7a593cf4a70559bbb')
    """
    df_soc_cand = df_soc_cand.merge(pd.read_sql(sql, ddbconn_soc), left_on='DEPARTMENT', right_on='translate')
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp']
    df_soc_cand['insert_timestamp'] = datetime.datetime.now()
    vincere_custom_migration.psycopg2_bulk_insert(df_soc_cand, ddbconn_soc,cols, 'additional_form_values')


if False:
    # for contact: aefe09350c67f75bc0d5233f721a167e
    sql = """
    select a.translate, 'add_con_info' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.field_id as constraint_id
    from configurable_form_language a
    join configurable_form_field_value b on a.language_code=b.title_language_code
    where field_id in (select id from configurable_form_field where  field_key ='aefe09350c67f75bc0d5233f721a167e')
    and form_id in (select form_id from configurable_form_field where  field_key ='aefe09350c67f75bc0d5233f721a167e')
    """
    df_soc_cont = df_soc_cont.merge(pd.read_sql(sql, ddbconn_soc), left_on='DEPARTMENT', right_on='translate')
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp']
    df_soc_cont['insert_timestamp'] = datetime.datetime.now()
    vincere_custom_migration.psycopg2_bulk_insert(df_soc_cont, ddbconn_soc,cols, 'additional_form_values')

