# -*- coding: UTF-8 -*-
import connection_string
import pymssql
import psycopg2
import logger.logger
import vincere_custom_migration
import re
import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 2000)
pd.set_option('display.width', 1000)


def remove_unreadable_chars(x):
    x_fmt = x
    temp = re.findall(r'\\ltrch(.*?)}', x_fmt)
    if len(temp):
        x_fmt = ' '.join(temp)
    return x_fmt


mylog = logger.logger.get_logger('hojona.log')

fr_mentalhealth = connection_string.client_hojona_mentalhealth
fr_socialcare = connection_string.client_hojona_socialcare
to_mentalhealth = connection_string.production_hojona_mentalhealth
to_socialworkers = connection_string.production_hojona_socialworkers

sdbconn_men = pymssql.connect(server=fr_mentalhealth.get('server'), user=fr_mentalhealth.get('user'), password=fr_mentalhealth.get('password'), database=fr_mentalhealth.get('database'), as_dict=True)
ddbconn_men = psycopg2.connect(host=to_mentalhealth.get('server'), user=to_mentalhealth.get('user'), password=to_mentalhealth.get('password'), database=to_mentalhealth.get('database'), port=to_mentalhealth.get('port'))

sdbconn_soc = pymssql.connect(server=fr_socialcare.get('server'), user=fr_socialcare.get('user'), password=fr_socialcare.get('password'), database=fr_socialcare.get('database'), as_dict=True)
ddbconn_soc = psycopg2.connect(host=to_socialworkers.get('server'), user=to_socialworkers.get('user'), password=to_socialworkers.get('password'), database=to_socialworkers.get('database'), port=to_socialworkers.get('port'))

sql = """
select
b.COMPANYID as company_external_id,
c.CONTACTID as contact_external_id,
c.CONTACTID as candidate_external_id,
null as position_external_id,
28953 as user_account_id,
a.CREATEDATE as insert_timestamp,
'comment' as category,
a.CREATEUSERID,
a.REGARDING as content
from TBL_ACTIVITY a
left join TBL_COMPANY_ACTIVITY b on a.ACTIVITYID=b.ACTIVITYID
left join TBL_CONTACT_ACTIVITY c on a.ACTIVITYID=c.ACTIVITYID
where a.REGARDING is not null and a.REGARDING != ''
union
select
b.COMPANYID as company_external_id,
c.CONTACTID as contact_external_id,
c.CONTACTID as candidate_external_id,
null as position_external_id,
28953 as user_account_id,
a.CREATEDATE as insert_timestamp,
'comment' as category,
a.CREATEUSERID,
concat(a.REGARDING, ' ', a.DETAILS) as content
from TBL_HISTORY a
left join TBL_COMPANY_HISTORY b on a.HISTORYID=b.HISTORYID
left join TBL_CONTACT_HISTORY c on a.HISTORYID=c.HISTORYID
union 
select
b.COMPANYID as company_external_id,
c.CONTACTID as contact_external_id,
c.CONTACTID as candidate_external_id,
null as position_external_id,
			 28953 as user_account_id,
a.CREATEDATE as insert_timestamp,
'comment' as category,
a.CREATEUSERID,
a.NOTETEXT as content
from TBL_NOTE a
left join TBL_COMPANY_NOTE b on a.NOTEID=b.NOTEID
left join TBL_CONTACT_NOTE c on a.NOTEID=c.NOTEID
"""

df_act_men = pd.read_sql(sql, sdbconn_men)
df_act_soc = pd.read_sql(sql, sdbconn_soc)

df_act_men['content'] = df_act_men.apply(lambda x: remove_unreadable_chars(x['content']), axis=1)
df_act_men['content'] = df_act_men.apply(lambda x: x['content'].replace('\r', '\n'), axis=1)
df_act_men['content'] = df_act_men.apply(lambda x: re.sub(r'[\n]{2,}', '\n', x['content']), axis=1)

df_act_soc['content'] = df_act_soc.apply(lambda x: remove_unreadable_chars(x['content']), axis=1)
df_act_soc['content'] = df_act_soc.apply(lambda x: x['content'].replace('\r', '\n'), axis=1)
df_act_soc['content'] = df_act_soc.apply(lambda x: re.sub(r'[\n]{2,}', '\n', x['content']), axis=1)

# vincere_custom_migration.insert_activities_1(df_act_men, ddbconn_men, mylog, delete_flag=True)
vincere_custom_migration.insert_activities_1(df_act_soc, ddbconn_soc, mylog, delete_flag=True)
