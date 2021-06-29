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
cf.read('pa_config.ini')
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
# assert False
# %% candidate
sql = """
select px.*
from candidate c
join (select P.idperson as candidate_externalid, p.internationalvalue_string
               from personx P
where isdeleted = '0') px on c.idperson = px.candidate_externalid
"""
candidate = pd.read_sql(sql, engine_postgre_src)
candidate = candidate.dropna()
countries = candidate.internationalvalue_string.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(candidate[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='internationalvalue') \
    .drop('variable', axis='columns') \
    .dropna()
countries['internationalvalue'] = countries['internationalvalue'].str.strip()
df2 = pd.DataFrame({"candidate_externalid":[1, 2, 3,4],
                    "internationalvalue":['Slovenia','Bolivia','Guyana','Paraguay']})

countries.loc[countries['internationalvalue']=='India']
countries.loc[countries['internationalvalue']=='Indian', 'internationalvalue'] = 'India'
countries = countries.append(df2)

tem1 = pd.DataFrame(countries['internationalvalue'].value_counts().keys(), columns=['international'])
tem1['matcher'] = tem1['international'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem2 = pd.read_csv('international.csv')
tem2['matcher'] = tem2['Vincere International'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem3 = tem1.merge(tem2, on='matcher', suffixes=['', '_y'], how='outer', indicator=True)
tem3['Vincere International'].unique()
tem3 = tem3.loc[tem3['Vincere International'].notnull()]
tem4 = countries.merge(tem3, left_on='internationalvalue', right_on='Vincere International')

cand_international_api = 'ec4e9ffb271414d5beea9addc6a80d1a'
cand = tem4[['candidate_externalid', 'internationalvalue']]
cand = cand.drop_duplicates()
cand = cand.loc[cand.internationalvalue != '']
vincere_custom_migration.insert_candidate_muti_selection_checkbox(cand, 'candidate_externalid', 'internationalvalue', cand_international_api, connection)

# %% contact
sql = """
select cont_info.* from
(select p.idperson as contact_externalid
      , p.internationalvalue_string
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company
where companyid in ('5714'
,'5745'
,'5841'
,'4138'
,'4990'
,'1611'
,'236'
,'1494'
,'379'
,'5038'
,'650'
,'3038'
,'1046'
,'4020'
,'4063'
,'107'
,'1030'
,'2159'
,'3982'
,'2179'
,'5508'
,'757'
,'1792'
,'4634'
,'281'
,'3535'
,'4348'
,'5846'
,'5855'
,'5869'
,'5940'
,'5941'
,'5982'
,'5991'
,'5996'
,'5999'
,'6002'
,'6004'
,'6005'
,'6006'
,'6007'
,'6008'
,'6009'
,'6010'
,'6011'
,'6012'
,'6013'
,'6014'
,'6015'
,'6016'
,'6017'
,'6018'
,'6019'
,'6021'
,'6025'
,'6026'
,'6027'
,'6028'
,'6029'
,'6031'
,'6034'
,'6035'
,'6037'
,'6038'
,'6039'
,'6040'
,'6042'
,'6043'
,'6044'
,'6045'
,'6046'
,'6047'
,'6048'
,'6049'
,'6050'
,'6051'
,'6052'
,'6053'
,'6054'
,'6055'
,'6056'
,'6057'
,'6058'
,'6060'
,'6078'
,'6079'
,'6080'
,'6081'
,'6082'
,'6083'
,'6096'
,'6097'
,'6098'
,'6099'
,'6100'
,'6176'
,'6237'
,'6241')) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
where cont.rn = 1) cont_info
"""
contact = pd.read_sql(sql, engine_postgre_src)
contact = contact.dropna()
countries = contact.internationalvalue_string.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(contact[['contact_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['contact_externalid'], value_name='internationalvalue') \
    .drop('variable', axis='columns') \
    .dropna()
countries['internationalvalue'] = countries['internationalvalue'].str.strip()
df2 = pd.DataFrame({"contact_externalid":['Taiwan'
,'India'
,'Pakistan'
,'Lithuania'
,'Latvia'
,'Estonia'
,'Greece'
,'Slovenia'
,'Czech Republic'
,'Malta'
,'Cyprus'
,'Bulgaria'
,'Turkey'
,'Denmark'
,'Luxembourg'
,'Slovakia'
,'Portugal'
,'Finland'
,'Austria'
,'United Arab Emirates'
,'Mexico'
,'Argentina'
,'Peru'
,'Ecuador'
,'Chile'
,'Bolivia'
,'Guyana'
,'Venezuela'
,'Uruguay'
,'Paraguay'
,'Central America'
,'The Caribbean'
,'Oceania'
],
                    "internationalvalue":['Taiwan'
,'India'
,'Pakistan'
,'Lithuania'
,'Latvia'
,'Estonia'
,'Greece'
,'Slovenia'
,'Czech Republic'
,'Malta'
,'Cyprus'
,'Bulgaria'
,'Turkey'
,'Denmark'
,'Luxembourg'
,'Slovakia'
,'Portugal'
,'Finland'
,'Austria'
,'United Arab Emirates'
,'Mexico'
,'Argentina'
,'Peru'
,'Ecuador'
,'Chile'
,'Bolivia'
,'Guyana'
,'Venezuela'
,'Uruguay'
,'Paraguay'
,'Central America'
,'The Caribbean'
,'Oceania'
]})

countries.loc[countries['internationalvalue']=='India']
countries.loc[countries['internationalvalue']=='Indian', 'internationalvalue'] = 'India'
countries = countries.append(df2)

tem1 = pd.DataFrame(countries['internationalvalue'].value_counts().keys(), columns=['international'])
tem1['matcher'] = tem1['international'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem2 = pd.read_csv('international.csv')
tem2['matcher'] = tem2['Vincere International'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem3 = tem1.merge(tem2, on='matcher', suffixes=['', '_y'], how='outer', indicator=True)

tem3['Vincere International'].unique()
tem3 = tem3.loc[tem3['Vincere International'].notnull()]
tem4 = countries.merge(tem3, left_on='internationalvalue', right_on='Vincere International')

cont_international_api = '88083269acadddefb4fe5654f5066bfe'
cont = tem4[['contact_externalid', 'internationalvalue']]
cont = cont.drop_duplicates()
cont = cont.loc[cand.internationalvalue != '']
vincere_custom_migration.insert_contact_muti_selection_checkbox(cont, 'contact_externalid', 'internationalvalue', cont_international_api, connection)

