# -*- coding: UTF-8 -*-
# import sys
# sys.path.append('D:\Tony\Working\DMvincere')
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
cf.read('pa_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% data connections
conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# assert False
# %% company
company = pd.read_sql("""
select c.idcompany, c.companyname, u.useremail, c.createdby
from company c
left join "user" u on u.iduser = c.iduser
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
,'6241')
""", engine_postgre_src)

contact = pd.read_sql("""
select p.idperson, p.firstname, p.lastname, p.emailprivate, cont.idcompany
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
where cont.rn = 1
""", engine_postgre_src)

candidate = pd.read_sql("""
select px.idperson
     , px.firstname
     , px.lastname
     , px.emailprivate
     , px.useremail
     , ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
from candidate c
join (select personx.idperson, personx.firstname, personx.lastname, emailprivate, personx.createdon, u.useremail from personx left join "user" u on u.iduser = personx.iduser where isdeleted = '0') px on c.idperson = px.idperson

""", engine_postgre_src)
# assert False
# %% transpose
company.rename(columns={
    'idcompany': 'company-externalId',
    'companyname': 'company-name',
    'useremail': 'company-owners',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'idperson': 'contact-externalId',
    'idcompany': 'contact-companyId',
    'firstname': 'contact-firstName',
    'lastname': 'contact-lastName',
    'emailprivate': 'contact-email',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

candidate.rename(columns={
    'idperson': 'candidate-externalId',
    'firstname': 'candidate-firstName',
    'lastname': 'candidate-lastName',
    'emailprivate': 'candidate-email',
    'useremail': 'candidate-owners',
}, inplace=True)
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)

# %% to csv files
candidate.to_csv(os.path.join(standard_file_upload, '6_candidate.csv'), index=False)
contact.to_csv(os.path.join(standard_file_upload, '4_contact.csv'), index=False)
company.to_csv(os.path.join(standard_file_upload, '2_company.csv'), index=False)

if len(default_company):
    default_company.to_csv(os.path.join(standard_file_upload, '1_default_company.csv'), index=False)