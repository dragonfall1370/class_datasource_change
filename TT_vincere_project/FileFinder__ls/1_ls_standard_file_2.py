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
cf.read('ls_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)

sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% data connections
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
# conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# assert False
# %% company

contact = pd.read_sql("""
select per.idperson, per.firstname, per.lastname, per.EmailWork, u.useremail
from PersonX per
left join "user" u on u.iduser = per.iduser
where idPerson not  in (
select p.idperson
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
left join "user" u on u.iduser = p.iduser
where cont.rn = 1
and p.isdeleted = '0')
""", engine_sqlite)
contact['idcompany'] = None
assert False
# %% transpose
contact.rename(columns={
    'idPerson': 'contact-externalId',
    'idcompany': 'contact-companyId',
    'FirstName': 'contact-firstName',
    'LastName': 'contact-lastName',
    'UserEmail': 'contact-owners',
    'EmailWork': 'contact-email',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)


# %% to csv files
contact.to_csv(os.path.join(standard_file_upload, '4_contact_more.csv'), index=False)
if len(default_company):
    default_company.to_csv(os.path.join(standard_file_upload, '1_default_company.csv'), index=False)