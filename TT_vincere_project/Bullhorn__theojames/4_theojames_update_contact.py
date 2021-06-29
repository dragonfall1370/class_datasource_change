# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import datetime
import re
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
cf.read('theo_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)
# assert False
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% connect data
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

from common import vincere_contact, vincere_company
vcont = vincere_contact.Contact(engine_postgre.raw_connection())
vcom = vincere_company.Company(engine_postgre.raw_connection())

# %% contact
contact = pd.read_sql("""
select 
cont.clientId as contact_externalid
, com.company_externalid
, cont.firstName as contact_firstname
, cont.lastName as contact_lastname
, cont.middleName as contact_middlename
, cont.email as contact_email
, cont.dateAdded as reg_date
, cont.address1
, cont.address2
, cont.city
, cont.state
, cont.zip
, c.COUNTRY as country
, cont.mobile as mobile_phone
, cont.nickName as preferred_name
, cont.occupation as job_title
, cont.phone as primary_phone
, cont.phone2 as home_phone
, cont.email2 as personal_email
, cont.division as department
, cont.comments
, cont.customText2
, cont.companyDescription
, cont.employmentPreference
, cont.fax
, cont.namePrefix
, cont.preferredContact
, cont.referredBy
, cont.reportToUserID
, cont.status
, cont.businessSectorIDList
, cont.desiredCategories
, cont.desiredSkills
from bullhorn1.Client cont
join (
	select 
	com.clientCorporationID as company_externalid
	, com.name as company_name
	, com.dateAdded
	from bullhorn1.BH_ClientCorporation com
	join bullhorn1.BH_Department de on com.departmentID = de.departmentID
	where com.status != 'Archive' 
	and de.name = 'Theo James Recruitment Limited'
) com 
	on (cont.clientCorporationID = com.company_externalid)
left join tmp_country c on cont.countryID = c.CODE
where cont.isDeleted <>1 and cont.status != 'Archive'
;
""", engine_mssql)
contact.contact_externalid = contact.contact_externalid.astype(str)
contact.company_externalid = contact.company_externalid.astype(str)
assert False

# %% owner
cont_owner = pd.read_sql('select * from cont_owner', engine_sqlite)
cp9 = vcont.update_owner(cont_owner, mylog)

# %% industry
industry = contact[['contact_externalid', 'businessSectorIDList']]

industry = industry.businessSectorIDList.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(industry[['contact_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['contact_externalid'], value_name='businessSectorID') \
    .drop('variable', axis='columns') \
    .dropna()
industry = industry.loc[industry.businessSectorID.str.strip() != '']
industry.businessSectorID = industry.businessSectorID.astype(int)
industry = industry.merge(pd.read_sql("select businessSectorID, name from bullhorn1.BH_BusinessSectorList;", engine_mssql), on='businessSectorID')

from common import vincere_candidate
import datetime
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())
tem = industry[['name']].drop_duplicates()
tem['insert_timestamp'] = datetime.datetime.now()
vcand.append_industry(tem, mylog)
cp8 = vcont.insert_contact_industry(industry, mylog)


# %% department
vcont.update_department(contact, mylog)

# %% note 2
note = contact[[
    'contact_externalid'
                ]]

prefixs = [
    'BH ID'
]

note = note.where(note.notnull(), None)
note['note'] = note.apply(lambda x: '\n'.join([': '.join(e) for e in zip(prefixs, x) if e[1]]), axis='columns')
cp7 = vcont.update_note_2(note, dest_db, mylog)

# %% mobile
vcont.update_mobile_phone(contact, mylog)
vcont.update_primary_phone(contact, mylog)
vcont.update_home_phone(contact, mylog)
# %% nick name
vcont.update_preferred_name(contact, mylog)
vcont.update_job_title(contact, mylog)
vcont.update_personal_email(contact, mylog)

# %% reg date
vcont.update_reg_date(contact, mylog)

# %% location name/address
contact['location_name'] = contact[['address1', 'address2', 'city', 'state', 'zip', 'country']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
contact['address'] = contact.location_name

# %%
# assign contacts's addresses to their companies
comaddr = contact[['company_externalid', 'address']].drop_duplicates()
cp1 = vcom.insert_company_location(comaddr, mylog)
comaddr.loc[comaddr.company_externalid=='6948']

# %%
# assign the new addesses to contacts work location
tem2 = contact[['contact_externalid', 'company_externalid', 'address']]
cp2 = vcont.insert_contact_work_location(tem2, mylog)
tem2.loc[tem2.contact_externalid=='11134']

# %%
# %% city
comaddr = contact[['company_externalid', 'address', 'city', 'zip', 'state', 'country']].drop_duplicates()
cp3 = vcom.update_location_city_2(comaddr, dest_db, mylog)

# %% postcode
comaddr['post_code'] = comaddr.zip
cp4 = vcom.update_location_post_code_2(comaddr, dest_db, mylog)

# %% state
cp5 = vcom.update_location_state_2(comaddr, dest_db, mylog)

# %% country
comaddr['country_code'] = comaddr.country.map(vcom.get_country_code)
cp6 = vcom.update_location_country_2(comaddr, dest_db, mylog)