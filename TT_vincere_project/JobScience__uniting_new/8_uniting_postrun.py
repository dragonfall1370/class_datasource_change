# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import psycopg2
import re
import pymssql
import warnings
from common import vincere_common
import os
import datetime
import common.vincere_standard_migration as vincere_standard_migration
import common.vincere_custom_migration as vincere_custom_migration
import pandas as pd
from dateutil.relativedelta import relativedelta
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('un_config.ini')
mylog = log.get_info_logger(cf['default'].get('log_file'))
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
review_db = cf[cf['default'].get('dest_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)


ddbconn = psycopg2.connect(host=review_db.get('server'), user=review_db.get('user'), password=review_db.get('password'), database=review_db.get('database'), port=review_db.get('port'))
ddbconn.set_client_encoding('UTF8')

# vincere_custom_migration.run_after_standard_upload('pound', ddbconn)
assert False

# %%-----------------------------------------------
destination_db_connection = ddbconn
default_currency = 'pound'
offer = pd.read_sql("""
select
currency_type
, position_type
, employment_type
, working_hour_per_day, working_day_per_week, working_hour_per_week, working_day_per_month, working_week_per_month
, position_candidate_id, id
 from offer;
""", destination_db_connection)

#
# load position_candidates have offers
position_candidate = pd.read_sql("""
select
currency_type
, status
, candidate_id
, offer_date
, placed_date
, hire_date
, work_start_date
, position_description_id, id
from position_candidate where id in (select position_candidate_id from offer)
and work_start_date is null
""", destination_db_connection)

position_candidate['work_start_date'] = position_candidate.apply(lambda x: x['offer_date'] + relativedelta(months=+1), axis=1)

vincere_custom_migration.psycopg2_bulk_update(position_candidate, destination_db_connection, ['work_start_date', ], ['id', ], 'position_candidate')

#
# load position_descriptions have offers
position_description = pd.read_sql("""
select 
    currency_type, id
    , company_id, contact_id 
from position_description where id in (select position_description_id from position_candidate where id in (select position_candidate_id from offer))
""", destination_db_connection)

#
# load candidates have offers
candidate = pd.read_sql("""
select 
male, phone, current_location_id, gender_title, email as cand_email, id 
from candidate where id in (select candidate_id from position_candidate where id in (select position_candidate_id from offer))
""", destination_db_connection)

#
# load companies have offers
company = pd.read_sql("""
select 
id, name as comp_name, user_account_id
from company where id in (
select company_id from position_description where id in (select position_description_id from position_candidate where id in (select position_candidate_id from offer))
)
""", destination_db_connection)

#
# load companies' locations have offers
company_location = pd.read_sql("""
select 
min(id) as client_billing_location_id, company_id 
from company_location 
where company_id in (
    select company_id from position_description where id in (select position_description_id from position_candidate where id in (select position_candidate_id from offer))
) group by company_id
""", destination_db_connection)

#
# load contacts have offers
contact = pd.read_sql("""
    select 
    id, first_name, middle_name, last_name, email as cont_email, phone as cont_phone
    from contact where id in (
    select contact_id from position_description where id in (select position_description_id from position_candidate where id in (select position_candidate_id from offer))
    )
    """, destination_db_connection)
contact.fillna('', inplace=True)
contact['cont_name'] = contact.apply(lambda x: ' '.join([x.first_name, x.middle_name, x.last_name]).replace('  ', ' '), axis=1)

#
# set default currency for job
updf = position_description[position_description['currency_type'].isnull()]
updf['currency_type'].fillna(default_currency, inplace=True)

vincere_custom_migration.psycopg2_bulk_update(updf, destination_db_connection, ['currency_type', ], ['id', ], 'position_description')
position_description['currency_type'].fillna(default_currency, inplace=True)

#
# set default currency for offer based on job's currency
position_candidate.rename(columns={'id': 'position_candidate_id'}, inplace=True)
offer = offer.merge(position_candidate, on='position_candidate_id')
position_description.rename(columns={'id': 'position_description_id'}, inplace=True)
offer = offer.merge(position_description, on='position_description_id')
candidate.rename(columns={'id': 'candidate_id'}, inplace=True)
offer = offer.merge(candidate, on='candidate_id')
company.rename(columns={'id': 'company_id'}, inplace=True)
offer = offer.merge(company, on='company_id')
offer = offer.merge(company_location, on='company_id', how='left')
contact.rename(columns={'id': 'contact_id'}, inplace=True)
offer = offer.merge(contact, on='contact_id')

# id_x: offer id - needed for bulk update
# offer['id'] = offer['id_x']
# set default working_hour_per_day

offer['working_hour_per_day'].fillna(8, inplace=True)
# set default working_day_per_week
offer['working_day_per_week'].fillna(5, inplace=True)
# set default working_hour_per_week
offer['working_hour_per_week'].fillna(8 * 5, inplace=True)
# set default working_day_per_month
offer['working_day_per_month'].fillna(22, inplace=True)
# set default working_week_per_month
offer['working_week_per_month'].fillna(4, inplace=True)
vincere_custom_migration.psycopg2_bulk_update(offer, destination_db_connection, ['currency_type', 'working_hour_per_day', 'working_day_per_week', 'working_hour_per_week', 'working_day_per_month', 'working_week_per_month'], ['id', ], 'offer')

#
# generate default offer_personal_info for each offer
offer_personal_info = pd.DataFrame()
offer_personal_info['offer_id'] = offer['id']
offer_personal_info['gender_title'] = offer['gender_title']
offer_personal_info['last_name'] = offer['last_name'] # contact last_name: WRONG SHOULD BE COME FROM CANDIDATE
offer_personal_info['first_name'] = offer['first_name'] # contact first_name: WRONG SHOULD BE COME FROM CANDIDATE
offer_personal_info['middle_name'] = offer['middle_name'] # contact middle_name: WRONG SHOULD BE COME FROM CANDIDATE
offer_personal_info['male'] = offer['male']
offer_personal_info['phone'] = offer['phone']
offer_personal_info['email'] = offer['cand_email']
offer_personal_info['offer_date'] = offer['offer_date']
offer_personal_info['placed_date'] = offer['offer_date']
# offer_personal_info['placed_date'] = offer['placed_date']
offer_personal_info['start_date'] = offer['work_start_date']
offer_personal_info['current_location_id'] = offer['current_location_id']
offer_personal_info['client_company_id'] = offer['company_id']
offer_personal_info['client_company_name'] = offer['comp_name']
offer_personal_info['client_contact_id'] = offer['contact_id']
offer_personal_info['client_contact_name'] = offer['cont_name']
offer_personal_info['client_contact_email'] = offer['cont_email']
offer_personal_info['client_contact_phone'] = offer['cont_phone']
offer_personal_info['client_tax_exempt'] = 0
offer_personal_info['terms'] = 0
offer_personal_info['tax_rate'] = 0
offer_personal_info['net_total'] = 0
offer_personal_info['other_invoice_items_total'] = 0
offer_personal_info['invoice_total'] = 0
offer_personal_info['use_profit'] = 1
offer_personal_info['offer_letter_signatory_user_id'] = offer['user_account_id']
offer_personal_info['export_data_to'] = 'other'
offer_personal_info['client_billing_location_id'] = offer['client_billing_location_id']

# check existed offer_personal_info
existed_offper_info = pd.read_sql("select offer_id from offer_personal_info", destination_db_connection)
offer_personal_info = offer_personal_info.loc[~offer_personal_info['offer_id'].isin(existed_offper_info['offer_id'])]
assert False
offer_personal_info['phone'] = offer_personal_info['phone'].apply(lambda x: x.split(',')[0] if x else x)
vincere_custom_migration.psycopg2_bulk_insert(offer_personal_info, destination_db_connection, offer_personal_info.columns, 'offer_personal_info')

cur = destination_db_connection.cursor()
cur.execute("update company_location set location_name=address where location_name is null and address is not null;")
destination_db_connection.commit()
cur.execute("update common_location set location_name=address where location_name is null and address is not null;")
destination_db_connection.commit()
cur.execute("update user_account set timezone=(select timezone from user_account where id=-10) where timezone is null;")
destination_db_connection.commit()
cur.execute(r"update company set note=replace(note, '\n', chr(10)) where note is not null;")
destination_db_connection.commit()

# update offer.position_type by position_description.position_type
cur.execute(r"""
update offer set position_type = data.val
from (select o.id, pd.position_type from offer o
join position_candidate pc on o.position_candidate_id = pc.id
join position_description pd on pc.position_description_id = pd.id
) as data (id, val)
where offer.id = data.id;
""")
destination_db_connection.commit()
cur.close()

# reupdate candidate info
opi = pd.read_sql("""
select
    opi.id,
    c.gender_title,
    c.first_name,
    c.last_name,
    c.middle_name,
    c.male,
    c.phone,
    c.home_phone,
    c.email,
    c.address1,
    c.city,
    c.zipcode,
    c.country,
    c.state,
    c.date_of_birth,
    c.nickname as preferred_name,
    c.current_location_id
from offer_personal_info opi
join offer o on opi.offer_id = o.id
join position_candidate pc on o.position_candidate_id = pc.id
join candidate c on pc.candidate_id = c.id
--where opi.first_name like '%DEFAULT%'
""", destination_db_connection)
# vincere_custom_migration.execute_sql_update("select * into offer_personal_info_bk20190529 from offer_personal_info", ddbconn)
opi['phone'] = opi['phone'].apply(lambda x: x.split(',')[0] if x else x)
# opi = opi.where(opi.notnull(), None)
vincere_custom_migration.psycopg2_bulk_update(opi, destination_db_connection, ['gender_title', 'first_name', 'last_name', 'middle_name', 'male', 'phone', 'home_phone', 'email', 'address1', 'city', 'zipcode', 'country', 'state', 'preferred_name', 'current_location_id'], ['id'], 'offer_personal_info')
tem2 = opi[['id', 'date_of_birth']].dropna()
tem2['date_of_birth'] = pd.to_datetime(tem2['date_of_birth'])
vincere_custom_migration.psycopg2_bulk_update(tem2, destination_db_connection, ['date_of_birth'], ['id'], 'offer_personal_info')