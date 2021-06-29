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
cf.read('saleexpert.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
db_source = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('review_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
connection_str = "mysql+pymysql://{user}:{password}@{server}/{db}" \
    .format(user=db_source.get('user')
            , password=db_source.get('password')
            , server=db_source.get('server')
            , db=db_source.get('database'))
engine = sqlalchemy.create_engine(connection_str)

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
# %%
sql = """
select cand.*
      , concat('SE',cand.user_id) as candidate_externalid
     , nullif(u_mail.reg_mail,'') as owner
     , nullif(emp_type.stellenart_de,'') as stellenart_de
     , nullif(country_code.countries_iso2,'') as  location_country_code
     , nullif(countries.names_text,'') as country_name
     , nullif(citizen.countries_iso2,'') as  nationality
     , nullif(cbh.text_de,'') as source
     , nullif(z.text_de,'') as marital
from (select u.user_id
     ,nullif(u.vorname,'') as vorname
     ,nullif(u.nachname,'') as nachname
     ,nullif(u.titel,'') as titel
     ,nullif(u.geschlecht,'') as geschlecht
     ,nullif(u.strasse,'') as strasse
     ,nullif(u.land,'') as land
     ,nullif(u.plz,'') as plz
     ,nullif(u.ort,'') as ort
     ,nullif(l.reg_mail,'') as reg_mail
     ,nullif(p.zeitpunkt,'') as zeitpunkt
     ,nullif(p.telefon,'') as telefon
     ,nullif(p.telefon_firma,'') as telefon_firma
     ,nullif(p.handy,'') as handy
     ,nullif(p.handy_firma,'') as handy_firma
     ,nullif(p.mail_firma,'') as mail_firma
     ,nullif(p.geburtsdatum,'') as geburtsdatum
     ,nullif(p.staatsangehoerigkeit,'') as staatsangehoerigkeit
     ,nullif(p.zuordnung_intern,'') as zuordnung_intern
     ,nullif(p.gesuchte_stellenart,'') as gesuchte_stellenart
     ,nullif(p.gehalt_ist,'') as gehalt_ist
     ,nullif(p.gehalt_soll,'') as gehalt_soll
     ,nullif(p.gehalt_aufteilung,'') as gehalt_aufteilung
     ,nullif(p.gesuchte_taetigkeit_1,'') as gesuchte_taetigkeit_1
     ,nullif(p.zivilstand,'') as zivilstand
     ,nullif(herkunft_bewerber,'') as herkunft_bewerber
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
left join user_login l on u.user_id = l.user_id
where p.profil_id is not null) cand
left join user_login u_mail on cand.zuordnung_intern = u_mail.user_id
left join cat_stellenart emp_type on cand.gesuchte_stellenart = emp_type.id
left join cat_countries_names countries on cand.land = countries.countries_id and countries.names_sprache = 'de'
left join cat_countries country_code on cand.land = country_code.countries_id
left join cat_countries citizen on cand.staatsangehoerigkeit = citizen.countries_id
left join cat_zivilstand z on cand.zivilstand = z.id
left join cat_bewerber_herkunft cbh on cand.herkunft_bewerber = cbh.id
order by cand.zeitpunkt asc
"""
candidate = pd.read_sql(sql, engine)
candidate = candidate.drop_duplicates()
assert False
# %% job type
jobtp = candidate[['candidate_externalid']]
jobtp['desired_job_type'] = 'permanent'
jobtp = jobtp.drop_duplicates()
cp = vcand.update_desired_job_type(jobtp, mylog)

# %% currency
tem = candidate[['candidate_externalid']]
tem['currency_of_salary'] = 'euro'
tem = tem.drop_duplicates()
cp = vcand.update_currency_of_salary(tem, mylog)


# %% current_salary
tem = pd.read_csv('Sales Experts - Salary - annual_salary.csv')
tem['candidate_externalid'] = tem['user_id'].astype(str)
tem['candidate_externalid'] = 'SE'+tem['candidate_externalid']
tem['annual_salary'] = tem['annual_salary'].apply(lambda x: x.replace(',',''))
tem['current_salary'] = tem['annual_salary'].astype(float)
cp = vcand.update_current_salary(tem, mylog)

# %% current_salary 2
tem = pd.read_csv('Sales Experts - Salary - annual_salary2.csv')
tem = tem.dropna()
tem['candidate_externalid'] = tem['user_id'].apply(lambda x: str(x).split('.')[0])
tem['candidate_externalid'] = 'SE'+tem['candidate_externalid']
tem['annual_salary'] = tem['annual_salary'].apply(lambda x: x.replace(',',''))
tem['current_salary'] = tem['annual_salary'].astype(float)
cp = vcand.update_current_salary(tem, mylog)

# %% current_salary
tem = pd.read_csv('Sales Experts - Salary - other benefits.csv')
tem['candidate_externalid'] = tem['user_id'].astype(str)
tem['candidate_externalid'] = 'SE'+tem['candidate_externalid']
cp = vcand.update_other_benefits(tem, mylog)

# %% address
tem = candidate[['candidate_externalid', 'strasse', 'ort', 'plz', 'country_name']].drop_duplicates()
tem['location_name'] = tem[['strasse', 'ort', 'plz', 'country_name']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem['address'] = tem['location_name']
cp2 = vcand.insert_common_location_v2(tem, dest_db, mylog)

#%%
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
city = candidate[['candidate_externalid','ort']].dropna().drop_duplicates()
city['city'] = city['ort']
vcand.update_location_city(city, mylog)
#
pc = candidate[['candidate_externalid', 'plz']].dropna().drop_duplicates()
pc['post_code'] = pc['plz']
vcand.update_location_post_code(pc, mylog)
#
ctry = candidate[['candidate_externalid', 'location_country_code']].dropna().drop_duplicates()
ctry['country_code'] = ctry['location_country_code']
vcand.update_location_country_code(ctry, mylog)

# %% employment type
# emp_type = candidate[['candidate_externalid', 'stellenart_de']].dropna()
# emp_type['employment_type'] = emp_type['stellenart_de']
# emp_type.loc[emp_type.employment_type.isin(['perm', 'contract', 'fulltime', 'Vollzeit']), 'employment_type'] = 0  # full time
# emp_type.loc[emp_type.employment_type.isin(['temp', 'parttime','Teilzeit']), 'employment_type'] = 1  # part time
# emp_type.loc[emp_type.employment_type.isin(['casual', 'consultant']), 'employment_type'] = 2  # casual
# emp_type.loc[emp_type.employment_type.isin(['labourhire', ]), 'employment_type'] = 3  # labour hire
# emp_type['candidate_externalid'] = emp_type['candidate_externalid'].astype(str)
# emp_type = emp_type.merge(vcand.candidate, on=['candidate_externalid'])
# emp_type['employment_type'] = pd.to_numeric(emp_type.employment_type, errors='coerce')
# emp_type = emp_type.loc[emp_type.employment_type.notnull()]
#
# vincere_custom_migration.psycopg2_bulk_update_tracking(emp_type, connection, ['employment_type', ], ['id', ], 'candidate', mylog)
# vcand.update_employment_type(emp_type, mylog)
#


# %% marital
tem = candidate[['candidate_externalid', 'marital']].dropna().drop_duplicates()
tem['marital'].unique()
tem.loc[tem['marital']=='ledig', 'maritalstatus'] = 1
tem.loc[tem['marital']=='verheiratet', 'maritalstatus'] = 2
tem.loc[tem['marital']=='geschieden', 'maritalstatus'] = 3
tem.loc[tem['marital']=='Lebensgemeinschaft', 'maritalstatus'] = 6
tem.loc[tem['marital']=='getrennt lebend', 'maritalstatus'] = 5
tem.loc[tem['marital']=='verwitwet', 'maritalstatus'] = 4
tem2 = tem[['candidate_externalid','maritalstatus']].dropna()
cp = vcand.update_marital_status(tem2, mylog)

# %% owner
# owner = candidate[['candidate_externalid','owner']].dropna().drop_duplicates()
# owner['email'] = owner['owner']
# tem2 = owner[['candidate_externalid', 'email']]
# tem2 = tem2.merge(pd.read_sql("select id, external_id as candidate_externalid, candidate_owner_json from candidate", vcand.ddbconn), on=['candidate_externalid'])
# tem2.candidate_owner_json.fillna('', inplace=True)
# tem2 = tem2.merge(pd.read_sql("select id as user_account_id, email from user_account", vcand.ddbconn), on='email')
# tem2['candidate_owner_json'] = tem2.user_account_id.map(lambda x: '{"ownerId":"%s"}' % x)
#
# tem2 = tem2.groupby('id').apply(lambda subdf: list(set(subdf.candidate_owner_json))).reset_index().rename(columns={0: 'candidate_owner_json'})
# tem2.candidate_owner_json = tem2.candidate_owner_json.map(lambda x: '[%s]' % ', '.join(x))
# tem2.candidate_owner_json = tem2.candidate_owner_json.astype(str).map(lambda x: x.replace("'", ''))
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, connection, ['candidate_owner_json', ], ['id', ], 'candidate', mylog)

# vcand.update_owner(owner, mylog)

# %% phones
home_phone = candidate[['candidate_externalid', 'telefon']].dropna().drop_duplicates()
home_phone['home_phone'] = home_phone['telefon']
cp = vcand.update_home_phone(home_phone, mylog)

primary_phone = candidate[['candidate_externalid', 'handy']].dropna().drop_duplicates()
primary_phone['primary_phone'] = primary_phone['handy']
cp = vcand.update_primary_phone(primary_phone, mylog)

work_phone = candidate[['candidate_externalid', 'telefon_firma']].dropna().drop_duplicates()
work_phone['work_phone'] = work_phone['telefon_firma']
cp = vcand.update_work_phone(work_phone, mylog)

mobile_phone = candidate[['candidate_externalid', 'handy', 'handy_firma']]
mobile_phone['mobile_phone'] = candidate[['handy', 'handy_firma']].apply(lambda x: ','.join([e for e in x if e]), axis=1)
mobile_phone = mobile_phone.loc[mobile_phone['mobile_phone'] != '']
cp = vcand.update_mobile_phone(mobile_phone, mylog)

# %% emails
work_email = candidate[['candidate_externalid', 'mail_firma']].dropna().drop_duplicates()
work_email['work_email'] = work_email['mail_firma']
cp = vcand.update_work_email(work_email, mylog)

# %% source
tem = candidate[['candidate_externalid', 'source']].dropna().drop_duplicates()
cp = vcand.insert_source(tem)

# %% current employer
cur_emp = candidate[['candidate_externalid', 'gesuchte_taetigkeit_1']].dropna().drop_duplicates()
cur_emp['current_employer'] = None
cur_emp['current_job_title'] = cur_emp['gesuchte_taetigkeit_1']
cur_emp['current_job_title'] = cur_emp['current_job_title'].apply(lambda x: x.replace('\u00AD','') if x else x)
cur_emp['current_job_title'] = cur_emp['current_job_title'].apply(lambda x: x.replace('\u00A0','') if x else x)
cur_emp['current_job_title'] = cur_emp['current_job_title'].apply(lambda x: x.replace('\x01','') if x else x)
vcand.update_candidate_current_employer_title(cur_emp, mylog)

# %% preferred name
tem = candidate[['candidate_externalid', 'titel']].dropna().drop_duplicates()
tem = tem.loc[tem['titel'] != '']
tem['preferred_name'] = tem['titel']
vcand.update_preferred_name(tem, mylog)

# %% citizenship
tem = candidate[['candidate_externalid', 'nationality']].dropna().drop_duplicates()
vcand.update_nationality(tem, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
salutation = candidate[['candidate_externalid', 'geschlecht']].dropna()
salutation.loc[salutation['geschlecht']=='m', 'Salutation'] = 'Mr'
salutation.loc[salutation['geschlecht']=='w', 'Salutation'] = 'Ms'
tem = salutation[['candidate_externalid', 'Salutation']].dropna().drop_duplicates()
tem['gender_title'] = tem['Salutation']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
vcand.update_gender_title(tem, mylog)

# %% note
note1 = pd.read_sql("""
select concat('SE',u.user_id) as user_id,
       nullif(u.titel,'') as titel,
       nullif(p.geburtsort,'') as geburtsort,
       nullif(p.berufsbereich,'') as berufsbereich,
       nullif(p.eingangsart,'') as eingangsart,
       nullif(p.status1,'') as status1,
       nullif(p.negativ,'') as negativ,
       nullif(p.erfahrung_fuehrung_mitarbeiter,'') as erfahrung_fuehrung_mitarbeiter,
       nullif(p.hobbies,'') as hobbies,
       nullif(p.profil_text_bewerber,'') as ich_suche,
       nullif(p.eigener_pkw,'') as eigener_pkw
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
where p.profil_id is not null""", engine)

note1 = note1.drop_duplicates()
# note1 = note1.where(note1.notnull(),None)

note2 = pd.read_sql("""
select concat('SE',u.user_id) as user_id,
       project.project,
       project.status,
       project.zum
from user_data u
left join (select up.projekt_id,
       up.user_id,
       p.titel as project,
       cat_status.kuerzel_de as status,
       up.status_datum as zum
from user_projekte up
left join projekte p on up.projekt_id = p.id
left join cat_bewerber_projektstatus cat_status on up.status = cat_status.id) project ON u.user_id = project.user_id;
""", engine)
note2 = note2.dropna()
note2 = note2.where(note1.notnull(),None)
# assert False
note2['zum'] = note2['zum'].astype(str)
note2['projekt'] = note2[['project', 'status', 'zum']].apply(lambda x: ' - '.join([e for e in x if e]), axis=1)
note2 = note2.groupby(['user_id'])['projekt'].apply(lambda x: ', '.join(x)).reset_index()
note1 = note1.merge(note2, left_on='user_id', right_on='user_id', how='left')

note1 = note1.merge(pd.read_sql("select id, text_de as Eingangsart from cat_eingangsart;", engine),  left_on='eingangsart', right_on='id', how='left')
note1 = note1.merge(pd.read_sql("select id, text_de as berufskategorie from cat_berufsbereich;", engine),  left_on='berufsbereich', right_on='id', how='left')
note1 = note1.merge(pd.read_sql("select id, kuerzel_de as status from cat_bewerber_status;", engine),  left_on='status1', right_on='id', how='left')
note1 = note1.merge(pd.read_sql("select id, text_de as personalverantwortung from cat_fuehrungserfahrung;", engine),  left_on='erfahrung_fuehrung_mitarbeiter', right_on='id', how='left')
# note1['eigener_pkw'] = note1['eigener_pkw'].apply(lambda x: 'Ja' if float(x) == 1.0 else '')
# note1 = note1.where(note1.notnull(),None)
note1.loc[note1['eigener_pkw']==1.0, 'eigener_pkw'] = 'Ja'
# note1['eigener_pkw'].unique()

# assert False
note1['user_id'] = note1['user_id'].astype(str)
note1 = note1.fillna('')
note1['note'] = note1[['titel', 'geburtsort', 'negativ',
                     'hobbies', 'ich_suche', 'eigener_pkw',
                     'projekt', 'Eingangsart', 'berufskategorie',
                     'status', 'personalverantwortung']] \
    .apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip(['\n\nTitel', '\n\nGeburtsort', '\n\nAnmerkungen (wird nicht publiziert)',
                                                                        '\n\nHobbies', '\n\nIch suche','\n\nPKW',
                                                                        '\n\nProjekt/Mandant', '\n\nEingangsart', '\n\nBerufskategorie',
                                                                        '\n\nStatus', '\n\nPersonalverantwortung'], x) if e[1]]), axis=1)\
    .map(lambda x: x.replace('\n', '<br/>'))
note = note1.loc[note1['note']!='']
note['candidate_externalid'] = note['user_id']
# note['note'] = note['note'].apply(lambda x: x.replace('nan',''))
# note.loc[note['note'].str.contains('nan')]
vcand.update_note(note, mylog)


# %% dob
tem = candidate[['candidate_externalid', 'geburtsdatum']].dropna().drop_duplicates()
tem = tem.loc[tem['geburtsdatum'] != '0000-00-00']
tem['date_of_birth'] = pd.to_datetime(tem['geburtsdatum'], errors='coerce')
vcand.update_date_of_birth(tem, mylog)



# %%
tem = candidate[['candidate_externalid', 'strasse', 'ort', 'plz', 'country_name']].drop_duplicates()
tem['location_name'] = tem[['strasse', 'ort', 'plz', 'country_name']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem['address'] = tem['location_name']

tem2 = tem[['candidate_externalid', 'address']]
tem2['address1'] = tem2['address']
candidate_location = pd.read_sql("""
select
cl.id as current_location_id,
c.id as candidate_id,
c.external_id as candidate_externalid,
c.first_name,
c.middle_name,
c.last_name
, cl.address
, cl.location_name
from candidate c
join common_location cl on c.current_location_id=cl.id
""", vcand.ddbconn)

tem3 = tem2.merge(candidate_location, on=['candidate_externalid'])
tem3['address'] = tem3['address1']
tem3['location_name'] = tem3['address']
tem3['id'] = tem3['current_location_id'].apply(lambda x: int(str(x).split('.')[0]))
vincere_custom_migration.psycopg2_bulk_update_tracking(tem3, vcand.ddbconn, ['address','location_name' ], ['id', ], 'common_location', mylog)