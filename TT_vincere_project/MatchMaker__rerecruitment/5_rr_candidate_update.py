# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
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
cf.read('rr_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

# %%
candidate = pd.read_sql("""
select CONCAT('',peo_no) as candidate_externalid, peo_forename, peo_surname
     , nullif(peo_title,'') as peo_title
     , nullif(peo_known,'') as peo_known
     , nullif(peo_establish,'') as peo_establish
     , nullif(peo_street,'') as peo_street
     , nullif(peo_district,'') as peo_district
     , nullif(peo_town,'') as peo_town
     , nullif(peo_county,'') as peo_county
     , nullif(peo_postcode,'') as peo_postcode
     , nullif(peo_country,'') as peo_country
     , nullif(peo_home_tel,'') as peo_home_tel
     , nullif(peo_other_tel,'') as peo_other_tel
     , nullif(peo_work_tel,'') as peo_work_tel
     , nullif(peo_branch,'') as peo_branch
     , nullif(peo_www,'') as peo_www
     , peo_reloc
     , peo_reg_date
     , nullif(peo_division,'') as peo_division
     , nullif(peo_source,'') as peo_source
     , nullif(peo_status,'') as peo_status
     , peo_date_birth
     , nullif(peo_nationality,'') as peo_nationality
     , peo_visa_expiry
     , nullif(peo_visa_type,'') as peo_visa_type
     , nullif(peo_licence,'') as peo_licence
     , nullif(peo_hgv_licence,'') as peo_hgv_licence
     , nullif(peo_transport,'') as peo_transport
     , nullif(peo_ni,'') as peo_ni
     , nullif(peo_empl_type,'') as peo_empl_type
     , nullif(peo_payroll_no,'') as peo_payroll_no
     , nullif(peo_awr_type,'') as peo_awr_type
     , nullif(peo_ltd_cli_name,'') as peo_ltd_cli_name
     , nullif(peo_ltd_reg_no,'') as peo_ltd_reg_no
     , nullif(peo_vat_code,'') as peo_vat_code
     , nullif(peo_bank_name,'') as peo_bank_name
     , nullif(peo_bank_acc_name,'') as peo_bank_acc_name
     , nullif(peo_cnt_accountno,'') as peo_cnt_accountno
     , nullif(peo_bank_sort_code,'') as peo_bank_sort_code
     , nullif(peo_bank_society_no,'') as peo_bank_society_no
     , nullif(peo_tpb,'') as peo_tpb
     , nullif(peo_langs,'') as peo_langs
     , nullif(peo_alert_color,'') as peo_alert_color
     , nullif(peo_alert_text,'') as peo_alert_text
from people
where peo_flag =1
""", engine_mssql)
assert False
# %% location name/address
c_location = candidate[['candidate_externalid','peo_establish', 'peo_street','peo_district','peo_town','peo_county','peo_postcode','peo_country']]
c_location['address'] = c_location[['peo_establish', 'peo_street','peo_district','peo_town','peo_county','peo_postcode','peo_country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
c_location['location_name'] = c_location[['peo_town','peo_county','peo_postcode','peo_country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
c_location = c_location.loc[c_location['address']!='']
cp2 = vcand.insert_common_location_v2(c_location, dest_db, mylog)
# update city
vcand = vincere_candidate.Candidate(engine_postgre_review.raw_connection())
comaddr = c_location[['candidate_externalid','peo_establish', 'peo_street','peo_district','peo_town','peo_county','peo_postcode','peo_country']].drop_duplicates()\
    .rename(columns={'peo_town': 'city', 'peo_county': 'state', 'peo_postcode': 'post_code', 'peo_establish': 'address_line1', 'peo_street': 'address_line2','peo_district': 'district'})

tem = comaddr[['candidate_externalid', 'address_line1']].dropna()
cp3 = vcand.update_address_line1_2(tem, dest_db, mylog)

tem = comaddr[['candidate_externalid', 'address_line2']].dropna()
cp3 = vcand.update_address_line2_2(tem, dest_db, mylog)

tem = comaddr[['candidate_externalid', 'city']].dropna()
cp3 = vcand.update_location_city2(tem, dest_db, mylog)
# update state
tem = comaddr[['candidate_externalid', 'state']].dropna()
cp4 = vcand.update_location_state2(tem, dest_db, mylog)
# update district
tem = comaddr[['candidate_externalid', 'district']].dropna()
cp4 = vcand.update_location_district(tem, mylog)
# update postcode
tem = comaddr[['candidate_externalid', 'post_code']].dropna()
cp5 = vcand.update_location_post_code2(tem, dest_db, mylog)
#  update country
from common import vincere_company
vcom = vincere_company.Company(connection)
tem = comaddr[['candidate_externalid','peo_country']].dropna()
tem['country_code'] = tem['peo_country'].map(vcom.get_country_code)
# tem['CandidateCountry'].unique()
cp6 = vcand.update_location_country_code(tem, mylog)
#
# tem = comaddr[['candidate_externalid']]
# tem['location_type'] = 'PERSONAL_ADDRESS'
# vcand.update_location_type(tem, mylog)
# %% home phones
home_phone = candidate[['candidate_externalid', 'peo_home_tel']].dropna().drop_duplicates()
home_phone['home_phone'] = home_phone['peo_home_tel']
cp = vcand.update_home_phone2(home_phone, dest_db, mylog)

# %% work phones
wphone = candidate[['candidate_externalid', 'peo_work_tel']].dropna().drop_duplicates()
wphone['work_phone'] = wphone['peo_work_tel']
cp = vcand.update_work_phone(wphone, mylog)

# %% mobile/primary phones
mphone = candidate[['candidate_externalid', 'peo_other_tel']].dropna().drop_duplicates()
mphone['mobile_phone'] = mphone['peo_other_tel']
mphone['primary_phone'] = mphone['mobile_phone']
cp = vcand.update_mobile_phone(mphone, mylog)
cp = vcand.update_primary_phone(mphone, mylog)

# %% knownas
tem = candidate[['candidate_externalid', 'peo_known']].dropna().drop_duplicates()
tem['preferred_name'] = tem['peo_known']
cp = vcand.update_preferred_name(tem, mylog)

# %% web
tem = candidate[['candidate_externalid', 'peo_www']].dropna().drop_duplicates()
tem['website'] = tem['peo_www']
cp = vcand.update_website(tem, mylog)

# %% reg_date
tem = candidate[['candidate_externalid','peo_reg_date']].dropna().drop_duplicates()
tem['peo_reg_date'] = tem['peo_reg_date'].astype(str)
tem['peo_reg_date_1'] = tem['peo_reg_date'].apply(lambda x: x[0:4] if x else x)
tem['peo_reg_date_2'] = tem['peo_reg_date'].apply(lambda x: x[4:6] if x else x)
tem['peo_reg_date_3'] = tem['peo_reg_date'].apply(lambda x: x[6:9] if x else x)
tem['peo_reg_date'] = tem['peo_reg_date_1'] +'-'+tem['peo_reg_date_2']+'-'+tem['peo_reg_date_3']
tem['reg_date'] = pd.to_datetime(tem['peo_reg_date'], format='%Y/%m/%d %H:%M:%S')
vcand.update_reg_date(tem, mylog)

# %% dob
tem = candidate[['candidate_externalid','peo_date_birth']].dropna().drop_duplicates()
tem = tem.loc[tem['peo_date_birth']!=0]
tem['peo_date_birth'] = tem['peo_date_birth'].astype(str)
tem['peo_date_birth_1'] = tem['peo_date_birth'].apply(lambda x: x[0:4] if x else x)
tem['peo_date_birth_2'] = tem['peo_date_birth'].apply(lambda x: x[4:6] if x else x)
tem['peo_date_birth_3'] = tem['peo_date_birth'].apply(lambda x: x[6:9] if x else x)
tem['peo_date_birth'] = tem['peo_date_birth_1'] +'-'+tem['peo_date_birth_2']+'-'+tem['peo_date_birth_3']
tem = tem.loc[tem['peo_date_birth_1']>'1900']
tem = tem.loc[tem['peo_date_birth_1']<'2021']
# tem = tem.loc[tem['peo_date_birth']!='1085-11-01']
tem['date_of_birth'] = pd.to_datetime(tem['peo_date_birth'], format='%Y/%m/%d %H:%M:%S')
vcand.update_dob(tem, mylog)

# %% job type
tem = candidate[['candidate_externalid', 'peo_tpb']].dropna().drop_duplicates()
tem2['peo_tpb'].unique()
tem1 = tem.loc[tem['peo_tpb']=='BOTH']
tem2 = tem.loc[tem['peo_tpb']!='BOTH']
tem2.loc[tem2['peo_tpb']=='TEMP', 'desired_job_type'] = 'temporary'
tem2.loc[tem2['peo_tpb']=='PERM', 'desired_job_type'] = 'permanent'
tem2.loc[tem2['peo_tpb']=='CONT', 'desired_job_type'] = 'contract'

tem3 = tem1.copy()
tem4 = tem1.copy()
tem3['desired_job_type'] = 'temporary'
tem4['desired_job_type'] = 'permanent'
tem = pd.concat([tem2,tem3,tem4])
cp = vcand.update_desired_job_type(tem, mylog)

# %% nationanlity
tem = candidate[['candidate_externalid', 'peo_nationality']].dropna().drop_duplicates()
tem['nationality'] = tem['peo_nationality'].map(vcom.get_country_code)
cp = vcand.update_nationality(tem, mylog)

# %% visa renew date
tem = candidate[['candidate_externalid', 'peo_visa_expiry']].dropna().drop_duplicates()
tem = tem.loc[tem['peo_visa_expiry']!=0]
tem['peo_visa_expiry'] = tem['peo_visa_expiry'].astype(str)
tem['peo_visa_expiry_1'] = tem['peo_visa_expiry'].apply(lambda x: x[0:4] if x else x)
tem['peo_visa_expiry_2'] = tem['peo_visa_expiry'].apply(lambda x: x[4:6] if x else x)
tem['peo_visa_expiry_3'] = tem['peo_visa_expiry'].apply(lambda x: x[6:9] if x else x)
tem['peo_visa_expiry'] = tem['peo_visa_expiry_1'] +'-'+tem['peo_visa_expiry_2']+'-'+tem['peo_visa_expiry_3']
tem = tem.loc[tem['peo_visa_expiry_1']>'1900']
tem = tem.loc[tem['peo_visa_expiry_1']<'2021']
tem['visa_renewal_date'] = pd.to_datetime(tem['peo_visa_expiry'], format='%Y/%m/%d %H:%M:%S')
cp = vcand.update_visa_renewal_date(tem, dest_db, mylog)

# %% visa type
tem = candidate[['candidate_externalid', 'peo_visa_type']].dropna().drop_duplicates()
tem['visa_type'] = tem['peo_visa_type']
cp = vcand.update_visa_type(tem, dest_db, mylog)

# %% current employer
# cur_emp = candidate[['candidate_externalid', 'COMPANY', 'TITLE']].dropna().drop_duplicates()
# cur_emp['current_employer'] = cur_emp['COMPANY'].str.strip()
# cur_emp['current_job_title'] = cur_emp['TITLE'].str.strip()
# cur_emp['current_job_title'] = cur_emp['current_job_title'].apply(lambda x: x.replace('\\','|'))
# cur_emp['current_employer'] = cur_emp['current_employer'].apply(lambda x: x.replace('\\','|'))
# vcand.update_candidate_current_employer_title_v2(cur_emp, dest_db, mylog)
# %% note
note = candidate[['candidate_externalid', 'peo_reloc','peo_branch','peo_division','peo_source','peo_licence','peo_hgv_licence','peo_transport','peo_langs','peo_alert_color','peo_alert_text','peo_awr_type','peo_vat_code']].drop_duplicates()
tem1 = pd.read_sql("""
select CONCAT('',peo_no) as candidate_externalid, peo_pen from peo_pen
""", engine_mssql)
tem2 = pd.read_sql("""
select CONCAT('',peo_no) as candidate_externalid, peo_testimonial from peo_testimonial
""", engine_mssql)
tem3 = pd.read_sql("""
select CONCAT('',peo_no) as candidate_externalid, peo_hands from peo_hands
""", engine_mssql)
note = note.merge(tem1, on='candidate_externalid', how='left')
note = note.merge(tem2, on='candidate_externalid', how='left')
note = note.merge(tem3, on='candidate_externalid', how='left')
note = note.where(note.notnull(),None)
note['note'] = note[['candidate_externalid', 'peo_reloc','peo_branch','peo_division','peo_source','peo_licence','peo_hgv_licence','peo_transport','peo_langs','peo_alert_color','peo_alert_text','peo_awr_type','peo_vat_code','peo_pen','peo_testimonial','peo_hands']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Person ID', 'Relocate','Office','Sector','Source','Licence','LGV licence','Transport','Languages','Alert Colour','Alert Text','AWR type','VAT Code','Pen Picture','Testimonial','Health and Safety'], x) if e[1]]), axis=1)
cp7 = vcand.update_note2(note, dest_db, mylog)

# %% education
edu = pd.read_sql(""" 
select CONCAT('',peo_no) as candidate_externalid
         , edu_quals
         , edu_school
         , edu_from
         , edu_to
    from peo_educ
""", engine_mssql)

edu['edu_from'] = edu['edu_from'].astype(str)
edu['edu_from_1'] = edu['edu_from'].apply(lambda x: x[0:4] if x else x)
edu['edu_from_2'] = edu['edu_from'].apply(lambda x: x[4:6] if x else x)
edu['edu_from_3'] = edu['edu_from'].apply(lambda x: x[6:9] if x else x)
edu['edu_from'] = edu['edu_from_1'] +'-'+edu['edu_from_2']+'-'+edu['edu_from_3']

edu['edu_to'] = edu['edu_to'].astype(str)
edu['edu_to_1'] = edu['edu_to'].apply(lambda x: x[0:4] if x else x)
edu['edu_to_2'] = edu['edu_to'].apply(lambda x: x[4:6] if x else x)
edu['edu_to_3'] = edu['edu_to'].apply(lambda x: x[6:9] if x else x)
edu['edu_to'] = edu['edu_to_1'] +'-'+edu['edu_to_2']+'-'+edu['edu_to_3']
edu['edu_to'] = edu['edu_to'].apply(lambda x: x.replace('0--',''))
edu['edu_from'] = edu['edu_from'].apply(lambda x: x.replace('0--',''))

edu['education_summary'] = edu[[
    'edu_from', 'edu_to'
    ,'edu_school','edu_quals']]\
     .apply(lambda x: '\n'.join([': '.join(e) for e in zip([
    'From', 'To'
    ,'Establihment','Qualifications'], x) if e[1]]), axis=1)

vcand.update_education_summary(edu, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = candidate[['candidate_externalid', 'peo_title']].dropna().drop_duplicates()
tem['gender_title'] = tem['peo_title']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
tem2 = tem[['candidate_externalid','gender_title']].dropna().drop_duplicates()
cp = vcand.update_gender_title(tem2, mylog)

# %% currency salary
tem = candidate[['candidate_externalid']]
tem['currency_of_salary'] = 'pound'
vcand.update_currency_of_salary(tem, mylog)

# # %% desire annual salary
# tem = candidate[['candidate_externalid', 'Salary_Amt']].dropna().drop_duplicates()
# tem['desire_salary'] = tem['Salary_Amt'].astype(float)
# vcand.update_desire_salary(tem, mylog)
#
# # %% desire contract rate
# tem = candidate[['candidate_externalid', 'OTE_Amt']].dropna().drop_duplicates()
# tem['desired_contract_rate'] = tem['OTE_Amt'].astype(float)
# vcand.update_desired_contract_rate(tem, mylog)

# %% wh
wh = pd.read_sql("""
select CONCAT('',pc.peo_no) as candidate_externalid
         , emp_from
         , emp_to
         , emp_cli_name
         , emp_job_title
         , emp_salary
         , emp_ote
         , CONCAT(p.peo_forename,' ',p.peo_surname) as report_to
         , emp_benefits
    from peo_career pc
    left join people p on pc.emp_reporting_to_peo_no = p.peo_no
""", engine_mssql)
wh['emp_from'] = wh['emp_from'].astype(str)
wh['emp_from_1'] = wh['emp_from'].apply(lambda x: x[0:4] if x else x)
wh['emp_from_2'] = wh['emp_from'].apply(lambda x: x[4:6] if x else x)
wh['emp_from_3'] = wh['emp_from'].apply(lambda x: x[6:9] if x else x)
wh['emp_from'] = wh['emp_from_1'] +'-'+wh['emp_from_2']+'-'+wh['emp_from_3']

wh['emp_to'] = wh['emp_to'].astype(str)
wh['emp_to_1'] = wh['emp_to'].apply(lambda x: x[0:4] if x else x)
wh['emp_to_2'] = wh['emp_to'].apply(lambda x: x[4:6] if x else x)
wh['emp_to_3'] = wh['emp_to'].apply(lambda x: x[6:9] if x else x)
wh['emp_to'] = wh['emp_to_1'] +'-'+wh['emp_to_2']+'-'+wh['emp_to_3']
wh['emp_to'] = wh['emp_to'].apply(lambda x: x.replace('0--',''))
wh['emp_from'] = wh['emp_from'].apply(lambda x: x.replace('0--',''))

wh['emp_salary'] = wh['emp_salary'].apply(lambda x: str(x))
wh['emp_ote'] = wh['emp_ote'].apply(lambda x: str(x))

wh['experience'] = wh[['emp_from', 'emp_to','emp_cli_name','emp_job_title','emp_salary','emp_ote','report_to','emp_benefits']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['From', 'To','Company','Position','Salary','OTE','Reporting to','Benefits'], x) if e[1]]), axis=1)
cp7 = vcand.update_exprerience_work_history2(wh, dest_db, mylog)

# %% status
tem = candidate[['candidate_externalid','peo_status']].dropna().drop_duplicates()
tem['name'] = tem['peo_status']
tem1 = tem[['name']].drop_duplicates()
tem1['owner'] =''
vcand.create_status_list(tem1, mylog)
vcand.add_candidate_status(tem, mylog)

# %% comp
compl = pd.read_sql("""
select Reference_Id as candidate_externalid, Table_Name,ur.* from UDF_Record ur
left join UDF_Table ut on ur.Table_Id = ut.Table_Id
where Table_Name = ' COMPLIANCE'
and Reference_Id not in (
39367
,2896
,37207
,22106
,25798
,35819
,50227
,48100
,46300
,47257)
""", engine_mssql)
compl['candidate_externalid'] = compl['candidate_externalid'].astype(str)

#     'Field_Value_1', 'Field_Value_2'
#     ,'Field_Value_3' ,'Field_Value_4'
#     ,'Field_Value_5','Field_Value_6'
#     ,'Field_Value_7','Field_Value_7'
#     ,'Field_Value_9','Field_Value_10'
#
#     ,'Field_Value_11','Field_Value_12'
#     ,'Field_Value_13','Field_Value_14'
#     ,'Field_Value_15','Field_Value_16'
#     ,'Field_Value_17'
#     ,'Field_Value_18','Field_Value_19'
#     ,'Field_Value_20'
#
#     ,'Field_Value_21','Field_Value_22'
#     ,'Field_Value_23','Field_Value_24']]\

#     'ID Confirmed', 'ID Type'
#     'ID date confirm','Reg Form Complete'
#     ,'Reg Form Date','Criminal Conviction'
#     ,'DBS Update Service','NI Number'
#     ,'Contract','Contract Received Date'
#     ,'48 Hour signed', 'bank detail received'
#     ,'face to face', 'cons met'
#     ,'Has Own Transport','Nationality'
#     ,'Notes'
#     ,'Settlement Status', 'part time'
#     ,'source'
#     ,'Job Board','Next of Kin Contact'
#     ,'Contact Number','Relationship'

# %% payment type
tem = candidate[['candidate_externalid', 'peo_empl_type']].dropna().drop_duplicates()
tem['peo_empl_type'].unique()
tem.loc[tem['peo_empl_type']=='LTD', 'paymenttype'] = 'Ltd'
tem.loc[tem['peo_empl_type']=='PAYE', 'paymenttype'] = 'PAYE'
tem2 = tem[['candidate_externalid','paymenttype']].dropna()
vcand.update_payment_type(tem2, mylog)

# %% source
tem = pd.read_sql("""
select Reference_Id as candidate_externalid, Table_Name,coalesce(Field_Value_21, Field_Value_20) as source from UDF_Record ur
left join UDF_Table ut on ur.Table_Id = ut.Table_Id
where Table_Name = ' COMPLIANCE'
and Reference_Id not in (
39367
,2896
,37207
,22106
,25798
,35819
,50227
,48100
,46300
,47257)
""", engine_mssql)
tem['candidate_externalid'] = tem['candidate_externalid'].astype(str)
tem = tem.dropna()
tem = tem.loc[tem['source']!='none']
tem = tem.loc[tem['source']!='n/a']
tem = tem.loc[tem['source']!='N/A']
tem['source'].unique() # n/a none N/A
cp = vcand.insert_source(tem)

# %% delete
tem = pd.read_sql("""
select Candidate_Id as candidate_externalid
     , Status_Code
     , Description
from Candidate c
left join (select code, Description from Lookup where Table_Name in ('CANDIDATE_STATUS')) s on s.Code = c.Status_Code
where Description in
      ('blanked'
      ,'COVID-19'
      ,'Do Not Transfer'
      ,'Uncontactable'
      ,'Withdraw - 12months'
      ,'Withdraw - 7years')
and Candidate_Id not in (
39367
,2896
,37207
,22106
,25798
,35819
,50227
,48100
,46300
,47257)
""", engine_mssql)
tem['candidate_externalid'] = tem['candidate_externalid'].astype(str)
tem = tem.merge(vcand.candidate, on=['candidate_externalid'])
tem['deleted_timestamp'] = datetime.datetime.now()
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vcand.ddbconn, ['deleted_timestamp', ], ['id', ], 'candidate', mylog)

# %% check
tem = pd.read_sql("""
select Candidate_Id as candidate_externalid
     , Status_Code
     , Description
from Candidate c
left join (select code, Description from Lookup where Table_Name in ('CANDIDATE_STATUS')) s on s.Code = c.Status_Code
where Description = 'Unsuitable'
""", engine_mssql)
tem['candidate_externalid'] = tem['candidate_externalid'].astype(str)
tem = tem.merge(vcand.candidate, on=['candidate_externalid'])
tem['deleted_timestamp'] = datetime.datetime.now()
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vcand.ddbconn, ['deleted_timestamp', ], ['id', ], 'candidate', mylog)