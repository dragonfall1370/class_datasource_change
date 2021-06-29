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
cf.read('ca_config.ini')
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
assert False
# %%
candidate = pd.read_sql("""
select Candidate_Id as candidate_externalid
     , Candidate_Code
     , First_Name
     , Last_Name
     , Title
     , Salutation
     , DOB
     , Gender
     , Line_1
     , Line_2
     , Line_3
     , Postcode
     , town
     , county
     , country
     , Phone_Code_1
     , Phone_Number_1
     , Extension_1
     , Phone_Code_2
     , Phone_Number_2
     , Extension_2
     , Mobile_Code
     , Mobile_Number
     , Status_Code
     , Active_YN
     , Candidate_Type
     , Created_DTTM
     , Temporary_YN, Permenant_YN
     , Salary_Amt
     , OTE_Amt
     , Notes
from Candidate c
join (
select p.*, Phone_Code_1, Phone_Number_1,Extension_1, Phone_Code_2, Phone_Number_2,Extension_2, Mobile_Code, Mobile_Number, Fax_Code, Fax_Number, Email_Address, Web_Address
from Person p join Contact_Info ci on ci.Contact_Info_Id = p.Contact_Info_Id) e on c.Person_Id = e.Person_Id
left join (select Address_Id, Line_1, Line_2, Line_3, c.Description as county, co.Description as country, t.Description as town, Postcode
from Address a
left join County c on a.County_Id = c.County_Id
left join Country co on a.Country_Id = co.Country_Id
left join Town t on a.Town_Id = t.Town_Id) addr on addr.Address_Id = e.Address_Id
""", engine_mssql)
candidate['candidate_externalid'] = candidate['candidate_externalid'].astype(str)
assert False
# %% location name/address
c_location = candidate[['candidate_externalid','Line_1', 'Line_2','Line_3','town','county','Postcode','country']]
c_location['address'] = c_location[['Line_1', 'Line_2','Line_3','town','county','Postcode','country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
c_location['location_name'] = c_location[['town','country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
c_location = c_location.loc[c_location['address']!='']
cp2 = vcand.insert_common_location_v2(c_location, dest_db, mylog)
# update city
vcand = vincere_candidate.Candidate(engine_postgre_review.raw_connection())
comaddr = c_location[['candidate_externalid','Line_1', 'Line_2','Line_3','town','Postcode','county','country']].drop_duplicates()\
    .rename(columns={'town': 'city', 'county': 'state', 'Postcode': 'post_code', 'Line_1': 'address_line1'})
tem = comaddr[['candidate_externalid', 'address_line1']].dropna()
cp3 = vcand.update_address_line1_2(tem, dest_db, mylog)

tem = comaddr[['candidate_externalid', 'Line_2','Line_3']]
tem['address_line2'] = tem[['Line_2','Line_3']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem = tem.loc[tem['address_line2']!='']
cp3 = vcand.update_address_line2_2(tem, dest_db, mylog)

tem = comaddr[['candidate_externalid', 'city']].dropna()
cp3 = vcand.update_location_city2(tem, dest_db, mylog)
# update state
tem = comaddr[['candidate_externalid', 'state']].dropna()
cp4 = vcand.update_location_state2(tem, dest_db, mylog)
# update postcode
tem = comaddr[['candidate_externalid', 'post_code']].dropna()
cp5 = vcand.update_location_post_code2(tem, dest_db, mylog)
#  update country
from common import vincere_company
vcom = vincere_company.Company(connection)
tem = comaddr[['candidate_externalid','country']].dropna()
tem['country_code'] = tem.country.map(vcom.get_country_code)
# tem['CandidateCountry'].unique()
cp6 = vcand.update_location_country_code(tem, mylog)

tem = comaddr[['candidate_externalid']]
tem['location_type'] = 'PERSONAL_ADDRESS'
vcand.update_location_type(tem, mylog)
# %% home phones
home_phone = candidate[['candidate_externalid', 'Phone_Code_1', 'Phone_Number_1','Extension_1']]
home_phone['home_phone'] = home_phone[['Phone_Code_1', 'Phone_Number_1','Extension_1']].apply(lambda x: ' '.join([e for e in x if e]), axis=1)
home_phone = home_phone.loc[home_phone['home_phone']!='']
cp = vcand.update_home_phone2(home_phone, dest_db, mylog)

# %% work phones
wphone = candidate[['candidate_externalid', 'Phone_Code_2', 'Phone_Number_2','Extension_2']]
wphone['work_phone'] = wphone[['Phone_Code_2', 'Phone_Number_2','Extension_2']].apply(lambda x: ' '.join([e for e in x if e]), axis=1)
wphone = wphone.loc[wphone['work_phone']!='']
cp = vcand.update_work_phone(wphone, mylog)

# %% mobile/primary phones
mphone = candidate[['candidate_externalid', 'Mobile_Code', 'Mobile_Number']]
mphone['mobile_phone'] = mphone[[ 'Mobile_Code', 'Mobile_Number']].apply(lambda x: ' '.join([e for e in x if e]), axis=1)
mphone = mphone.loc[mphone['mobile_phone']!='']
mphone['primary_phone'] = mphone['mobile_phone']
cp = vcand.update_mobile_phone(mphone, mylog)
cp = vcand.update_primary_phone(mphone, mylog)

# # %% primary phones
# indt = candidate[['candidate_externalid', 'Mobile']].dropna().drop_duplicates()
# indt['primary_phone'] = indt['Mobile']
# cp = vcand.update_primary_phone(indt, mylog)

# %% job type
tem1 = candidate[['candidate_externalid', 'Temporary_YN']].dropna().drop_duplicates()
tem1['Temporary_YN'].unique()
tem1.loc[tem1['Temporary_YN']=='Y', 'desired_job_type'] = 'temporary'
tem1  = tem1[['candidate_externalid', 'desired_job_type']].dropna().drop_duplicates()

tem2 = candidate[['candidate_externalid', 'Permenant_YN']].dropna().drop_duplicates()
tem2['Permenant_YN'].unique()
tem2.loc[tem2['Permenant_YN']=='Y', 'desired_job_type'] = 'permanent'
tem2  = tem2[['candidate_externalid', 'desired_job_type']].dropna().drop_duplicates()
tem = pd.concat([tem1, tem2])
tem = tem.drop_duplicates()
cp = vcand.update_desired_job_type(tem, mylog)

# %% gender
tem = candidate[['candidate_externalid', 'Gender']].dropna().drop_duplicates()
tem = tem.drop_duplicates()
tem['Gender'].unique()
tem.loc[tem['Gender']=='M', 'male'] = 1
tem.loc[tem['Gender']=='F', 'male'] = 0
tem2 = tem[['candidate_externalid','male']].dropna()
tem2['male'] = tem2['male'].astype(int)
cp = vcand.update_gender(tem2, mylog)

# %% current employer
# cur_emp = candidate[['candidate_externalid', 'COMPANY', 'TITLE']].dropna().drop_duplicates()
# cur_emp['current_employer'] = cur_emp['COMPANY'].str.strip()
# cur_emp['current_job_title'] = cur_emp['TITLE'].str.strip()
# cur_emp['current_job_title'] = cur_emp['current_job_title'].apply(lambda x: x.replace('\\','|'))
# cur_emp['current_employer'] = cur_emp['current_employer'].apply(lambda x: x.replace('\\','|'))
# vcand.update_candidate_current_employer_title_v2(cur_emp, dest_db, mylog)
# %% note
note = candidate[['candidate_externalid', 'Notes']].dropna().drop_duplicates()

compl = pd.read_sql("""
select Reference_Id as candidate_externalid, Table_Name,ur.* from UDF_Record ur
left join UDF_Table ut on ur.Table_Id = ut.Table_Id
where Table_Name = ' COMPLIANCE'
""", engine_mssql)
compl['candidate_externalid'] = compl['candidate_externalid'].astype(str)

cand_note = pd.read_sql("""select Candidate_Id, Notes, Note_DTTM from Candidate_Note""", engine_mssql)
cand_note['candidate_externalid'] = cand_note['Candidate_Id'].astype(str)
compl['compl'] = compl[[
    'Field_Value_1', 'Field_Value_2'
    ,'Field_Value_4'
    ,'Field_Value_5','Field_Value_6'
    ,'Field_Value_7'
    ,'Field_Value_9','Field_Value_10'
    ,'Field_Value_11'
    # ,'Field_Value_15','Field_Value_16'
    ,'Field_Value_18'
    # ,'Field_Value_21'
    ,'Field_Value_22'
    ,'Field_Value_23','Field_Value_24']]\
     .apply(lambda x: '\n'.join([': '.join(e) for e in zip([
    'ID Confirmed', 'ID Type'
    ,'Reg Form Complete'
    ,'Reg Form Date','Criminal Conviction'
    ,'DBS Update Service'
    ,'Contract','Contract Received Date'
    ,'48 Hour signed'
    # ,'Has Own Transport','Nationality'
    ,'Settlement Status'
    # ,'Job Board'
    ,'Next of Kin Contact'
    ,'Contact Number','Relationship'], x) if e[1]]), axis=1)
# 'Field_Value_19','Field_Value_20' emp type  source
compl = compl.groupby('candidate_externalid')['compl'].apply(lambda x: '\n'.join(x)).reset_index()
compl['compl'] = '\n---Compliance---\n'+compl['compl']

cand_note['Note_DTTM'] = cand_note['Note_DTTM'].astype(str)
cand_note['cand_note'] = cand_note[['Notes','Note_DTTM']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Notes','Created'], x) if e[1]]), axis=1)
cand_note = cand_note.groupby('candidate_externalid')['cand_note'].apply(lambda x: '\n'.join(x)).reset_index()
cand_note['cand_note'] = '\n---Notes---\n'+cand_note['cand_note']


tem = note.merge(compl, on='candidate_externalid', how='left')
tem = tem.merge(cand_note, on='candidate_externalid', how='left')
tem = tem.where(tem.notnull(),None)
tem = tem.drop_duplicates()
# tem['note'] = tem[['Candidate_Code','Notes','cand_ex_client','client_ex_cand','compl','cand_note']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Candidate Code','Notes','','','',''], x) if e[1]]), axis=1)
tem['note'] = tem[['Notes','compl','cand_note']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Notes','',''], x) if e[1]]), axis=1)


tem2 = tem[['candidate_externalid', 'note']].dropna()
tem2['title'] = 'IV Notes'
tem2['insert_timestamp'] = datetime.datetime.now()
cp7 = vcand.insert_internal_note(tem2, mylog)



cp7 = vcand.update_note2(tem, dest_db, mylog)

# %% education
edu = pd.read_sql(""" 
select Reference_Id as candidate_externalid, Table_Name,ur.* from UDF_Record ur
left join UDF_Table ut on ur.Table_Id = ut.Table_Id
where Table_Name = 'Edu Comp'
""", engine_mssql)
edu['candidate_externalid'] = edu['candidate_externalid'].astype(str)

edu['education_summary'] = edu[[
    'Field_Value_1', 'Field_Value_2'
    ,'Field_Value_3','Field_Value_4'
    ,'Field_Value_5','Field_Value_6'
    ,'Field_Value_7','Field_Value_8'
    ,'Field_Value_9','Field_Value_10'
    ,'Field_Value_11','Field_Value_12'
    ,'Field_Value_13','Field_Value_14'
    ,'Field_Value_15','Field_Value_16'
    ,'Field_Value_17'
    ,'Field_Value_19','Field_Value_20'
    ,'Field_Value_21','Field_Value_22'
    ,'Field_Value_23','Field_Value_24']]\
     .apply(lambda x: '\n'.join([': '.join(e) for e in zip([
    'DBS Certificate', 'DBS Update Check Date'
    ,'DBS No','Reference 1'
    ,'Reference 2','Ref Notes'
    ,'Source','RTW ID'
    ,'2nd Photo ID','ID Type'
    ,'Proof Address 1','Proof Address 2'
    ,'Nat Ins Proof (formal doc)','Timeline Complete'
    ,'Reg Doc (fully complete)','Contract Signed'
    ,'Safeguarding Cert'
    ,'Qualified Teacher','QTS Number'
    ,'NCTL Trace Check','CDR form signed'
    ,'Criminal Declaration Form','Notes'], x) if e[1]]), axis=1)

edu = edu.groupby('candidate_externalid')['education_summary'].apply('\n\n'.join).reset_index()
vcand.update_education_summary(edu, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = candidate[['candidate_externalid', 'Title']].dropna().drop_duplicates()
tem['gender_title'] = tem['Title']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
tem2 = tem[['candidate_externalid','gender_title']].dropna().drop_duplicates()
cp = vcand.update_gender_title(tem2, mylog)

# %% preferred name
tem = candidate[['candidate_externalid', 'Salutation']].dropna().drop_duplicates()
tem['preferred_name'] = tem['Salutation']
vcand.update_preferred_name_v2(tem, dest_db, mylog)

# %% dob
tem = candidate[['candidate_externalid', 'DOB']].dropna().drop_duplicates()
tem['date_of_birth'] = tem['DOB'].apply(lambda x: x[0:10] if x else x)
tem['date_of_birth'] = pd.to_datetime(tem['date_of_birth'])
vcand.update_dob(tem, mylog)

# %% reg date
tem = candidate[['candidate_externalid', 'Created_DTTM']].dropna().drop_duplicates()
tem['reg_date'] = tem['Created_DTTM'].apply(lambda x: x[0:10] if x else x)
tem['reg_date'] = pd.to_datetime(tem['reg_date'])
vcand.update_reg_date(tem, mylog)

# %% currency salary
tem = candidate[['candidate_externalid']]
tem['currency_of_salary'] = 'pound'
vcand.update_currency_of_salary(tem, mylog)

# %% desire annual salary
tem = candidate[['candidate_externalid', 'Salary_Amt']].dropna().drop_duplicates()
tem['desire_salary'] = tem['Salary_Amt'].astype(float)
vcand.update_desire_salary(tem, mylog)

# %% desire contract rate
tem = candidate[['candidate_externalid', 'OTE_Amt']].dropna().drop_duplicates()
tem['desired_contract_rate'] = tem['OTE_Amt'].astype(float)
vcand.update_desired_contract_rate(tem, mylog)

# %% reference
ref1 = pd.read_sql("""
select Reference_Id as candidate_externalid, Table_Name,ur.* from UDF_Record ur
left join UDF_Table ut on ur.Table_Id = ut.Table_Id
where Table_Name = 'Working 1'
""", engine_mssql)
ref1['candidate_externalid'] = ref1['candidate_externalid'].astype(str)

ref2 = pd.read_sql("""
select Reference_Id as candidate_externalid, Table_Name,ur.* from UDF_Record ur
left join UDF_Table ut on ur.Table_Id = ut.Table_Id
where Table_Name = 'Working 2'
""", engine_mssql)
ref2['candidate_externalid'] = ref2['candidate_externalid'].astype(str)

ref1['reference'] = ref1[[
    'Field_Value_1', 'Field_Value_2'
    ,'Field_Value_3','Field_Value_4'
    ,'Field_Value_5','Field_Value_6'
    ,'Field_Value_7','Field_Value_8'
    ,'Field_Value_9','Field_Value_10'
    ,'Field_Value_11','Field_Value_12'
    ,'Field_Value_13','Field_Value_14'
    ,'Field_Value_15','Field_Value_16'
    ,'Field_Value_17','Field_Value_18'
    ,'Field_Value_19','Field_Value_20'
    ,'Field_Value_21','Field_Value_22'
    ,'Field_Value_23','Field_Value_24']]\
     .apply(lambda x: '\n'.join([': '.join(e) for e in zip([
    'Contact Name', 'Contact Position'
    ,'Company','Address 1'
    ,'Address 2','Address 3'
    ,'Address 4','Post Code'
    ,'Tel No','Email'
    ,'Position Held','Dates'
    ,'Ref Sent On','Ref Rec''d'
    ,'Timekeeping','Work Performance'
    ,'Work with Others','Attendance'
    ,'Honesty / Integrity','Accuracy of Work'
    ,'Speed of Work','Flexibility / Adaptability'
    ,'Notes','Initials of Ref Taker'], x) if e[1]]), axis=1)
ref1['reference'] = '---Ref 1---\n'+ref1['reference']

ref2['reference'] = ref2[[
    'Field_Value_1', 'Field_Value_2'
    ,'Field_Value_3','Field_Value_4'
    ,'Field_Value_5','Field_Value_6'
    ,'Field_Value_7','Field_Value_8'
    ,'Field_Value_9','Field_Value_10'
    ,'Field_Value_11','Field_Value_12'
    ,'Field_Value_13','Field_Value_14'
    ,'Field_Value_15','Field_Value_16'
    ,'Field_Value_17','Field_Value_18'
    ,'Field_Value_19','Field_Value_20'
    ,'Field_Value_21','Field_Value_22'
    ,'Field_Value_23','Field_Value_24']]\
     .apply(lambda x: '\n'.join([': '.join(e) for e in zip([
    'Contact Name', 'Contact Position'
    ,'Company','Address 1'
    ,'Address 2','Address 3'
    ,'Address 4','Post Code'
    ,'Tel No','Email'
    ,'Position Held','Dates'
    ,'Ref Sent On','Ref Rec''d'
    ,'Timekeeping','Work Performance'
    ,'Work with Others','Attendance'
    ,'Honesty / Integrity','Accuracy of Work'
    ,'Speed of Work','Flexibility / Adaptability'
    ,'Notes','Initials of Ref Taker'], x) if e[1]]), axis=1)
ref2['reference'] = '---Ref 2---\n'+ref2['reference']

rela = pd.concat([ref1[['candidate_externalid','reference']],ref2[['candidate_externalid','reference']]])
rela = rela.drop_duplicates()
vcand.update_reference(rela, dest_db, mylog)

# %% status
tem = pd.read_sql("""
select Candidate_Id as candidate_externalid
     , Status_Code
     , Description
from Candidate c
left join (select code, Description from Lookup where Table_Name in ('CANDIDATE_STATUS')) s on s.Code = c.Status_Code
where Description in
      ('Found Own Job'
      ,'Found Own Job CONSENT'
      ,'Comp in Progress'
      ,'Comp in Progress CONSENT'
      ,'DO NOT USE'
      ,'Placed Perm CEA'
      ,'Placed Perm CEA CONSENT'
      ,'Pre-Registered'
      ,'Pre-Registered CONSENT'
      ,'Ready to Work'
      ,'Ready to Work CONSENT'
      ,'Refs Pending'
      ,'Refs Pending CONSENT')
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
tem.loc[tem['Description'] == 'Found Own Job', 'name'] = 'Passive'
tem.loc[tem['Description'] == 'Found Own Job CONSENT', 'name'] = 'Passive'
tem.loc[tem['Description'] == 'Comp in Progress', 'name'] = 'Awaiting Docs'
tem.loc[tem['Description'] == 'Comp in Progress CONSENT', 'name'] = 'Awaiting Docs'
tem.loc[tem['Description'] == 'DO NOT USE', 'name'] = 'Blacklisted'
tem.loc[tem['Description'] == 'Placed Perm CEA', 'name'] = 'Placed Perm'
tem.loc[tem['Description'] == 'Placed Perm CEA CONSENT', 'name'] = 'Placed Perm'
tem.loc[tem['Description'] == 'Pre-Registered', 'name'] = 'Active'
tem.loc[tem['Description'] == 'Pre-Registered CONSENT', 'name'] = 'Active'
tem.loc[tem['Description'] == 'Ready to Work', 'name'] = '100% Compliant'
tem.loc[tem['Description'] == 'Ready to Work CONSENT', 'name'] = '100% Compliant'
tem.loc[tem['Description'] == 'Refs Pending', 'name'] = 'Awaiting Docs'
tem.loc[tem['Description'] == 'Refs Pending CONSENT', 'name'] = 'Awaiting Docs'
tem1 = tem[['name']]
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
# %% citizenship
tem = compl[['candidate_externalid', 'Field_Value_16']].dropna().drop_duplicates()
tem['nationality'] = tem['Field_Value_16'].map(vcand.get_country_code)
tem = tem.loc[tem['nationality']!='']
vcand.update_nationality(tem, mylog)

# %% met/not met
tem = compl[['candidate_externalid', 'Field_Value_13']].dropna().drop_duplicates()
tem['Field_Value_13'].unique()
tem.loc[tem['Field_Value_13']=='Yes', 'status'] = 1
tem.loc[tem['Field_Value_13']=='No', 'status'] = 2
tem2 = tem[['candidate_externalid','status']].dropna()
vcand.update_met_notmet(tem2, mylog)

# %% emp type
tem = compl[['candidate_externalid', 'Field_Value_19']].dropna().drop_duplicates()
tem['Field_Value_19'].unique()
tem.loc[tem['Field_Value_19']=='Y', 'employment_type'] = 'parttime'
tem2 = tem[['candidate_externalid','employment_type']].dropna()
vcand.update_employment_type(tem2, mylog)

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

# %% unsub
tem = pd.read_sql("""
select c.Candidate_Id,e.Email_Address as email
from Candidate c
join (
select Person_Id, Email_Address, First_Name ,Last_Name
from Person p join Contact_Info ci on ci.Contact_Info_Id = p.Contact_Info_Id) e on c.Person_Id = e.Person_Id
join (
select Candidate_Id, Description
from Candidate_Skill cs
join Skill s on s.Skill_Id = cs.Skill_Id
where Description =' 1. UNSUBSCRIBED') s on s.Candidate_Id = c.Candidate_Id
where Email_Address is not null
and c.Candidate_Id not in (
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
tem = tem.loc[tem['email'].str.contains('@')]
tem['subscribed'] = 0
vcand.email_subscribe(tem, mylog)

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