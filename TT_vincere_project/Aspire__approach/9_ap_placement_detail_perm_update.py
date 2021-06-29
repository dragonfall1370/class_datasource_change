# -*- coding: UTF-8 -*-
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
cf.read('ap_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
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

from common import vincere_placement_detail
vplace = vincere_placement_detail.PlacementDetail(connection)
# %% permanent
perm_info = pd.read_sql("""
select
       concat('', p.Reference) as job_externalid
     , concat('', p.Person_Reference) as candidate_externalid
     , Start_Date, Placed_Date
     , nullif(trim(Salary_Details),'') as Salary_Details
     , Invoice_Date
     , Commission_Total
     , Commission_Net
     , Commission_Gross
     , Active
     , VAT_Amount
from Job_Perm_Placement_View p
""", engine_mssql)
assert False
# %% start date/end date
stdate = perm_info[['job_externalid', 'candidate_externalid', 'Start_Date']].dropna()
stdate['start_date'] = stdate['Start_Date']
stdate['start_date'] = pd.to_datetime(stdate['start_date'])
cp1 = vplace.update_startdate_only_for_placement_detail(stdate, mylog)

# %% invoice date
stdate = perm_info[['job_externalid', 'candidate_externalid', 'Invoice_Date']].dropna()
stdate['invoice_date'] = stdate['Invoice_Date']
stdate['invoice_date'] = pd.to_datetime(stdate['invoice_date'])
cp1 = vplace.update_invoice_date(stdate, mylog)

# %% offer date/place date
# jobapp_offer_date = placement_detail_info[['job_externalid', 'candidate_externalid', 'OfferAgreedDate']]
# jobapp_offer_date['offer_date'] = pd.to_datetime(jobapp_offer_date['OfferAgreedDate'])
jobapp_placed_date = perm_info[['job_externalid', 'candidate_externalid', 'Placed_Date']].dropna()
jobapp_placed_date['placed_date'] = pd.to_datetime(jobapp_placed_date['Placed_Date'])
# jobapp_created_date['sent_date'] = pd.to_datetime(jobapp_created_date['jobapp_created_date'])
# vplace.update_offerdate(jobapp_offer_date, mylog)
vplace.update_placeddate(jobapp_placed_date, mylog)
# vplace.update_sent_date(jobapp_created_date, mylog)

# %% salary
tem = perm_info[['job_externalid', 'candidate_externalid', 'Commission_Total']].dropna().rename(columns={'Commission_Total':'annual_salary'})
tem['annual_salary'] = tem['annual_salary'].astype(float)
cp6 = vplace.update_offer_annual_salary(tem, mylog)

# %% quick fee
vplace.update_use_quick_fee_forecast_for_permanent_job()

tem = perm_info[['job_externalid', 'candidate_externalid', 'Commission_Net','Commission_Total']].dropna()
tem['percentage_of_annual_salary'] = tem['Commission_Net']/tem['Commission_Total']
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary'].fillna(0.0)
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary'].astype(float)
tem.percentage_of_annual_salary = tem.percentage_of_annual_salary.round(2)
tem.percentage_of_annual_salary = tem.percentage_of_annual_salary*100
cp5, cp6 = vplace.update_percentage_of_annual_salary(tem, mylog)

# %% placement note
note = perm_info[['job_externalid', 'candidate_externalid', 'Salary_Details', 'Active']].dropna()
note.loc[note['Active']=='Active', 'Active'] = 'Ongoing'
note['note'] = note[['Salary_Details','Active']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Package Details', 'Ongoing placement'], x) if e[1]]), axis=1)

cp0 = vplace.update_internal_note(note, mylog)

# %% invoice note
note = perm_info[['job_externalid', 'candidate_externalid', 'Commission_Gross', 'VAT_Amount']].dropna()
note['Commission_Gross'] = note['Commission_Gross'].astype(str)
note['VAT_Amount'] = note['VAT_Amount'].astype(str)
note['invoice_message'] = note[['Commission_Gross', 'VAT_Amount']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Invoice Total', 'VAT'], x) if e[1]]), axis=1)

cp0 = vplace.update_invoice_note(note, mylog)

# %% split
tem = pd.read_sql("""
select concat('', p.Reference) as job_externalid
     , concat('', p.Person_Reference) as candidate_externalid, o.*
from Job_Perm_Placement_View p
 join (
select Placement_Reference, Consultant_Reference, Split, Email from Commission c
left join Commission_Split cs on cs.Commission_Reference = c.Reference
left join (
select q.Reference,Email
from consultant_lookup q
left join (
    SELECT a.Reference,
           a.Person_Reference,
           UserName,
           Password,
           + ISNULL(NULLIF(RTRIM(Forename) + ' ', ' '), '')
               + ISNULL(RTRIM(Surname), '') AS Full_Name,
           Forename,
           Surname,
           dbo.GetcontactDetail(a.person_reference, 9, 'Main Business')  as Telephone,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Fax')   as Fax,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Email') as Email,
           SuperUser,
           a.active
    FROM Consultant a
             INNER JOIN Person b
                        ON a.person_reference = b.reference) cv on cv.Reference = q.Consultant_Reference
where q.reference_type = 6 and nullif(Email, '') is not null) cons on cons.Reference = Consultant_Reference) o on o.Placement_Reference = p.Placement_reference
""", engine_mssql)

vplace.insert_profit_split_mode_percentage(tem, mylog)

