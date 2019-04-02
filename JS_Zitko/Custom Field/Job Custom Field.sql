--CIS confirmation
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
CIS_confirmation__c as 'CustomValue',
'CIS confirmation' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where CIS_confirmation__c <> ''

--Reason For Role
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
Reason_For_Role__c as 'CustomValue',
'Reason For Role' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where Reason_For_Role__c <> ''

--Reporting To
select 'add_job_info' as 'Additional_type',
a.Id as 'External_id',
concat(b.LastName,' ',b.FirstName,nullif(concat(' - ',b.email),' - ')) as 'CustomValue',
'Reporting To' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c a 
left join Contact b on a.Reporting_To__c = b.Id 
where Reporting_To__c <> ''

--Size Of Team
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
Size_Of_Team__c as 'CustomValue',
'Size Of Team' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where Size_Of_Team__c <> ''

--Key Skill or Qualification ( Core Field - map to note )
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
Key_Skill_or_Qualification__c as 'CustomValue',
'Key Skill or Qualification' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where Key_Skill_or_Qualification__c <> ''

--Role Base
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
Role_Base__c as 'CustomValue',
'Role Base' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where Role_Base__c <> ''

--Working Hours
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
Working_Hours__c as 'CustomValue',
'Working Hours' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where Working_Hours__c <> ''

--USP
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
USP__c as 'CustomValue',
'USP' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where USP__c <> ''

--Callout / Overtime
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
Callout_Overtime__c as 'CustomValue',
'Callout / Overtime' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where Callout_Overtime__c <> ''

--Benefits
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
Benefits__c as 'CustomValue',
'Benefits' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where Benefits__c <> ''

--Car / Car Allowance
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
Car_Car_Allowance__c as 'CustomValue',
'Car / Car Allowance' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where Car_Car_Allowance__c <> ''



--Vacancy Ownership
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
Vacancy_Ownership__c as 'CustomValue',
'Vacancy Ownership' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where Vacancy_Ownership__c <> ''

--Time Open
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
Time_Open__c as 'CustomValue',
'Time Open' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where Time_Open__c <> ''

--Candidates Seen
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
Candidates_Seen__c as 'CustomValue',
'Candidates Seen' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where Candidates_Seen__c <> ''

--Other Agencies
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
Other_Agencies__c as 'CustomValue',
'Other Agencies' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where Other_Agencies__c <> ''

--Interview Process
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
Interview_Process__c as 'CustomValue',
'Interview Process' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where Interview_Process__c <> ''

--Client Expectations
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
Client_Expectations__c as 'CustomValue',
'Client Expectations' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where Client_Expectations__c <> ''

--Max Pay Rate ( Core Field )
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
ts2__Max_Pay_Rate__c as 'CustomValue',
'Max Pay Rate' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where ts2__Max_Pay_Rate__c <> ''

--Min Pay Rate ( Core Field )
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
ts2__Min_Pay_Rate__c as 'CustomValue',
'Min Pay Rate' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c where ts2__Min_Pay_Rate__c <> ''

--Estimated Start Date
select 'add_job_info' as 'Additional_type',
Id as 'External_id',
ts2__Estimated_Start_Date__c as 'CustomValue',
'Estimated Start Date' as 'lookup_name',
getdate() as insert_timestamp
from ts2__Job__c