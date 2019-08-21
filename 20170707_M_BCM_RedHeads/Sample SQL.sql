All categories will be retrieved 'ContactMainTable'

1. Contact

select * from ContactMainTable where Type = 1 and IsDeletedLocally = 0

--Details of contact can be found from this table
select * from ContactDetailsTable where FirstName like '%Mohan%' and LastName like '%Krishna%'

--Can be queried from
select * from BCMContactsView

2. Company
select * from ContactMainTable where Type = 2 and IsDeletedLocally = 0

--Can be queried from
select * from BCMAccountsView

3. Differentiate contacts / candidates
select * from ContactCategoriesTable where CategoryName in ('Placed Candidate',
'Employee',
'Undesired applicant',
'Applicant',
'IT Applicant',
'Inactive Applicant'
)

--> The mapping for Candidate

select * from ContactCategoriesTable where CategoryName in ('Former undesired employee',
'Competitor',
'Personal',
'Former employee',
'Inactive Client',
'Client',
'Source of Candidates',
'urgent',
'Restraint of Trade',
'Supplier',
'Reference')

--> The mapping for Contact

4. Related to files - Can be found from
select * from ContactAttachments

select * from Attachments

select count(*) from ActivityAttachments

select top 100 * from ActivitiesTable where ActivityType = 2 -- mainly coming from this table

5. Job can be found from table 
select * from OpportunityFullView

--or can be queried from 
select * from ContactMainTable where Type = 3 and IsDeletedLocally = 0

---Custom Fields
select cast(UserField7 as nvarchar(max)) from UserFields where ContactServiceID = 158

select UserField7 from UserFields where ContactServiceID = 158

/* To review all User Fields - values are stored in binary varchar */
select top 100 concat('UserField',EF.UserFieldIndex), UFD.FieldName from 
--[dbo].[EntityTypesTable]
[dbo].[EntityUserFields] EF
left join 
[UserFieldDefinitions] UFD on EF.FieldGUID = UFD.FieldGUID
where UFD.FieldName like '%Vendor%'


---Candidate Note/Summary
SELECT distinct table_name, COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE 1=1
--and TABLE_NAME = 'YourTableName' 
AND TABLE_SCHEMA='dbo'
and COLUMN_NAME like '%ContactServiceID%'
and table_name not like '%View%'

----Contact and Job App
select * from ContactMainTable where Type = 1 and ContactServiceID in (35254, 35262)
and Subject like '%SAP FI%'

select * from EntityReferences where LinkID = 34953

---Company / Contact / Job fields
AccountIMAPIView --> Company
ContactIMAPIView --> Contact
OpportunityIMAPIView --> Job

select UFD.*, EF.*, UFDT.*
from UserFieldDefinitions UFD
left join EntityUserFields EF on EF.FieldGUID = UFD.FieldGUID
left join UserFieldDataTypes UFDT on UFDT.DataTypeID = UFD.DataType
where UFD.FieldName like '%Client Priority%'


---Activities
select * from ActivitiesTable where ReferredEntryId = '98B04D25-511E-4253-AC36-6A03BAE6F950'

select * from ActivityTypesTable

select * from ActivityContacts where ContactID = 35262

select * from ActivitiesTable where 1=1

--ActivityType = 14 
and ActivityID in (336783,336784,336785,336786,336787,336988)

select ATT.ActivityTypeName, * from ActivitiesTable ACP
left join ActivityTypesTable ATT on ACP.ActivityType = ATT.ActivityTypeID
where ACP.ActivityType = 2

---Job Application
select * from EntityReferences 
where ContactServiceID in (select ContactServiceID from ContactMainTable where Type = 3)
--where LinkItemType = 1 order by ContactServiceID

select * from ContactMainTable where ContactServiceID in (182, 193, 202, 428, 431) ---job application links

select * from ContactMainTable where ContactServiceID in (291, 42, 29937, 29846, 27363) ---

---Job file attachments
select * from ActivitiesTable where ActivityID in (select ActivityID from ActivityContacts where ContactID in (select ContactServiceID from ContactMainTable where type = 3))

--Functional Expertise
with Functional as (
select distinct StringValue from PicklistsMasterList where PicklistID = '71712E03-65F5-4B1E-B786-39F09DFAF410'
UNION ALL
select distinct StringValue from PicklistsMasterList where PicklistID = 'AF54F6C5-8A52-4744-B786-192D98337BFE')

select distinct * from Functional

--Find duplicated documents
select ac.ContactID, act.Subject, count(act.Subject) from ActivityContacts ac
left join ActivitiesTable act on act.ActivityID = ac.ActivityID
where act.ActivityType = 2
and ac.ContactID in (select ContactServiceID from ContactMainTable where Type = 3 and IsDeletedLocally = 0)
group by ac.ContactID, act.Subject
having count(act.Subject) > 1

--with GROUP BY to get the latest modifiedOn
 JobAttachment as (select ac.ContactID, replace(replace(act.Subject,'.txt','.doc'),',','') as FileName, max(act.ModifiedOn) as ModifiedOn
from ActivityContacts ac --> Remove comma in the file name
left join ActivitiesTable act on act.ActivityID = ac.ActivityID
where ac.ContactType = 3 and act.ActivityType = 2 --> ContactType 2 for Company, 1 for Contact, 3 for Job | ActivityType 2 is for File
and (act.Subject like '%.pdf' or act.Subject like '%.doc%' or act.Subject like '%.xls%' or act.Subject like '%.rtf' or act.Subject like '%.html' or act.Subject like '%.txt')
group by ac.ContactID, act.Subject)

--