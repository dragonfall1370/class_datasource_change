select count(*) from ContactMainTable
where ContactServiceID in (
	select distinct ContactServiceID from ContactCategoriesTable
	where CategoryName not in ('Non-finance'))
and Type = 1 and IsDeletedLocally = 0 --1092

select count(*) from ContactMainTable
where ContactServiceID in (
	select distinct ContactServiceID from ContactCategoriesTable
	where CategoryName in ('Non-finance'))
and Type = 1 and IsDeletedLocally = 0 --458

select * from ContactMainTable
where Type = 1 and IsDeletedLocally = 0 --3804

select * from ContactMainTable
where Type = 1 and IsDeletedLocally = 1 --57

select * from ContactMainTable
where Type = 2 and IsDeletedLocally = 0 -- 484

select * from ContactMainTable
where Type = 2 and IsDeletedLocally = 1 --3

---
---
select * from ContactMainTable cmt
left join ContactDetailsTable cdt on cdt.ContactServiceID = cmt.ContactServiceID
where cmt.Type = 1
and cmt.ContactServiceID in (select ContactServiceID from ContactCategoriesTable
	where CategoryName in ('Non-finance')
	group by ContactServiceID)
and cmt.ParentType is NULL

select * from EntityUserFields
--where FieldGUID = 'FB4DF3DD-4608-497C-BF95-2DC935DC1273'
where FieldGUID = '56C137DA-23AF-4075-97EA-3D8806E341AB' | userindex = 6

select * from UserFieldTableIds

select * from UserFieldDefinitions - 
> Moederta(a)l(en): FieldGUID: FB4DF3DD-4608-497C-BF95-2DC935DC1273 > data type: 8 > Display: 1 
> Ervaring: 1CCB1740-C2B2-4BBD-870E-2E02E2CF1572
> IT kennis: 779DECB3-E0C8-489B-B461-E68818EF2072
> Beschikbaar vanaf: 2A0C4481-BF9D-42BE-89DD-AB7C1341C32F
> Algemene opmerkingen: 372B7CD7-156B-498B-AEAA-9DD52C174C57

select * from UserFieldDataTypes > Picklist

select * from EntityTypesTable

select cast(UserField6 as nvarchar(max)) from UserFields

select ufd.FieldGUID, ufd.FieldName, ufd.DataTableID, ufd.DataType, ufd.DisplayFormat, euf.EntityType, euf.UserFieldIndex, euf.DataID
from UserFieldDefinitions ufd
left join EntityUserFields euf on euf.FieldGUID = ufd.FieldGUID
where ufd.FieldGUID in ()

---
---
select * from Attachments
-------
select ca.ContactServiceID, ca.AttachmentID, concat(a.ID,'_',a.AttachLongFileName) as ContactPhoto
from ContactAttachments ca
left join Attachments a on a.ID = ca.AttachmentID
where a.AttachmentContactPhoto = 1
and a.ID in (select max(AttachmentID) as maxID from ContactAttachments group by ContactServiceID)
-------
select distinct ac.ContactID, replace(replace(act.Subject,'.txt','.doc'),',','') as FileName
, max(act.ModifiedOn) as ModifiedOn
from ActivityContacts ac
left join ActivitiesTable act on act.ActivityID = ac.ActivityID
where ac.ContactType = 1 and act.ActivityType = 2
and (act.Subject like '%.pdf' or act.Subject like '%.doc%' or act.Subject like '%.xls%' 
or act.Subject like '%.rtf' or act.Subject like '%.html' or act.Subject like '%.txt')
group by ac.ContactID, act.Subject

select * from ActivityContacts

select ActivityID, Subject, ActivityNote, CompressedRichText, CreatedBy from ActivitiesTable
where LinkToOriginal not like '%Z:\%'
and Subject like '%.doc%'

select distinct ac.ContactID, replace(replace(act.Subject,'.txt','.doc'),',','') as FileName, convert(varbinary(max), act.CompressedRichText, 1)
--, max(act.ModifiedOn) as ModifiedOn
from ActivityContacts ac
left join ActivitiesTable act on act.ActivityID = ac.ActivityID
where ac.ContactType = 1 and act.ActivityType = 2
and (act.Subject like '%.pdf' or act.Subject like '%.doc%' or act.Subject like '%.xls%' 
or act.Subject like '%.rtf' or act.Subject like '%.html' or act.Subject like '%.txt')
--group by ac.ContactID, act.Subject, act.CompressedRichText

select * from ContactAttachments
where AttachmentID = 115
where ContactServiceID = 250

select * from Attachments
where ID in (115)


---------------------------------
--DOUBLE CHECK AFTER INJECTING CUSTOM FIELDS

select * from activity

select * from activity_contact 
where contact_id = 33257

select * from contact
where external_id = 'CA2696'

select distinct external_id from candidate
where deleted_timestamp is NULL
and external_id = 'CA2696'
------------
with candidateact as (
select candidate_id, count(content) as count from activity
group by candidate_id)

select id, external_id, count from candidate c
left join candidateact ca on ca.candidate_id = c.id
where id in (select candidate_id from candidateact)
and external_id = 'CA2533'

------------
select id, external_id from contact where id not in (
select contact_id from contact_group_contact)

select * from contact

select * from candidate
where external_id = 'CA6'

select * from candidate_functional_expertise
where candidate_id = 66930