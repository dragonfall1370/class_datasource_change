select ApplicantId, EmploymentTypeId, len(EmploymentTypeId) as Length  from applicants
 where employmentTypeId is not null
select * from EmploymentTypes
select * from VW_Applicant_info where employmentTypeId is not null
select * from VW_APPLICANT_GRID_VIEW where EmploymentType is not null
select ApplicantId, count(ApplicantId) from applicantEmploymentTypes
group by ApplicantId
having count(ApplicantId)>1

with TempWorkHistory as (select a.ApplicantId, wh.WorkHistoryId, wh.PersonName, wh.ClientID, wh.Company, wh.PlacementID,
 wh.FromDate, wh.ToDate, wh.Description JobTitle, wh.EmploymentTypeId, et.Description JobType, wh.PositionAttributeId, att.Description Position,
 ROW_NUMBER() OVER(PARTITION BY a.ApplicantId ORDER BY wh.WorkHistoryId ASC) AS rn
from Applicants a
 left join WorkHistory wh on a.ApplicantId = wh.ApplicantID
 left join Placements p on wh.placementID = p.placementId
 left join EmploymentTypes et on wh.EmploymentTypeId = et.EmploymentTypeId
 left join Attributes att on wh.PositionAttributeId = att.AttributeId)

, TempCurrentEmployer as (select a.ApplicantId, max(w.WorkHistoryId) as CurrentWorkId
from Applicants a
left join WorkHistory w on a.ApplicantId = w.ApplicantID
group by a.ApplicantId)
, CurrentEmployer as (select tce.ApplicantId, w. WorkHistoryId, w.Company, w.Description JobTitle, w.FromDate, w.ToDate
 from TempCurrentEmployer tce left join WorkHistory w on tce.CurrentWorkId = w.WorkHistoryId)
select * from CurrentEmployer
order by ApplicantId
select * from Applicants
select * from WorkHistory where applicantId = 20231
select * from WorkHistory --where ApplicantID = 20326
select * from VW_APPLICANT_INFO where EmploymentType is not null
select * from ApplicantActions order by ApplicantId
select * from Person
select * from Nationality
select * from users
select * from ClientContacts
select * from vw_Applicant_Info
select * from Person
select PersonID, PersonName, Surname, o.ObjectTypeId
from Person p left join Objects o on p.PersonID = o.ObjectId
where o.ObjectTypeId in (1,4) 

select a.ApplicantId, o.FileAs
from Applicants a left join Objects o on a.ApplicantId = o.ObjectID
where o.ObjectTypeId in (3)

select a.ApplicantId, p.Salutation
from Applicants a left join Person p on a.ApplicantId = p.PersonID
select a.ApplicantId, p.PersonName, p.Surname, p.GenderValueId, lv.ValueName
from Applicants a left join Person p on a.ApplicantId = p.PersonID
    left join ListValues lv on lv.ListValueId = p.GenderValueId

select a.ApplicantId, p.nationalityId, n.Nationality
from Applicants a left join Person p on a.ApplicantId = p.PersonID
					left join Nationality n on p.NationalityId = n.NationalityId

select a.ApplicantId, a.StatusId, aStt.Description
from Applicants a left join ApplicantStatus aStt on a.StatusId = aStt.ApplicantStatusId

select a.ApplicantId, a.SourceId, src.Description
from Applicants a left join Sources src on a.SourceId = src.SourceId

select a.ApplicantId, a.LocationId, lcn.Description
from Applicants a left join Locations lcn on a.LocationId = lcn.LocationId

select * from VW_DOCUMENT_GRID
select * from VW_CLIENT_GRID_VIEW
select count(*) from NotebookItems 
select count(*) from NotebookItemContent
select * from NotebookFolders 
select count(*) from NotebookLinkTypes 
select * from JobDocuments
select * from NotebookTypes 
select * from NotebookLinks where Objectid is not null
order by ObjectId

select NotebookItemId, count(NotebookItemId)
 from Documents group by NotebookItemId
having count(NotebookItemId) >1

select NotebookItemId, count(NotebookItemId)
 from NotebookLinks group by NotebookItemId
having count(NotebookItemId) >1


select * from Clientprofile
select doc.DocumentId, doc.NotebookItemId, nl.ObjectId, nl.ClientId, nl.JobId
from Documents doc-- left join NotebookItems ni on doc.NotebookItemId = ni.NotebookItemId
left join NotebookLinks nl on doc.NotebookItemId = nl.NotebookItemId
--where ObjectId = 1
order by doc.NotebookItemId
select * from Objects where ObjectId = 1
--------------------------------------------------------------------------------------------------
with tempItems as(SELECT NotebookItemId, 
     STUFF(
         (SELECT ',' + concat('Doc',DocumentID)
          from  Documents
          WHERE NotebookItemId = doc.NotebookItemId
    order by DocumentID asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS Document
FROM Documents as doc
GROUP BY doc.NotebookItemId)
, tempObjects as (select ti.NotebookItemId, ti.Document, nl.ObjectId, nl.ClientId, nl.JobId
from tempItems ti left join NotebookLinks nl on ti.NotebookItemId = nl.NotebookItemId)
--select * from tempObjects where Objectid is not null order by ObjectId
SELECT ObjectId, 
     STUFF(( SELECT ',' + Document
          from  TempObjects
          WHERE ObjectID = toj.ObjectId
    order by ObjectId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS Item
FROM TempObjects as toj
GROUP BY toj.Objectid
select NotebookItemId, ObjectId from NotebookLinks where ObjectId is not null

--select ObjectId, count(ObjectId)
-- from tempObjects group by ObjectId having count(ObjectId)>1

select NotebookItemId, count(NotebookItemId)
 from tempObjects group by NotebookItemId having count(NotebookItemId)>1

select * from INTERVIEWS
select * from VW_APPLICANT_ACTION_GRID
order by ApplicantId

SELECT NotebookItemId, ObjectId, ROW_NUMBER() OVER(PARTITION BY ObjectId ORDER BY NotebookItemId ASC) AS rn 
from NotebookLinks where Objectid is not null

SELECT NotebookItemId, ObjectId, ROW_NUMBER() OVER(PARTITION BY NotebookItemId ORDER BY ObjectId ASC) AS rn 
from NotebookLinks where Objectid is not null

select * from Documents where NotebookItemId = 4
select * from vw_Notebook_grid 
select * from NotebookLinks
select * from ClientContacts
select * from VW_PLACEMENT_GRID
select * from Interviews order by ApplicantActionId
select * from NotebookFolders where NotebookFolderId = 156
-------
select cc.ClientContactId, nbl.NotebookItemId, vng.[From], vng.Recipients, vng.Subject, vng.CreatedOn
from ClientContacts cc left join NotebookLinks nbl on cc.ContactPersonId = nbl.ObjectId
left join VW_NOTEBOOK_GRID vng on nbl.NotebookItemId = vng.NotebookItemId
------
select JobId, aa.ApplicantActionId, itv.InterviewId, itv.InterviewDate, itv.InterviewTime, itv.Notes
from ApplicantActions aa left join Interviews itv on aa.ApplicantActionId = itv.ApplicantActionId
where aa.JobId is not null
select from Interviews itv left join 
select * from Applicants
order by JobId
select * from Objects
select * from Person

SELECT distinct table_name, COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE 1=1
--and TABLE_NAME = 'YourTableName' 
AND TABLE_SCHEMA='dbo'
and COLUMN_NAME like '%DocumentID%'

select * from CVSendDocuments

select NotebookitemId, ObjectId, count(*) from NotebookLinks
group by NotebookitemId, Objectid having count(*)> 1





select * from NotebookFolders
select * from EmploymentTypes
select * from Users
select * from Person
select * from Objects
select * from Jobs
select * from VW_JOB_GRID_VIEW
select * from ContractPlacements
select * from INFORMATION_SCHEMA.COLUMNS 
where COLUMN_NAME like '%JobRefNo%' 
order by TABLE_NAME

select * from Address

with canAddress as (select apt.ApplicantId, A.AddressId, A.Building, A.Street, A.District, A.City, A.PostCode
from Applicants apt left join Address as A on apt.ApplicantId = A.ObjectId)

select ApplicantId, count(ApplicantId)
from canAddress
group by ApplicantId
having count(ApplicantId)>1

where AddressId is not null

Select * from ApplicantProfile
select * from Documents
select a.ApplicantId, CV.CVRefNo, CV.SectorId, CV.Publish, CV.Description, CV.CreatedOn, CV.UpdatedOn
 from Applicants a left join CV on a.ApplicantId = CV.ApplicantId
order by ApplicantId
select * from Sectors
select a.ApplicantId, wh.PersonName, wh.ClientID, wh.Company, wh.PlacementID , wh.FromDate, wh.ToDate, wh.Description, wh.EmploymentTypeId, et.Description, wh.PositionAttributeId, att.Description
from Applicants a
 left join WorkHistory wh on a.ApplicantId = wh.ApplicantID
 left join Placements p on wh.placementID = p.placementId
 left join EmploymentTypes et on wh.EmploymentTypeId = et.EmploymentTypeId
 left join Attributes att on wh.PositionAttributeId = att.AttributeId
order by ApplicantID
select ApplicantId, p.Notes
from applicants a left join person p on a.ApplicantId = p.PersonID

select * from Applicants 

select * from VW_APPLICANT_INFO
where ApplicantSurname = 'Williams' and ApplicantName = 'Paul'

select * from ApplicantProfile
select CVId, ApplicantId from CV where ApplicantId = '16608'
where EmploymentType is not null
order by ApplicantId

select * from WorkHistory
select * from VW_APPLICANT_ACTION_GRID
select * from VW_APPLICANT_INFO where EmploymentTypeDescription is not null
order by ApplicantId
--where PrimaryEmailAddress is not null
select * from Applicants

select ApplicantId, PrimaryEmailAddress
from VW_APPLICANT_INFO where PrimaryEmailAddress is not null
order by ApplicantId

select ApplicantId, count(ApplicantId)
from VW_APPLICANT_INFO
group by ApplicantId
having count(ApplicantId)>1
--phone
select a.ApplicantId, p.CommunicationTypeId, p.NumTrimmed
from Applicants a left join Phones p on a.ApplicantId = p.ObjectID
where p.CommunicationTypeId = 79
order by ApplicantId
--mobile
select a.ApplicantId, p.CommunicationTypeId, p.NumTrimmed
from Applicants a left join Phones p on a.ApplicantId = p.ObjectID
where p.CommunicationTypeId = 83

select a.ApplicantId, p.CommunicationTypeId, p.Num
from Applicants a left join Phones p on a.ApplicantId = p.ObjectID
where p.CommunicationTypeId = 78

select * from CommunicationTypes
select * from ApplicantEmploymentTypes
select * from Applicants where ProfileDocument is not null

select ApplicantId, count(ApplicantId)
 from ApplicantEmploymentTypes
 group by ApplicantId
 having count(ApplicantId)>1
 select * from CommunicationTypes
  select * from Phones
 -------------------------

select applicantId, source, ApplicantFileAs from VW_APPLICANT_GRID_VIEW where source is not null

select a.ApplicantId, agv.ApplicantFileAs, p.CommunicationTypeId, p.Num as LinkedIn
from Applicants a left join Phones p on a.ApplicantId = p.ObjectID
left join VW_APPLICANT_GRID_VIEW agv on a.ApplicantId = agv.ApplicantId
where (p.CommunicationTypeId = 89  or p.CommunicationTypeId = 91) and p.Num like '%linkedin%'

select * from ApplicantProfile where ProfileDocument is not null
select * from  VW_CONTACT_GRID_VIEW

 select cc.ClientContactId, cc.ContactPersonId, cgv.PersonFileAs, p.CommunicationTypeId, ct.Description, p.NumTrimmed
from ClientContacts cc left join Phones p on cc.ContactPersonId = p.ObjectID
left join VW_CONTACT_GRID_VIEW cgv on cc.ClientContactId = cgv.ClientContactId
left join CommunicationTypes ct on p.CommunicationTypeId = ct.CommunicationTypeId
where p.CommunicationTypeId = 83
-----------
 select concat('FR',a.ApplicantId) as CandidateExternalId, -10 as userId, ng.CreatedOn as CommentTimestamp
		, ng.CreatedOn as InsertTimeStamp, -10 as AssignedUserId, 1 as RelatedStatus
		,ltrim(Stuff(
				Coalesce('Created On: ' + NULLIF(convert(varchar(20),ng.CreatedOn,120), ''), '')
				+ Coalesce(char(10)+ 'Notebook Type: ' + NULLIF(ng.NotebookType, ''), '')
				+ Coalesce(char(10)+ 'From: ' + NULLIF(ng.[From], ''), '')
				+ Coalesce(char(10)+ 'Recipient(s): ' + NULLIF(left(ng.Recipients,len(ng.Recipients)-1), ''), '')
				+ Coalesce(char(10)+ 'Subject: ' + NULLIF(ng.Subject, ''), '')
			, 1, 0, '') ) as 'CommentContent'
from VW_NOTEBOOK_GRID ng 
 left join NotebookLinks nl on ng.NotebookItemId = nl.NotebookItemId
 left join Applicants a on nl.ObjectId = a.ApplicantId
where ApplicantId is not NULL and ng.NotebookType = 'Email'
order by a.ApplicantId, ng.CreatedOn desc

select * from VW_NOTEBOOK_GRID * from NotebookLinkTypes
select * from NotebookLinks
select * from NotebookItems
select * from NotebookItemContent
select * from Hot







