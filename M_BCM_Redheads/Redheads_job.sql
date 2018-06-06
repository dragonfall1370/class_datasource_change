with
--DOCUMENT / ACTIVITY ATTACHMENTS
 JobAttachment as (select distinct ac.ContactID, replace(replace(act.Subject,'.txt','.doc'),',','') as FileName, max(act.ModifiedOn) as ModifiedOn
from ActivityContacts ac --> Remove comma in the file name
left join ActivitiesTable act on act.ActivityID = ac.ActivityID
where ac.ContactType = 3 and act.ActivityType = 2 --> ContactType 2 for Company, 1 for Contact, 3 for Job | ActivityType 2 is for File
and (act.Subject like '%.pdf' or act.Subject like '%.doc%' or act.Subject like '%.xls%' or act.Subject like '%.rtf' or act.Subject like '%.html' or act.Subject like '%.txt')
group by ac.ContactID, act.Subject)

, JobFiles as (SELECT
     ContactID,
     STUFF(
         (SELECT ',' + FileName
          from  JobAttachment
          WHERE ContactID = a.ContactID
		  order by ModifiedOn desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS JobFiles
FROM JobAttachment as a
GROUP BY a.ContactID)


--If contactID is empty or null, get max contactID
, ContactMaxID as (select case when ParentContactServiceID is NULL then 9999999 else ParentContactServiceID end as ParentContactServiceID, max(ContactServiceID) as ContactMaxID from ContactMainTable
where Type = 1 and IsDeletedLocally = 0
group by ParentContactServiceID)

--DUPLICATION REGCONITION
, dup as (SELECT ContactServiceID, ParentContactServiceID, Subject, ROW_NUMBER() OVER(PARTITION BY Subject ORDER BY ContactServiceID, ParentContactServiceID ASC) AS rn 
FROM ContactMainTable where Type = 3 and IsDeletedLocally = 0)

--CONTACT IS CANDIDATE, JOB CANNOT BE LINKED --> recognize that jobs may link to candidate, so to get default contact instead
, ContactCandidate as (select distinct ContactServiceID from ContactCategoriesTable where CategoryName not in ('Competitor', 'Personal', 'Inactive Client', 'Client', 'Source of Candidates', 'Restraint of Trade', 'Supplier','Reference'))

--MAIN SCRIPT
select
case --> check if contact is contact/candidate - if NOT, map with default contact RH9999999
when cmt.ParentType = 1 and cmt.ParentContactServiceID in (select ContactServiceID from ContactCandidate) then 'RH9999999'
when cmt.ParentType = 1 and cmt.ParentContactServiceID not in (select ContactServiceID from ContactCandidate) then concat('RH',cmt.ParentContactServiceID)
when cmt.ParentType = 2 then concat('RH',cm.ContactMaxID)
else 'RH9999999' end as 'position-contactId'
, cm.ParentContactServiceID as '(Company4maxContact)'
, cm.ContactMaxID as '(maxContactID)'
, cmt.ParentContactServiceID
, concat('RH',cmt.ContactServiceID) as 'position-externalId'
, iif(cmt.ContactServiceID in (select ContactServiceID from dup where dup.rn > 1)
	, iif(dup.Subject = '' or dup.Subject is NULL,concat('Not job title-',dup.ContactServiceID),concat(dup.Subject,'-',dup.ContactServiceID))
	, iif(cmt.Subject = '' or cmt.Subject is null,concat('Not job title-',cmt.ContactServiceID),cmt.Subject)) as 'position-title'
, cmt.Subject as '(OriginalJobTitle)'
, iif(cdt.AssignedTo = '' or cdt.AssignedTo is NULL
	,concat(right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)),'@redheads.co.za')
	,concat(right(cdt.AssignedTo,len(cdt.AssignedTo)-charindex('\',cdt.AssignedTo)),'@redheads.co.za')) as 'position-owner'
, convert(varchar(10),cast(uf.userField14 as datetime),120) as 'position-startDate'
, case when cdt.OpportunityCloseDate is NULL and cdt.OpportunityStage in ('Closed Lost','Closed Won','Closed - position cancelled','Closed - internal appointment') then convert(varchar(10),getdate()-1,120)
	else convert(varchar(10),cdt.OpportunityCloseDate,120) end as 'position-endDate'
, jf.JobFiles as 'position-document'
, concat(coalesce('Notes - Job Specification: ' + cast(cdt.ContactNotes as nvarchar(max)),'')
	, iif(cast(uf.UserField1 as nvarchar(max)) = '' or uf.UserField1 is NULL,'',concat('Requirements Qualification: ',cast(uf.UserField1 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField2 as nvarchar(max)) = '' or uf.UserField2 is NULL,'',concat('Skills: ',cast(uf.UserField2 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField3 as nvarchar(max)) = '' or uf.UserField3 is NULL,'',concat('Experience: ',cast(uf.UserField3 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField4 as nvarchar(max)) = '' or uf.UserField4 is NULL,'',concat('Personality: ',cast(uf.UserField4 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField5 as nvarchar(max)) = '' or uf.UserField5 is NULL,'',concat('EE: ',cast(uf.UserField5 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField6 as nvarchar(max)) = '' or uf.UserField6 is NULL,'',concat('Other Requirements: ',cast(uf.UserField6 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField8 as nvarchar(max)) = '' or uf.UserField8 is NULL,'',concat('Task: ',cast(uf.UserField8 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField42 as nvarchar(max)) = '' or uf.UserField42 is NULL,'',concat('Reports To: ',cast(uf.UserField42 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField10 as nvarchar(max)) = '' or uf.UserField10 is NULL,'',concat('Contractor / Perm: ',cast(uf.UserField10 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField11 as nvarchar(max)) = '' or uf.UserField11 is NULL,'',concat('Duration of Contract: ',cast(uf.UserField11 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField12 as nvarchar(max)) = '' or uf.UserField12 is NULL,'',concat('Hourly Rate: ',cast(uf.UserField12 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField13 as nvarchar(max)) = '' or uf.UserField13 is NULL,'',concat('Salary Range: ',cast(uf.UserField13 as nvarchar(max)),char(10)))) as 'position-internalDescription'
, concat(concat('BCM external ID: ',cmt.ContactServiceID),char(10)
	, coalesce('Source: ' + cdt.LeadSource + char(10),'')
	, concat('Created by: ',right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)),char(10))
	, iif(cast(uf.UserField33 as nvarchar(max)) = '' or uf.UserField33 is NULL,'',concat('Vacancy Number: ',cast(uf.UserField33 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField48 as nvarchar(max)) = '' or uf.UserField48 is NULL,'',concat('Discipline 1: ',cast(uf.UserField48 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField49 as nvarchar(max)) = '' or uf.UserField49 is NULL,'',concat('Discipline 2: ',cast(uf.UserField49 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField45 as nvarchar(max)) = '' or uf.UserField45 is NULL,'',concat('Priority of Opportunity: ',cast(uf.UserField45 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField46 as nvarchar(max)) = '' or uf.UserField46 is NULL,'',concat('Urgency: ',cast(uf.UserField46 as nvarchar(max)),char(10)))
	, iif(cdt.OpportunityStage = '' or cdt.OpportunityStage is NULL,'',concat('Sales Stage: ',cdt.OpportunityStage,char(10)))
	, coalesce('Most recent request: ' + convert(varchar(10),cast(uf.userField14 as datetime),120) + char(10),'')
	, iif(cast(uf.UserField15 as nvarchar(max)) = '' or uf.UserField15 is NULL,'',concat('Previous request: ',cast(uf.UserField15 as nvarchar(max)),char(10)))
	, coalesce('Start Date: ' + convert(varchar(10),cast(uf.userField16 as datetime),120) + char(10),'')
	, iif(cast(uf.UserField17 as nvarchar(max)) = '' or uf.UserField17 is NULL,'',concat('Start Date Note: ',cast(uf.UserField17 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField50 as nvarchar(max)) = '' or uf.UserField50 is NULL,'',concat('Selling Points: ',cast(uf.UserField50 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField25 as nvarchar(max)) = '' or uf.UserField25 is NULL,'',concat('Redheads Database: ',cast(uf.UserField25 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField26 as nvarchar(max)) = '' or uf.UserField26 is NULL,'',concat('Pnet: ',cast(uf.UserField26 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField28 as nvarchar(max)) = '' or uf.UserField28 is NULL,'',concat('CareerJunction: ',cast(uf.UserField28 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField54 as nvarchar(max)) = '' or uf.UserField54 is NULL,'',concat('LinkedIn: ',cast(uf.UserField54 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField18 as nvarchar(max)) = '' or uf.UserField18 is NULL,'',concat('Potential Candidates: ',cast(uf.UserField18 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField19 as nvarchar(max)) = '' or uf.UserField19 is NULL,'',concat('Rejected By Redheads: ',cast(uf.UserField19 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField20 as nvarchar(max)) = '' or uf.UserField20 is NULL,'',concat('Candidate 1 referred: ',cast(uf.UserField20 as nvarchar(max)),char(10)))
	, coalesce('Referral 1 date: ' + convert(varchar(10),cast(uf.userField21 as datetime),120) + char(10),'')
	, iif(cast(uf.UserField34 as nvarchar(max)) = '' or uf.UserField34 is NULL,'',concat('Candidate 2 referred: ',cast(uf.UserField34 as nvarchar(max)),char(10)))
	, coalesce('Referral 2 date: ' + convert(varchar(10),cast(uf.userField37 as datetime),120) + char(10),'')
	, iif(cast(uf.UserField35 as nvarchar(max)) = '' or uf.UserField35 is NULL,'',concat('Candidate 3 referred: ',cast(uf.UserField35 as nvarchar(max)),char(10)))
	, coalesce('Referral 3 date: ' + convert(varchar(10),cast(uf.userField36 as datetime),120) + char(10),'')
	, iif(cast(uf.UserField38 as nvarchar(max)) = '' or uf.UserField38 is NULL,'',concat('Candidate 4 referred: ',cast(uf.UserField38 as nvarchar(max)),char(10)))
	, coalesce('Referral 4 date: ' + convert(varchar(10),cast(uf.userField39 as datetime),120) + char(10),'')
	, iif(cast(uf.UserField40 as nvarchar(max)) = '' or uf.UserField40 is NULL,'',concat('Candidate 5 referred: ',cast(uf.UserField40 as nvarchar(max)),char(10)))
	, coalesce('Referral 5 date: ' + convert(varchar(10),cast(uf.userField27 as datetime),120) + char(10),'')
	, iif(cast(uf.UserField23 as nvarchar(max)) = '' or uf.UserField23 is NULL,'',concat('Interview: ',cast(uf.UserField23 as nvarchar(max)),char(10)))
	, coalesce('Interview Date: ' + convert(varchar(10),cast(uf.userField43 as datetime),120) + char(10),'')
	, iif(cast(uf.UserField22 as nvarchar(max)) = '' or uf.UserField22 is NULL,'',concat('Further Assessments: ',cast(uf.UserField22 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField24 as nvarchar(max)) = '' or uf.UserField24 is NULL,'',concat('Regretted: ',cast(uf.UserField24 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField44 as nvarchar(max)) = '' or uf.UserField44 is NULL,'',concat('Placed Candidates: ',cast(uf.UserField44 as nvarchar(max)),char(10)))
	, coalesce('Redheads Website: ' + convert(varchar(10),cast(uf.userField31 as datetime),120) + char(10),'')
	, coalesce('Website Ad Removed: ' + convert(varchar(10),cast(uf.userField52 as datetime),120) + char(10),'')
	, coalesce('Pnet Ad: ' + convert(varchar(10),cast(uf.userField32 as datetime),120) + char(10),'')
	, iif(cast(uf.UserField47 as nvarchar(max)) = '' or uf.UserField47 is NULL,'',concat('CareerJunction Ad: ',cast(uf.UserField47 as nvarchar(max)),char(10)))
	, iif(cast(uf.UserField53 as nvarchar(max)) = '' or uf.UserField53 is NULL,'',concat('LinkedIn Ad: ',cast(uf.UserField53 as nvarchar(max)),char(10)))
	) as 'position-note'
, concat(iif(cbn.ContactBusinessNote = '' or cbn.ContactBusinessNote is NULL,'',concat('***Business notes: ',char(10),cbn.ContactBusinessNote,'<hr>'))
		, iif(ccn.ContactCommunicationNote = '' or ccn.ContactCommunicationNote is NULL,'',concat('***Communication notes: ',char(10),ccn.ContactCommunicationNote))) as 'position-comment'
from ContactMainTable cmt
left join dup on dup.ContactServiceID = cmt.ContactServiceID
left join ContactMaxID cm on cm.ParentContactServiceID = cmt.ParentContactServiceID
left join ContactDetailsTable cdt on cdt.ContactServiceID = cmt.ContactServiceID
left join UserFields uf on uf.ContactServiceID = cmt.ContactServiceID
left join JobFiles jf on jf.ContactID = cmt.ContactServiceID
left join ContactBusinessNote cbn on cbn.ContactID = cmt.ContactServiceID
left join ContactCommunicationNote ccn on ccn.ContactID = cmt.ContactServiceID
left join ContactCandidate cc on cc.ContactServiceID = cmt.ParentContactServiceID
where cmt.Type = 3 and cmt.IsDeletedLocally = 0 --> Type 1 for Contact | 2 for Company | 3 for Job
--and cmt.ParentContactServiceID not in (select ContactServiceID from ContactMainTable where Type = 1 and IsDeletedLocally = 1) --> to escape deleted contacts
--and cmt.ParentType = 2 --> to compare if ParentType = 2 (jobs link with company instead of contact)