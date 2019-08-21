with ContactMaxID as (select 
case when ct.intCompanyId is NULL then '9999999'
else ct.intCompanyId end as CompanyId
, max(intCompanyTierContactId) as ContactMaxID 
from lCompanyTierContact ctc left join dCompanyTier ct on ctc.intCompanyTierId = ct.intCompanyTierId
group by intCompanyId)

, tempJobOwner as (select cj.*, u.vchEmail, ROW_NUMBER() OVER(PARTITION BY intJobId ORDER BY intConsultantId ASC) AS rn
 from lConsultantJob cj left join dUser u on cj.intConsultantId = u.intUserId)

, jobOwner as (SELECT intJobId, 
     STUFF(
         (SELECT ',' + vchEmail
          from  tempJobOwner
          WHERE intJobId =jo.intJobId
    order by intJobId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS jobOwner
FROM tempJobOwner as jo
GROUP BY jo.intJobId)

----------------Get all events of a job to a comment
, tempEvents as (select intJobId, ej.intEventId, sdtEventDate, vchShortname, vchEventDetail, vchEventActionName
from lEventJob ej left join dEvent e on ej.intEventId = e.intEventId
				  left join dUser u on e.intLoggedById = u.intUserId
				  left join svw_EventAction sea on e.sintEventActionId = sea.sintEventActionId)

, JobEvents as (select intJobId,
STUFF(
         (SELECT '<hr>' + 'Event Date: ' + convert(varchar(20),sdtEventDate,120) + char(10) + 'Logged By: ' + vchShortname + char(10)
		  + coalesce('Action: ' + vchEventActionName + char(10), '')
		  + iif(vchEventDetail = '' or vchEventDetail is null,'',concat('Event Detail: ',char(10),vchEventDetail))
          from  tempEvents
          WHERE intJobId = te.intJobId
		  order by sdtEventDate desc
          FOR XML PATH (''),TYPE).value('.','nvarchar(MAX)')
          , 1, 4, '')  AS eventComment
FROM tempEvents as te
GROUP BY te.intJobId)

, combinedJobLocation as (select j.intJobId
	, Stuff(
			  Coalesce(' ' + NULLIF(ltrim(rtrim(vchAddressLine1)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(vchAddressLine2)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(vchAddressLine3)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(vchTown)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(vchCounty)), ''), '')
			+ Coalesce(', ' + NULLIF(rc.vchCountryName, ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(vchPostCode)), ''), '')
			, 1, 1, '') as 'jobLocation'
from dJob j left join dJobLocation jl on j.intJobId = jl.intJobId
			left join refCountry rc on jl.sintCountryId = rc.sintCountryId)

--, tempJobAttachment as(
--SELECT intJobId, aj.intAttachmentId, ROW_NUMBER() OVER(PARTITION BY intJobId ORDER BY aj.intAttachmentId ASC) AS rn,
--		 concat(aj.intAttachmentId,'_', 
--		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
--		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
--		 as attachmentName
--from lAttachmentJob aj left join dAttachment a on aj.intAttachmentId = a.intAttachmentId
--where vchFileType not in ('.mp4'))
--Because the file name of the msg file is not followed this format so we have to use the file name from the actual msg file

, tempJobAttachment as(
SELECT intJobId, aj.intAttachmentId--, ROW_NUMBER() OVER(PARTITION BY intJobId ORDER BY aj.intAttachmentId ASC) AS rn
	, concat(aj.intAttachmentId,'_', 
		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
		as attachmentName
from lAttachmentJob aj left join dAttachment a on aj.intAttachmentId = a.intAttachmentId
where vchFileType not in ('.mp4'))
--union--NJF Contract have no attachment from job event
--select ej.intJobId, ae.intAttachmentId 
--	, concat(ae.intAttachmentId,'_', 
--		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
--		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
--		as attachmentName
--from lEventJob ej left join dEvent e on ej.intEventId = e.intEventId
--				left join lAttachmentEvent ae on ej.intEventId = ae.intEventId
--				left join dAttachment a on ae.intAttachmentId = a.intAttachmentId
--where ae.intAttachmentId is not null and vchFileType not in ('.mp4'))

--union--NJF Contract have no email attachment
--select ej.intJobId, ae.intAttachmentId, em.msgfilename as attachmentName
--from lEventJob ej left join dEvent e on ej.intEventId = e.intEventId
--				left join lAttachmentEvent ae on ej.intEventId = ae.intEventId
--				left join dAttachment a on ae.intAttachmentId = a.intAttachmentId
--				left join email em on ae.intAttachmentId = em.AttachmentID
--				left join dUser u on ej.intInsertedById = u.intUserId
--				left join dPerson p1 on u.intPersonId = p1.intPersonId
--where em.AttachmentID is not null)

, jobAttachment as (SELECT intJobId, 
     STUFF(
         (SELECT ',' + replace(attachmentName,'%','_')
          from  tempJobAttachment
          WHERE intJobId =ja.intJobId
    order by intJobId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS jobAttachments
FROM tempJobAttachment as ja
GROUP BY ja.intJobId)

--DUPLICATE JOB
, dup as (SELECT intJobId, vchClientJobTitle, ROW_NUMBER() OVER(PARTITION BY vchClientJobTitle ORDER BY intJobId ASC) AS rn 
FROM dJob)

--select * from ContactMaxID order by CompanyId

--MAIN SCRIPT
--insert into importJob
select 
case 
	when (ctc.intCompanyTierContactId = '' or ctc.intCompanyTierContactId is NULL) and j.intCompanyId in (select CompanyId from ContactMaxID) then concat('NJFC',cm.ContactMaxID)
	when (ctc.intCompanyTierContactId = '' or ctc.intCompanyTierContactId is NULL) and j.intCompanyId not in (select CompanyID from ContactMaxID) then 'NJFC9999999'
	when ctc.intCompanyTierContactId is NULL and j.intCompanyId is NULL then 'NJFC9999999'
	else concat('NJFC',ctc.intCompanyTierContactId) end as 'position-contactId'
, j.intCompanyId as 'CompanyID'
, j.intMainContactId as 'MainContactId'
, ctc.intCompanyTierContactId as 'ContactID'
, concat('NJFC',j.intJobId) as 'position-externalId'
, j.vchClientJobTitle as 'position-title(old)'
, iif(j.intJobId in (select intJobId from dup where dup.rn > 1)
	, iif(dup.vchClientJobTitle = '' or dup.vchClientJobTitle is NULL,concat('NJF Contract - No job title - ID',dup.intJobId),concat(dup.vchClientJobTitle,' - ', dup.intJobId))
	, iif(j.vchClientJobTitle = '' or j.vchClientJobTitle is null,concat ('NJF Contract - No job title - ID',j.intJobId),j.vchClientJobTitle)) as 'position-title'
, iif(j.sintNumberOfPlaces = 0, 1, j.sintNumberOfPlaces) as 'position-headcount'
, iif(j.tintPayCurrencyId = 0, '', rc.vchCurrencyName) as 'position-currency'
, iif(j.tintJobType = 0 or j.tintJobType = 1, 'PERMANENT','CONTRACT') as 'position-type'
, jo.jobOwner as 'position-owners'
, j.vchDescription as 'position-internalDescription'
, left(je.eventComment,32000) as 'position-comment'-- this field is not supported importing so have to inject
, j.datStartDate as 'position-startDate'
, ja.jobAttachments as 'position-document'
, iif(j.datEndDate is not null, j.datEndDate,iif(j.tintJobStatusId in (105,101,107,108,6,5,109,110,111,102,106), DATEADD(m, 1, j.datStartDate), null)) as 'position-endDate'
, left(
	concat('Job External ID: NJFC',j.intJobId,char(10)
	, concat(char(10),'Voyager Job Code: ',j.vchStandardRefCode,char(10))
	, iif(j.tintJobType = 0, concat(char(10),'Job Type: Permanent',char(10), char(10)),iif(j.tintJobType = 1, concat(char(10),'Job Type: Permanent',char(10), char(10)),concat(char(10),'Job Type: Contract',char(10), char(10))))--need answer from customer about the job type
	, iif(jl.jobLocation = '' or jl.jobLocation is NULL,'',concat('Job Location: ',jl.jobLocation,char(10),char(10)))
	, iif(rjs.vchJobStatusName = '' or rjs.vchJobStatusName is NULL,'',concat('Job Status: ',rjs.vchJobStatusName,char(10),char(10)))
	, iif(j.vchClientRef = '' or j.vchClientRef is NULL,'',concat('Client Ref: ',j.vchClientRef,char(10),char(10)))
	, iif(j.decBasicMinSalary is NULL,'',concat('Salary Min: ',j.decBasicMinSalary,char(10),char(10)))
	, iif(j.decBasicMaxSalary is NULL,'',concat('Salary Max: ',j.decBasicMaxSalary,char(10),char(10)))
	, iif(j.decMaxSalaryFeePercentage is NULL,'',concat('Fee % Max: ',j.decMaxSalaryFeePercentage,char(10),char(10)))
	, iif(j.decMinSalaryFee is NULL,'',concat('Fee Value Min: ',j.decMinSalaryFee,char(10),char(10)))
	, iif(j.decMaxSalaryFee is NULL,'',concat('Fee Value Max: ',j.decMaxSalaryFee,char(10),char(10)))
	, iif(j.vchComments = '','',concat('Comments: ',char(10),j.vchComments)))
	,32000) as 'position-note'
from dJob j left join ContactMaxID cm on j.intCompanyId = cm.CompanyId
			left join dup on j.intJobId = dup.intJobId
			left join lCompanyTierContact ctc on j.intMainContactId = ctc.intContactId and j.intMainContactCompanyTierId = ctc.intCompanyTierId
			left join refCurrency rc on j.tintPayCurrencyId = rc.tintCurrencyId
			left join jobOwner jo on j.intJobId = jo.intJobId
			left join refJobStatus rjs on j.tintJobStatusId = rjs.tintJobStatusId
			left join JobEvents je on j.intJobId = je.intJobId
			left join combinedJobLocation jl on j.intJobId = jl.intJobId
			left join jobAttachment ja on j.intJobId = ja.intJobId
			left join dCompany com on j.intCompanyId = com.intCompanyId
--where len(ja.jobAttachments) > 32000
--where j.datEndDate is not null--tintJobType = 2
--where j.intJobId in (4177,4236,3063,4361,747,1708,4683,4822,4992,5233,857,2850,4223,5124,5270,5371,5402)
--where ctc.intCompanyTierContactId in (33316,80764) or j.intJobId in (4177,4236,3063,4361,747,1708,4683,4822,4992,5233,857,2850,4223,5124,5270,5371,5402)
--where j.intJobId in (4177,4236)--,3063,4361,747,1708,4683,4822,4992,5233,857,2850,4223,5124,5270,5371,5402)
--where j.intCompanyId in (2,455)
--j.intCompanyId in (5559,6254,499,592,1436,1670,1887,2054,6275,6397,6676,2,4546,5081,5508,6251,6488,6504,6803)
--ctc.intCompanyTierContactId = 80764
--and rjs.vchJobStatusName not in ('Open','Ongoing','On Hold','Partially Filled','Always Looking','Urgent','speculative')
--select * from dCompany where intCompanyId = 5559
--select * from refJobStatus

--select * from dJob where intCompanyId = 6254
--select * from refJobStatus