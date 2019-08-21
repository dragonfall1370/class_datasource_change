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
	,case when vchFileType like '.eml' then null --e.msgfilename  
		 else
		 concat(aj.intAttachmentId,'_', 
		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
		 end as attachmentName
from lAttachmentJob aj left join dAttachment a on aj.intAttachmentId = a.intAttachmentId
						--left join email e on a.intAttachmentId = e.AttachmentID
where vchFileType not in ('.mp4')
/*union
select ej.intJobId, ae.intAttachmentId , null --, em.msgfilename as attachmentName
from lEventJob ej left join dEvent e on ej.intEventId = e.intEventId
				left join lAttachmentEvent ae on ej.intEventId = ae.intEventId
				left join dAttachment a on ae.intAttachmentId = a.intAttachmentId
				--left join email em on ae.intAttachmentId = em.AttachmentID
				left join dUser u on ej.intInsertedById = u.intUserId
				left join dPerson p1 on u.intPersonId = p1.intPersonId
where em.AttachmentID is not null*/
)

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
--select top 100 * from jobAttachment


--DUPLICATE JOB
, dup as (SELECT intJobId, vchClientJobTitle, ROW_NUMBER() OVER(PARTITION BY vchClientJobTitle ORDER BY intJobId ASC) AS rn 
FROM dJob)

--select * from ContactMaxID order by CompanyId

--MAIN SCRIPT
--insert into importJob
select 
case 
	when (ctc.intCompanyTierContactId = '' or ctc.intCompanyTierContactId is NULL) and j.intCompanyId in (select CompanyId from ContactMaxID) then concat('NJFS',cm.ContactMaxID)
	when (ctc.intCompanyTierContactId = '' or ctc.intCompanyTierContactId is NULL) and j.intCompanyId not in (select CompanyID from ContactMaxID) then 'NJFS9999999'
	when ctc.intCompanyTierContactId is NULL and j.intCompanyId is NULL then 'NJFS9999999'
	else concat('NJFS',ctc.intCompanyTierContactId) end as 'position-contactId'
, j.intCompanyId as 'CompanyID'
, j.intMainContactId as 'MainContactId'
, ctc.intCompanyTierContactId as 'ContactID'
, concat('NJFS',j.intJobId) as 'position-externalId'
--, j.vchClientJobTitle as 'position-title(old)'
, iif(dup.rn > 1
	, iif(dup.vchClientJobTitle = '' or dup.vchClientJobTitle is NULL,concat('Job title - ID ',dup.intJobId),concat(dup.vchClientJobTitle,' - ', dup.intJobId))
	, iif(j.vchClientJobTitle = '' or j.vchClientJobTitle is null,concat ('Job title - ID ',j.intJobId),j.vchClientJobTitle)) as 'position-title'

, iif(j.tintJobType = 0 or j.tintJobType = 1, 'PERMANENT','CONTRACT') as 'position-type'

, j.datStartDate as 'position-startDate' --WRONG-->>>
--       , decPackageMinSalary
--       , decPackageMaxSalary
--, decOTEMinSalary
--, decOTEMaxSalary
--, tintSalaryIntervalId
--, decMinSalaryFee
--, decMaxSalaryFee
--, decMinSalaryFeePercentage
--, decMaxSalaryFeePercentage
--       , decCapFeeAt
--       , decRateFeePercentage
--       , decMinRateFee
--       , decMaxRateFee
--, iif(j.decBasicMinSalary is NULL,'',concat('Salary Min: ',j.decBasicMinSalary,char(10),char(10)))
--, iif(j.decBasicMaxSalary is NULL,'',concat('Salary Max: ',j.decBasicMaxSalary,char(10),char(10)))
--, iif(j.decMinSalaryFeePercentage is NULL,'',concat('Fee % Min: ',j.decMinSalaryFeePercentage,char(10),char(10)))
, 1 as 'use_quick_fee_forecast'
, convert(float, iif(j.decMaxSalaryFeePercentage is NULL,'',j.decMaxSalaryFeePercentage) ) as 'percentage_of_annual_salary' --'Use Quick Fee Forecast' --WRONG-->>>
--, j.decSalaryFeePercentage
--, iif(j.decMinSalaryFee is NULL,'',concat('Fee Value Min: ',j.decMinSalaryFee,char(10),char(10))) as 'Compensation SALARY FROM'
, convert(float, iif(j.decMaxSalaryFee is NULL,'',j.decMaxSalaryFee) ) as 'gross_annual_salary - pay_rate' -- 'Compensation SALARY TO' --WRONG-->>>
-- select count(*) --1502 -- select distinct intCandidateReportsToId --datStartDate --tintJobStatusId, decBasicMinSalary, decBasicMaxSalary -- select distinct j.tintJobStatusId as 'Custom Field > Job Status' -->> dbo.refJobStatus -- select *
from dJob j left join ContactMaxID cm on j.intCompanyId = cm.CompanyId
			left join dup on j.intJobId = dup.intJobId
			left join lCompanyTierContact ctc on j.intMainContactId = ctc.intContactId and j.intMainContactCompanyTierId = ctc.intCompanyTierId
			left join refCurrency rc on j.tintPayCurrencyId = rc.tintCurrencyId
			left join jobOwner jo on j.intJobId = jo.intJobId
			left join jobOwner jo2 on j.intCandidateReportsToId = jo2.intJobId -- Reports To
			left join refJobStatus rjs on j.tintJobStatusId = rjs.tintJobStatusId
			left join JobEvents je on j.intJobId = je.intJobId
			left join combinedJobLocation jl on j.intJobId = jl.intJobId
			left join jobAttachment ja on j.intJobId = ja.intJobId
			left join dCompany com on j.intCompanyId = com.intCompanyId
--where j.decMinSalaryFee > j.decMaxSalaryFee
where j.tintJobType in (0,1)
--and j.intJobId = 2379
;


--select * from dJob j where  j.intJobId = 2379
with t as (
select 
          concat('NJFS',p.intJobId) as 'intJobId'
       , concat('NJFS',p.intCandidateId) as 'intCandidateId'
       , p.decSalary as 'salary'
       , p.decSalaryFeePercentage as 'fee'
       , p.decSalaryFeeValue 'profit'
       , iif(j.tintJobType = 0 or j.tintJobType = 1, 'PERMANENT','CONTRACT') as 'position-type'
       , p.dtInserted as 'offer_date'
       , p.datStartDate as 'start_date'
       , p.datPlacedDate as 'placed_date'
-- select count(*) -- select p.*
from dPlacement p 
left join dJob j on j.intJobId = p.intJobId --where p.intJobId = 2379
)

-- >>> for JOB TYPE = PERMANENT  only
select 
       intJobId as 'job_external_id'
       , intCandidateId as 'candidate_external_id'
       , 3 as 'draft_offer=' 
       , 1 as 'position_type='
       , getdate() as 'latest_update_date='
       , -10 as 'latest_user_id='
       , salary as 'gross_annual_salary='
       , salary as 'payrate='
       , profit as 'projected_profit='
       , fee as 'percentage_of_annual_salary='
       , convert(datetime, offer_date) as 'offer_date='
       , convert(datetime, start_date) as 'start_date='
       , convert(datetime, placed_date) as 'placed_date='
from t where [position-type] = 'PERMANENT'
and intJobId = 'NJFS162'  and intCandidateId = 'NJFS1847'
--'NJFS2379'


/*
select 
  xExport_Del
, dtConfirmed
, dtInserted
, dtUpdated
, datPlacedDate
, datStartDate
, datEndDate
from dPlacement p  where p.intJobId = 2379
*/