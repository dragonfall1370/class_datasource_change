with ContactMaxID as (select 
case when ct.intCompanyId is NULL then '0'
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

, dup as (SELECT intJobId, vchClientJobTitle, ROW_NUMBER() OVER(PARTITION BY vchClientJobTitle ORDER BY intJobId ASC) AS rn 
FROM dJob)


--MAIN SCRIPT
--insert into importJob
select 
case 
	when (ctc.intCompanyTierContactId = '' or ctc.intCompanyTierContactId is NULL) and j.intCompanyId in (select CompanyId from ContactMaxID) then cm.ContactMaxID
	when (ctc.intCompanyTierContactId = '' or ctc.intCompanyTierContactId is NULL) and j.intCompanyId not in (select CompanyID from ContactMaxID) then '0'
	when ctc.intCompanyTierContactId is NULL and j.intCompanyId is NULL then '0'
	else ctc.intCompanyTierContactId end as 'position-contactId'
, j.intCompanyId as 'CompanyID'
, j.intMainContactId as 'MainContactId'
, ctc.intCompanyTierContactId as 'ContactID'
, j.intJobId as 'position-externalId'
, j.vchClientJobTitle as 'position-title(old)'
, iif(j.intJobId in (select intJobId from dup where dup.rn > 1)
	, iif(dup.vchClientJobTitle = '' or dup.vchClientJobTitle is NULL,concat('No job title - ID',dup.intJobId),concat(dup.vchClientJobTitle,' - ', dup.intJobId))
	, iif(j.vchClientJobTitle = '' or j.vchClientJobTitle is null,concat ('No job title - ID',j.intJobId),j.vchClientJobTitle)) as 'position-title'
, iif(j.sintNumberOfPlaces = 0, 1, j.sintNumberOfPlaces) as 'position-headcount'
, iif(j.tintPayCurrencyId = 0, '', rc.vchCurrencyName) as 'position-currency'
, iif(j.tintJobType = 0 or j.tintJobType = 1, 'PERMANENT','CONTRACT') as 'position-type'
, jo.jobOwner as 'position-owners'
, j.vchDescription as 'position-internalDescription'
, j.datStartDate as 'position-startDate'
--, iif(j.datEndDate is not null, j.datEndDate,iif(j.tintJobStatusId in (105,101,107,108,6,5,109,110,111,102,106), DATEADD(m, 1, j.datStartDate), null)) as 'position-endDate'
, left(
	concat('Job External ID: ',j.intJobId,char(10)
	, concat(char(10),'Voyager Job Code: ',j.vchStandardRefCode,char(10))
	, iif(j.tintJobType = 0, concat(char(10),'Job Type: Permanent',char(10), char(10)),iif(j.tintJobType = 1, concat(char(10),'Job Type: Permanent',char(10), char(10)),concat(char(10),'Job Type: Contract',char(10), char(10))))--need answer from customer about the job type
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
			left join dCompany com on j.intCompanyId = com.intCompanyId