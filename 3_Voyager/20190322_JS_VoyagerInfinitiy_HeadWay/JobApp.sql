with 
maxInterviewLevel as (
       select intInterviewProcessId, max(intInterviewId) as maxInterviewLevelId--max(tintInterviewLevelId) as maxInterviewLebel
       from dInterview
       group by intInterviewProcessId)

, tempInterview as (
       select i.*
       from maxInterviewLevel mil 
       left join dInterview i on mil.maxInterviewLevelId = i.intInterviewId)

, latestPlacement as (
       select js.intJobId, js.intCandidateId, max(dtupdated) as latestUpdate--max(js.intPlacementId)--, vchSystemEventAction
       from dPlacement js 
       group by js.intJobId, js.intCandidateId)

, tempPlacement as (
       select p.*
       from dPlacement p left join latestPlacement lp on p.intJobId = lp.intJobId and p.intCandidateId = lp.intCandidateId and p.dtupdated = lp.latestUpdate
       where latestUpdate is not null)

, jobApp as (
       select js.intJobId, /*j.datStartDate*/ j.dtInserted as datStartDate, j.intCompanyId as CompID, js.intCandidateId, tintShortlisted, tintCommunicated, tintInterested, tintSummarySubmit, tintCVSubmit, tintInterview, i.intInterviewProcessId, iif(intInterviewId is null, 999999,intInterviewId) as intInterviewId, i.tintInterviewLevelId, vchInterviewLevelName, i.tintInterviewStatusId, vchInterviewStatusName, tintOffered, tintPlaced, intPlacementId, p.tintPlacementStatusId, vchPlacementStatusName, vchSystemEventAction
       from dJobShortlist js
       left join tempPlacement p on js.intJobId = p.intJobId and js.intCandidateId = p.intCandidateId
       left join refPlacementStatus ps on p.tintPlacementStatusId = ps.tintPlacementStatusId
       left join sEventAction ea on js.tintSystemEventActionId = ea.tintSystemEventActionId
       left join dInterviewProcess ip on js.intJobId = ip.intJobId and js.intCandidateId = ip.intCandidateId
       left join tempInterview i on ip.intInterviewProcessId = i.intInterviewProcessId
       left join refInterviewLevel il on i.tintInterviewLevelId = il.tintInterviewLevelId
       left join refInterviewStatus ris on i.tintInterviewStatusId = ris.tintInterviewStatusId
       left join dJob j on js.intJobId = j.intJobId)
       --where tintPlaced = 1 and intPlacementId is not null
       
--select distinct tintInterviewLevelId, vchInterviewLevelName from jobApp
--select * from  jobApp --where tintInterview>1 --where tintPlacementStatusId = 4 or tintPlacementStatusId =9
--select distinct from 
--select * from jobApp where intJobId = 5118
--CREATE TABLE temp_Can (
--    intCandidateId int
--);
--select distinct intCandidateId
--from  jobApp where intInterviewId <> 14264 and intInterviewId <> 13470 and CompID in (2,455)
--insert into importJobApp
, ja0 as (
select intJobId as intJobId, --'application-positionExternalId',
		intCandidateId as intCandidateId, --'application-candidateExternalId',
		isnull(datStartDate,'') as datStartDate,
		--*,
		isnull(iif(tintPlaced = 1, iif(tintPlacementStatusId in (4,9),'SENT','OFFERED'), --PLACED
		iif(tintOffered >1, 'SENT',
		iif(tintOffered =1, 'OFFERED',
		iif(tintInterviewStatusId in (6,7,8,9), 'SENT',
		iif(tintInterviewLevelId in (0,1,101,104), 'FIRST_INTERVIEW',
		iif(tintInterviewLevelId in (2,3,102,103,105,106), 'SECOND_INTERVIEW',
		iif(tintCVSubmit = 1 or tintSummarySubmit = 1, 'SENT','SHORTLISTED'))))))),'') as stage --'application-Stage'
from  jobApp
--where intInterviewId <> 14264 and intInterviewId <> 13470 --and CompID in (2,455)--and intInterviewId = 7583
--and intCandidateId in (48445,31880)
--and intJobid in (4177,4236,4361,4683,4822,4992,5124,5227,5228,5229,5230,5231,5232,5233,5262,5270,5371,5402,5417,5418,5419,5420,5421,5422,5423,5424,5425,5426,5427,5428,5429,5430,5431,5432) 
--or intCandidateId in (48445,31880)--,44007,15490,44296,16796,38402,44798,44982,38455,15944,45834,45826,10987,22607,11200,52754,53354,19816,40589,44976,45050,44823,3334,44314,45794,22999,51044,2732,52304,44988)
)
--select * from refPlacementStatus

--select * from dJobShortlist where intJobId = 4221

, ja1 ("application-positionExternalId","application-candidateExternalId", datStartDate, "application-Stage", rn) as (
       SELECT 
                intJobId
              , intCandidateId
              , convert(datetime, datStartDate) as datStartDate
              , stage
              , rn = ROW_NUMBER() OVER (PARTITION BY intJobId,intCandidateId,stage ORDER BY intJobId desc) FROM ja0 )

 -- select * from ja1 where ja1.rn = 1     -->> IMPORT: JOB APPLICATION

-- GROSS ANNUAL SALARY
select
       ja1.*
       , isnull(j.datStartDate,'') as 'position-startDate'
       , 1 as 'use_quick_fee_forecast'
       --, convert(float, iif(j.decMaxSalaryFeePercentage is NULL,'',j.decMaxSalaryFeePercentage) ) as 'percentage_of_annual_salary' --'Use Quick Fee Forecast'
       --, convert(float, iif(j.decMaxSalaryFee is NULL,'',j.decMaxSalaryFee) ) as 'gross_annual_salary - pay_rate' -- 'Compensation SALARY TO'
from ja1 
left join dJob j on j.intJobId = ja1.[application-positionExternalId]
--where ja1.rn = 1 and ja1.[application-Stage] = 'PLACEMENT_PERMANENT'
--and [application-positionExternalId] = 'NJFS2379'