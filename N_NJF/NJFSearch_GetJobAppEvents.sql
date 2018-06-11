with maxInterviewLevel as (
select intInterviewProcessId, max(intInterviewId) as maxInterviewLevelId--max(tintInterviewLevelId) as maxInterviewLebel
from dInterview --where sdtStart <> '1900-01-01 00:00:00'
group by intInterviewProcessId)

, tempInterview as (
select i.*
from maxInterviewLevel mil left join dInterview i on mil.maxInterviewLevelId = i.intInterviewId)
--select * from tempInterview

, latestPlacement as (select js.intJobId, js.intCandidateId, max(dtupdated) as latestUpdate--max(js.intPlacementId)--, vchSystemEventAction
from dPlacement js 
group by js.intJobId, js.intCandidateId)

, tempPlacement as (select p.*
from dPlacement p left join latestPlacement lp on p.intJobId = lp.intJobId and p.intCandidateId = lp.intCandidateId and p.dtupdated = lp.latestUpdate
where latestUpdate is not null)

, temp as (
select js.intJobId,j.vchClientJobTitle, per.intPersonId, per.vchForename, per.vchSurname, coalesce(NULLIF(per.vchForename, ''), '') + Coalesce(' ' + NULLIF(per.vchSurname, ''), '') as candidateName,
 js.intCandidateId, ctc.intCompanyTierContactId,coalesce(NULLIF(per1.vchForename, ''), '') + Coalesce(' ' + NULLIF(per1.vchSurname, ''), '') as ContactName, tintShortlisted,
 tintCommunicated, tintInterested, tintSummarySubmit, tintCVSubmit, tintInterview, i.intInterviewProcessId,
 intInterviewId, i.tintInterviewLevelId, vchInterviewLevelName, i.tintInterviewStatusId, vchInterviewStatusName, i.sdtStart,
 js.dtInserted, js.dtUpdated , tintOffered, tintPlaced, intPlacementId, datPlacedDate, vchPlacementStatusName, vchSystemEventAction
from dJobShortlist js
 --left join tempPlacement p on js.intJobId = p.intJobId and js.intCandidateId = p.intCandidateId
 left join dPlacement p on js.intJobId = p.intJobId and js.intCandidateId = p.intCandidateId
 left join refPlacementStatus ps on p.tintPlacementStatusId = ps.tintPlacementStatusId
 left join sEventAction ea on js.tintSystemEventActionId = ea.tintSystemEventActionId
 left join dInterviewProcess ip on js.intJobId = ip.intJobId and js.intCandidateId = ip.intCandidateId
 --left join tempInterview i on ip.intInterviewProcessId = i.intInterviewProcessId
 left join dInterview i on ip.intInterviewProcessId = i.intInterviewProcessId
 left join refInterviewLevel il on i.tintInterviewLevelId = il.tintInterviewLevelId
 left join refInterviewStatus ris on i.tintInterviewStatusId = ris.tintInterviewStatusId
 left join dCandidate can on js.intCandidateId = can.intCandidateId
 left join dPerson per on can.intPersonId = per.intPersonId
 left join dJob j on js.intJobId = j.intJobId
 left join lCompanyTierContact ctc on j.intMainContactId = ctc.intContactId and j.intMainContactCompanyTierId = ctc.intCompanyTierId
 left join dContact con on j.intMainContactId = con.intContactId
 left join dPerson per1 on con.intPersonId = per1.intPersonId
 )

--select * from temp where intjobid = 480--tintOffered <> 0
select concat('NJF',intJobId) as ExternalJobID
		,concat('NJF',intCompanyTierContactId) as ExternalContactID
		,concat('NJF',intCandidateId) as ExternalCandidateID
		, -10 as userId
		, dtInserted as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category
		,'contact' as contact_type
		,'candidate' as candidate_type
		,'job' as job_type
		, concat('--JOB APPLICATION--'
		,iif(vchClientJobTitle = '' or vchClientJobTitle is null, '', concat(char(10),'Job: ',vchClientJobTitle))
		,iif(ContactName = '' or ContactName is null, '', concat(char(10),'Contact: ',ltrim(ContactName)))
		,iif(candidateName = '' or candidateName is null, '', concat(char(10),'Candidate: ',ltrim(candidateName)))
		,iif(vchPlacementStatusName = '' or vchPlacementStatusName is null,'',concat(char(10), 'Placement Status: ',vchPlacementStatusName))
		,iif(datPlacedDate is null,'',concat(char(10), 'Placement Date: ',datPlacedDate))
		,iif(vchInterviewLevelName = '' or vchInterviewLevelName is null,'',concat(char(10), 'Interview Level: ',vchInterviewLevelName))
		,iif(vchInterviewStatusName = '' or vchInterviewStatusName is null,'',concat(char(10), 'Interview Status: ',vchInterviewStatusName))
		,iif(sdtStart is null,'',concat(char(10), 'Interview Start Date: ',convert(varchar(20),sdtStart,120)))
		,iif((tintSummarySubmit = 1 or tintCVSubmit = 1) and tintPlaced = 0 and tintOffered = 0 and tintInterview = 0 and vchSystemEventAction is not null,concat(char(10), 'Status: ',vchSystemEventAction),'')
		,iif(vchSystemEventAction is null and tintCommunicated <> 0,concat(char(10), 'Status: Communicated'),'')
		,iif(vchSystemEventAction is null and tintCommunicated = 0,concat(char(10), 'Status: Shortlisted'),'')
		,iif(vchSystemEventAction = '' or vchSystemEventAction is null,'',concat(char(10), 'Last Event Action: ',vchSystemEventAction))
		,iif(dtUpdated is null,'',concat(char(10), 'Updated: ',convert(varchar(20),dtUpdated,120)))
		--,iif(tintPlaced = 1, iif(tintPlacementStatusId in (4,9),'SENT','PLACED'),
		--iif(tintOffered >1, 'SENT',
		--iif(tintOffered =1, 'OFFERED',
		--iif(tintInterviewStatusId in (6,7,8,9), 'SENT',
		--iif(tintInterviewLevelId in (0,1,101,104), '1ST_INTERVIEW',
		--iif(tintInterviewLevelId in (2,3,102,103,105,106), '2ND_INTERVIEW',
		)
from temp --where intJobId = 480-- and tintInterview <> 0--vchSystemEventAction is null and tintCommunicated <>0--tintPlaced = 0 and tintOffered = 0 and tintInterview = 0 and (tintSummarySubmit = 1 or tintCVSubmit = 1) --and tintInterview = 0

