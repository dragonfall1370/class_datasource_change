with maxInterviewLevel as (
select intInterviewProcessId, max(intInterviewId) as maxInterviewLevelId--max(tintInterviewLevelId) as maxInterviewLebel
from dInterview
group by intInterviewProcessId)

, tempInterview as (
select i.*
from maxInterviewLevel mil left join dInterview i on mil.maxInterviewLevelId = i.intInterviewId)

, latestPlacement as (select js.intJobId, js.intCandidateId, max(dtupdated) as latestUpdate--max(js.intPlacementId)--, vchSystemEventAction
from dPlacement js 
group by js.intJobId, js.intCandidateId)

, tempPlacement as (select p.*
from dPlacement p left join latestPlacement lp on p.intJobId = lp.intJobId and p.intCandidateId = lp.intCandidateId and p.dtupdated = lp.latestUpdate
where latestUpdate is not null)

, temp as (select js.intJobId, per.intPersonId, per.vchForename, per.vchSurname, js.intCandidateId, tintShortlisted, tintCommunicated, tintInterested, tintSummarySubmit, tintCVSubmit, tintInterview, i.intInterviewProcessId, intInterviewId, i.tintInterviewLevelId, vchInterviewLevelName, i.tintInterviewStatusId, vchInterviewStatusName, js.dtUpdated , tintOffered, tintPlaced, intPlacementId, vchPlacementStatusName, vchSystemEventAction
from dJobShortlist js
 left join tempPlacement p on js.intJobId = p.intJobId and js.intCandidateId = p.intCandidateId
 left join refPlacementStatus ps on p.tintPlacementStatusId = ps.tintPlacementStatusId
 left join sEventAction ea on js.tintSystemEventActionId = ea.tintSystemEventActionId
 left join dInterviewProcess ip on js.intJobId = ip.intJobId and js.intCandidateId = ip.intCandidateId
 left join tempInterview i on ip.intInterviewProcessId = i.intInterviewProcessId
 left join refInterviewLevel il on i.tintInterviewLevelId = il.tintInterviewLevelId
 left join refInterviewStatus ris on i.tintInterviewStatusId = ris.tintInterviewStatusId
 left join dCandidate c on js.intCandidateId = c.intCandidateId
 left join dPerson per on c.intPersonId = per.intPersonId)
--where tintPlaced = 1 and intPlacementId is not null
--select i.intJobId, i.intCandidateId, i.tintInterview, i.intInterviewProcessId, intInterviewId, i.tintInterviewLevelId, i.vchInterviewLevelName, i.vchInterviewStatusName
--from temp i where i.intInterviewId is not null
--i.tintInterview > 0 and i.intInterviewId is null
--i.intInterviewId is null
--select intCandidateId, vchForename, vchSurname, tintShortlisted, tintCVSubmit, vchInterviewLevelName, vchInterviewStatusName, dtUpdated, vchSystemEventAction
--from temp where intJobId = 4276 order by intCandidateId--InterviewId <> 13470
--select * from dPerson where intPersonId = 112690 or intPersonId = 123234 or intPersonId = 124860
--where tintOffered =0  and tintPlaced = 0--or intPlacementId is not null
--where intJobId = 4907 and intCandidateId = 22807
--where intJobId = 4820 and intCandidateId = 44059
--order by intJobId, intCandidateId, tintInterview
--select distinct tintInterviewLevelId, vchInterviewLevelName from temp order by tintInterviewLevelId
select * from temp where intJobId = 1913--where tintInterviewLevelId in (101,104,103)

--select intJobId, intCandidateId, count(tintShortlisted)--, vchSystemEventAction
--from temp
--group by intJobId, intCandidateId
--having count(tintShortlisted) >1

--select * from dPlacement where intPlacementId = 438
--select * from refPlacementStatus

--select * from dJobShortlist
--4907-22807, 4820-44059
--select * from dJob where  tintJobType = 0 and vchDescription like '%contract%'
--select * from lAttributeJob
--select * from dPersonPhoto
select * from dInterviewProcess
select * from dInterview
