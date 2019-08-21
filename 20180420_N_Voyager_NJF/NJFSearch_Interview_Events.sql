with temp as (
select ei.intEventId,e.vchEventDetail,e.dtInserted,e.dtUpdated, ea.vchEventActionName, ei.intInterviewId,ip.intInterviewProcessId,ip.intJobId,j.vchClientJobTitle,
ip.intCandidateId, per.intPersonId, per.vchForename, per.vchSurname, coalesce(NULLIF(per.vchForename, ''), '') + Coalesce(' ' + NULLIF(per.vchSurname, ''), '') as candidateName,
 ctc.intCompanyTierContactId,coalesce(NULLIF(per1.vchForename, ''), '') + Coalesce(' ' + NULLIF(per1.vchSurname, ''), '') as ContactName, 
 vchInterviewLevelName, i.tintInterviewStatusId, vchInterviewStatusName, i.sdtStart, I.sdtEnd
from lEventInterview ei left join dEvent e on ei.intEventId = e.intEventId
	left join dInterview i on ei.intInterviewId = i.intInterviewId
	left join dInterviewProcess ip on i.intInterviewProcessId = ip.intInterviewProcessId
	left join refInterviewLevel il on i.tintInterviewLevelId = il.tintInterviewLevelId
	left join refInterviewStatus ris on i.tintInterviewStatusId = ris.tintInterviewStatusId
	left join dCandidate can on ip.intCandidateId = can.intCandidateId
	left join dPerson per on can.intPersonId = per.intPersonId
	left join dJob j on ip.intJobId = j.intJobId
	left join lCompanyTierContact ctc on j.intMainContactId = ctc.intContactId and j.intMainContactCompanyTierId = ctc.intCompanyTierId
	left join dContact con on j.intMainContactId = con.intContactId
	left join dPerson per1 on con.intPersonId = per1.intPersonId
	left join svw_EventAction ea on e.sintEventActionId = ea.sintEventActionId
 --where ei.intEventId = 407627
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
		, concat('--INTERVIEW EVENTS--'
		,iif(vchClientJobTitle = '' or vchClientJobTitle is null, '', concat(char(10),'Job: ',vchClientJobTitle))
		,iif(ContactName = '' or ContactName is null, '', concat(char(10),'Contact: ',ltrim(ContactName)))
		,iif(candidateName = '' or candidateName is null, '', concat(char(10),'Candidate: ',ltrim(candidateName)))
		,iif(vchEventDetail = '' or vchEventDetail is null,'',concat(char(10), 'Event Detail: ',vchEventDetail))
		,iif(sdtStart is null,'',concat(char(10), 'Start Date: ',convert(varchar(20),sdtStart,120)))
		,iif(sdtEnd is null,'',concat(char(10), 'End Date: ',convert(varchar(20),sdtEnd,120)))
		,iif(vchInterviewLevelName = '' or vchInterviewLevelName is null,'',concat(char(10), 'Interview Level: ',vchInterviewLevelName))
		,iif(vchInterviewStatusName = '' or vchInterviewStatusName is null,'',concat(char(10), 'Interview Status: ',vchInterviewStatusName))
		,iif(vchEventActionName = '' or vchEventActionName is null,'',concat(char(10), 'Event Action: ',vchEventActionName))
		,iif(dtUpdated is null,'',concat(char(10), 'Updated: ',convert(varchar(20),dtUpdated,120)))
		,concat(char(10),'Event ID: ',intEventId)
		--,iif(tintPlaced = 1, iif(tintPlacementStatusId in (4,9),'SENT','PLACED'),
		--iif(tintOffered >1, 'SENT',
		--iif(tintOffered =1, 'OFFERED',
		--iif(tintInterviewStatusId in (6,7,8,9), 'SENT',
		--iif(tintInterviewLevelId in (0,1,101,104), '1ST_INTERVIEW',
		--iif(tintInterviewLevelId in (2,3,102,103,105,106), '2ND_INTERVIEW',
		)
from temp --where intJobId = 480-- and tintInterview <> 0--vchSystemEventAction is null and tintCommunicated <>0--tintPlaced = 0 and tintOffered = 0 and tintInterview = 0 and (tintSummarySubmit = 1 or tintCVSubmit = 1) --and tintInterview = 0

