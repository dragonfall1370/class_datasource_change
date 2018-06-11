with tempEventJob as (
select ej.intEventId, ej.intJobId, j.intCompanyId,
	j.vchClientJobTitle, j.vchStandardRefCode, 
	e.sdtEventDate, e.intLoggedById, e.tintDirection, e.bitAutoEvent, e.tintEventType, e.sintEventActionId, e.vchEventDetail, e.dtInserted,
	vchEventActionName,
	u.vchShortname, coalesce(NULLIF(p1.vchForename, ''), '') + Coalesce(' ' + NULLIF(p1.vchSurname, ''), '') as UserName
from lEventJob ej left join dEvent e on ej.intEventId = e.intEventId
				left join dJob j on ej.intJobId = j.intJobId
				left join svw_EventAction ea on e.sintEventActionId = ea.sintEventActionId
				left join dUser u on e.intLoggedById = u.intUserId
				left join dPerson p1 on u.intPersonId = p1.intPersonId
) --and tect.intContactId = 1484
--where p.intPersonId = 103140--
--select * from tempEventCandidate --where intCompanyTierContactId = 24204
select concat('NJFC',intJobId) as JobExternalId, -10 as userId
		, dtInserted as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'job' as type
		--, intEventId
		, concat(
				iif(sdtEventDate is null, '', concat('Event Date: ',convert(varchar(20),sdtEventDate,120))),
				iif(vchEventDetail = '' or vchEventDetail is null, '', concat(char(10),'Event Details: ',vchEventDetail)),
				iif(vchEventActionName = '' or vchEventActionName is null, '', concat(char(10),'Event Action: ',vchEventActionName)),
				iif(UserName = '' or UserName is null, '', concat(char(10),'Logged By: ',UserName)),
				concat(char(10),'Event ID: ', intEventId)
				) as commentContent
from tempEventJob
