--select * from lEventCandidateTelecom
--Get Candidate Events
with tempEventCandidateTelecom as (select ect.intEventId, ct.intCandidateId , p.vchForename, p.vchSurname, ct.vchDescription, ct.vchValue, ct.vchExtension, concat (ct.vchValue, ct.vchExtension) as telValue
from lEventCandidateTelecom ect left join dCandidateTelecom ct on ect.intCandidateTelecomId = ct.intCandidateTelecomId
				left join dCandidate c on ct.intCandidateId = c.intCandidateId
				left join dPerson p on c.intPersonId = p.intPersonId)
-------above table has some events using 2 telecom type

, tempEventContact as (select ectc.intEventId contactEventId, ectc.intCompanyTierId, ectc.intContactId as ContactId, 
	--ctc.intCompanyTierContactId,
	ct.vchCompanyTierName,
	--p.vchForeName as contactForeName, p.vchSurname as contactSurName, 
	coalesce(NULLIF(p.vchForename, ''), '') + Coalesce(' ' + NULLIF(p.vchSurname, ''), '') as contactName
from lEventCompanyTierContact ectc left join dEvent e on ectc.intEventId = e.intEventId
				left join dContact c on ectc.intContactId = c.intContactId
				left join dPerson p on c.intPersonId = p.intPersonId
				left join lCompanyTierContact ctc on ectc.intContactId = ctc.intContactId and ectc.intCompanyTierId = ctc.intCompanyTierId
				left join dCompanyTier ct on ectc.intCompanyTierId = ct.intCompanyTierId)

, tempEventCandidate as (
select ec.intEventId as EventId, ec.intCandidateId as CandidateId, 
	e.sdtEventDate, e.intLoggedById, e.tintDirection, e.bitAutoEvent, e.tintEventType, e.sintEventActionId, e.vchEventDetail, e.dtInserted,
	--p.vchForeName as foreName, p.vchSurname as surName, 
	Coalesce(NULLIF(p.vchForename, ''), '') + Coalesce(' ' + NULLIF(p.vchSurname, ''), '') as candidateName, vchEventActionName,
	tect.*, Coalesce(NULLIF(tect.vchForename, ''), '') + Coalesce(' ' + NULLIF(tect.vchSurname, ''), '') as toCandidateName,
	tec.*,
	u.vchShortname, coalesce(NULLIF(p1.vchForename, ''), '') + Coalesce(' ' + NULLIF(p1.vchSurname, ''), '') as UserName
from lEventCandidate ec left join dEvent e on ec.intEventId = e.intEventId
				left join dCandidate c on ec.intCandidateId = c.intCandidateId
				left join dPerson p on c.intPersonId = p.intPersonId
				left join svw_EventAction ea on e.sintEventActionId = ea.sintEventActionId
				left join tempEventCandidateTelecom tect on ec.intEventId = tect.intEventId
				left join tempEventContact tec on ec.intEventId = tec.contactEventId
				left join dUser u on e.intLoggedById = u.intUserId
				left join dPerson p1 on u.intPersonId = p1.intPersonId
-- where tect.telValue like '%@%'-- is not null 
--where e.vchEventDetail <> ''
) --and tect.intContactId = 1484
--where p.intPersonId = 103140--
--select * from tempEventCandidate --where intCompanyTierContactId = 24204
select CandidateId as External_Id, -10 as user_account_Id
		, dtInserted as Insert_TimeStamp, -10 as AssignedUserId, 'comment' as category, 'candidate' as type
		, contactName
		, concat(
				concat('-----MIGRATED FROM EVENTS-----',char(10)),
				iif(sdtEventDate is null, '', concat('Event Date: ',convert(varchar(20),sdtEventDate,120))),
				iif(vchEventDetail = '' or vchEventDetail is null, '', concat(char(10),'Event Details: ',vchEventDetail)),
				iif(vchEventActionName = '' or vchEventActionName is null, '', concat(char(10),'Event Action: ',vchEventActionName)),
				iif(contactName = '' or contactName is null, '', concat(char(10),'Logged Against: ',char(10),'- ',contactName)),
				iif(toCandidateName = '' or toCandidateName is null, '', concat(char(10),'To/From: ',toCandidateName, iif(telValue = '' or telValue is null,'',concat(' (',telValue,')')))),
				iif(UserName = '' or UserName is null, '', concat(char(10),'Logged By: ',UserName)),
				concat(char(10),'Event ID: ', EventId)
				) as Content
from tempEventCandidate --where CandidateId = 49053
