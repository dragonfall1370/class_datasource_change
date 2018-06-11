--select distinct intEventId from lEventCompany
--select * from lEventCompanyTier
--select * from lEventCompanyTelecom
--select * from dCompanyTelecom
with tempCompEvent as (
select ec.intEventId, ct.intCompanyId
from lEventCompanyTier ec left join dCompanyTier ct on ec.intCompanyTierId = ct.intCompanyTierId
union 
select intEventId, intCompanyId
from lEventCompany)

-------There are only 12 records in lEventCompanyTelecom so we can skip this data
--, tempEventContact as (select ectc.intEventId contactEventId, --ectc.intCompanyTierId, ectc.intContactId as ContactId, 
--	--ctc.intCompanyTierContactId,
--	ct.vchCompanyTierName,
--	--p.vchForeName as contactForeName, p.vchSurname as contactSurName, 
--	coalesce(NULLIF(p.vchForename, ''), '') + Coalesce(' ' + NULLIF(p.vchSurname, ''), '') as contactName
--from lEventCompanyTierContact ectc left join dEvent e on ectc.intEventId = e.intEventId
--				left join dContact c on ectc.intContactId = c.intContactId
--				left join dPerson p on c.intPersonId = p.intPersonId
--				left join lCompanyTierContact ctc on ectc.intContactId = ctc.intContactId and ectc.intCompanyTierId = ctc.intCompanyTierId
--				left join dCompanyTier ct on ectc.intCompanyTierId = ct.intCompanyTierId)

, tempEventCompany as (
select ec.intEventId as EventId, ec.intCompanyId as CompanyId, 
	c.vchCompanyName,
	e.sdtEventDate, e.intLoggedById, e.tintDirection, e.bitAutoEvent, e.tintEventType, e.sintEventActionId, e.vchEventDetail, e.dtInserted,
	vchEventActionName,
	tec.*,
	u.vchShortname, coalesce(NULLIF(p1.vchForename, ''), '') + Coalesce(' ' + NULLIF(p1.vchSurname, ''), '') as UserName
from tempCompEvent ec left join dEvent e on ec.intEventId = e.intEventId
				left join dCompany c on ec.intCompanyId = c.intCompanyId
				left join svw_EventAction ea on e.sintEventActionId = ea.sintEventActionId
				left join tempEventContact tec on ec.intEventId = tec.contactEventId
				left join dUser u on e.intLoggedById = u.intUserId
				left join dPerson p1 on u.intPersonId = p1.intPersonId
-- where tect.telValue like '%@%'-- is not null 
--where e.vchEventDetail <> ''
) --and tect.intContactId = 1484
--where p.intPersonId = 103140--
--select * from tempEventCandidate --where intCompanyTierContactId = 24204
select concat('NJF',CompanyId) as CompanyExternalId, -10 as userId
		, dtInserted as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'company' as type
		, contactName, EventId
		, concat(
				iif(sdtEventDate is null, '', concat('Event Date: ',convert(varchar(20),sdtEventDate,120))),
				iif(vchEventDetail = '' or vchEventDetail is null, '', concat(char(10),'Event Details: ',vchEventDetail)),
				iif(vchEventActionName = '' or vchEventActionName is null, '', concat(char(10),'Event Action: ',vchEventActionName)),
				iif(contactName = '' or contactName is null, '', concat(char(10),'Logged Against: ',char(10),'- ',contactName)),
				iif(UserName = '' or UserName is null, '', concat(char(10),'Logged By: ',UserName)),
				concat(char(10),'Event ID: ', EventId)
				) as commentContent
from tempEventCompany --where EventId = 321783
