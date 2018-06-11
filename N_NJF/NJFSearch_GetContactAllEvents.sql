with tempEventContactTelecom as (select ectct.intEventId, ctct.intContactId , p.vchForename, p.vchSurname, ctct.vchDescription, ctct.vchValue, ctct.vchExtension
from lEventCompanyTierContactTelecom ectct left join dCompanyTierContactTelecom ctct on ectct.intCompanyTierContactTelecomId = ctct.intCompanyTierContactTelecomId
				left join dContact c on ctct.intContactId = c.intContactId
				left join dPerson p on c.intPersonId = p.intPersonId)
-------aboew table has some event using 2 telecom type
, tempEventContact as (select ectc.intEventId as EventId, ectc.intCompanyTierId, ectc.intContactId as ContactId, 
	ctc.intCompanyTierContactId,
	e.sdtEventDate, e.intLoggedById, e.tintDirection, e.bitAutoEvent, e.tintEventType, e.sintEventActionId, e.vchEventDetail, e.dtInserted,
	ct.vchCompanyTierName,
	p.vchForeName as foreName, p.vchSurname as surName, 
	coalesce(NULLIF(p.vchForename, ''), '') + Coalesce(' ' + NULLIF(p.vchSurname, ''), '') as contactName, vchEventActionName,
	tect.*, Coalesce(NULLIF(tect.vchForename, ''), '') + Coalesce(' ' + NULLIF(tect.vchSurname, ''), '') as toContactName,
	u.vchShortname, coalesce(NULLIF(p1.vchForename, ''), '') + Coalesce(' ' + NULLIF(p1.vchSurname, ''), '') as UserName
from lEventCompanyTierContact ectc left join dEvent e on ectc.intEventId = e.intEventId
				left join dContact c on ectc.intContactId = c.intContactId
				left join dPerson p on c.intPersonId = p.intPersonId
				left join lCompanyTierContact ctc on ectc.intContactId = ctc.intContactId and ectc.intCompanyTierId = ctc.intCompanyTierId
				left join dCompanyTier ct on ectc.intCompanyTierId = ct.intCompanyTierId
				left join svw_EventAction ea on e.sintEventActionId = ea.sintEventActionId
				left join tempEventContactTelecom tect on ectc.intEventId = tect.intEventId
				left join dUser u on e.intLoggedById = u.intUserId
				left join dPerson p1 on u.intPersonId = p1.intPersonId
--where tect.vchValue is not null 
) --and tect.intContactId = 1484
--where p.intPersonId = 103140--
--select *  from tempEventContact where UserName like 'Jame%'--intCompanyTierContactId = 24204
select concat('NJF',intCompanyTierContactId) as ContactExternalId,EventId, -10 as userId
		, dtInserted as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'contact' as type
		, ROW_NUMBER() OVER(PARTITION BY EventId ORDER BY intCompanyTierContactId ASC) AS rn
		, concat(
				iif(sdtEventDate is null, '', concat('Event Date: ',convert(varchar(20),sdtEventDate,120))),
				iif(vchEventDetail = '' or vchEventDetail is null, '', concat(char(10),'Event Details: ',vchEventDetail)),
				iif(vchEventActionName = '' or vchEventActionName is null, '', concat(char(10),'Event Action: ',vchEventActionName)),
				iif(contactName = '' or contactName is null, '', concat(char(10),'Logged Against: ',contactName, iif(vchCompanyTierName = '','',concat(' (',vchCompanyTierName,')')))),
				iif(toContactName = '' or toContactName is null, '', concat(char(10),'To/From: ',toContactName, iif(vchValue = '' or vchValue is null,'',concat(' (',vchValue,')')))),
				iif(UserName = '' or UserName is null, '', concat(char(10),'Logged By: ',UserName)),
				concat(char(10),'Event ID: ', EventId)
				) as commentContent
from tempEventContact--year(dtInserted)>2016
--order by intCompanyTierContactId