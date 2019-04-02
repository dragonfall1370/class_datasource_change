with tempEventContactTelecom as (select ectct.intEventId, ctct.intContactId , p.vchForename, p.vchSurname, ctct.vchDescription, ctct.vchValue, ctct.vchExtension
from lEventCompanyTierContactTelecom ectct left join dCompanyTierContactTelecom ctct on ectct.intCompanyTierContactTelecomId = ctct.intCompanyTierContactTelecomId
				left join dContact c on ctct.intContactId = c.intContactId
				left join dPerson p on c.intPersonId = p.intPersonId)
-------aboew table has some event using 2 telecom type
, tempEventContact as (select ectc.intEventId as EventId, ectc.intCompanyTierId, ectc.intContactId as ContactId, 
	ctc.intCompanyTierContactId, ct.intCompanyId,
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
select intCompanyTierContactId as External_Id,EventId, -10 as user_account_Id
		, dtInserted as Insert_TimeStamp, -10 as AssignedUserId, 'comment' as category, 'contact' as type
		--, ROW_NUMBER() OVER(PARTITION BY EventId ORDER BY intCompanyTierContactId ASC) AS rn
		, REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
                                        REPLACE( REPLACE( REPLACE( REPLACE( 
				concat(
				iif(sdtEventDate is null, '', concat('Event Date: ',convert(varchar(20),sdtEventDate,120))),
				iif(vchEventDetail = '' or vchEventDetail is null, '', concat(char(10),'Event Details: ',vchEventDetail)),
				iif(vchEventActionName = '' or vchEventActionName is null, '', concat(char(10),'Event Action: ',vchEventActionName)),
				iif(contactName = '' or contactName is null, '', concat(char(10),'Logged Against: ',contactName, iif(vchCompanyTierName = '','',concat(' (',vchCompanyTierName,')')))),
				iif(toContactName = '' or toContactName is null, '', concat(char(10),'To/From: ',toContactName, iif(vchValue = '' or vchValue is null,'',concat(' (',vchValue,')')))),
				iif(UserName = '' or UserName is null, '', concat(char(10),'Logged By: ',UserName)),
				concat(char(10),'Event ID: ', EventId) ) 
		,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
                                        ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
                                        ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
                                        ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
                                        ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
                                        ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'') as Content
from tempEventContact