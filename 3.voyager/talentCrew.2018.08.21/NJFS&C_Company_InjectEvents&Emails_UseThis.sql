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
				left join dPerson p1 on u.intPersonId = p1.intPersonId)

select concat('NJF',tec.CompanyId) as CompanyExternalId, -10 as userId
		, tec.dtInserted as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'company' as type
		, contactName, EventId
		, concat(
				iif(em.AttachmentID is null, '-----MIGRATED FROM EVENTS-----', '-----MIGRATED FROM EVENTS & EMAILS-----'),
				iif(sdtEventDate is null, '', concat(CHAR(10),'Event Date: ',convert(varchar(20),sdtEventDate,120))),
				iif(vchEventDetail = '' or vchEventDetail is null, '', concat(char(10),'Event Details: ',vchEventDetail)),
				iif(vchEventActionName = '' or vchEventActionName is null, '', concat(char(10),'Event Action: ',vchEventActionName)),
				iif(contactName = '' or contactName is null, '', concat(char(10),'Logged Against: ',char(10),'- ',contactName)),
				iif(UserName = '' or UserName is null, '', concat(char(10),'Logged By: ',UserName)),
				iif(tec.dtInserted = '' or tec.dtInserted is null, '', concat(char(10),'Inserted Date: ',convert(varchar(20),tec.dtInserted,120))),
				iif(em.AttachmentID is null, '',
					concat(
					concat(char(10),char(10),'-----EMAIL''S CONTENT-----'),
					iif(msgfilename = '' or msgfilename is null, '', concat(char(10),'MSG file name: ',msgfilename)),
					iif(em.sentdate is null or em.sentdate in ('  -   -  : :',''), '', concat(CHAR(10),'Sent Date: ',convert(varchar(20),em.sentdate,120))),
					iif(em.receiveddate is null or em.receiveddate in ('  -   -  : :',''), '', concat(CHAR(10),'Received Date: ',convert(varchar(20),em.receiveddate,120))),
					iif(em.emailfrom = '' or em.emailfrom is null, '', concat(char(10),'From: ',em.emailfrom)),
					iif(em.emailto = '' or em.emailto is null, '', concat(char(10),'To: ',em.emailto)),
					iif(em.CC = '' or em.CC is null, '', concat(char(10),'CC: ',em.CC)),
					iif(em.subject = '' or em.subject is null, concat(char(10),'Subject: ',a.vchAttachmentName), concat(char(10),'Subject: ',em.subject)),
					iif(em.bodytext like '' or em.bodytext is null, '', iif(em.bodytext like '%<p>%</p>%', concat(char(10),'Body Text: ',char(10),[dbo].[udf_StripHTML](bodytext),char(10)),concat(char(10),'Body Text: ',char(10),bodytext,char(10)))),
					iif(em.AttachmentID is null, '', concat(char(10),'----------------------------------------',char(10))),
					concat(char(10),'Email ID: ', ae.intAttachmentId))),
				concat(char(10),'Event ID: ', EventId)
				) as commentContent
from tempEventCompany tec left join lAttachmentEvent ae on tec.EventId = ae.intEventId
				left join dAttachment a on ae.intAttachmentId = a.intAttachmentId
				left join email em on ae.intAttachmentId = em.AttachmentID
				--left join dUser u on ec.intInsertedById = u.intUserId
				--left join dPerson p1 on u.intPersonId = p1.intPersonId
--where em.AttachmentID is not null