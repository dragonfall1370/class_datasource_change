--select * from lEventCandidateTelecom
--Get Candidate Events
with tempEventCandidateTelecom as (select ect.intEventId, ct.intCandidateId , p.vchForename, p.vchSurname, ct.vchDescription, ct.vchValue, ct.vchExtension, concat (ct.vchValue, ct.vchExtension) as telValue
from lEventCandidateTelecom ect left join dCandidateTelecom ct on ect.intCandidateTelecomId = ct.intCandidateTelecomId
				left join dCandidate c on ct.intCandidateId = c.intCandidateId
				left join dPerson p on c.intPersonId = p.intPersonId)
-------above table has some events using 2 telecom type

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
				left join dPerson p1 on u.intPersonId = p1.intPersonId)

select concat('NJF',CandidateId) as CanExternalId, -10 as userId
		, ec.dtInserted as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'candidate' as type
		, contactName
		, concat(
				iif(em.AttachmentID is null, '-----MIGRATED FROM EVENTS-----', '-----MIGRATED FROM EVENTS & EMAILS-----'),
				iif(sdtEventDate is null, '', concat(char(10),'Event Date: ',convert(varchar(20),sdtEventDate,120))),
				iif(vchEventDetail = '' or vchEventDetail is null, '', concat(char(10),'Event Details: ',vchEventDetail)),
				iif(vchEventActionName = '' or vchEventActionName is null, '', concat(char(10),'Event Action: ',vchEventActionName)),
				iif(contactName = '' or contactName is null, '', concat(char(10),'Logged Against: ',char(10),'- ',contactName)),
				iif(toCandidateName = '' or toCandidateName is null, '', concat(char(10),'To/From: ',toCandidateName, iif(telValue = '' or telValue is null,'',concat(' (',telValue,')')))),
				iif(UserName = '' or UserName is null, '', concat(char(10),'Logged By: ',UserName)),
				iif(ec.dtInserted = '' or ec.dtInserted is null, '', concat(char(10),'Inserted Date: ',convert(varchar(20),ec.dtInserted,120))),
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
from tempEventCandidate ec left join lAttachmentEvent ae on ec.EventId = ae.intEventId
				left join dAttachment a on ae.intAttachmentId = a.intAttachmentId
				left join email em on ae.intAttachmentId = em.AttachmentID