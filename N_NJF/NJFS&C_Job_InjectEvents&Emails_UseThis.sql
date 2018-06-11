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
select concat('NJFS',intJobId) as JobExternalId, -10 as userId
		, tej.dtInserted as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'job' as type
		--, intEventId
		, concat(
				iif(em.AttachmentID is null, '-----MIGRATED FROM EVENTS-----', '-----MIGRATED FROM EVENTS & EMAILS-----'),
				iif(sdtEventDate is null, '', concat(char(10),'Event Date: ',convert(varchar(20),sdtEventDate,120))),
				iif(vchEventDetail = '' or vchEventDetail is null, '', concat(char(10),'Event Details: ',vchEventDetail)),
				iif(vchEventActionName = '' or vchEventActionName is null, '', concat(char(10),'Event Action: ',vchEventActionName)),
				iif(UserName = '' or UserName is null, '', concat(char(10),'Logged By: ',UserName)),
				iif(tej.dtInserted = '' or tej.dtInserted is null, '', concat(char(10),'Inserted Date: ',convert(varchar(20),tej.dtInserted,120))),
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
				concat(char(10),'Event ID: ', tej.intEventId)
				) as commentContent
from tempEventJob tej left join lAttachmentEvent ae on tej.intEventId = ae.intEventId
				left join dAttachment a on ae.intAttachmentId = a.intAttachmentId
				left join email em on ae.intAttachmentId = em.AttachmentID
