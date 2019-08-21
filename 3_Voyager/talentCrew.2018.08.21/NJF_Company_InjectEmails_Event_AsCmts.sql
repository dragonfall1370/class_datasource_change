with tempCompEvent as (
select ec.intEventId, ct.intCompanyId, ec.intInsertedById, ec.dtInserted
from lEventCompanyTier ec left join dCompanyTier ct on ec.intCompanyTierId = ct.intCompanyTierId
union 
select intEventId, intCompanyId, intInsertedById, dtInserted
from lEventCompany)

select distinct ae.intAttachmentId
--concat('NJFS',ec.intCompanyId) as CompanyExternalId, -10 as userId
--		, e.dtInserted as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'company' as type, vchAttachmentName
--		--,em.subject, a.vchAttachmentName
--		, concat('-----MIGRATED FROM EMAILS-----',
--				iif(msgfilename = '' or msgfilename is null, '', concat(char(10),'MSG file name: ',msgfilename)),
--				iif(em.sentdate is null or em.sentdate in ('  -   -  : :',''), '', concat(CHAR(10),'Sent Date: ',convert(varchar(20),em.sentdate,120))),
--				iif(em.receiveddate is null or em.receiveddate in ('  -   -  : :',''), '', concat(CHAR(10),'Received Date: ',convert(varchar(20),em.receiveddate,120))),
--				iif(em.emailfrom = '' or em.emailfrom is null, '', concat(char(10),'From: ',em.emailfrom)),
--				iif(em.emailto = '' or em.emailto is null, '', concat(char(10),'To: ',em.emailto)),
--				iif(em.CC = '' or em.CC is null, '', concat(char(10),'CC: ',em.CC)),
--				iif(em.subject = '' or em.subject is null, concat(char(10),'Subject: ',a.vchAttachmentName), concat(char(10),'Subject: ',em.subject)),
--				iif(em.bodytext like '' or em.bodytext is null, '', iif(em.bodytext like '%<p>%</p>%', concat(char(10),'Body Text: ',char(10),[dbo].[udf_StripHTML](bodytext),char(10)),concat(char(10),'Body Text: ',char(10),bodytext,char(10)))),
--				iif(e.vchEventDetail = '' or e.vchEventDetail is null, '', concat(char(10),'Event Detail: ',e.vchEventDetail)),
--				iif(p1.vchForename = '' and  p1.vchSurname = '', '', concat(char(10),'Inserted By: ',coalesce(NULLIF(p1.vchForename, ''), '') + Coalesce(' ' + NULLIF(p1.vchSurname, ''), ''))),
--				iif(ec.dtInserted = '' or ec.dtInserted is null, '', concat(char(10),'Inserted Date: ',ec.dtInserted)),
--				concat(char(10),'Attachment ID: ', ae.intAttachmentId),
--				concat(char(10),'Event ID: ', ec.intEventId)
--				) as commentContent
from tempCompEvent ec left join dEvent e on ec.intEventId = e.intEventId
				left join lAttachmentEvent ae on ec.intEventId = ae.intEventId
				left join dAttachment a on ae.intAttachmentId = a.intAttachmentId
				left join email em on ae.intAttachmentId = em.AttachmentID
				left join dUser u on ec.intInsertedById = u.intUserId
				left join dPerson p1 on u.intPersonId = p1.intPersonId
where em.AttachmentID is not null
--order by ctc.intCompanyTierContactId