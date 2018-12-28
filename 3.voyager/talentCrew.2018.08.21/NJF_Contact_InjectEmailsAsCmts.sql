select concat('NJF',intCompanyTierContactId) as ContactExternalId, -10 as userId
		, actc.dtInserted as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'contact' as type
		,e.subject, a.vchAttachmentName
		, concat('-----MIGRATED FROM EMAILS-----',
				iif(msgfilename = '' or msgfilename is null, '', concat(char(10),'MSG file name: ',msgfilename)),
				iif(e.sentdate is null or e.sentdate in ('  -   -  : :',''), '', concat(CHAR(10),'Sent Date: ',convert(varchar(20),e.sentdate,120))),
				iif(e.receiveddate is null or e.receiveddate in ('  -   -  : :',''), '', concat(CHAR(10),'Received Date: ',convert(varchar(20),e.receiveddate,120))),
				iif(e.emailfrom = '' or e.emailfrom is null, '', concat(char(10),'From: ',e.emailfrom)),
				iif(e.emailto = '' or e.emailto is null, '', concat(char(10),'To: ',e.emailto)),
				iif(e.CC = '' or e.CC is null, '', concat(char(10),'CC: ',e.CC)),
				iif(e.subject = '' or e.subject is null, concat(char(10),'Subject: ',a.vchAttachmentName), concat(char(10),'Subject: ',e.subject)),
				iif(e.bodytext like '' or e.bodytext is null, '', iif(e.bodytext like '%<p>%</p>%', concat(char(10),'Body Text: ',char(10),[dbo].[udf_StripHTML](bodytext),char(10)),concat(char(10),'Body Text: ',char(10),bodytext,char(10)))),
				iif(p1.vchForename = '' and  p1.vchSurname = '', '', concat(char(10),'Inserted By: ',coalesce(NULLIF(p1.vchForename, ''), '') + Coalesce(' ' + NULLIF(p1.vchSurname, ''), ''))),
				iif(actc.dtInserted = '' or actc.dtInserted is null, '', concat(char(10),'Inserted Date: ',actc.dtInserted)),
				concat(char(10),'Attachment ID: ', actc.intAttachmentId)
				) as commentContent
from lAttachmentCompanyTierContact actc left join dAttachment a on actc.intAttachmentId = a.intAttachmentId
							 left join lCompanyTierContact ctc on actc.intContactId = ctc.intContactId and actc.intCompanyTierId = ctc.intCompanyTierId
							 left join email e on a.intAttachmentId = e.AttachmentID
							 left join dUser u on actc.intInsertedById = u.intUserId
							 left join dPerson p1 on u.intPersonId = p1.intPersonId
where e.AttachmentId is not null
