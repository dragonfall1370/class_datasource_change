select concat('NJF',ac.intCandidateId) as CanExternalId, -10 as userId
		, ac.dtInserted as InsertTimeStamp, -10 as AssignedUserId, 'comment' as category, 'candidate' as type
		--,e.subject, a.vchAttachmentName
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
				iif(ac.dtInserted = '' or ac.dtInserted is null, '', concat(char(10),'Inserted Date: ',ac.dtInserted)),
				concat(char(10),'Attachment ID: ', ac.intAttachmentId)
				) as commentContent
from lAttachmentCandidate ac left join dAttachment a on ac.intAttachmentId = a.intAttachmentId
							 left join email e on a.intAttachmentId = e.AttachmentID
							 left join dUser u on ac.intInsertedById = u.intUserId
							 left join dPerson p1 on u.intPersonId = p1.intPersonId
where e.AttachmentId is not null --and bodytext like '%<p>%</p>%'--where CandidateId = 49053 and 

--SELECT intCandidateId, ac.intAttachmentId, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY ac.intAttachmentId ASC) AS rn
--from lAttachmentCandidate ac left join email e on ac.intAttachmentId = e.AttachmentID
--where e.AttachmentId is not null
