with tempJobAttachment as(
SELECT aj.intJobId, aj.intAttachmentId, ROW_NUMBER() OVER(PARTITION BY aj.intJobId ORDER BY aj.intAttachmentId ASC) AS rn,
		 concat(aj.intAttachmentId,'_', 
		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
		 as attachmentName
from lAttachmentJob aj left join dAttachment a on aj.intAttachmentId = a.intAttachmentId
left join dJob j on aj.intJobId = j.intJobid
where vchFileType not in ('.eml','.mp4') and intCompanyId in (2,455))

, jobAttachment as (SELECT intJobId, 
     STUFF(
         (SELECT ',' + replace(attachmentName,'%','_')
          from  tempJobAttachment
          WHERE intJobId =ja.intJobId
    order by intJobId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS jobAttachments
FROM tempJobAttachment as ja
GROUP BY ja.intJobId)
select * from jobAttachment where intJobId = 4623
select * from lAttachmentJob where intJobId = 4623
-----------------------------Get Candidate Attachment
, tempCanAttachment as(
SELECT intCandidateId, ac.intAttachmentId, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY ac.intAttachmentId ASC) AS rn,
		 concat(ac.intAttachmentId,'_', 
		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
		 as attachmentName
from lAttachmentCandidate ac left join dAttachment a on ac.intAttachmentId = a.intAttachmentId
where vchFileType not in ('.eml','.mp4') and intCandidateId = 42147)

, canAttachment as (SELECT intCandidateId, 
     STUFF(
         (SELECT ',' + replace(attachmentName,'%','_')
          from  tempCanAttachment
          WHERE intCandidateId =ca.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS canAttachments
FROM tempCanAttachment as ca
GROUP BY ca.intCandidateId)

--select * from  canAttachment

-----------------------------Get Company and Company Tier Attachment
with tempCompAttachment as(
select act.intAttachmentId, ct.intCompanyId
from lAttachmentCompanyTier act left join dCompanyTier ct on act.intCompanyTierId = ct.intCompanyTierId
where ct.intCompanyId in (2,455)
union 
select ac.intAttachmentId, ac.intCompanyId
from lAttachmentCompany ac where ac.intCompanyId in (2,455))

--select * from tempCompAttachment
, tempCompAttachment1 as(
SELECT intCompanyId, ca.intAttachmentId, ROW_NUMBER() OVER(PARTITION BY intCompanyId ORDER BY ca.intAttachmentId ASC) AS rn,
		 concat(ca.intAttachmentId,'_', 
		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
		 as attachmentName
from tempCompAttachment ca left join dAttachment a on ca.intAttachmentId = a.intAttachmentId
where vchFileType not in ('.eml','.mp4'))

, compAttachment as (SELECT intCompanyId, 
     STUFF(
         (SELECT ',' + replace(attachmentName,'%','_')
          from  tempCompAttachment1
          WHERE intCompanyId =ca.intCompanyId
    order by intCompanyId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS companyAttachments
FROM tempCompAttachment1 as ca
GROUP BY ca.intCompanyId)

select ca.intCompanyId, c.vchCompanyName, ca.companyAttachments
from compAttachment ca left join dCompany c on ca.intCompanyId = c.intCompanyId

--select * from  compAttachment

-----------------------------Get Contact Attachment
, tempConAttachment as(
SELECT intCompanyTierContactId, actc.intAttachmentId, ROW_NUMBER() OVER(PARTITION BY intCompanyTierContactId ORDER BY actc.intAttachmentId ASC) AS rn,
		 concat(actc.intAttachmentId,'_', 
		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
		 as attachmentName
from lAttachmentCompanyTierContact actc left join dAttachment a on actc.intAttachmentId = a.intAttachmentId
	 left join lCompanyTierContact ctc on actc.intContactId = ctc.intContactId and actc.intCompanyTierId = ctc.intCompanyTierId
	 left join dCompanyTier ct on actc.intCompanyTierId = ct.intCompanyTierId
	 where vchFileType not in ('.eml','.mp4') and ct.intCompanyId in (2,455))

, conAttachment as (SELECT intCompanyTierContactId, 
     STUFF(
         (SELECT ',' + replace(attachmentName,'%','_')
          from  tempConAttachment
          WHERE intCompanyTierContactId =ca.intCompanyTierContactId
    order by intCompanyTierContactId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,1, '')  AS contactAttachments
FROM tempConAttachment as ca
GROUP BY ca.intCompanyTierContactId)

select * from  conAttachment

select * from  conAttachment
select * from jobAttachment
select * from lAttachmentCompanyTierContact
select * from rmAttachmentBinaryDocuments

