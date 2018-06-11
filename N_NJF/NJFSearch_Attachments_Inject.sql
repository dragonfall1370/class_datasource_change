with tempJobAttachment as(
SELECT intJobId, aj.intAttachmentId, ROW_NUMBER() OVER(PARTITION BY intJobId ORDER BY aj.intAttachmentId ASC) AS rn,
		 concat(aj.intAttachmentId,'_', 
		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
		 as attachmentName
from lAttachmentJob aj left join dAttachment a on aj.intAttachmentId = a.intAttachmentId
where vchFileType not in ('.eml','.mp4'))

SELECT concat('NJF',intJobId) as JobExternalId,'POSITION' as entity_type, 'resume' as document_type, attachmentName
		, iif(rn=1, 1,0) as default_file
from tempJobAttachment-- where intCandidateId = 41

--select * from dCompany where intCompanyId = 16
select * from lAttachmentCompanyTierContact where intAttachmentid = 115961
select * from lCompanyTierContact where intCompanyTierId = 152 and intContactId = 37656
select * from dContact where intContactId = 74770
SELECT * FROM dCandidate WHERE INTCANDIDATEID = 41
select * from dPerson where intPersonId =87
select * from dAttachment where intAttachmentid = 115961099

---------------------
--with tempConAttachment as(
--SELECT intCompanyTierContactId, actc.intAttachmentId, ROW_NUMBER() OVER(PARTITION BY intCompanyTierContactId ORDER BY actc.intAttachmentId ASC) AS rn,
--		 concat(actc.intAttachmentId,'_', 
--		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
--		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
--		 as attachmentName
--from lAttachmentCompanyTierContact actc left join dAttachment a on actc.intAttachmentId = a.intAttachmentId
--	 left join lCompanyTierContact ctc on actc.intContactId = ctc.intContactId and actc.intCompanyTierId = ctc.intCompanyTierId
--	 where vchFileType not in ('.eml','.mp4'))

--, conAttachment as (SELECT intCompanyTierContactId, 
--     STUFF(
--         (SELECT char(10) + attachmentName
--          from  tempConAttachment
--          WHERE intCompanyTierContactId =ca.intCompanyTierContactId
--    order by intCompanyTierContactId asc
--          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
--          ,1,1, '')  AS contactAttachments
--FROM tempConAttachment as ca
--GROUP BY ca.intCompanyTierContactId)

--select ca.intCompanyTierContactId, com.vchCompanyName, p.vchForename, p.vchMiddlename, p.vchSurname, ca.contactAttachments
--from conAttachment ca left join lCompanyTierContact ctc on ca.intCompanyTierContactId = ctc.intCompanyTierContactId
--left join dContact c on ctc.intContactId = c.intContactId
--left join dPerson p on c.intPersonId = p.intPersonId
--left join dCompanyTier ct on ctc.intCompanyTierId = ct.intCompanyTierId
--left join dCompany com on ct.intCompanyId = com.intCompany