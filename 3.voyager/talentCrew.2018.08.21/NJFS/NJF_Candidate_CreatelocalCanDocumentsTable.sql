create table localCanDocuments
(intCandidateId int,
CanDocs nvarchar(max)
);

--------------------------------------CANDIDATE RESUMES
with CVName as (
select intCandidateId, dtInserted, concat('CV',intCandidateCVId,'_',convert(date,dtInserted),
 --coalesce('_' + NULLIF(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(vchCVName,'?',''),' ',''),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':',''),char(10),''),char(13),''),''),''), vchFileType)
 iif(right(vchCVName,4)=vchFileType or right(vchCVName,5)=vchFileType,concat('_',replace(vchCVName,'/','.')),iif(vchFileType= '.', concat('_',vchCVName,'.docx'),concat('_',replace(vchCVName,'/','.'),vchFileType)))) as CVFullName
from dCandidateCV)
, CanResumes as (select intCandidateId, STUFF(
					(Select ',' + replace(replace(CVFullName,' ','_'),'%','_')
					from CVName 
					where intCandidateId = cvn.intCandidateId
    order by intCandidateId asc, dtInserted desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'CVName'
FROM CVName as cvn
GROUP BY cvn.intCandidateId)

, tempCanAttachment as(
SELECT intCandidateId, ac.intAttachmentId, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY ac.intAttachmentId ASC) AS rn,
		 concat(ac.intAttachmentId,'_', 
		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
		 as attachmentName
from lAttachmentCandidate ac left join dAttachment a on ac.intAttachmentId = a.intAttachmentId
where vchFileType not in ('.mp4'))-- and intCandidateId = 42147)

/*, tempCanAttachment as(
SELECT intCandidateId, ac.intAttachmentId--, ROW_NUMBER() OVER(PARTITION BY intCandidateId ORDER BY ac.intAttachmentId ASC) AS rn
		,case when vchFileType like '.eml' then null --e.msgfilename 
		else
		 concat(ac.intAttachmentId,'_', 
		 iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_')
		 , concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':','_'),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'), vchFileType)))
		 end as attachmentName
from lAttachmentCandidate ac left join dAttachment a on ac.intAttachmentId = a.intAttachmentId
							 --left join email e on a.intAttachmentId = e.AttachmentID
where vchFileType not in ('.mp4')
*//*union  --union with email files got from candidate events
select ec.intCandidateId, ae.intAttachmentId, em.msgfilename as attachmentName--, a.vchAttachmentName
from lEventCandidate ec left join dEvent e on ec.intEventId = e.intEventId
				--left join dCandidate c on ec.intCandidateId = c.intCandidateId
				left join lAttachmentEvent ae on ec.intEventId = ae.intEventId
				left join dAttachment a on ae.intAttachmentId = a.intAttachmentId
				left join email em on ae.intAttachmentId = em.AttachmentID
where em.AttachmentID is not null*//*
)*/

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

, tempCanDocuments as (select * from CanResumes union select * from canAttachment)
--select * from tempCanDocuments
--select * from tempCan where ApplicantId = 142
insert into localCanDocuments 
select intCandidateId, STUFF(
					(Select ',' + CVName
					from tempCanDocuments 
					where intCandidateId = tcd.intCandidateId
    order by intCandidateId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS 'CanDocs'
FROM tempCanDocuments as tcd
GROUP BY tcd.intCandidateId;

select *,len(CanDocs)
from localCanDocuments
where len(CanDocs) > 32000
