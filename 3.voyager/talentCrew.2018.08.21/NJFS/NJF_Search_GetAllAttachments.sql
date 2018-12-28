DECLARE CURSOR_DocumentIds CURSOR FOR 
--(SELECT intAttachmentId FROM dAttachment where vchFileType not in ('.eml','.mp4'))-- where intAttachmentId <100)--------------Get all Attachments
(SELECT intAttachmentId FROM dAttachment where vchFileType = '.eml')--bitSubAttachment = 0)
-------------get Job Attachments
--(select aj.intAttachmentId
--from lAttachmentJob aj left join dAttachment a on aj.intAttachmentId = a.intAttachmentId
--left join dJob j on aj.intJobId = j.intJobid
--where vchFileType not in ('.eml','.mp4') and intCompanyId in (2,455))
------------------get Candidate Attachments
--(select ac.intAttachmentId from lAttachmentCandidate ac left join dAttachment a on ac.intAttachmentId = a.intAttachmentId
--where vchFileType not in ('.eml','.mp4') and intCandidateId in (select intCandidateId from temp_Can))
------------------get Company Attachments
--(select act.intAttachmentId
--from lAttachmentCompanyTier act left join dCompanyTier ct on act.intCompanyTierId = ct.intCompanyTierId
--where ct.intCompanyId in (2,455)
--union 
--select ac.intAttachmentId
--from lAttachmentCompany ac where ac.intCompanyId in (2,455))

------------------get Company Attachments
--(select actc.intAttachmentId
--from lAttachmentCompanyTierContact actc left join dAttachment a on actc.intAttachmentId = a.intAttachmentId
--	 left join lCompanyTierContact ctc on actc.intContactId = ctc.intContactId and actc.intCompanyTierId = ctc.intCompanyTierId
--	 left join dCompanyTier ct on actc.intCompanyTierId = ct.intCompanyTierId
--	 where vchFileType not in ('.eml','.mp4') and ct.intCompanyId in (2,455))

DECLARE @DocumentID INT;

OPEN CURSOR_DocumentIds

FETCH NEXT FROM CURSOR_DocumentIds INTO @DocumentID
WHILE (@@FETCH_STATUS <> -1)
BEGIN
  DECLARE @ImageData varbinary(max);
  SELECT @ImageData = (SELECT convert(varbinary(max), binDocbytes, 1) FROM dAttachmentBinaryDocuments WHERE intAttachmentId = @DocumentID);

  DECLARE @Path nvarchar(1024);
  SELECT @Path = 'G:\SEML'; --> Only affects on localserver

  DECLARE @Filename NVARCHAR(1024);
  SELECT @Filename = (
  SELECT concat(intAttachmentId,'_',
  iif(right(vchAttachmentName,4)=vchFileType or right(vchAttachmentName,5)=vchFileType,concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':',''),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'),'%','_'), '.zip')
	, concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(vchAttachmentName,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':',''),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'),'%','_'), vchFileType, '.zip')))
  FROM dAttachment WHERE intAttachmentId = @DocumentID);

  DECLARE @FullPathToOutputFile NVARCHAR(2048);
  SELECT @FullPathToOutputFile = @Path + '\' + @Filename;

  DECLARE @ObjectToken INT
  EXEC sp_OACreate 'ADODB.Stream', @ObjectToken OUTPUT;
  EXEC sp_OASetProperty @ObjectToken, 'Type', 1;
  EXEC sp_OAMethod @ObjectToken, 'Open';
  EXEC sp_OAMethod @ObjectToken, 'Write', NULL, @ImageData;
  EXEC sp_OAMethod @ObjectToken, 'SaveToFile', NULL, @FullPathToOutputFile, 2;
  EXEC sp_OAMethod @ObjectToken, 'Close';
  EXEC sp_OADestroy @ObjectToken;

  FETCH NEXT FROM CURSOR_DocumentIds INTO @DocumentID
END
CLOSE CURSOR_DocumentIds
DEALLOCATE CURSOR_DocumentIds

-- Make sure the following statement is executed to enable file IO
-- From http://msdn.microsoft.com/en-us/library/ms191188.aspx
--------- --------- --------- --------- --------- --------- --------- 
--Run below first then run all above!
sp_configure 'show advanced options', 1;
GO
RECONFIGURE;
GO
sp_configure 'Ole Automation Procedures', 1;
GO
RECONFIGURE;
GO

ref: https://stackoverflow.com/questions/1366544/how-to-export-image-field-to-file