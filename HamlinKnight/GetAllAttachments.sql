DECLARE CURSOR_DocumentIds CURSOR FOR 
--(SELECT intAttachmentId FROM dAttachment where vchFileType not in ('.eml','.mp4'))-- where intAttachmentId <100)--------------Get all Attachments
(SELECT linkfile_ref FROM linkfile where file_extension = 'EML' and file_contents is not null)--bitSubAttachment = 0)


-------------------------------------------

DECLARE @DocumentID INT;

OPEN CURSOR_DocumentIds

FETCH NEXT FROM CURSOR_DocumentIds INTO @DocumentID
WHILE (@@FETCH_STATUS <> -1)
BEGIN
  DECLARE @ImageData varbinary(max);
  SELECT @ImageData = (SELECT convert(varbinary(max), file_contents, 1) FROM linkfile WHERE linkfile_ref = @DocumentID);

  DECLARE @Path nvarchar(1024);
  SELECT @Path = 'D:\SQL_dump\HamlinKnight\DocumentEML'; --> Only affects on localserver

  DECLARE @Filename NVARCHAR(1024);
  SELECT @Filename = (
  SELECT concat(linkfile_ref,'_',
  iif(right(displayname,4)=file_extension or right(displayname,5)=file_extension,concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(displayname,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':',''),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'),'%','_'), '.zip')
	, concat(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(left(displayname,220),'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':',''),char(10),''),char(13),''),'>',']'),'<','['),char(9),'_'),'%','_'),'.',file_extension, '.zip')))
  FROM linkfile WHERE linkfile_ref = @DocumentID);

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

--------- --------- --------- --------- --------- --------- --------- 
--Run below first then run all above!
/*sp_configure 'show advanced options', 1;
GO
RECONFIGURE;
GO
sp_configure 'Ole Automation Procedures', 1;
GO
RECONFIGURE;
GO
*/



