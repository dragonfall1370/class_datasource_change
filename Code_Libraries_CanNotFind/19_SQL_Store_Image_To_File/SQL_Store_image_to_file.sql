--SPECIFIC SCRIPT TO RETRIEVE COMPANY DOCUMENTS
DECLARE CURSOR_DocumentIds CURSOR FOR (SELECT doc_id FROM tblCompanyDocs)

DECLARE @DocumentID INT;

OPEN CURSOR_DocumentIds

FETCH NEXT FROM CURSOR_DocumentIds INTO @DocumentID
WHILE (@@FETCH_STATUS <> -1)
BEGIN
  DECLARE @ImageData varbinary(max);
  SELECT @ImageData = (SELECT convert(varbinary(max), doc, 1) FROM tblCompanyDocs WHERE doc_id = @DocumentID);

  DECLARE @Path nvarchar(1024);
  SELECT @Path = 'G:\VC_SkillSolved\RestoredDocuments\Company'; --> Only affects on localserver

  DECLARE @Filename NVARCHAR(1024);
  SELECT @Filename = (SELECT concat(doc_id,'_',replace(replace(doc_name,',',''),'.',''),rtrim(ltrim(doc_ext))) 
						FROM tblCompanyDocs WHERE doc_id = @DocumentID);       

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
sp_configure 'show advanced options', 1;
GO
RECONFIGURE;
GO
sp_configure 'Ole Automation Procedures', 1;
GO
RECONFIGURE;
GO