DECLARE CURSOR_DocumentIds CURSOR FOR (SELECT DocumentID FROM DocumentContent)

DECLARE @DocumentID INT;

OPEN CURSOR_DocumentIds

FETCH NEXT FROM CURSOR_DocumentIds INTO @DocumentID
WHILE (@@FETCH_STATUS <> -1)
BEGIN
  DECLARE @ImageData varbinary(max);
  SELECT @ImageData = (SELECT convert(varbinary(max), Document, 1) FROM DocumentContent WHERE DocumentID = @DocumentID);

  DECLARE @Path nvarchar(1024);
  SELECT @Path = 'G:\VC_fillrecruitment\Documents'; --> Only affects on localserver

  DECLARE @Filename NVARCHAR(1024);
  SELECT @Filename = (SELECT concat(DocumentID,FileExtension) FROM DocumentContent WHERE DocumentID = @DocumentID);

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
sp_configure 'show advanced options', 1;
GO
RECONFIGURE;
GO
sp_configure 'Ole Automation Procedures', 1;
GO
RECONFIGURE;
GO

ref: https://stackoverflow.com/questions/1366544/how-to-export-image-field-to-file