DECLARE CURSOR_DocumentIds CURSOR FOR (select cvs.DocumentId from CVSendDocuments cvs
				left join ApplicantActions aa on aa.ApplicantActionId = cvs.ApplicantActionId
				left join Documents d on d.DocumentID = cvs.DocumentId
				left join DocumentContent dc on d.DocumentId = dc.DocumentId
				where 1=1
				--and cvs.DocumentId = @DocumentID
				and dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.txt','.htm','.html')
				and aa.JobId is not NULL)

DECLARE @DocumentID INT;

OPEN CURSOR_DocumentIds

FETCH NEXT FROM CURSOR_DocumentIds INTO @DocumentID
WHILE (@@FETCH_STATUS <> -1)
BEGIN
  DECLARE @ImageData varbinary(max);
  SELECT @ImageData = (SELECT convert(varbinary(max), Document, 1) FROM DocumentContent WHERE DocumentID = @DocumentID);

  DECLARE @Path nvarchar(1024);
  SELECT @Path = 'G:\NextPhase\CVSend'; --> Only affects on localserver where database is create
---D:\EXEC\Documents

  DECLARE @Filename NVARCHAR(1024);
  SELECT @Filename = (select concat_ws('_','NP_CVSend', d.DocumentID, concat(aa.JobId, dc.FileExtension))
				from CVSendDocuments cvs
				left join ApplicantActions aa on aa.ApplicantActionId = cvs.ApplicantActionId
				left join Documents d on d.DocumentID = cvs.DocumentId
				left join DocumentContent dc on d.DocumentId = dc.DocumentId
				where 1=1
				and cvs.DocumentId = @DocumentID
				and dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.txt','.htm','.html')
				and aa.JobId is not NULL);

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