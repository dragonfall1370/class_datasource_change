DECLARE CURSOR_DocumentIds CURSOR FOR (select DocumentID from dbo.Documents)

DECLARE @DocumentID INT;

OPEN CURSOR_DocumentIds

FETCH NEXT FROM CURSOR_DocumentIds INTO @DocumentID
WHILE (@@FETCH_STATUS <> -1)
BEGIN
  DECLARE @ImageData varbinary(max);
  SELECT @ImageData = (SELECT convert(varbinary(max), Document, 1) FROM DocumentContent WHERE DocumentID = @DocumentID);

  DECLARE @Path nvarchar(1024);
  SELECT @Path = 'G:\NextPhase\Interview'; --> Only affects on localserver where database is create
---D:\EXEC\Documents

  DECLARE @Filename NVARCHAR(1024);
  SELECT @Filename = (select concat_ws('_','NP_D', d.DocumentID, concat(aa.ApplicantId, dc.FileExtension))
				from InterviewDocuments id
				left join Interviews i on i.InterviewId = id.InterviewId
				left join ApplicantActions aa on aa.ApplicantActionId = i.ApplicantActionId
				left join Documents d on d.NoteBookItemId = id.NoteBookItemId
				left join DocumentContent dc on dc.DocumentId = d.DocumentId
		where d.DocumentID = @DocumentID
		and id.InterviewId = 7829
		and dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.pdf','.doc','.docx','.xls','.xlsx','.rtf','.msg','.txt','.htm','.html'));

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