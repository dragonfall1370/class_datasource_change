DECLARE CURSOR_DocumentIds CURSOR FOR (select t.TemplateID
		from ClientContacts cc
		left join Templates t on t.ObjectId = cc.ContactPersonId
		left join TemplateDocument td on td.TemplateId = t.TemplateId
		where 1=1
		--and t.TemplateId = @DocumentID
		and td.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.msg','.txt','.htm','.html')
		and t.TemplateTypeId = 53)

DECLARE @DocumentID INT;

OPEN CURSOR_DocumentIds

FETCH NEXT FROM CURSOR_DocumentIds INTO @DocumentID
WHILE (@@FETCH_STATUS <> -1)
BEGIN
  DECLARE @ImageData varbinary(max);
  SELECT @ImageData = (SELECT convert(varbinary(max), Document, 1) FROM TemplateDocument WHERE TemplateId = @DocumentID);

  DECLARE @Path nvarchar(1024);
  SELECT @Path = 'G:\NextPhase\ContactDoc'; --> Only affects on localserver where database is create
---D:\EXEC\Documents

  DECLARE @Filename NVARCHAR(1024);
  SELECT @Filename = (select concat_ws('_','NP_S', t.TemplateId, concat(cc.ClientContactId, td.FileExtension))
		from ClientContacts cc
		left join Templates t on t.ObjectId = cc.ContactPersonId
		left join TemplateDocument td on td.TemplateId = t.TemplateId
		where 1=1
		and t.TemplateId = @DocumentID
		and td.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.msg','.txt','.htm','.html')
		and t.TemplateTypeId = 53);

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