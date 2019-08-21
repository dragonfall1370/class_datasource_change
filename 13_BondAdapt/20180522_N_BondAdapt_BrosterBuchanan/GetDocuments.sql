
DECLARE CURSOR_DocumentIds CURSOR FOR (SELECT DOC_ID FROM DOCUMENTS
where doc_id in(select doc.DOC_ID
from LK_ENTITIES_JOURNAL ej left join DOCUMENTS doc on ej.JOURNAL_ID = doc.OWNER_ID
							left join PROP_CAND_GEN cg on ej.ENTITY_ID = cg.REFERENCE
							left join PROP_PERSON_GEN pg on cg.REFERENCE = pg.REFERENCE--23170 rows
--left join PROP_CAND_PREF cp on cp.REFERENCE = cg.REFERENCE
where doc.OWNER_ID is not null and cg.REFERENCE is not null))
--DECLARE CURSOR_DocumentIds CURSOR FOR (SELECT DOC_ID FROM DOCUMENTS)

DECLARE @DocumentID INT;

OPEN CURSOR_DocumentIds

FETCH NEXT FROM CURSOR_DocumentIds INTO @DocumentID
WHILE (@@FETCH_STATUS <> -1)
BEGIN
  DECLARE @ImageData varbinary(max);
  SELECT @ImageData = (SELECT convert(varbinary(max), DOCUMENT, 1) FROM DOCUMENTS WHERE DOC_ID = @DocumentID);

  DECLARE @Path nvarchar(1024);
  SELECT @Path = 'E:\OneDrive - HRBoss\DataImport\BrosterBuchanan\BB_Script\doc1'; --> Only affects on localserver

  DECLARE @Filename NVARCHAR(1024);
  SELECT @Filename = (
  SELECT cast(DOC_ID as varchar(max)) + '_' 
						+ replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(DOC_NAME,'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':',''),char(10),''),char(13),'')
						+ '.' + replace(coalesce(nullif(FILE_EXTENSION,''),PREVIEW_TYPE),'txt','rtf')
  FROM DOCUMENTS WHERE DOC_ID = @DocumentID);

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