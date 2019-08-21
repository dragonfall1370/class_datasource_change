---------------
--CONTACT DOCUMENTS
---------------
DECLARE CURSOR_DocumentIds CURSOR FOR (select distinct ac.ActivityID
		from ActivityContacts ac
		left join ActivitiesTable act on act.ActivityID = ac.ActivityID
		where ac.ContactType = 1 and act.ActivityType = 2
		and (act.Subject like '%.pdf' or act.Subject like '%.doc%' or act.Subject like '%.xls%' 
		or act.Subject like '%.rtf' or act.Subject like '%.html' or act.Subject like '%.txt')
		group by ac.ContactID, act.Subject)

DECLARE @ActivityID INT;

OPEN CURSOR_DocumentIds

FETCH NEXT FROM CURSOR_DocumentIds INTO @ActivityID
WHILE (@@FETCH_STATUS <> -1)
BEGIN
  DECLARE @ImageData varbinary(max);
  SELECT @ImageData = (SELECT convert(varbinary(max), act.CompressedRichText, 1)
			from ActivityContacts ac
			left join ActivitiesTable act on act.ActivityID = ac.ActivityID
			where ac.ContactType = 1 and act.ActivityType = 2
			and (act.Subject like '%.pdf' or act.Subject like '%.doc%' or act.Subject like '%.xls%' 
			or act.Subject like '%.rtf' or act.Subject like '%.html' or act.Subject like '%.txt')
			AND ac.ActivityID = @ActivityID);

  DECLARE @Path nvarchar(1024);
  SELECT @Path = 'G:\VC_catalize\Documents\ContactDocument'; --> Only affects on localserver

  DECLARE @Filename NVARCHAR(1024);
  SELECT @Filename = (select replace(replace(act.Subject,'.txt','.doc'),',','')
				from ActivityContacts ac
				left join ActivitiesTable act on act.ActivityID = ac.ActivityID
				where ac.ContactType = 1 and act.ActivityType = 2
				and (act.Subject like '%.pdf' or act.Subject like '%.doc%' or act.Subject like '%.xls%' 
				or act.Subject like '%.rtf' or act.Subject like '%.html' or act.Subject like '%.txt')
				AND ac.ActivityID = @ActivityID
				group by ac.ContactID, act.Subject
			);

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

  FETCH NEXT FROM CURSOR_DocumentIds INTO @ActivityID
END
CLOSE CURSOR_DocumentIds
DEALLOCATE CURSOR_DocumentIds
