---------------
--CONTACT PHOTO
---------------
DECLARE CURSOR_DocumentIds CURSOR FOR (select ca.AttachmentID
						from ContactAttachments ca
						left join Attachments a on a.ID = ca.AttachmentID
						where a.AttachmentContactPhoto = 1
						and a.ID in (select max(AttachmentID) as maxID from ContactAttachments group by ContactServiceID))

DECLARE @AttachmentID INT;

OPEN CURSOR_DocumentIds

FETCH NEXT FROM CURSOR_DocumentIds INTO @AttachmentID
WHILE (@@FETCH_STATUS <> -1)
BEGIN
  DECLARE @ImageData varbinary(max);
  SELECT @ImageData = (SELECT convert(varbinary(max), a.AttachDataBin, 1)
			from ContactAttachments ca
			left join Attachments a on a.ID = ca.AttachmentID
			where a.AttachmentContactPhoto = 1
			and a.ID in (select max(AttachmentID) as maxID from ContactAttachments group by ContactServiceID)
			AND ca.AttachmentID = @AttachmentID);

  DECLARE @Path nvarchar(1024);
  SELECT @Path = 'G:\VC_catalize\Documents\Contact'; --> Only affects on localserver

  DECLARE @Filename NVARCHAR(1024);
  SELECT @Filename = (select concat(a.ID,'_',a.AttachLongFileName)
				from ContactAttachments ca
				left join Attachments a on a.ID = ca.AttachmentID
				where a.AttachmentContactPhoto = 1
				and a.ID in (select max(AttachmentID) as maxID from ContactAttachments group by ContactServiceID)
			AND ca.AttachmentID = @AttachmentID);

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

  FETCH NEXT FROM CURSOR_DocumentIds INTO @AttachmentID
END
CLOSE CURSOR_DocumentIds
DEALLOCATE CURSOR_DocumentIds
