DECLARE @SQLIMG VARCHAR(MAX),
@cvId int,
@cvContent VARBINARY(MAX),
@fileExtension VARCHAR(4),
@filePath VARCHAR(MAX),
@ObjectToken INT

DECLARE cvsCur CURSOR FAST_FORWARD FOR 
        SELECT [CVId], [CV], [FileExtension] from [CVContents] x order by CVId desc

OPEN cvsCur 

FETCH NEXT FROM cvsCur INTO @cvId, @cvContent, @fileExtension

WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @filePath = concat(
			'F:\VC_ScopePersonel\CvsExtracted\'
			, @cvId
			, '_'
			, replace(replace(replace(replace(convert(varchar,getdate(),121),'-',''),':',''),'.',''),' ','')
			, @fileExtension
		)

        PRINT concat('extracting cv (Id: ', @cvId, ') to ', @filePath)
        --PRINT @SQLIMG

        EXEC sp_OACreate 'ADODB.Stream', @ObjectToken OUTPUT
        EXEC sp_OASetProperty @ObjectToken, 'Type', 1
        EXEC sp_OAMethod @ObjectToken, 'Open'
        EXEC sp_OAMethod @ObjectToken, 'Write', NULL, @cvContent
        EXEC sp_OAMethod @ObjectToken, 'SaveToFile', NULL, @filePath, 2
        EXEC sp_OAMethod @ObjectToken, 'Close'
        EXEC sp_OADestroy @ObjectToken

		PRINT concat('extracted cv (Id: ', @cvId, ') to ', @filePath)

        FETCH NEXT FROM cvsCur INTO @cvId, @cvContent, @fileExtension
    END 

CLOSE cvsCur
DEALLOCATE cvsCur