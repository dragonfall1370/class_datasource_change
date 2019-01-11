DECLARE @SQLIMG VARCHAR(MAX),
@cvId int,
@docName varchar(max),
@cvContent VARBINARY(MAX),
@fileExtension VARCHAR(4),
@filePath VARCHAR(MAX),
@ObjectToken INT

DECLARE cvsCur CURSOR FAST_FORWARD FOR
	select
	concat(
		y.ApplicantId
		, '_'
		, replace(
			isnull(nullif(trim(isnull(a.FileAs, 'NoFileName')), ''), 'NoFileName')
			, ' ', '_'
		)
		, FileExtension
	) as DocName
	, z.[CV]
	from CV y
	left join CVContents z on y.CVId = z.CVId
	left join (select ObjectId, FileAs from Objects where ObjectTypeId = 1) a on y.ApplicantId = a.ObjectID
	where
	z.FileExtension in
	-- supported doc type for document
	--(
	--	'.pdf'
	--	,'.doc'
	--	,'.rtf'
	--	,'.xls'
	--	,'.xlsx'
	--	,'.docx'
	--	,'.png'
	--	,'.jpg'
	--	,'.jpeg'
	--	,'.gif'
	--	,'.bmp'
	--	,'.msg'
	--)
	-- supported doc type for resume
	(
		'.pdf'
		,'.doc'
		,'.docx'
		,'.rtf'
		,'.xls'
		,'.xlsx'
		,'.html'
		,'.txt'
	)
	order by y.CVId desc

OPEN cvsCur 

FETCH NEXT FROM cvsCur INTO @docName, @cvContent

WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @filePath = concat(
			'F:\VC_ScopePersonnel\CvExtracted\'
			, @docName
		)

        PRINT concat('extracting cv [', @docName, '] to ', @filePath)
        --PRINT @SQLIMG

        EXEC sp_OACreate 'ADODB.Stream', @ObjectToken OUTPUT
        EXEC sp_OASetProperty @ObjectToken, 'Type', 1
        EXEC sp_OAMethod @ObjectToken, 'Open'
        EXEC sp_OAMethod @ObjectToken, 'Write', NULL, @cvContent
        EXEC sp_OAMethod @ObjectToken, 'SaveToFile', NULL, @filePath, 2
        EXEC sp_OAMethod @ObjectToken, 'Close'
        EXEC sp_OADestroy @ObjectToken

		PRINT concat('extracted cv [', @docName, '] to ', @filePath)

        FETCH NEXT FROM cvsCur INTO @docName, @cvContent
    END 

CLOSE cvsCur
DEALLOCATE cvsCur