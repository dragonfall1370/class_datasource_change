drop table if exists VCComDocContent

;with

tmpClients as (
	select
	ObjectID
	--, ObjectTypeId
	--, FileAs
	--, FlagText
	, LocationId
	from Objects
	where ObjectTypeId = 2 -- Client
)
--select count(*) from tmpClients --39137
, tmpClientNotebookLinks as (
	select
	NotebookLinkId
	, NotebookItemId
	, NotebookLinkTypeId
	, ObjectId
	, JobId
	, ClientId
	from NotebookLinks
	where ClientId is not null
)

, tmpResult1 as (
	select
	x.ObjectID
	--, x.FileAs
	--, x.FlagText
	, a.DocumentID
	, replace(replace(trim(isnull(a.Description, '')), ' ', '_'), '%', '_') as DocName
	from tmpClients x
	left join tmpClientNotebookLinks y on x.ObjectID = y.ClientId
	left join NotebookItems z on y.NotebookItemId = z.NotebookItemId
	left join Documents a on z.NotebookItemId = a.NotebookItemId
	where Description not like 'CV[ -]%'
)

, tmpResult2 as (
	select
	x.ObjectID
	--, x.FileAs
	--, x.FlagText
	, x.DocumentID
	, x.DocName
	, trim(isnull(y.FileExtension, '')) as FileExtension
	, y.Document
	from tmpResult1 x
	left join DocumentContent y on x.DocumentID = y.DocumentId
	where
	y.FileExtension in
	-- supported doc type for document
	(
		'.pdf'
		,'.doc'
		,'.rtf'
		,'.xls'
		,'.xlsx'
		,'.docx'
		,'.png'
		,'.jpg'
		,'.jpeg'
		,'.gif'
		,'.bmp'
		,'.msg'
	)
)


, tmpResult3 as (
	select
	x.ObjectID
	--, x.FileAs
	--, x.FlagText
	, x.DocumentID
	, concat(ObjectID, '_',  DocumentID, '_'
		, iif(lower(right(x.DocName, len(FileExtension))) = lower(x.FileExtension), x.DocName, concat(trim('.' from x.DocName), x.FileExtension))
	) as DocName
	, Document as DocContent
	from tmpResult2 x
)

select * into VCComDocContent from tmpResult3

GO

---------------------------------------------------------------------------------------------------
DECLARE
--@SQLIMG VARCHAR(MAX),
--@objectId int,
--@docId int,
@docContent VARBINARY(MAX),
--@fileExtension VARCHAR(4),
@docName varchar(255),
@filePath VARCHAR(MAX),
@ObjectToken INT

DECLARE cvsCur CURSOR FAST_FORWARD FOR
	SELECT DocName, DocContent from VCComDocContent x order by ObjectID, DocumentID, DocName

OPEN cvsCur 

FETCH NEXT FROM cvsCur INTO @docName, @docContent

WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @filePath = concat(
			'F:\VC_ScopePersonnel\DocExtracted\ComDocs\'
			, @docName
		)

        PRINT concat('extracting doc (name: ', @docName, ') to ', @filePath)
        --PRINT @SQLIMG

        EXEC sp_OACreate 'ADODB.Stream', @ObjectToken OUTPUT
        EXEC sp_OASetProperty @ObjectToken, 'Type', 1
        EXEC sp_OAMethod @ObjectToken, 'Open'
        EXEC sp_OAMethod @ObjectToken, 'Write', NULL, @docContent
        EXEC sp_OAMethod @ObjectToken, 'SaveToFile', NULL, @filePath, 2
        EXEC sp_OAMethod @ObjectToken, 'Close'
        EXEC sp_OADestroy @ObjectToken

		PRINT concat('extracted doc (name: ', @docName, ') to ', @filePath)

        FETCH NEXT FROM cvsCur INTO @docName, @docContent
    END 

CLOSE cvsCur
DEALLOCATE cvsCur