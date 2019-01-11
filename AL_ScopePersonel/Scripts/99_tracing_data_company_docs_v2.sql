drop table if exists VCComDocs

;with

tmpClients as (
	select
	ObjectID
	--, ObjectTypeId
	, FileAs
	, FlagText
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
	, x.FileAs
	, x.FlagText
	, a.DocumentID
	, replace(replace(trim(isnull(a.Description, '')), ' ', '_'), '%', '_') as FileName
	from tmpClients x
	left join tmpClientNotebookLinks y on x.ObjectID = y.ClientId
	left join NotebookItems z on y.NotebookItemId = z.NotebookItemId
	left join Documents a on z.NotebookItemId = a.NotebookItemId
	where Description not like 'CV[ -]%'
)

, tmpResult2 as (
	select
	x.ObjectID
	, x.FileAs
	, x.FlagText
	, x.DocumentID
	, x.FileName
	, trim(isnull(y.FileExtension, '')) as FileExtension
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
	, x.FileAs
	, x.FlagText
	, x.DocumentID
	, iif(lower(right(x.FileName, len(FileExtension))) = lower(x.FileExtension), x.FileName, concat(trim('.' from x.FileName), x.FileExtension)) as FileName
	from tmpResult2 x
)

select
ObjectId as ClientId
, string_agg(cast(concat(ObjectID, '_',  DocumentID, '_', FileName) as varchar(max)), ',') Docs

into VCComDocs

from tmpResult3
group by ObjectID

select * from VCComDocs

--5252_184601_image002.png,5252_209894_OutlookEmoji-1463408475259_MO-GIVIAN-FIX-AUTO-LUTON-01-sn.jpg.jpg,5252_184603_OutlookEmoji-1469647867006_PastedImage.png,5252_184588_OutlookEmoji-1463408475259_MO-GIVIAN-FIX-AUTO-LUTON-01-sn.jpg.jpg,5252_184589_OutlookEmoji-1469647867006_PastedImage.png,5252_184602_OutlookEmoji-1463408475259_MO-GIVIAN-FIX-AUTO-LUTON-01-sn.jpg.jpg,5252_209895_OutlookEmoji-1469647867006_PastedImage.png,5252_184600_image001.jpg

--select * from Objects
--where ObjectId = 45459

--select * from Clients
----where ClientID = 45459

--select * from Documents where ClientId is not null

--select * from [TagFiles]
--where ClientId is not null

--select * from ObjectTypes



--select * from Clients
--order by ClientID desc

--select top 10 * from Objects


--where ClientId is not null

--select * from NotebookLinkTypes

--select * from Objects where ObjectID = 31344
--select * from Clients where ClientID = 31341