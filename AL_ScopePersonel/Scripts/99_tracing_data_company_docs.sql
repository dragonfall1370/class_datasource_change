--  ObjectTypeId	SystemCode	Description
--1	APP	Applicant
--2	CLNT	Client
--3	CCT	Contact
--4	PER	Person
declare @objectType int
set @objectType = 1

drop table if exists #TmpTab1

select
distinct
z.ObjectId
, a.FileAs
, z.JobId
, z.ClientId
, x.DocumentID
, x.Description
, x.DocumentStatusId
--, c.Document
, c.FileExtension
--, x.NotebookItemId
--, y.NotebookTypeId
--, y.NotebookFolderId
, y.Subject
, z.NotebookLinkTypeId

into #TmpTab1

from Documents x
join NotebookItems y on x.NotebookItemId = y.NotebookItemId
join NotebookLinks z on y.NotebookItemId = z.NotebookItemId
join Objects a on z.ObjectId = a.ObjectID
--join ObjectTypes b on a.ObjectTypeId = b.ObjectTypeId
join DocumentContent c on x.DocumentID = c.DocumentId
where a.ObjectTypeId = @objectType
and FileExtension in
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
and Description not like 'CV%'
-- supported doc type for resume
--(
--	'.pdf'
--	,'.doc'
--	,'.docx'
--	,'.rtf'
--	,'.xls'
--	,'.xlsx'
--	,'.html'
--	,'.txt'
--)

--and DocumentStatusId = 10 -- Good

drop table if exists #TmpTab2

select
*
, row_number() over(partition by ObjectId, Description order by DocumentId desc) rn

into #TmpTab2

from #TmpTab1

drop table if exists #TmpTab1
drop table if exists VCCompanyDocs

select
ObjectId
, FileAs
, JobId
, ClientId
, DocumentID
, Description
, FileExtension
, Subject

into VCCompanyDocs

from #TmpTab2

where rn = 1

drop table if exists #TmpTab2

select * from VCCompanyDocs


--select * from DocumentStatus

--SELECT top 100 [NotebookLinkId]
--    ,[NotebookItemId]
--    ,[NotebookLinkTypeId]
--    ,x.[ObjectId]
--    ,[JobId]
--    ,[ClientId]
--FROM [scope].[dbo].[NotebookLinks] x
--join Objects y on x.ObjectId = y.ObjectID
--where [NotebookLinkTypeId] in (21, 27)
--and y.ObjectTypeId = 1

--select * from NotebookItems where NotebookItemId = 905864
--select * from documents where  NotebookItemId = 905864

--select * from Objects where ObjectID = 117240

--select * from ObjectTypes

--select * from NotebookLinkTypes 
--NotebookLinkTypeId	SystemCode	Description
--18	NOTLNK_TYP_EMAIL_TO	Email TO
--19	NOTLNK_TYP_EMAIL_CC	Email CC
--20	NOTLNK_TYP_EMAIL_BCC	Email BCC
--21	NOTLNK_TYP_REFERENCE	Reference
--22	NOTLNK_TYP_FAX	Fax
--23	NOTLNK_TYP_FAX_TRANS_ID	Fax Transaction Id
--24	NOTLNK_TYP_SMS	SMS
--25	NOTLNK_TYP_REVIEWLIST	ReviewList
--26	NOTLNK_TYP_LETTER_TO	Letter To
--27	NOTLNK_TYP_PRIMARY	Primary
--28	NOTLNK_TYP_EMAIL_FROM	Email From
--29	NOTLNK_TYP_SMS_FROM	Sms From
