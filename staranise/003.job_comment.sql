/*
with
--  publicdoc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY =6532897 AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--, internaldoc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY =6532850 AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
  publicdoc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY =6532897 AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--select * from publicdoc where DOC_ID is not null and owner_id = 494235
--select distinct FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY in (6532897,6532850)
, internaldoc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY =6532850 AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
select * from publicdoc where DOC_ID is not null and owner_id in (494235,840379)
*/

select top 100
-- select
	cj.JOB as 'position-externalId' 
	, cj.CONTACT as 'position-contactId'
	, jg.JOB_TITLE as 'position-title'
	, iif(publicdoc.doc = '' or publicdoc.doc is null, 'To be updated', cast(publicdoc.doc as varchar(max))) as 'public_description'
	, iif(internaldoc.doc = '' or internaldoc.doc is null, 'To be updated', cast(internaldoc.doc as varchar(max))) as 'full_description' --Internal job description
	--, ltrim(replace(doc.doc,'ï»¿','')) as 'comment_content'
-- select top 100 * -- select COUNT(*) --2940
from PROP_X_CLIENT_JOB cj --3095 rows
left join PROP_JOB_GEN jg on cj.JOB = jg.REFERENCE
left join (SELECT OWNER_ID, STUFF((SELECT char(10) + ltrim(replace(convert(varchar(max),convert(varbinary(max),DOCUMENT)) ,'ï»¿','')) as doc from DOCUMENTS WHERE DOC_CATEGORY =6532897 AND FILE_EXTENSION in ('TXT') and OWNER_ID = a.OWNER_ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID) publicdoc on jg.REFERENCE = publicdoc.OWNER_ID
left join (SELECT OWNER_ID, STUFF((SELECT char(10) + ltrim(replace(convert(varchar(max),convert(varbinary(max),DOCUMENT)) ,'ï»¿','')) as doc from DOCUMENTS WHERE DOC_CATEGORY =6532850 AND FILE_EXTENSION in ('TXT') and OWNER_ID = a.OWNER_ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID) internaldoc on jg.REFERENCE = internaldoc.OWNER_ID
--left join (select owner_id,UPDATED_DATE,convert(varchar(max),convert(varbinary(max),DOCUMENT)) as doc from DOCUMENTS where FILE_EXTENSION = 'txt' ) doc on jg.REFERENCE = doc.OWNER_ID
where -- publicdoc.doc is not null or internaldoc.doc is not null
jg.REFERENCE = 394868 --in (494235) --,840379)