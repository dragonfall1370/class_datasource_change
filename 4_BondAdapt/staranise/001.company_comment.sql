/*
create table dbo.DOCUMENTS_TMP
        (DOC_ID int,
        OWNER_ID int,
        DOCUMENT varchar(max),
        UPDATED_DATE datetime
        ) */

-- select distinct FILE_EXTENSION from DOCUMENTS


select    cg.REFERENCE as 'company-externalId'
        , cg.NAME as 'company-name'
        , cast('-10' as int) as userid
        , doc.UPDATED_DATE as 'comment_timestamp|insert_timestamp'
	, ltrim(replace(doc.doc,'ï»¿','')) as 'comment_content'
-- select count(*) --14639
from PROP_CLIENT_GEN cg
left join (select owner_id,UPDATED_DATE,convert(varchar(max),convert(varbinary(max),DOCUMENT)) as doc from DOCUMENTS where FILE_EXTENSION in ('txt','rtf') ) doc on cg.REFERENCE = doc.OWNER_ID
--where doc.doc is not null
where doc.doc is null or doc.UPDATED_DATE is null
where cg.REFERENCE = 395217
--cg.name = 'PwC - PricewaterhouseCoopers Ltd (Hong Kong)'