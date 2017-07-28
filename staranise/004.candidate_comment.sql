
with comments as (
select
	 pg.REFERENCE as 'candidate-externalId'
	, Coalesce(NULLIF(replace(pg.FIRST_NAME,'?',''), ''), 'No Firstname') as 'contact-firstName'
	, Coalesce(NULLIF(replace(pg.LAST_NAME,'?',''), ''), 'No Lastname') as 'contact-lastName'
        , cast('-10' as int) as 'userid'
        , cast('4' as int) as 'contact_method'
        , cast('1' as int) as 'related_status'
        , doc.UPDATED_DATE as 'feedback_timestamp|insert_timestamp'
        , ltrim(dbo.UTF8_TO_NVARCHAR(convert(varbinary(max),DOCUMENT))) as test1
        , ltrim(dbo.UTF8_TO_NVARCHAR(convert(varbinary(max),DOCUMENT))) as test2
        , ltrim(replace(convert(varchar(max),convert(varbinary(max),DOCUMENT)) ,'ï»¿','')) as test3
        --, ltrim(dbo.DecodeUTF8String(convert(varbinary(max),DOCUMENT))) as test4
        
        , iif(  doc.DOC_CATEGORY in (31190,31159),
                ltrim(dbo.UTF8_TO_NVARCHAR(convert(varbinary(max),DOCUMENT)))
                --dbo.DecodeUTF8String(convert(nvarchar(max),convert(varbinary(max),DOCUMENT)))
                ,ltrim(replace(convert(varchar(max),convert(varbinary(max),doc.DOCUMENT)) ,'ï»¿',''))) as 'comment_body'
        --, cast(dbo.UTF8_TO_NVARCHAR(doc.doc) as nvarchar(max)) as com
        --, dbo.DecodeUTF8String(doc.doc) as comments
        , doc.doc_id
        , doc.DOC_CATEGORY
-- select count(*) -- select top 200
from PROP_PERSON_GEN pg --87780 rows
left join (select doc_id,DOC_CATEGORY, owner_id,UPDATED_DATE,DOCUMENT from DOCUMENTS where FILE_EXTENSION in ('txt')) doc on pg.REFERENCE = doc.OWNER_ID where doc.DOC_CATEGORY in (31159) and doc_id != 1316492 )

select * -- top 100 * --select count(*) --93657 -- 87779
from comments where comment_body is not null and comment_body <> '' --and DOC_CATEGORY in (31159)
 and [candidate-externalId] < 678749 -- 675080 --




-----------------------------

--left join (select owner_id,UPDATED_DATE,ltrim(replace(convert(varchar(max),convert(varbinary(max),DOCUMENT)) ,'ï»¿','')) as doc from DOCUMENTS where FILE_EXTENSION in ('txt') ) doc on pg.REFERENCE = doc.OWNER_ID
where doc.doc_id in (909484,893735,898197,903362) --and pg.REFERENCE = 675080 --and pg.REFERENCE < 678749 --
--doc.doc is not null and doc.doc != '' 
order by pg.REFERENCE desc

select doc_id, owner_id,UPDATED_DATE,ltrim(replace(convert(varchar(max),convert(varbinary(max),DOCUMENT)) ,'ï»¿','')) as doc from DOCUMENTS where FILE_EXTENSION in ('txt') and OWNER_ID = 675080 and DOC_CATEGORY in (31159)
select doc_id, owner_id,UPDATED_DATE,ltrim(replace(convert(varchar(max),convert(varbinary(max),DOCUMENT)) ,'ï»¿','')) COLLATE Latin1_General_CI_AS as doc from DOCUMENTS where FILE_EXTENSION in ('txt') and OWNER_ID = 675080 and DOC_CATEGORY in (31159)
select doc_id, owner_id,UPDATED_DATE,dbo.UTF8_TO_NVARCHAR(convert(varbinary(max),DOCUMENT )) as doc from DOCUMENTS where FILE_EXTENSION in ('txt') and OWNER_ID = 675080 and DOC_CATEGORY in (31159)
select doc_id, owner_id,UPDATED_DATE,dbo.DecodeUTF8String(dbo.UTF8_TO_NVARCHAR(convert(varbinary(max),DOCUMENT))) as doc from DOCUMENTS where FILE_EXTENSION in ('txt') and OWNER_ID = 675080  and DOC_CATEGORY in (31159)
select doc_id, owner_id,UPDATED_DATE,dbo.DecodeUTF8String(convert(varchar(max),convert(varbinary(max),DOCUMENT))) as doc from DOCUMENTS where FILE_EXTENSION in ('txt') and OWNER_ID = 675080 and DOC_CATEGORY in (31159)
select doc_id, owner_id,UPDATED_DATE,dbo.DecodeUTF8String(convert(nvarchar(max),convert(varbinary(max),DOCUMENT))) as doc from DOCUMENTS where FILE_EXTENSION in ('txt') and OWNER_ID = 675080 and doc_id = 1316492 and DOC_CATEGORY in (31159)


select top 1000 doc_id, owner_id,UPDATED_DATE
        ,iif(DOC_CATEGORY = 31190,ltrim(convert(nvarchar(max),convert(varbinary(max),DOCUMENT))), convert(varchar(max),convert(varbinary(max),DOCUMENT)) ) as doc
-- select count(*)
from DOCUMENTS where FILE_EXTENSION in ('txt') and DOC_CATEGORY = 31190 and OWNER_ID = 675080 and doc_id = 1316492 -- and PREVIEW_TYPE = 'html' --

select top 1000 doc_id, owner_id,UPDATED_DATE
        ,convert(varchar(max),convert(varbinary(max),DOCUMENT)) as doc
        , *
-- select count(*)
from DOCUMENTS where FILE_EXTENSION in ('txt') and doc_id in (909484,893735,898197,903362)









with comments as (
select
	 pg.REFERENCE as 'candidate-externalId'
	/*, Coalesce(NULLIF(replace(pg.FIRST_NAME,'?',''), ''), 'No Firstname') as 'contact-firstName'
	, Coalesce(NULLIF(replace(pg.LAST_NAME,'?',''), ''), 'No Lastname') as 'contact-lastName'
        , cast('-10' as int) as 'userid'
        , cast('4' as int) as 'contact_method'
        , cast('1' as int) as 'related_status'
        , doc.UPDATED_DATE as 'feedback_timestamp|insert_timestamp' */
        , ltrim(dbo.UTF8_TO_NVARCHAR(convert(varbinary(max),DOCUMENT))) as test1
        , ltrim(replace(convert(varchar(max),convert(varbinary(max),DOCUMENT)) ,'ï»¿','')) as test2
        , ltrim(replace(convert(nvarchar(max),convert(varbinary(max),DOCUMENT)) ,'ï»¿','')) as test3
        --, ltrim(dbo.DecodeUTF8String(convert(varbinary(max),DOCUMENT))) as test4
        , doc.doc_id
        , doc.DOC_CATEGORY
-- select top 200
from PROP_PERSON_GEN pg --87780 rows
left join (select doc_id,DOC_CATEGORY, owner_id,UPDATED_DATE,DOCUMENT from DOCUMENTS where FILE_EXTENSION in ('txt')) doc on pg.REFERENCE = doc.OWNER_ID 
where pg.REFERENCE < 678749 and doc.DOC_CATEGORY in (31159,7023002,6532843,31190,7023010,6532840) )

select top 100 * --select count(*) --93657 -- 87779
from comments where comment_body is not null and comment_body <> ''



select distinct doc.DOC_CATEGORY,count(doc.DOC_CATEGORY)
from PROP_PERSON_GEN pg
left join (select doc_id,DOC_CATEGORY, owner_id,UPDATED_DATE,DOCUMENT from DOCUMENTS where FILE_EXTENSION in ('txt')) doc on pg.REFERENCE = doc.OWNER_ID 
where pg.REFERENCE < 678749 group by doc.DOC_CATEGORY
(null)	0
31159	4
7023002	70
6532843	319
31190	354
7023010	753
6532840	2241
6532841	14870
6532839	43080