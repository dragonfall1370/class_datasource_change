Select * from 	PROP_CONSENT	 where REFERENCE = 44530;
select * from dbo.PROP_SHORT_GEN where "LAST_OF_TM" is not null;
select * from dbo.PROP_VARIABLES where "CN_LAST_ERR" is not null;
select * from dbo.PROP_VARIABLES where "LAST_EXP_DT" is not null;
select * from dbo.PROP_VARIABLES where "LAST_EXP_TM" is not null;
select * from dbo.PROP_VARIABLES where "LAST_NAME" is not null;
select * from dbo.PROP_WL_GEN where "LAST_LOGIN" is not null;
select * from dbo.PROP_CLOCK_GEN_AW where "LASTWWEEK_AW" is not null;
select * from dbo.PROP_JOB_ACENTRAL where "LAST_EXP_DT" is not null;
select * from dbo.PROP_JOB_ACENTRAL where "LAST_EXP_TM" is not null;
select * from dbo.PROP_PAYROLL_HIST where "LAST_EXP_DT" is not null;
with
 doc(OWNER_ID, DOC_ID, DOC_CATEGORY, NOTE) as (SELECT D.OWNER_ID, D.DOC_ID, DOC_CATEGORY, DC.NOTE from DOCUMENTS D left join DOCUMENT_CONTENT DC on DC.DOC_ID = d.DOC_ID WHERE FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip'))
--select top 3 * from doc where OWNER_ID = 394903
--SELECT * FROM doc WHERE charindex(NOTE, char(26)) > 0
 --SELECT top 5 * FROM doc WHERE CHARINDEX(CHAR(1), NOTE) <> 0
 --SELECT top 5 REPLACE(NOTE COLLATE Latin1_General_BIN, char(26), '') from doc

/*
, comment(OWNER_ID, NOTE) as (SELECT OWNER_ID
	,STUFF((SELECT char(10) + 'NOTE: ' + NOTE from doc WHERE NOTE != '' and NOTE is not null and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 0, '') AS doc
	FROM doc as a where DOC_CATEGORY = 6532839 GROUP BY a.OWNER_ID)
select top 10 * from comment where NOTE is not null --and OWNER_ID = 394903
*/

, comment(OWNER_ID, NOTE) as (SELECT OWNER_ID
  	, STUFF((
	   SELECT ', ' + REPLACE(NOTE COLLATE Latin1_General_BIN, char(26), '') --REPLACE(ISNULL(NOTE, ''), CHAR(26), '')
	   from doc WHERE NOTE != '' and NOTE is not null and OWNER_ID = a.OWNER_ID
	   FOR XML PATH('')--, TYPE
	  )--.value('.', 'nvarchar(MAX)')
	  , 1, 1, '') As docabc
        FROM doc as a GROUP BY a.OWNER_ID)
select top 2 * from comment --where NOTE is not null

/*
REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
REPLACE( REPLACE( REPLACE( REPLACE( 
    < YOUR EXPRESSION TO BE CLEANED >
,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'')
*/

ALTER TABLE position_candidate_feedback ADD COLUMN candidate_externalid	int4
ALTER TABLE position_candidate_feedback RENAME COLUMN "candidate_externalid" TO doc_id

select id,external_id from candidate where id = 54283

select *
-- select count(*)
from position_candidate_feedback --where candidate_id = 54283 -- 14493 rows
where --candidate_externalid =  1390508
comment_body like 'General Notes:%' -- 4373
--comment_body like 'Job Description: _%' -- 4373
--comment_body like 'Contact Note:%' -- 4373
comment_body like 'Client Description: _%' -- 4373
--comment_body like 'Client Email:%' -- 9772
--comment_body like 'Client Overview:%' -- 4845
comment_body like 'Client Visit Notes:%' -- 4406
and candidate_externalid is not null and comment_body is not null order by candidate_externalid desc

select id,external_id from company
-------------
select co.id,co.name,co.external_id from company co where external_id::int = 395090 --4092
select count(*) from position_candidate_feedback --48060

-----------------------------------------------------------------------------
select co.id,co.name,co.external_id, c2.doc_id, c2.owner_id, cm.id, cm.candidate_id, cm.comment_body
from company co
left join position_candidate_fact2 c2 on co.external_id::int = c2.owner_id::int
left join position_candidate_feedback cm on c2.doc_id::int = cm.doc_id::int
where co.external_id::int = 394865
--co.name like '%Instinet Pacific Limited%'
--comment_body is not null

select co.id,co.name,co.external_id, c2.doc_id, c2.owner_id, cm.id, cm.candidate_id, cm.comment_body
from company co
left join position_candidate_fact2 c2 on co.external_id::int = c2.owner_id::int
left join position_candidate_feedback cm on c2.doc_id::int = cm.doc_id::int
where co.external_id::int = 394865
-----------------------------------------------------------------------------

update position_candidate_feedback abc
set candidate_id = co.id
from company co
left join position_candidate_fact2 c2 on co.external_id::int = c2.owner_id::int
left join position_candidate_feedback pcf on c2.doc_id::int = pcf.doc_id::int
where co.external_id::int = 394865
and abc.doc_id = pcf.doc_id



select candidate_id, doc_id from position_candidate_feedback
where doc_id is not null
limit 10


select pcf.candidate_id, pcf.doc_id, f2.owner_id from position_candidate_feedback pcf
left outer join position_candidate_fact2 f2 ON f2.doc_id::int = pcf.doc_id::int
where pcf.doc_id in (1000007)



select pcf.candidate_id, pcf.doc_id, f2.owner_id, c.external_id, c.id from position_candidate_feedback pcf
left outer join position_candidate_fact2 f2 ON f2.doc_id::int = pcf.doc_id::int
left outer join company c On c.external_id::int = f2.owner_id::int
where 1=1
and pcf.doc_id::int in (877807)
limit 100


select c.id, c.external_id from company c where c.external_id::int = 408979





update position_candidate_feedback abc
set candidate_id = c.id
from position_candidate_feedback pcf
left outer join position_candidate_fact2 f2 ON f2.doc_id::int = pcf.doc_id::int
left outer join company c On c.external_id::int = f2.owner_id::int
where 1=1
and pcf.doc_id::int in (877807)
and abc.doc_id = pcf.doc_id


update position_candidate_feedback
set candidate_id = 9318
where doc_id = 877807


select id from candidate where external_id::int = 877807


-----------------------------------------------



select *
from company co
left join position_candidate_fact2 c2 on co.external_id::int = c2.owner_id::int
left join position_candidate_feedback pcf on c2.doc_id::int = pcf.doc_id::int
where co.external_id::int = 394865



delete from position_candidate_feedback where id > 0
delete from position_candidate_feedback where comment_body like 'General Notes:%' --comment_body like 'Contact Note:%' -- 4373

select * from position_candidate_fact2 c2 where c2.owner_id::int = 394865

ALTER TABLE position_candidate_fact2 RENAME COLUMN "0);" TO owner_id2

update position_candidate_fact2
set doc_id =  replace(owner_id,'owner_id) VALUES (','')

update position_candidate_fact2
set owner_id =  replace(owner_id2,');','')
where owner_id2 = ' 11445);'

select distinct owner_id,owner_id2 from position_candidate_fact2
where owner_id2 != ' 11445);'



----------------------------

SELECT distinct X_CLIENT.CLIENT,LE.NAME FROM PROP_X_LE_CLIENTS X_CLIENT INNER JOIN PROP_LE_GEN LE ON LE.REFERENCE = X_CLIENT.LE where name like '%samsung%'

select  STREET2 from PROP_ADDRESS ADDRESS                                                   INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID where CONFIG_NAME = 'Primary' and ADDRESS.REFERENCE = <<PROP_CLIENT_GEN.REFERENCE>>
select REFERENCE,CONFIG_NAME,STREET1,TOWN,STATE,COUNTRY,POST_CODE from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID where CONFIG_NAME = 'Primary'    ) address ON cg.REFERENCE = address.REFERENCE

SELECT REFERENCE,TEL_NUMBER FROM PROP_TELEPHONE WHERE OCC_ID =2034418

SELECT MN.DESCRIPTION as Source FROM PROP_CLIENT_GEN CG INNER JOIN MD_MULTI_NAMES MN ON MN.ID = CG.SOURCE
SELECT MN.DESCRIPTION as Currency FROM PROP_CLIENT_GEN CG INNER JOIN MD_MULTI_NAMES MN ON MN.ID = CG.CURRENCY

SELECT REFERENCE,SAL_AGREED FROM PROP_CLIENT_TC

--SELECT GUARANTEE FROM PROP_CLIENT_TC
SELECT CLIENT_TC.REFERENCE,MN.DESCRIPTION as GUARANTEE FROM PROP_CLIENT_TC CLIENT_TC INNER JOIN MD_MULTI_NAMES MN ON MN.ID = CLIENT_TC.GUARANTEE

SELECT REFERENCE,PSA FROM PROP_CLIENT_TC

SELECT * FROM DOCUMENTS where DOC_CATEGORY = 7022996


-------------

with
 doc(OWNER_ID, DOC_ID) as (
 	SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(500)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY in (31185,7022996) AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')
 	  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--select * from doc where doc.DOC_ID::int = 1113487

-- select * from DOCUMENTS WHERE doc_ID IN (1043445,1043480,1043811,1113487)
 select distinct file_extension from DOCUMENTS WHERE doc_name like '%SAL%Terms%'
 select count(*) from DOCUMENTS WHERE doc_name like '%SAL%Terms%' -- 141

--select count(*) from doc where doc.DOC_ID is not null

--select top 100
select
	cg.REFERENCE as 'company-externalId'
	, cg.NAME as 'company-name'
	, d.doc_id
	, d.Doc_category as 'company-category'
	, d.doc_name
	, d.file_extension
-- select count(*) -- select *
from PROP_CLIENT_GEN cg where cg.NAME like '%samsonite%' -- reference IN (1043445,1043480,1043811,1113487)
--left join doc on cg.REFERENCE = doc.OWNER_ID
left join DOCUMENTS d on cg.reference = d.OWNER_ID
where cg.NAME like '%samsonite%'
-- d.doc_name like '%SAL%'
-- d.doc_ID IN (1043445,1043480,1043811,1113487)
--d.Doc_category = 31190
--where d.file_extension = 'rtf'
--where ho.NAME is not null
--doc.DOC_ID is not null
--where cg.name like '%samsung%'
--where cg.reference = 435015
--where own.NAME is not null
order by cg.REFERENCE
---------------------
select id,name,currency_type from position_description where id = 29920
select distinct currency_type from position_description SGD
update position_description set currency_type = 'HKD' where currency_type is null
