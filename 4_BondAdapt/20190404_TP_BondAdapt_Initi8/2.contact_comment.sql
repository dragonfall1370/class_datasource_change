-- with contact as (select CLIENT,CONTACT from (SELECT CLIENT,CONTACT,ROW_NUMBER() OVER(PARTITION BY cg.CONTACT ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cg) c where c.rn = 1)

/*, doc_note (OWNER_ID, DOC_ID, NOTE) as (
        SELECT D.OWNER_ID, D.DOC_ID, D.DOC_NAME --,DC.NOTE 
        from DOCUMENTS D 
        --left join DOCUMENT_CONTENT DC on DC.DOC_ID = d.DOC_ID 
        WHERE D.DOC_CATEGORY = 6532841 
        AND D.FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip'))
--select top 100 * from doc_note where OWNER_ID = 394903
, doc(OWNER_ID, NOTE) as (SELECT OWNER_ID, STUFF((SELECT char(10) + 'NOTE: ' + NOTE from doc_note WHERE NOTE != '' and NOTE is not null and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 0, '')  AS doc FROM doc_note as a GROUP BY a.OWNER_ID)
--select top 50 * from doc where OWNER_ID = 394903
*/

select --top 100
--select distinct
	ccc.CONTACT as 'externalId', pg.person_id
	, replace(pg.FIRST_NAME,'?','') as 'contact-firstName'
	, case when ( replace(pg.LAST_NAME,'?','') = '' or pg.LAST_NAME is null) then 'No Lastname' else replace(pg.LAST_NAME,'?','') end as 'contact-lastName'
	, cast('-10' as int) as userid
	, doc.CREATED_DATE as insert_timestamp, doc.*
	, ltrim(replace(doc.doc,'ï»¿','')) as 'contact-comments'
-- select count(*) --38547
from PROP_X_CLIENT_CON cc
left join (select CLIENT,CONTACT from (SELECT CLIENT,CONTACT,ROW_NUMBER() OVER(PARTITION BY cg.CONTACT ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cg) c where c.rn = 1) ccc on cc.CONTACT = ccc.CONTACT
left join (select REFERENCE, person_id, FIRST_NAME, LAST_NAME, MIDDLE_NAME from PROP_PERSON_GEN) pg on ccc.CONTACT = pg.REFERENCE
left join (select OWNER_ID, DOC_CATEGORY, DOC_NAME, DOC_DESCRIPTION, FILE_EXTENSION, CREATED_DATE, CREATED_BY, UPDATED_DATE, UPDATED_BY, NOTES, [DEFAULT], [SIZE], STATUS, OWNER_TYPE, PREVIEW_TYPE, convert(varchar(max),convert(varbinary(max),DOCUMENT)) as doc from DOCUMENTS /*where FILE_EXTENSION in ('txt')*/ ) doc on ccc.CONTACT = doc.OWNER_ID
where doc.doc is not null and doc.doc != ''
and ccc.CONTACT = 116658944942
order by ccc.CONTACT desc


-- JOURNAL
SELECT
         ENTITY_ID as 'externalID'
       , cast('-10' as int) as 'user_account_id'
       , 'comment' as 'category'
       , 'contact' as 'type'
       , JE.creation_date as 'insert_timestamp'
       , Stuff(   + Coalesce('Workflow Name: ' + NULLIF(convert(nvarchar(max),bo.description), '') + char(10), '')
                  + Coalesce('User: ' + NULLIF(convert(nvarchar(max),eg.name ), '') + char(10), '')
                  + Coalesce('Notes: ' + NULLIF(convert(nvarchar(max),[dbo].[udf_StripHTML](JE.J_NOTES) ), '') + char(10), '')
                  + Coalesce('Permanent Candidate: ' + NULLIF(convert(nvarchar(max),pg1.fullname ), '') + char(10), '')
                  + Coalesce('Contact External: ' + NULLIF(convert(nvarchar(max),pg2.fullname ), '') + char(10), '')
                  --+ Coalesce('External Interview: ' + NULLIF(convert(nvarchar(max),eg.name ), '') + char(10), '')
                  + Coalesce('Client: ' + NULLIF(convert(nvarchar(max),cg.name ), '') + char(10), '')
                  --+ Coalesce('Progress: ' + NULLIF(convert(nvarchar(max),eg.name ), '') + char(10), '')
                  + Coalesce('Contract Job: ' + NULLIF(convert(nvarchar(max),jg.job_title ), '') + char(10), '')
                  --+ Coalesce('Documents: ' + NULLIF(convert(nvarchar(max),eg.name ), '') + char(10), '')
                , 1, 0, '') as 'content'
-- select COUNT(*) --46128
FROM LK_ENTITIES_JOURNAL J INNER JOIN JOURNAL_ENTRIES JE ON JE.ID = J.JOURNAL_ID 
left join prop_employee_gen eg on eg.user_ref = je.creator_id
left join (select ID, description from MD_MULTI_NAMES MN where LANGUAGE = 10010) bo on bo.id = je.bo_id
left join dbo.PROP_PERSON_GEN pg1 on pg1.reference = je.entity_id_1
left join dbo.PROP_PERSON_GEN pg2 on pg2.reference = je.entity_id_2
left join PROP_CLIENT_GEN cg on cg.reference = je.entity_id_4
left join PROP_JOB_GEN jg on jg.reference = je.entity_id_6
left join (select CONTACT from PROP_X_CLIENT_CON) con on con.CONTACT = J.ENTITY_ID
WHERE con.CONTACT is not null 
and J.ENTITY_ID = 116658944942
and JE.J_NOTES != '' and JE.J_NOTES is not null

