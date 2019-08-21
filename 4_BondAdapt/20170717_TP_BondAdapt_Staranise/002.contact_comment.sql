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
	ccc.CONTACT as 'externalId'
	, replace(pg.FIRST_NAME,'?','') as 'contact-firstName'
	, case when ( replace(pg.LAST_NAME,'?','') = '' or pg.LAST_NAME is null) then 'No Lastname' else replace(pg.LAST_NAME,'?','') end as 'contact-lastName'
	, cast('-10' as int) as userid
	, doc.UPDATED_DATE as insert_timestamp
	, ltrim(replace(doc.doc,'ï»¿','')) as 'contact-comments'
-- select count(*) --38547
from PROP_X_CLIENT_CON cc
left join (select CLIENT,CONTACT from (SELECT CLIENT,CONTACT,ROW_NUMBER() OVER(PARTITION BY cg.CONTACT ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cg) c where c.rn = 1) ccc on cc.CONTACT = ccc.CONTACT
left join (select REFERENCE,FIRST_NAME,LAST_NAME, MIDDLE_NAME, JOB_TITLE, LINKED_IN,chifullname,chinesename from PROP_PERSON_GEN) pg on ccc.CONTACT = pg.REFERENCE
left join (select owner_id,UPDATED_DATE,convert(varchar(max),convert(varbinary(max),DOCUMENT)) as doc from DOCUMENTS where FILE_EXTENSION in ('txt') ) doc on ccc.CONTACT = doc.OWNER_ID
where doc.doc is not null and doc.doc != '' and ccc.CONTACT = 874634
order by ccc.CONTACT desc
