with tempcanDocs as (
select ej.entity_id,doc.DOC_ID,cg.REFERENCE, ej.JOURNAL_ID,doc.owner_id,doc_category,doc.doc_name,doc.doc_description,doc.file_extension, doc.OWNER_TYPE, pg.FIRST_NAME,pg.LAST_NAME
	, cast(DOC_ID as varchar(max)) + '_' 
						+ replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(DOC_NAME,'?',''),' ','_'),'/','.'),'''',''),',','_'),'*','~'),'"','^'),'\',''),'|',''),':',''),char(10),''),char(13),'')
						+ '.' + replace(coalesce(nullif(FILE_EXTENSION,''),PREVIEW_TYPE),'txt','rtf') as fulldocName
from LK_ENTITIES_JOURNAL ej left join DOCUMENTS doc on ej.JOURNAL_ID = doc.OWNER_ID
							left join PROP_CAND_GEN cg on ej.ENTITY_ID = cg.REFERENCE
							left join PROP_PERSON_GEN pg on cg.REFERENCE = pg.REFERENCE--23170 rows
--left join PROP_CAND_PREF cp on cp.REFERENCE = cg.REFERENCE
where doc.OWNER_ID is not null and cg.REFERENCE is not null
and ej.entity_id = 116672294397
)

, canDocs as(
select *, ROW_NUMBER() OVER(PARTITION BY REFERENCE ORDER BY fulldocName ASC) AS rn  
from tempcanDocs 
where right(fulldocName,4) <> 'html' and right(fulldocName,3) <> 'htm')

--select * from documents
--where FILE_EXTENSION in ('zip', 'doc', 'docx', 'xls', 'xlsx', 'pdf', 'rtf', 'png', 'jpg', 'gif', 'bmp')

SELECT concat('BB',REFERENCE) as CandidateExternalId,'CANDIDATE' as entity_type, 'resume' as document_type, fulldocName
		, iif(rn=1,1,0) as default_file
from canDocs 