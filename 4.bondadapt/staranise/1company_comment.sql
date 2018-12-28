select * from position_candidate_feedback;
select distinct comment_timestamp from company_comment
select id,company_id,user_id,comment_content from company_comment
select * from position_candidate_fact2 where owner_id::int = 395082
select id,external_id from company where id = 11437

update company_comment 
set comment_content = cf.comment_body
-- select cf.comment_body,cf.doc_id, c.id, c.external_id, cc.company_id,cc.user_id,cc.comment_content --select count(*) --34762/61388
from position_candidate_feedback cf
left join position_candidate_fact2 cf2 on cf.doc_id::int = cf2.doc_id::int
left join company c on c.external_id::int = cf2.owner_id::int
left join company_comment cc on c.id::int = cc.company_id
where c.external_id is null or c.name like '%3M%'

insert into company_comment(company_id,user_id,comment_content)
select c.id,-10, replace(replace(cf.comment_body,'</p>',''),'<p>','') ,cf.doc_id
from position_candidate_feedback cf
left join position_candidate_fact2 cf2 on cf.doc_id::int = cf2.doc_id::int
left join company c on c.external_id::int = cf2.owner_id::int
left join company_comment cc on c.id::int = cc.company_id
where c.external_id is not null 
--and c.name like '%Herbert%'
and c.id = 13136
order by cf.doc_id::int


delete from company_comment where id > 0 --company_id = 11437
select count(*) from bulk_upload_document_mapping
delete from bulk_upload_document_mapping where id > 0
SELECT * FROM DOCUMENTS  left join PROP_CLIENT_GEN on  OWNER_ID = PROP_CLIENT_GEN.REFERENCE where DOC_CATEGORY in (7023000,31185)
-----------------
select * from DOCUMENTS d left join company c on d.owner_id::int = c.external_id::int and c.external_id is not null
and doc_id = 1008983

insert into company_comment(company_id,user_id,comment_content)
select c.name,c.id,-10,d.document, d.updated_date --, d.*-- replace(replace(d.document,'</p>',''),'<p>','')
	--cc.company_id,cc.user_id,cc.comment_content	, c.id,c.external_id	, d.*
-- select count(*) --14359
from DOCUMENTS d
left join company c on d.owner_id::int = c.external_id::int
where d.DOC_CATEGORY in (7022996,7023000,6532843,7023004,31190) and d.document != '' and d.document is not null and c.external_id is not null and c.id = 13136 --16861
order by d.updated_date

------------------------------
update company_comment
set comment_timestamp = d.updated_date

-- select c.name,c.id,-10,d.document, d.updated_date --, d.*-- replace(replace(d.document,'</p>',''),'<p>','')
	,c.external_id
	,cc.id,cc.company_id,cc.user_id,cc.comment_timestamp --,cc.comment_content
-- select count(*) --14359
from DOCUMENTS d
left join company c on d.owner_id::int = c.external_id::int
left join company_comment cc on c.id = cc.company_id
where d.DOC_CATEGORY in (7022996,7023000,6532843,7023004,31190) and d.document != '' and d.document is not null and c.external_id is not null and c.id = 9399 --16861
order by d.updated_date

select count(*) from company_comment where company_id = 9399 --225
select * from company_comment where company_id = 9399