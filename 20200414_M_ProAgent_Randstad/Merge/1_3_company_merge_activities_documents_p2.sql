-->>MERGE ACTIVITIES

/*Check max activity before duplicating and merging companies
select max(id), count(id)
from activity --max=1044993 | count=637167

select max(activity_id), count(activity_id)
from activity_company --max=1044945 | count=146389
*/

insert into activity(company_id, user_account_id, insert_timestamp, content, category, type)
select distinct m.vc_company_id as company_id
--, a.company_id origin_com_id
--, m.com_ext_id
, coalesce(a.user_account_id, -10) as user_account_id
, a.insert_timestamp
, '【Merged from PA: ' || m.com_ext_id || '】' || chr(10) || chr(13) || a.content as content
, a.category
, a.type
from activity a
join mike_tmp_company_dup_check m on m.vc_pa_company_id = a.company_id --5236 rows
where 1=1
--and vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check) --1400 rows | using if identifying dup partially

--->>ACTIVITY INDEX AFTER MEGRED<<---
insert into activity_company (activity_id, company_id, insert_timestamp)
select id
, company_id
, insert_timestamp
from activity
where id > 1044993
and company_id > 0


---COMPANY DOCUMENTS (NEW SCHEMA)
/*
select max(id), count(id)
from candidate_document --max=474324 | count=422430
*/

insert into candidate_document (company_id, candidate_id, position_description_id, contact_id, legal_doc_id, user_id, uploaded_filename, filesize
, saved_filename, mime_type, version_no, insert_timestamp, document_type, created, document_types_id, visible, entity_type)
select distinct m.vc_company_id as company_id
--, m.vc_pa_company_id
--, m.com_ext_id
, 0 candidate_id
, 0 position_description_id
, 0 contact_id
, 0 legal_doc_id
, 0 user_id
, m.com_ext_id || '_' || c.uploaded_filename as uploaded_filename
, filesize
, saved_filename
, mime_type
, version_no
, insert_timestamp
, document_type
, created
, document_types_id
, 1 visible
, 'COMPANY' entity_type
from candidate_document c
join mike_tmp_company_dup_check m on m.vc_pa_company_id = c.company_id --127 rows
where 1=1
and document_type = 'legal_document' 
--and vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check) --1400 rows | using if identifying dup partially