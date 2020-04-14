-->>MERGE ACTIVITIES
---Check max activity before duplicating and merging companies | max(id) 932471 --select max(id) from activity
insert into activity(company_id, user_account_id, insert_timestamp, content, category, type)
select distinct m.vc_company_id as company_id
--, a.company_id origin_com_id
--, m.com_ext_id
, a.user_account_id
, a.insert_timestamp
, '【Merged from PA: ' || m.com_ext_id || '】' || chr(10) || chr(13) || a.content as content
, a.category
, a.type
from activity a
join mike_tmp_company_dup_check m on m.vc_pa_company_id = a.company_id --16830 rows
where 1=1
--and a.company_id in (65875, 40665, 59016, 57405) --audit sample companies


--->>ACTIVITY INDEX AFTER MEGRED<<---
insert into activity_company (activity_id, company_id, insert_timestamp)
select id
, company_id
, insert_timestamp
from activity
where id > 932471
and company_id > 0

/*---check from tmp table
select *
from mike_tmp_company_dup_check
where vc_pa_company_id = 65875
---*/
/*--SAMPLE COMPANIES
select *
from activity
where company_id in (65875, 40665, 59016, 57405) --pa_company
order by company_id desc

select *
from company_legal_document
where company_id in (65875, 40665, 59016, 57405)
---*/

-->>MERGE DOCUMENTS
---Company documents linked with legal_document | create a backup company_id before merging
---BACKUP LEGAL DOC ID
ALTER TABLE company_legal_document
ADD COLUMN company_id_bkup bigint --backing up current PA-VC company_id

update company_legal_document
set company_id_bkup = company_id
where company_id in (select id from company where external_id ilike 'CPY%') --859 bkup legal_doc_id
--order by insert_timestamp desc

/* EDIT LEGAL DOCUMENT TYPE
update company_legal_document
set type = '契約日'
where company_id in (select id from company where external_id ilike 'CPY%')
and type = '1' --297 records
*/
/* COUNTS FROM candidate_document with merged legal_doc_id
select *
from candidate_document
where legal_doc_id in (select id from company_legal_document where company_id_bkup > 0)
*/

update company_legal_document clg
set company_id = m.vc_company_id
, title = title || '-' || m.com_ext_id
from mike_tmp_company_dup_check m 
where m.vc_pa_company_id = clg.company_id
and clg.company_id in (select id from company where external_id ilike 'CPY%')
and clg.company_id_bkup > 0 --313 updated records

/* CHECK REFERENCES
---After merged
select *
from company_legal_document
where company_id <> company_id_bkup
and company_id_bkup > 0


select clg.id as legal_doc_id
, clg.company_id
, clg.company_id_bkup
, clg.type
, title || '-' || m.com_ext_id as new_title
, m.com_ext_id
, m.vc_company_id --to be applied as new company_id
, m.vc_pa_company_id
from company_legal_document clg
join mike_tmp_company_dup_check m on m.vc_pa_company_id = clg.company_id
where 1=1
and company_id in (select id from company where external_id ilike 'CPY%')
and clg.company_id_bkup > 0 --313 rows


select *
from candidate_document
where 1=1
and legal_doc_id in (select id from company_legal_document where company_id in (select vc_pa_company_id from mike_tmp_company_dup_check)) --491 rows
and legal_doc_id in (select id from company_legal_document where company_id_bkup > 0) --1310 rows


select id 
from company_legal_document 
where company_id_bkup > 0
and company_id in (select vc_pa_company_id from mike_tmp_company_dup_check)
*/