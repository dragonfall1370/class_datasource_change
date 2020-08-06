-->>MERGE ACTIVITIES
---Check max activity before duplicating and merging candidates | max(id) 698207 --select max(id) from activity
insert into activity(candidate_id, user_account_id, insert_timestamp, content, category, type)
select m.vc_candidate_id as candidate_id
--, a.candidate_id origin_cand_id
--, m.external_id
, a.user_account_id
, a.insert_timestamp
, '【Merged from PA: ' || m.cand_ext_id || '】' || chr(10) || chr(13) || a.content as content
, a.category
, a.type
from activity a
join mike_tmp_candidate_dup_check m on m.vc_pa_candidate_id = a.candidate_id --21926 rows
--and a.candidate_id in (219653, 207588, 201023)

--->>ACTIVITY INDEX AFTER MEGRED<<---
insert into activity_candidate (activity_id, candidate_id, insert_timestamp)
select id
, candidate_id
, insert_timestamp
from activity
where id > 650378
and candidate_id > 0
and id not in (select activity_id from activity_candidate)

/*---check from tmp table
select *
from mike_tmp_candidate_dup_check
where vc_pa_candidate_id = 156924
---*/
/*--SAMPLE CANDIDATES
select *
from activity
where candidate_id in (219653, 207588, 201023) --pa_candidate
order by candidate_id desc

select *
from candidate_document
where candidate_id in (219653, 207588, 201023)

select max(id), count(*)
from candidate_document
where candidate_id > 0
---*/

-->>MERGE DOCUMENTS
---Check max documents before duplicating and merging candidates | max(id) 454045 --select max(id) from candidate_document
insert into candidate_document(candidate_id, uploaded_filename, saved_filename, version_no, document_type, primary_document, google_viewer, temporary
			, document_types_id, customer_portal, visible, uploaded_filename_bk)
select m.vc_candidate_id as candidate_id
--, cd.candidate_id origin_cand_id
--, m.cand_ext_id
, m.cand_ext_id || '_' || cd.uploaded_filename as uploaded_filename
, cd.saved_filename
, cd.version_no
, cd.document_type
, 0 as primary_document --all merged documents will not be primary
, cd.google_viewer
, cd.temporary
, cd.document_types_id
, cd.customer_portal
, cd.visible
, m.vc_pa_candidate_id || '_' || cd.uploaded_filename_bk as uploaded_filename_bk --format: [merged_candidate_id]_[createddate]_[migrated_document_name]
from candidate_document cd
join mike_tmp_candidate_dup_check m on m.vc_pa_candidate_id = cd.candidate_id --19836 rows
where cd.document_type <> 'candidate_photo' --candidate_photo must be checked by the latest date
--and cd.candidate_id in (219653, 207588, 201023)