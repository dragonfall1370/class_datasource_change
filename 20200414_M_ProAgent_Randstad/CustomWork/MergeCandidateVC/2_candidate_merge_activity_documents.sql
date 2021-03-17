--#1 | ACTIVITY
select *
from activity
where candidate_id in (select candidate_id from mike_tmp_candidate_dup_name_mail_dob_master_slave)


--->>MAIN<<---
insert into activity(candidate_id, user_account_id, insert_timestamp, content, category, type, priority, status, related_type, kpi_action, action, contact_method, next_contact_date, next_contact_to_date)
select m.master as candidate_id --master candidate
--, a.candidate_id origin_cand_id
, a.user_account_id
, a.insert_timestamp
, '【Merged from candidate: ' || m.candidate_id || '】' || chr(10) || chr(13) || a.content as content
, a.category
, a.type
--additional info
, a.priority
, a.status
, a.related_type
, a.kpi_action
, a.action
, a.contact_method
, a.next_contact_date
, a.next_contact_to_date
from activity a
join mike_tmp_candidate_dup_name_mail_dob_master_slave m on m.candidate_id = a.candidate_id -- rows
where 1=1
--and nullif(a.content, '') is not NULL --all activities


--->>ACTIVITY INDEX AFTER MEGRED<<---
insert into activity_candidate (activity_id, candidate_id, insert_timestamp)
select id
, candidate_id
, insert_timestamp
from activity
where id > 756386
and candidate_id > 0
and id not in (select activity_id from activity_candidate)


--#2 | DOCUMENTS
--->>MAIN<<---Check max documents before duplicating and merging candidates
insert into candidate_document(candidate_id, uploaded_filename, saved_filename, version_no, document_type, primary_document, google_viewer, temporary
			, document_types_id, customer_portal, visible, uploaded_filename_bk)
select m.master as candidate_id --candidate
--, cd.candidate_id origin_cand_id
, 'S' || m.candidate_id || '_' ||cd.uploaded_filename as uploaded_filename --format: [S: Slave]_[slave_candidate]_[current_uploaded_filename]
, cd.saved_filename
, cd.version_no
, cd.document_type
, 0 as primary_document --all merged documents will not be primary
, cd.google_viewer
, cd.temporary
, cd.document_types_id
, cd.customer_portal
, cd.visible
, m.candidate_id || '_' || cd.uploaded_filename_bk as uploaded_filename_bk --format: [merged_candidate_id]_[createddate]_[migrated_document_name]
from candidate_document cd
join mike_tmp_candidate_dup_name_mail_dob_master_slave m on m.candidate_id = cd.candidate_id --19836 rows
where cd.document_type <> 'candidate_photo' --candidate_photo must be checked by the latest date
--and cd.candidate_id in ()