/* ADD AND BACKUP CANDIDATE PHOTO
ALTER TABLE candidate_document
add column photo_saved_filename_bkup character varying (1000)

update candidate_document
set photo_saved_filename_bkup = saved_filename
where document_type = 'candidate_photo' --6441

select *
into mike_bkup_candidate_document_photo_20200706
from candidate_document
where document_type = 'candidate_photo'
*/

with latest_candidate as (select m.*
	, c.saved_filename as pa_saved_filename
	from mike_tmp_candidate_dup_check m
	join (select candidate_id, saved_filename from candidate_document
							where document_type = 'candidate_photo') c on c.candidate_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and vc_pa_latest_date > coalesce(vc_latest_date, '1900-01-01') --5668 rows
	and c.saved_filename is not NULL
	) --33 photos

/* AUDIT UPDATE DOCUMENTS
select *
from candidate_document
where 1=1
--and document_type = 'candidate_photo'
and candidate_id in (select vc_candidate_id from latest_candidate)
*/
	
--MAIN SCRIPT
---Update if existing photos
update candidate_document cd
set saved_filename = m.pa_saved_filename
from latest_candidate m
where m.vc_candidate_id = cd.candidate_id
and document_type = 'candidate_photo'


--->>Change candidate photos if no existing photos
with latest_candidate as (select m.*
	, c.saved_filename as pa_saved_filename
	from mike_tmp_candidate_dup_check m
	join (select candidate_id, saved_filename from candidate_document
							where document_type = 'candidate_photo') c on c.candidate_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1 --already get latest candidate to update
	and vc_pa_latest_date > coalesce(vc_latest_date, '1900-01-01') --5668 rows
	and c.saved_filename is not NULL
	) --43 photos -- updated 3

, all_pacand_photo as (select m.*
	, c.saved_filename as pa_saved_filename
	from mike_tmp_candidate_dup_check m
	join (select candidate_id, saved_filename from candidate_document
							where document_type = 'candidate_photo') c on c.candidate_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1 --already get latest candidate to update
	--and vc_pa_latest_date > coalesce(vc_latest_date, '1900-01-01') --88 rows --remove this timestamp if not existing candidate photo
	and c.saved_filename is not NULL
	) --88 photos
	
	--select * from all_pacand_photo where vc_candidate_id not in (select vc_candidate_id from latest_candidate) --45 photos

/* AUDIT
select id, saved_filename
from candidate_document
where document_type = 'candidate_photo'
and candidate_id in (select vc_pa_candidate_id from all_pacand_photo)
and candidate_id not in (select vc_candidate_id from latest_candidate)



update candidate_document cd
set candidate_id = m.vc_candidate_id
from all_pacand_photo m
where m.vc_pa_candidate_id = cd.candidate_id
and document_type = 'candidate_photo'
and candidate_id not in (select vc_candidate_id from latest_candidate)
and vc_candidate_id not in (select candidate_id from candidate_document where document_type = 'candidate_photo')
*/


--FINAL SCRIPT FOR CANDIDATE PHOTOS MERGE
with all_pacand_photo as (select m.*
	, c.saved_filename as pa_saved_filename
	from mike_tmp_candidate_dup_check m
	join (select candidate_id, saved_filename from candidate_document
							where document_type = 'candidate_photo') c on c.candidate_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1 --already get latest candidate to update
	--and vc_pa_latest_date > coalesce(vc_latest_date, '1900-01-01') --88 rows --remove this timestamp if not existing candidate photo
	and c.saved_filename is not NULL
	) --88 photos

insert into candidate_document(candidate_id, uploaded_filename, saved_filename, version_no, document_type, primary_document, google_viewer, temporary
			, document_types_id, customer_portal, visible, entity_type, note, uploaded_filename_bk)
select m.vc_candidate_id as candidate_id --
, cd.uploaded_filename as uploaded_filename
, cd.saved_filename
, cd.version_no
, cd.document_type
, 0 as primary_document --all merged documents will not be primary
, cd.google_viewer
, cd.temporary
, cd.document_types_id
, cd.customer_portal
, cd.visible
, cd.entity_type
, cd.note
, m.vc_pa_candidate_id || '_' || cd.uploaded_filename_bk as uploaded_filename_bk --format: [merged_candidate_id]_[createddate]_[migrated_document_name]
from candidate_document cd
join all_pacand_photo m on m.vc_pa_candidate_id = cd.candidate_id -- rows
where cd.document_type = 'candidate_photo' --candidate_photo must be checked by the latest date
and vc_candidate_id not in (select candidate_id from candidate_document where document_type = 'candidate_photo')