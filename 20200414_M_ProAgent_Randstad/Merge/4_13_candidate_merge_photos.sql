/* ADD AND BACKUP CANDIDATE PHOTO
ALTER TABLE candidate_document
add column photo_saved_filename_bkup character varying (1000)

update candidate_document
set photo_saved_filename_bkup = saved_filename
where document_type = 'candidate_photo' --6441
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
update candidate_document cd
set saved_filename = m.pa_saved_filename
from latest_candidate m
where m.vc_candidate_id = cd.candidate_id
and document_type = 'candidate_photo'
