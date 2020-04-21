--UPDATE LATEST RESUME AT ORIGINAL CV
with cand_doc as (select id, candidate_id
				  , uploaded_filename
				  , primary_document
				  , row_number() over(partition by candidate_id order by created desc, id desc, primary_document desc) as rn
				  from candidate_document
				  where candidate_id > 0
				  and document_type = 'resume'
	)

/*
select *
into mike_tmp_candidate_doc_20190703 --backup filter query | 352566 records
from cand_doc
--where rn = 1
--and primary_document = 0 --97019
*/

update candidate_document
set primary_document = 1
from cand_doc
where candidate_document.id = cand_doc.id
and cand_doc.rn = 1
and candidate_document.primary_document = 0


--NEW RULES: 20190705
with doc_resume as (select id, candidate_id
					, uploaded_filename
					, saved_filename
					, document_type
					, created
					, primary_document
					from candidate_document
					where candidate_id in (select id from candidate where external_id is not NULL and deleted_timestamp is NULL)
					and document_type = 'resume'
				   )

, doc_original as (select *
					, row_number() over(partition by candidate_id order by created desc) as rn
					from doc_resume
					where uploaded_filename ilike '%CV -%' or uploaded_filename ilike '%RESUME%'
				   )
				   
select *
from doc_original

--UPDATE ALL PRIMARY TO 0
update candidate_document
set primary_document = 0
where candidate_id in (select id from candidate where external_id is not NULL and deleted_timestamp is NULL)
and document_type = 'resume'

--BACKUP
select *
into mike_candidate_document_resume_20190507
from candidate_document
					where candidate_id in (select id from candidate where external_id is not NULL and deleted_timestamp is NULL)
					and document_type = 'resume' --351337
					
--UPDATE PRIMARY DOCUMENT
update candidate_document
set primary_document = 1
from doc_original dor
where candidate_document.id = dor.id
and dor.rn = 1
and candidate_document.primary_document = 0