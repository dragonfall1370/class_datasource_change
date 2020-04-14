--COMPANY DOCS
with legal_doc as (select id
	from company_legal_document
	where company_id in (select id from company where external_id ilike 'CPY%' and deleted_timestamp is NULL)
	)
				   
select * from candidate_document where legal_doc_id in (select id from legal_doc)

--JOB DOCS
select *
from candidate_document
where position_description_id in (select id from position_description where external_id ilike 'JOB%')
order by id desc


--CANDIDATE DOCS
select *
from candidate_document
where candidate_id in (select id from candidate where external_id ilike 'CDT%' and deleted_timestamp is NULL)
and document_type = 'resume'
order by id desc


--CANDIDATE PHOTOS
select *
from candidate_document
where candidate_id in (select id from candidate where external_id ilike 'CDT%' and deleted_timestamp is NULL)
and document_type = 'candidate_photo'
order by id desc



