--GENERATING UID
select f.uniqueid as cand_ext_id
	, uuid_in(overlay(overlay(md5(random()::text || ':' || clock_timestamp()::text) placing '4' from 13) placing to_hex(floor(random()*(11-8+1) + 8)::int)::text from 17)::cstring) as UID
	, right("relative document path", position(E'\\' in reverse("relative document path")) - 1) as candidate_document --no need to change to rtf anymore
	, to_date(date, 'DD/MM/YY')::timestamp as created
	, case when lower(right("relative document path", position('.' in reverse("relative document path")))) in ('.doc','.rtf') then 'application/msword'
         when lower(right("relative document path", position('.' in reverse("relative document path")))) in ('.pdf') then 'application/pdf'
         when lower(right("relative document path", position('.' in reverse("relative document path")))) in ('.jpeg') then 'image/jpeg'
         when lower(right("relative document path", position('.' in reverse("relative document path")))) in ('.jpg') then 'image/jpg'
         when lower(right("relative document path", position('.' in reverse("relative document path")))) in ('.png') then 'image/png'
         when lower(right("relative document path", position('.' in reverse("relative document path")))) in ('.gif') then 'image/gif'
         when lower(right("relative document path", position('.' in reverse("relative document path")))) in ('.txt','.html','htm') then 'text/html'
         when lower(right("relative document path", position('.' in reverse("relative document path")))) in ('.xlsx','.xls') then 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
         when lower(right("relative document path", position('.' in reverse("relative document path")))) in ('.docx') then 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
         when lower(right("relative document path", position('.' in reverse("relative document path")))) in ('.csv') then 'application/vnd.ms-excel'
         else null end mime_type
	, description as note
--into candidate_document --#TEMP TABLE FOR RENAMING FILES | DOCUMENT MAPPING
from f01docs1_edited f 
join (select uniqueid from f01 where "101 candidate codegroup  23" = 'Y') c on c.uniqueid = f.uniqueid
where lower(right("relative document path", position('.' in reverse("relative document path"))))
	IN ('.pdf','.doc','.docx','.xls','.xlsx','.rtf','.htm','.html','.txt','.png','.jpg','.jpeg','.gif','.bmp','.msg')
	
	
--CANDIDATE DOCUMENT MAPPING (to VINCERE) + ADDED PRIMARY DOCUMENTS
with cand_doc as (select cand_ext_id
	, candidate_document as uploaded_filename
	, uid || right(candidate_document, position('.' in reverse(candidate_document))) as saved_filename
	, row_number() over(partition by cand_ext_id
							order by case when note in ('People Notes', 'Window 1') then 1 else 2 end desc --original CV should go first
							, case when right(candidate_document, position('.' in reverse(candidate_document))) in ('.pdf', '.doc', '.docx') then 1 else 2 end asc --pdf, doc, docx go first
							, created desc --latest file go first
						) as cand_doc_primary
	, mime_type
	, created
	, now() insert_timestamp
	, now() trigger_index_update_timestamp
	, 'resume' document_type
	, 1 document_types_id
	, 'CANDIDATE' entity_type
	, -10 user_account_id
	, cast(0.0 as float) successful_parsing_percent
	, note
	--, 0 primary_document
	, 0 legal_doc_id
	, -1 google_viewer
	, 1 "temporary"
	, 0 user_id
	, 0 s3_html_support
	, 0 customer_portal
	, 1 as visible
	, 1 version_no
	from candidate_document
	)
	
select cd.*
, case when cand_doc_primary=1 then 1
	else 0 end as primary_document
from cand_doc cd
join candidate_physical_doc cpd on cpd.file_id = cd.saved_filename --join to get mapping if physical exists


--CANDIDATE PHYSICAL DOCUMENTS CHANGE NAME (to CSV)
select candidate_document
, uid || right(candidate_document, position('.' in reverse(candidate_document))) as saved_filename
from candidate_document