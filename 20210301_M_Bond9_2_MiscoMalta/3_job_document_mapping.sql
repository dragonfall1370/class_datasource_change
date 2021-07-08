--GENERATING UID
select f.uniqueid as job_ext_id
	, uuid_in(overlay(overlay(md5(random()::text || ':' || clock_timestamp()::text) placing '4' from 13) placing to_hex(floor(random()*(11-8+1) + 8)::int)::text from 17)::cstring) as UID
	, right("relative document path", position(E'\\' in reverse("relative document path")) - 1) as job_document --no need to change to rtf anymore
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
--into job_document --#TEMP TABLE FOR RENAMING FILES | DOCUMENT MAPPING
from f03docs3_edited f 
where lower(right("relative document path", position('.' in reverse("relative document path"))))
	IN ('.pdf','.doc','.docx','.xls','.xlsx','.rtf','.htm','.html','.txt','.png','.jpg','.jpeg','.gif','.bmp','.msg')
	
	
--JOB DOCUMENT MAPPING (to VINCERE)
select job_ext_id
, job_document as uploaded_filename
, uid || right(job_document, position('.' in reverse(job_document))) as saved_filename
, mime_type
, created
, now() insert_timestamp
, now() trigger_index_update_timestamp
, 'legacy_job_document' document_type
, 20 document_types_id
, 'JOB' entity_type
, -10 user_account_id
, cast(0.0 as Float) successful_parsing_percent
, note
, 0 primary_document
, 0 legal_doc_id
, -1 google_viewer
, 1 "temporary"
, 0 user_id
, 0 s3_html_support
, 0 customer_portal
, 1 as visible
, 1 version_no
from job_document


--JOB PHYSICAL DOCUMENTS CHANGE NAME (to CSV)
select job_document
, uid || right(job_document, position('.' in reverse(job_document))) as saved_filename
from job_document