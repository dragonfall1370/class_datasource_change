--GENERATING UID
select f.uniqueid as con_ext_id
	, uuid_in(overlay(overlay(md5(random()::text || ':' || clock_timestamp()::text) placing '4' from 13) placing to_hex(floor(random()*(11-8+1) + 8)::int)::text from 17)::cstring) as UID
	, right("relative document path", position(E'\\' in reverse("relative document path")) - 1) as contact_document --no need to change to rtf anymore
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
--into contact_document --#TEMP TABLE FOR RENAMING FILES | DOCUMENT MAPPING
from f01docs1_edited f 
join (select uniqueid from f01 where "100 contact codegroup  23" = 'Y') c on c.uniqueid = f.uniqueid
where lower(right("relative document path", position('.' in reverse("relative document path"))))
	IN ('.pdf','.doc','.docx','.xls','.xlsx','.rtf','.htm','.html','.txt','.png','.jpg','.jpeg','.gif','.bmp','.msg')
	
	
--CONTACT DOCUMENT MAPPING (to VINCERE)
with contact_doc as (select con_ext_id
	, contact_document as uploaded_filename
	, uid || right(contact_document, position('.' in reverse(contact_document))) as saved_filename
	, mime_type
	, created
	, now() insert_timestamp
	, now() trigger_index_update_timestamp
	, 'legacy_contact_document' document_type
	, 19 document_types_id
	, 'CONTACT' entity_type
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
	from contact_document
	)
	
select cd.*
from contact_doc cd
join contact_physical_doc cpd on cpd.file_id = cd.saved_filename --join to get mapping if physical exists 


--CONTACT PHYSICAL DOCUMENTS CHANGE NAME (to CSV)
select contact_document
, uid || right(contact_document, position('.' in reverse(contact_document))) as saved_filename
from contact_document


--CHECK CONTACT DOCUMENT NOT FOUND IN PROD
create table mike_temp_contact_doc 
(file_id character varying (100))


--DELETE IF NOT EXISTING
select *
from candidate_document
where document_type = 'legacy_contact_document'
and saved_filename not in (select file_id from mike_temp_contact_doc)


select *
from candidate_document
where contact_id = 35545

delete from candidate_document
where document_type = 'legacy_contact_document'
and saved_filename not in (select file_id from mike_temp_contact_doc)

