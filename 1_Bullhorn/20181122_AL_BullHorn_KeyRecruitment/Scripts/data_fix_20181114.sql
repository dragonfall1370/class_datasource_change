select * from candidate
where email like
--'%Clydonw@gmail.com%'
--'%imtiyaz20504@gmail.com%'
--'%feziwemooi@yahoo.com%'
--'%dharma.rsa@gmail.com%'
--'%vusileeuw@gmail.com%'
'%kagisoafrica@gmail.com%'

update candidate
set email = 'tommie.potgieter@supergrp.com'
where id = 71582

update candidate
set email = 'feziwemooi@yahoo.com'
where id = 74835



select * from candidate
where external_id =
--'8781'
'23216'

select * from position_description
where external_id = '44'
-- 1 => 33087
-- 44 => 32130

select * from candidate
where external_id = '5382'
--68189

select * from position_candidate
where position_description_id = 32130
and candidate_id = 68189

select * from candidate_source

delete from candidate_source

select * from bulk_upload_document_mapping
where document_id is not null

select count(*) from company
-- 709

select count(*) from contact
-- 1384

select count(*) from position_description
-- 1121

select count(*) from candidate
-- 104646

select * from candidate_document

select * from contact_document

select * from position_description_document

select * from position_candidate_document

select * from document_types

select * from document_history

select id, external_id from candidate where id  = 61266

select * from user_account
where first_name like '%Danielle%'

select * from sub_functional_expertise