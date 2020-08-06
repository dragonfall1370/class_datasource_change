--Applicable only on PROD
select id
, first_name
, last_name
, company_id
, first_name_kana
, last_name_kana
, company_id_bkup
, external_id
from contact
where id in (select contact_id from mike_tmp_contact_dup_check where rn=1) --2472

--MAIN SCRIPT | Apply '【Duplicate】' for duplicate contact first_name
update contact
set first_name = '【Duplicate】'
where id in (select contact_id from mike_tmp_contact_dup_check where rn=1)