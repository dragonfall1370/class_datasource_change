--BACKUP LOCATION ID BEFORE MERGE
ALTER TABLE company_location
add column location_ext_id character varying(100)

update company_location
set location_ext_id = 'VC' || id::text

--Check script
select m.vc_pa_company_id
, m.com_ext_id
, m.vc_company_id --merged company
, cl.id as location_id
, 'PA-VC' || cl.id::text as location_ext_id
, cl.state
, cl.city
, cl.district
, cl.post_code
, cl.address
, cl.location_name
, '【Merged from PA: ' || m.com_ext_id || '】' as note
from mike_tmp_company_dup_check2 m
join company_location cl on cl.company_id = m.vc_pa_company_id
--where m.vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check) --using if missing in 1st dup check


--MAIN SCRIPT
insert into company_location(company_id, state, city, district, post_code, address, location_name, note, location_ext_id)
select m.vc_company_id company_id
, cl.state
, cl.city
, cl.district
, cl.post_code
, cl.address
, cl.location_name
, '【Merged from PA: ' || m.com_ext_id || '】' as note
, 'PA-VC' || cl.id::text as location_ext_id
from mike_tmp_company_dup_check2 m
join company_location cl on cl.company_id = m.vc_pa_company_id
--where m.vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check) --1569 rows --using if missing in 1st dup check



--->> PROD LOCATION MERGED
insert into company_location(company_id, state, city, district, post_code, address, location_name, note, location_ext_id)
select m.vc_company_id company_id
, cl.state
, cl.city
, cl.district
, cl.post_code
, cl.address
, cl.location_name
, '【Merged from PA: ' || m.com_ext_id || '】' as note
, 'PA-VC' || cl.id::text as location_ext_id
from mike_tmp_company_dup_check m
join company_location cl on cl.company_id = m.vc_pa_company_id
where m.vc_pa_company_id in (select vc_pa_company_id from mike_tmp_company_dup_check) --1569 rows
