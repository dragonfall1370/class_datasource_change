--VC TEMP COLUMN FOR CURRENT_LOCATION_ID
ALTER TABLE contact
add column current_location_id_bkup bigint

update contact
set current_location_id_bkup = current_location_id
where current_location_id > 0
and deleted_timestamp is NULL --28189 rows


---APPLICABLE FOR PROD CONTACT MERGE
with merged_new as (select m.merged_contact_id --VC contacts
	, c2.current_location_id vc_location --VC contacts address
	, m.contact_id --PA contacts
	, c.current_location_id vc_pa_location --PA contacts address
	from mike_tmp_contact_dup_check m
	left join contact c on c.id = m.contact_id
	left join contact c2 on c2.id = m.merged_contact_id
	where m.rn = 1 --2472
	and coalesce(m.pa_update_date, m.insert_timestamp) > coalesce(m.last_activity_date, m.vc_insert_timestamp) --conditions for latest contacts
	) 
	
--UPDATE CURRENT CONTACT LOCATION
update contact c
set current_location_id = m.vc_pa_location --switch to PA contacts address
from merged_new m
where m.merged_contact_id = c.id
and m.vc_pa_location > 0 --valid address


--MAIN SCRIPT
with merged_new as (select *
	from mike_tmp_contact_dup_check
	where rn = 1
	and coalesce(pa_update_date, insert_timestamp) > coalesce(last_activity_date, vc_insert_timestamp)
	)

select *
from common_location
where contact_id > 0
and contact_id in (select contact_id from merged_new)

--Update from spoon process


---ADDITIONAL CONTACTS (dup check 2)
with merged_new as (select *
	from mike_tmp_contact_dup_check2
	where rn = 1
	and coalesce(pa_update_date, insert_timestamp) > coalesce(last_activity_date, vc_insert_timestamp)
	and contact_id not in (select contact_id from mike_tmp_contact_dup_check)
	)

select *
from common_location
where contact_id > 0
and contact_id in (select contact_id from merged_new)