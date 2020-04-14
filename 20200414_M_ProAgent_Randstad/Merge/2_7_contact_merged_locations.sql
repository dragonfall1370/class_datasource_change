--VC TEMP COLUMN FOR CURRENT_LOCATION_ID
ALTER TABLE contact
add column current_location_id_bkup bigint

update contact
set current_location_id_bkup = current_location_id
where current_location_id > 0
and deleted_timestamp is NULL --28189 rows

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