with merged_new as (select *
	from mike_tmp_contact_dup_check
	where rn = 1
	and coalesce(pa_update_date, insert_timestamp) > coalesce(last_activity_date, vc_insert_timestamp)
	)

update contact c
set email = m.pa_contact_email
from merged_new m
where m.merged_contact_id = c.id
and m.pa_contact_email is not NULL
and m.pa_contact_email <> c.email


---ADDITIONAL CONTACTS (dup check 2)
with merged_new as (select *
	from mike_tmp_contact_dup_check2
	where rn = 1
	and coalesce(pa_update_date, insert_timestamp) > coalesce(last_activity_date, vc_insert_timestamp)
	and contact_id not in (select contact_id from mike_tmp_contact_dup_check)
	and pa_contact_email ilike '%_@_%.__%'
	) --25 rows

update contact c
set email = m.pa_contact_email
from merged_new m
where m.merged_contact_id = c.id
and m.pa_contact_email is not NULL
and m.pa_contact_email <> c.email
and m.pa_contact_email ilike '%_@_%.__%'
--18 rows