--AUDIT MERGED CONTACTS IF ANY CF
select *
from additional_form_values
where 1=1
and form_id = 1140
and field_id = 11276
and additional_id in (select merged_contact_id from mike_tmp_contact_dup_check) --0 records

---New contacts from PA
with merged_new as (select *
	from mike_tmp_contact_dup_check
	where rn = 1
	and coalesce(pa_update_date, insert_timestamp) > coalesce(last_activity_date, vc_insert_timestamp)
	)

select *
from additional_form_values
where 1=1
and form_id = 1140
and field_id = 11276
and additional_id in (select contact_id from merged_new)


---Old contacts from PA
with merged_old as (select *
	from mike_tmp_contact_dup_check
	where rn = 1
	and coalesce(pa_update_date, insert_timestamp) < coalesce(last_activity_date, vc_insert_timestamp)
	)

select *
from additional_form_values
where 1=1
and form_id = 1140
and field_id = 11276
and additional_id in (select contact_id from merged_old)


-->>MAIN SCRIPT
---New contacts from PA
select *
, 'add_con_info' additional_type
, 1140 form_id
, 11276 field_id
from mike_tmp_contact_dup_check
where rn = 1
and coalesce(pa_update_date, insert_timestamp) > coalesce(last_activity_date, vc_insert_timestamp, '1900-01-01') --139


---Old contacts from PA
select *
, 'add_con_info' additional_type
, 1140 form_id
, 11276 field_id
from mike_tmp_contact_dup_check
where rn = 1
and coalesce(pa_update_date, insert_timestamp) <= coalesce(last_activity_date, vc_insert_timestamp, '1900-01-01') --1586


/* AUDIT CHECK | NOK
select id
from contact
where id in (select merged_contact_id from mike_tmp_contact_dup_check)
and deleted_timestamp is NULL --1721

select distinct merged_contact_id 
from mike_tmp_contact_dup_check --1721
where rn = 1 --1717
*/

--#CF PANO | 11276 | Free Text
---select * from mike_tmp_contact_dup_check2
with merged_contact2 as (select merged_contact_id
	, contact_id
	, rn
	, row_number() over(partition by merged_contact_id order by coalesce(last_activity_date, vc_insert_timestamp) desc, contact_id desc) as contact_rn
	from mike_tmp_contact_dup_check2
	where rn = 1
	and contact_id not in (select contact_id from mike_tmp_contact_dup_check)
	and company_id = new_company_id)
	
, merged_new as (select m.merged_contact_id
	, m.contact_id
	, m.rn
	, a.field_value
	, 'add_con_info' additional_type
	, 1140 form_id
	, 11276 field_id
	from merged_contact2 m
	join (select * from additional_form_values where form_id = 1140 and field_id = 11276) a on a.additional_id = m.contact_id
	where 1=1
	and contact_rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	and contact_id not in (select contact_id from mike_tmp_contact_dup_check)
	) --577 rows

/* AUDIT DUE TO MUTLIPLE PA CONTACTS MAY LINK TO VC CONTACTS
select * from merged_new where merged_contact_id = 22713

select * from mike_tmp_contact_dup_check2 where merged_contact_id = 22713

select merged_contact_id, count(*)
from merged_new
group by merged_contact_id having count(*) > 1
*/
	
--IF USING OVERWRITE
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_con_info' additional_type
, merged_contact_id as additional_id
, 1140 form_id
, 11276 field_id
, field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
	
--upsert 577 rows
