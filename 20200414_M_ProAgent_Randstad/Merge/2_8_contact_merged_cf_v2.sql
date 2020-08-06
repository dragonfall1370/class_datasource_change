--#CF PANO | 11276 | Free Text
with merged_new as (select m.merged_contact_id --unique contact ID
	, m.contact_id --pa contact with VCID
	, m.rn
	, a.field_value
	, 'add_con_info' additional_type
	, 1140 form_id
	, 11276 field_id
	, row_number() over(partition by merged_contact_id order by contact_id desc) as contact_rn --add this condition to avoid multiple merged from PA to VC contacts
	from mike_tmp_contact_dup_check m
	join (select * from additional_form_values where form_id = 1140 and field_id = 11276) a on a.additional_id = m.contact_id
	where 1=1
	and m.rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	) --select * from merged_new group by merged_contact_id having count(*) > 1

/* AUDIT DUP CONTACTS
select distinct merged_contact_id
from merged_new 
--where merged_contact_id not in (select merged_contact_id from merged_new)
where 1=1
and contact_rn = 1
--and merged_contact_id=16730
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
where contact_rn = 1