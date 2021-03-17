--#CF AG Human Resources | 11301 | Dropdown
with merged_new as (select m.candidate_id
	, m.master
	, a.field_value
	, a2.field_value as master_field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11301 field_id
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join (select * from additional_form_values where form_id = 1139 and field_id = 11301) a on a.additional_id = m.candidate_id
	left join (select * from additional_form_values where form_id = 1139 and field_id = 11301) a2 on a2.additional_id = m.master
	where 1=1
	and rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	)
	
--IF USING OVERWRITE
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, master as additional_id
, 1139 form_id
, 11301 field_id
, field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
	
	
--#CF Cognitive path | 11302 | Dropdown
with merged_new as (select m.candidate_id
	, m.master
	, a.field_value
	, a2.field_value as master_field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11302 field_id
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	join (select * from additional_form_values where form_id = 1139 and field_id = 11302) a on a.additional_id = m.candidate_id
	join (select * from additional_form_values where form_id = 1139 and field_id = 11302) a2 on a2.additional_id = m.master
	where 1=1
	and rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	and a.field_value::int > 0 --if slave candidate selected a value
	)
	
--IF USING OVERWRITE
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, master as additional_id
, 1139 form_id
, 11302 field_id
, field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;


/* NO NEED
--[Duplicate] in Cognitive pathway > ID: 66 --Update slave candidate to [Duplicate]
select *
from additional_form_values
where field_id = 11302
and additional_id in (select candidate_id from mike_tmp_candidate_dup_name_mail_dob_master_slave)


insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, candidate_id as additional_id
, 1139 form_id
, 11302 field_id
, '66' field_value
from mike_tmp_candidate_dup_name_mail_dob_master_slave
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
*/	
	
--#CF PANO | 11303 | Free Text | No migration because master already confirmed

--#CF Website ID | 1284 | Free Text | as Registration route | no migration because of moving to Brief

--#CF Gender | 11304 | Drop down | no changes

--#CF Office in charge | 11312 | Dropdown
with merged_new as (select m.candidate_id
	, m.master
	, a.field_value
	, a2.field_value as master_field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11312 field_id
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	join (select * from additional_form_values where form_id = 1139 and field_id = 11312) a on a.additional_id = m.candidate_id
	join (select * from additional_form_values where form_id = 1139 and field_id = 11312) a2 on a2.additional_id = m.master
	where 1=1
	and rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	)
	
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, master as additional_id
, 1139 form_id
, 11312 field_id
, field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;

	
	
--#CF Desired Work Location | 11265 | Multiple selection
with merged_new as (select m.candidate_id
	, m.master
	, a.field_value
	, a2.field_value as master_field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11265 field_id
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	join (select * from additional_form_values where form_id = 1139 and field_id = 11265) a on a.additional_id = m.candidate_id
	join (select * from additional_form_values where form_id = 1139 and field_id = 11265) a2 on a2.additional_id = m.master
	where 1=1
	and rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	) -- rows

, distinct_field_value as (select distinct master
	, s.distinct_field_value
	from merged_new, UNNEST(string_to_array(field_value, ',')) s (distinct_field_value)
	) --select * from distinct_field_value

, field_value_group as (select master
	, string_agg(distinct_field_value, ',') as field_value_group
	from distinct_field_value
	group by master) --select * from field_value_group -- rows

--APPEND
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, master as additional_id
, 1139 form_id
, 11265 field_id
, field_value_group as field_value
from field_value_group
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;