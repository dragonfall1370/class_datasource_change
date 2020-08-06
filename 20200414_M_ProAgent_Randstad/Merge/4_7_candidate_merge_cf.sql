--#CF AG Human Resources | 11301 | Dropdown
with merged_new as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.rn
	, a.field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11301 field_id
	from mike_tmp_candidate_dup_check m
	join (select * from additional_form_values where form_id = 1139 and field_id = 11301) a on a.additional_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	)
	
--IF USING OVERWRITE
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, vc_candidate_id as additional_id
, 1139 form_id
, 11301 field_id
, field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
	
--#CF Cognitive path | 11302 | Dropdown
with merged_new as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.rn
	, a.field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11302 field_id
	from mike_tmp_candidate_dup_check m
	join (select * from additional_form_values where form_id = 1139 and field_id = 11302) a on a.additional_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	)
	
--IF USING OVERWRITE
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, vc_candidate_id as additional_id
, 1139 form_id
, 11302 field_id
, field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;

--[Duplicate] in Cognitive pathway > ID: 66
select *
from additional_form_values
where field_id = 11302
and additional_id in (select vc_pa_candidate_id from mike_tmp_candidate_dup_check)


insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, vc_pa_candidate_id as additional_id
, 1139 form_id
, 11302 field_id
, '66' field_value
from mike_tmp_candidate_dup_check
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;


--#CF PANO | 11303 | Free Text
with merged_new as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.rn
	, a.field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11303 field_id
	from mike_tmp_candidate_dup_check m
	join (select * from additional_form_values where form_id = 1139 and field_id = 11303) a on a.additional_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	)
	
--IF USING OVERWRITE
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, vc_candidate_id as additional_id
, 1139 form_id
, 11303 field_id
, field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
	
--#CF Website ID | 1284 | Free Text | as Registration route
with merged_new as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.rn
	, c.candidate_source_id
	, cs.name as field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 1284 field_id
	from mike_tmp_candidate_dup_check m
	join candidate c on c.id = m.vc_pa_candidate_id
	join candidate_source cs on cs.id = c.candidate_source_id
	where 1=1
	and rn = 1
	and cs.id is not NULL
	)
	
--IF USING OVERWRITE
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, vc_candidate_id as additional_id
, 1139 form_id
, 1284 field_id
, field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;


--#CF Gender | 11304 | Drop down
with merged_new as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.rn
	, a.field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11304 field_id
	from mike_tmp_candidate_dup_check m
	join (select * from additional_form_values where form_id = 1139 and field_id = 11304) a on a.additional_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	)
	
--IF USING OVERWRITE
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, vc_candidate_id as additional_id
, 1139 form_id
, 11304 field_id
, field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
	
	
--#CF Office in charge | 11312 | Dropdown
with merged_new as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.rn
	, a.field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11312 field_id
	from mike_tmp_candidate_dup_check m
	join (select * from additional_form_values where form_id = 1139 and field_id = 11312) a on a.additional_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	)
	
--IF USING OVERWRITE
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, vc_candidate_id as additional_id
, 1139 form_id
, 11312 field_id
, field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
	
	
--#CF Desired Work Location | 11265 | Multiple selection
with merged_new as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.rn
	, a.field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11265 field_id
	from mike_tmp_candidate_dup_check m
	join (select * from additional_form_values where form_id = 1139 and field_id = 11265) a on a.additional_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	) --9350 rows

, distinct_field_value as (select distinct vc_candidate_id
	, s.distinct_field_value
	from merged_new, UNNEST(string_to_array(field_value, ',')) s (distinct_field_value)
	)

, field_value_group as (select vc_candidate_id
	, string_agg(distinct_field_value, ',') as field_value_group
	from distinct_field_value
	group by vc_candidate_id) --select * from field_value_group --9350 rows

--APPEND
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, vc_candidate_id as additional_id
, 1139 form_id
, 11265 field_id
, field_value_group as field_value
from field_value_group
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
	
	
--#CF Language skill note | 11311 | Text Area
with merged_new as (select m.vc_candidate_id
	, m.vc_pa_candidate_id
	, m.rn
	, a.field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11311 field_id
	from mike_tmp_candidate_dup_check m
	join (select * from additional_form_values where form_id = 1139 and field_id = 11311) a on a.additional_id = m.vc_pa_candidate_id
	where 1=1
	and rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	)
	
--IF USING OVERWRITE
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, vc_candidate_id as additional_id
, 1139 form_id
, 11311 field_id
, field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
	