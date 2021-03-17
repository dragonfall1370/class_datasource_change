--#CF Language skill note | 11311 | Text Area
with merged_new as (select m.candidate_id
	, m.master
	, a.field_value
	, a2.field_value as master_field_value
	, a2.field_value || chr(10) || ('【Merged from candidate: ' || m.candidate_id || '】') || chr(10) || a.field_value as new_field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11311 field_id
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	join (select * from additional_form_values where form_id = 1139 and field_id = 11311) a on a.additional_id = m.candidate_id
	join (select * from additional_form_values where form_id = 1139 and field_id = 11311) a2 on a2.additional_id = m.master
	where 1=1
	and rn = 1
	and a.field_value is not NULL and a.field_value <> ''
	)
	
--IF USING OVERWRITE
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, master as additional_id
, 1139 form_id
, 11311 field_id
, new_field_value as field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
	
	
--#CF Latest employer | 11324 | Text Area | taking the latest company only
with latest_candidate as (select m.candidate_id
	, c1.insert_timestamp as slave_reg_date
	, ce1.last_activity_date as slave_last_activity_date
	, m.master
	, c2.insert_timestamp as master_reg_date
	, ce2.last_activity_date as master_last_activity_date
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate c1 on c1.id = m.candidate_id
	left join candidate_extension ce1 on ce1.candidate_id = m.candidate_id
	left join candidate_source cs1 on cs1.id = c1.candidate_source_id
	left join candidate c2 on c2.id = m.master
	left join candidate_extension ce2 on ce2.candidate_id = m.master
	left join candidate_source cs2 on cs2.id = c2.candidate_source_id
	where m.master is not NULL
	and rn = 1
	and coalesce(ce1.last_activity_date, c1.insert_timestamp) > coalesce(ce2.last_activity_date, c2.insert_timestamp) --slave more update than master
	order by m.candidate_id
)

, merged_new as (select m.candidate_id
	, m.master
	, a.field_value
	, a2.field_value as master_field_value
	, 'add_cand_info' additional_type
	, 1139 form_id
	, 11324 field_id
	from latest_candidate m
	join (select * from additional_form_values where form_id = 1139 and field_id = 11324) a on a.additional_id = m.candidate_id
	join (select * from additional_form_values where form_id = 1139 and field_id = 11324) a2 on a2.additional_id = m.master
	where 1=1
	and a.field_value is not NULL and a.field_value <> ''
	) --select * from merged_new
	
--IF USING OVERWRITE
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_cand_info' additional_type
, master as additional_id
, 1139 form_id
, 11324 field_id
, field_value
from merged_new m
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;