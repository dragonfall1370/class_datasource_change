---#CF | From ProAgent | Checkbox | From Vincere
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value, insert_timestamp)
select distinct 'add_cand_info' as additional_type
, id as additional_id
, 1111 as form_id --From ProAgent
, 9999 as field_id
, 1 as field_value --'YES'
, current_timestamp as insert_timestamp
from candidate
where external_id is not NULL
and external_id like 'CDT%'
and deleted_timestamp is NULL

--#CF | Latest employer | Text Field
select cand_ext_id
, 'add_cand_info' as additional_type
, 1139 as form_id
, 9999 as field_id
, origin_employer as field_value --latest employer in order
, current_timestamp as insert_timestamp
from cand_work_history --temp table for work history
where rn = 1
and nullif(origin_employer, '') is not NULL --121832 rows