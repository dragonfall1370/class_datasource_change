select *
from additional_form_values
where field_id = 1048
and additional_id in (select id from position_description where external_id ilike 'JOB%' and deleted_timestamp is NULL)


select *
from additional_form_values
where field_id = 1048
and insert_timestamp between '2020-07-04' and '2020-07-06'
and additional_id in (select id from position_description where external_id ilike 'JOB%' and deleted_timestamp is NULL) --134649

select *
from additional_form_values
where field_id = 1048
and insert_timestamp between '2020-07-06' and '2020-07-07' 
and additional_id in (select id from position_description where external_id ilike 'JOB%' and deleted_timestamp is NULL) --9462

--
select *
from additional_form_values
where field_id = 1048
and field_value = '1' --open
and additional_id in (select id from position_description where external_id ilike 'JOB%' and deleted_timestamp is NULL)


select *
from additional_form_values
where field_id = 1069
and insert_timestamp between '2020-07-04' and '2020-07-06'
and additional_id in (select id from position_description where external_id ilike 'JOB%' and deleted_timestamp is NULL)
