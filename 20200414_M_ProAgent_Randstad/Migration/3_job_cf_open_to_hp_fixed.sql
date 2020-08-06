--JOB TO OPEN | OPEN TO HP | 9446
select id, external_id
from position_description
where id in (select a.additional_id
from additional_form_values a
left join (select * from mike_bkup_additional_form_values_20200706 where field_id = 1048) m on a.additional_id = m.additional_id
where a.field_id = 1048
and m.additional_id is NULL)

--REFERENCE
select *
from additional_form_values
where field_id = 1048
--and field_value = '1'
and insert_timestamp between '2020-07-04' and '2020-07-05'

select additional_id, field_value
from additional_form_values
where field_id = 1048
and field_value = '1'
and insert_timestamp > '2020-07-06 11:00:00'

select additional_id, field_value
from mike_bkup_additional_form_values_20200706
where field_id = 1048
and field_value = '1'

select *
from additional_form_values a
left join mike_bkup_additional_form_values_20200706


select a.additional_id
, a.field_value
, m.additional_id
, m.field_value
from additional_form_values a
left join (select * from mike_bkup_additional_form_values_20200706 where field_id = 1048) m on a.additional_id = m.additional_id
where a.field_id = 1048
and m.additional_id is NULL