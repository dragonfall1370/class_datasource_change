select c.id
, c.first_name
, c.last_name
, c.email
, c.phone
, translate(field_value, '1', 'YES') as "Survey Completed"
from contact c
join (select * from additional_form_values where field_id = 11344 and field_value = '1') cf on cf.additional_id = c.id --field_value=1 'YES'
where c.deleted_timestamp is NULL
--and c.id = 28566
order by c.id