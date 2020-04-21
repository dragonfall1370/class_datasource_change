--List of Origin
select *
from companyorigin
order by "value"

--#CF Origin | 11265 | Drop down
select c.idcompany as com_ext_id
, c.idcompanyorigin
, trim(co.value) as field_value
, 'add_com_info' as additional_type
, 1007 as form_id
, 11265 as field_id
, current_timestamp as insert_timestamp
from company c
join companyorigin co on co.idcompanyorigin = c.idcompanyorigin
where c.idcompanyorigin is not NULL --23866