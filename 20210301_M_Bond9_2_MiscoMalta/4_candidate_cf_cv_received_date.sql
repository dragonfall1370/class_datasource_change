--CF | CV Received Date | Date format
select uniqueid as cand_ext_id
, 'add_cand_info' as additional_type
, 1111 as form_id
, 9999 as field_id
, to_date("104 cv receive date", 'DD/MM/YY') as field_date_value
, current_timestamp as insert_timestamp
from f01
where 1=1
and "101 candidate codegroup  23" = 'Y'
and "104 cv receive date" is not NULL