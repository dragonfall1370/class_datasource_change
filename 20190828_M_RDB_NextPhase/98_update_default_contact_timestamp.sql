--Update insert_timestamp for default contact
select *
from contact
where external_id ilike 'NP_DEF%'

update contact
set insert_timestamp = '2001-08-21 07:09:09.03'
where external_id ilike 'NP_DEF%' --608 rows