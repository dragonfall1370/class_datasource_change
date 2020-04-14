--UPDATE OTHER CONTACTS NAME
with contact_new_name as (select id, first_name, last_name, external_id
	, overlay(last_name placing '' from 1 for position('】' in last_name)) as new_last_name
	from contact
	where deleted_timestamp is NULL
	and external_id ilike 'REC%'
	and id not in (select contact_id from mike_tmp_contact_dup_check)
)

update contact c
set last_name = cnn.new_last_name
from contact_new_name cnn
where c.id = cnn.id
and c.external_id ilike 'REC%' --strict conditions
--updated 35786
