with contact_new_email as (select id, first_name, email, external_id
	, overlay(email placing '' from 1 for length(external_id) + 1) as new_email
	from contact
	where deleted_timestamp is NULL
	and external_id ilike 'REC%'
	and id not in (select contact_id from mike_tmp_contact_dup_check)
	and email is not NULL
)

update contact c
set email = cnn.new_email
from contact_new_email cnn
where c.id = cnn.id
and c.external_id ilike 'REC%' --strict conditions
--updated 21481