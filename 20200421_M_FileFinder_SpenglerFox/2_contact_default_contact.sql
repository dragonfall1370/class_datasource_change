select distinct idcompany as company_ext_id
		, 'CON' || idcompany as default_con_id
		, 'Default contact' as last_name
		, 'This is default contact for each company' as note
		from "assignment"
		where idcompany not in (select distinct idcompany from company_person where idcompany is not NULL)

	
--UDPATE DEFAULT CONTACT TIMESTAMP
select id, first_name, last_name, insert_timestamp, deleted_timestamp, external_id
from contact
where last_name ilike '%default%'
and deleted_timestamp is NULL --182

update contact
set insert_timestamp = insert_timestamp - interval '5 years'
where last_name ilike '%default%'
and deleted_timestamp is NULL --182