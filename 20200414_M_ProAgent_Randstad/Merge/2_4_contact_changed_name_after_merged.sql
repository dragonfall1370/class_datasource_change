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


--REFERENCE
select c.company_id
		, company_id_bkup
		, c.id as contact_id
		, lower(overlay(replace(replace(c.last_name, N'　', ''), ' ', '') placing '' from 1 for length(c.external_id) + 2)) as pa_contact_name
		, concat(lower(replace(replace(first_name_kana, N'　', ''), ' ', '')), lower(replace(replace(last_name_kana, N'　', ''), ' ', ''))) pa_contact_name_kana
		, c.email as vc_pa_origin_email
		, case when c.email like '%_@_%.__%' then lower(overlay(trim(c.email) placing '' from 1 for length(external_id) + 1))
			else NULL end as pa_contact_email
		, c.insert_timestamp
		, m.update_date as pa_update_date
from contact c
left join mike_tmp_pa_contact_merged m on m.contact_id = c.id --linked check pa information
where c.company_id <> c.company_id_bkup
and contact_id = 62698
order by c.company_id, c.id