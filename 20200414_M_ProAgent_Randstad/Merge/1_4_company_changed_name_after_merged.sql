--> Renaming NON_DUPLICATE companies
with company_new_name as (select id, name
	, overlay(name placing '' from 1 for position('】' in name)) as new_name --can use substring as well
	from company
	where external_id ilike 'CPY%'
	and deleted_timestamp is NULL --29625
	and id not in (select vc_pa_company_id from mike_tmp_company_dup_check) --24741 | 4884 dup
	)
	
update company c
set name = cnn.new_name
from company_new_name cnn
where c.id = cnn.id
and c.external_id ilike 'CPY%' --strict conditions


--> Renaming for DUPLICATE companies (from NON_DUPLICATE)
select id, name
	, external_id
	, concat_ws('', '【', external_id, '】', name) as new_name
from company
where external_id ilike 'CPY%'
and deleted_timestamp is NULL
and id in (select vc_pa_company_id from mike_tmp_company_dup_check2) --1400
and position('【' in name) <> 1 and position('】' in name) <> 11

--MAIN SCRIPT
update company
set name = concat_ws('', '【', external_id, '】', name)
where external_id ilike 'CPY%'
and deleted_timestamp is NULL
and id in (select vc_pa_company_id from mike_tmp_company_dup_check2) --1400
and position('【' in name) <> 1 and position('】' in name) <> 11