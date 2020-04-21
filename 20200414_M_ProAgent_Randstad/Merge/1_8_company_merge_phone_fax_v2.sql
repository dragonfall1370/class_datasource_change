--MERGED PHONE
with merged_phone_fax as (select m.vc_pa_company_id
	, nullif(c.phone, '') as phone
	, nullif(c.fax, '') as fax
	, m.vc_company_id
	, nullif(c2.phone, '') as merged_phone
	, nullif(c2.fax, '') as merged_fax
	, concat_ws(',', nullif(c2.phone, ''), nullif(c.phone, '')) as new_phone
	, concat_ws(',', nullif(c2.fax, ''), nullif(c.fax, '')) as new_fax
	from mike_tmp_company_dup_check2 m
	join company c on c.id = m.vc_pa_company_id
	join company c2 on c2.id = m.vc_company_id
	where 1 = 1
	and m.vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)
	)
	
, merged_phone_group as (select vc_company_id
	, string_agg(phone, ',') as phone_group
	, string_agg(fax, ',') as fax_group
	from merged_phone_fax
	where phone is not NULL or fax is not NULL
	group by vc_company_id)

update company c
set phone = concat_ws(',', nullif(c.phone, ''), nullif(m.phone_group, ''))
from merged_phone_group m
where m.vc_company_id = c.id
and m.phone_group is not NULL


--MERGED FAX
with merged_phone_fax as (select m.vc_pa_company_id
	, nullif(c.phone, '') as phone
	, nullif(c.fax, '') as fax
	, m.vc_company_id
	, nullif(c2.phone, '') as merged_phone
	, nullif(c2.fax, '') as merged_fax
	, concat_ws(',', nullif(c2.phone, ''), nullif(c.phone, '')) as new_phone
	, concat_ws(',', nullif(c2.fax, ''), nullif(c.fax, '')) as new_fax
	from mike_tmp_company_dup_check2 m
	join company c on c.id = m.vc_pa_company_id
	join company c2 on c2.id = m.vc_company_id
	where 1 = 1
	and m.vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)
	)
	
, merged_phone_group as (select vc_company_id
	, string_agg(phone, ',') as phone_group
	, string_agg(fax, ',') as fax_group
	from merged_phone_fax
	where phone is not NULL or fax is not NULL
	group by vc_company_id)

update company c
set fax = concat_ws(',', nullif(c.fax, ''), nullif(m.fax_group, ''))
from merged_phone_group m
where m.vc_company_id = c.id
and m.fax_group is not NULL