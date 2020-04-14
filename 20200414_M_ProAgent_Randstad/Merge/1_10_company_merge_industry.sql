--COMPANY INDUSTRY
with merged_industry as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.com_ext_id
	, m.rn
	, ci.industry_id
from mike_tmp_company_dup_check m
join company_industry ci on ci.company_id = m.vc_pa_company_id --6126
)

insert into company_industry (industry_id, company_id, insert_timestamp)
select industry_id
, vc_company_id as company_id
, current_timestamp insert_timestamp
from merged_industry
on conflict on constraint company_industry__pkey
	do nothing;