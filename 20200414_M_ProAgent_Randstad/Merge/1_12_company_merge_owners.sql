with merged_new as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, c.company_owners
	from mike_tmp_company_dup_check m
	join company c on c.id = m.vc_pa_company_id
	where 1=1
	and company_owners is not NULL --4884 rows | select id, company_owners, company_owner_ids from company where id = 27620

	UNION ALL
	select id
	, m.vc_pa_company_id
	, m.rn
	, company_owners
	from company c
	join mike_tmp_company_dup_check m on c.id = m.vc_company_id
	and company_owners is not NULL
) 
, distinct_owners as (select distinct vc_company_id, company_owners from merged_new where company_owners is not NULL and company_owners <> '')

, owners_group as (select vc_company_id, string_agg(company_owners, ',') as owners_group 
	from distinct_owners
	group by vc_company_id)

--MAIN SCRIPT
update company
set company_owners = owners_group
from owners_group c
where c.vc_company_id = company.id
--and vc_company_id <> 33172

/* AUDIT
select id, company_owners, company_owner_ids from company where id = 33172
*/