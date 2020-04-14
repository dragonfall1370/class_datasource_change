/* 
ALTER TABLE company
add column employees_number_bkup bigint

update company
set employees_number_bkup = employees_number
where employees_number > 0 --15222 rows
*/
with latest_company as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, c.employees_number
	from mike_tmp_company_dup_check m
	join company c on c.id = m.vc_pa_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) > coalesce(vc_latest_date, '1900-01-01')
	and employees_number is not NULL
) --1676 rows

, older_company as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, c.employees_number
	from mike_tmp_company_dup_check m
	join company c on c.id = m.vc_pa_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) < coalesce(vc_latest_date, '1900-01-01')
	and employees_number is not NULL
	and vc_company_id not in (select vc_company_id from latest_company)
) --1315 rows
--select * from older_company

, cf_group as (select *
	from latest_company
	UNION
	select *
	from older_company) --select * from cf_group

--MAIN SCRIPT
update company
set employees_number = cf.employees_number
from cf_group cf
where cf.vc_company_id = company.id --2991 rows
