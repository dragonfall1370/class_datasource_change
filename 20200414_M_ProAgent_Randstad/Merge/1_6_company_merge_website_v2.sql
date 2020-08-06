--BACKUP current company info
ALTER TABLE company
add column website_bkup character varying (1000)

update company
set website_bkup = website --56596 rows

--MAIN SCRIPT
with com_website as (select m.vc_pa_company_id
	, m.com_ext_id
	, c.website as pa_website
	, m.vc_company_id
	, c2.website
	from mike_tmp_company_dup_check2 m
	left join company c on  c.id = m.vc_pa_company_id
	left join company c2 on c2.id = m.vc_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) > coalesce(vc_latest_date, vc_reg_date, '1900-01-01')
	and nullif(c.website, '') is not NULL
	) --2448 rows
	
update company c
set website = cw.pa_website
from com_website cw
where cw.vc_company_id = c.id
and nullif(cw.pa_website, '') is not NULL --changed the conditions not overwritten if NULL