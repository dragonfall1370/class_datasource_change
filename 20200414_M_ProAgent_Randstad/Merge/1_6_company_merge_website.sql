--BACKUP current company info
ALTER TABLE company
add column website_bkup character varying (1000)

update company
set website_bkup = website --56596 rows

--MAIN SCRIPT
---only overwrite with the latest info
select m.vc_pa_company_id
, m.com_ext_id
, c.website as pa_website
, m.vc_company_id
, c2.website
from mike_tmp_company_dup_check m
left join company c on  c.id = m.vc_pa_company_id
left join company c2 on c2.id = m.vc_company_id
where 1=1
and rn = 1
and coalesce(vc_pa_update_date, vc_pa_reg_date) > coalesce(vc_latest_date, '1900-01-01') --2310 | already check company latest date
--and coalesce(vc_pa_update_date, vc_pa_reg_date) <= coalesce(vc_latest_date, '1900-01-01') --2574