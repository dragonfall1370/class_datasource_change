
---AUDIT DIFFERENT COMPANY OR CONTACT
select id, contact_id, contact_id_bkup, company_id, company_id_bkup
from position_description
where external_id ilike 'JOB%'
and (contact_id <> contact_id_bkup or company_id <> company_id_bkup)


--AUDIT CONTACT AND COMPANY
with merged_new as (
	select pd.id, pd.contact_id, pd.contact_id_bkup, pd.company_id, pd.company_id_bkup
	, c.id as contact_table_id
	, c.company_id contact_table_company_id
	, c.company_id_bkup contact_table_company_id_bkup
	, c.external_id
	from position_description pd
	left join contact c on c.id = pd.contact_id
	where pd.external_id ilike 'JOB%' --145727
	and pd.company_id != c.company_id
)

update position_description pd
set company_id = m.contact_table_company_id
from merged_new m
where pd.id = m.id
and pd.external_id ilike 'JOB%'