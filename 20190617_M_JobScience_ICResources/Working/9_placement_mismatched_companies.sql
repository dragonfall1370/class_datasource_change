select j.id
, p."id" as placement_id
, j.name as job_title
, a.name as company_name
, a.id as company_id
, c.firstname as contact_firstname
, c.lastname as contact_lastname
, c.accountid as contact_company_id
, a2.name as contact_company_name
from ts2_placement_c p
left join ts2_job_c j on p.ts2_job_c = j.id
left join account a on j.ts2_account_c = a.id
left join contact c on j.ts2_contact_c = c.id
left join account a2 on a2.id = c.accountid
where 1=1
--and ts2_job_number_c in ('JO-1710-90338', 'JO-1710-112904', 'JO-1710-109601')
and j.ts2_account_c <> c.accountid