with placementfilter as (select j.id as job_id
	, j.name
	, con.id as candidate_id
	, con.firstname as candidate_fname
	, con.lastname as candidate_lname
	, a.id as company_id
	, a.name as companyname
	, j.ts2_contact_c as contact
	, con2.firstname as contact_fname
	, con2.lastname as contact_lname
	, con2.accountid as contact_company_id
	, a2.name as contact_company_name
	, p.ts2_hiring_manager_c as hiring_manager
	, con3.firstname as hiring_manager_fname
	, con3.lastname as hiring_manager_lname
	, a3.name as hiring_manager_company
	, row_number() over(partition by j.id, con.id order by p.createddate desc) as rn
	, p.id as placement_id
	from ts2_placement_c p
	left join ts2_job_c j on p.ts2_job_c = j.id
	left join contact con on p.ts2_employee_c = con.id --candidate
	left join contact con2 on con2.id = j.ts2_contact_c --contact
	left join contact con3 on con3.id = p.ts2_hiring_manager_c --hiring manager
	left join account a on a.id = p.ts2_client_c --company of job
	left join account a2 on a2.id = con2.accountid --company of contact
	left join account a3 on a3.id = con3.accountid --company of hiring manager
	where 1=1
	--and p.ts2_client_c = '0010Y00000lKZBEQA4' --sample case
	and ts2_employee_c is not NULL
	and ts2_job_c is not NULL
	)

select firstname
, lastname
, name as jobtitle
, companyname
, contact_fname
, contact_lname
, contact_company_name
, placement_id
from placementfilter
where rn = 1