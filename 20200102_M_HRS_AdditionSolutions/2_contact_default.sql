--JOB links with same contacts but new companies
with dif_job as (select j.[__pk] as job_id
		, j.[_fk_company] as company_id
		, j.[_fk_contact] as contact_id
		, c._fk_company as contact_companyid
		from [20191030_155620_jobs] j
		left join (select * from [20191030_153350_contacts] where type = 'Contact') c on c.__pk = j.[_fk_contact]
		where j.[_fk_company] <> c._fk_company --different companies
		or j.[_fk_contact] is NULL)

--Different contact companies / default contact
select distinct 
	case when company_id is NULL or company_id not in (select __pk from [20191030_153350_companies]) then 'AS999999999'
		else concat('AS', company_id) end as company_id
	, case when company_id is NULL or company_id not in (select __pk from [20191030_153350_companies]) then 'AS999999999'
		else concat('AS99999', company_id) end as dif_contact
		, 'Default contact' as contact_lname
		, 'Default contact for this company' as contact_note
from dif_job --54 records
--where company_id is not NULL

UNION

select distinct
	case when _fk_company is NULL or _fk_company not in (select __pk from [20191030_153350_companies]) then 'AS999999999'
		else concat('AS', _fk_company) end as company_id
	, case when _fk_company not in (select __pk from [20191030_153350_companies]) or _fk_company is NULL then 'AS999999999'
		else concat('AS99999', _fk_company) end as dif_contact
	, 'Default contact' as contact_lname
	, 'Default contact for this company' as contact_note
	from [20191030_155620_jobs]
	where _fk_contact not in (select __pk from [20191030_153350_contacts] where type = 'Contact')