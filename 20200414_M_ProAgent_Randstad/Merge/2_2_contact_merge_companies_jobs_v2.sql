with contact_company as (select c.id, c.external_id 
	, c.company_id, c.company_id_bkup
	, m.vc_pa_company_id
	, m.vc_company_id
	from contact c
	join mike_tmp_company_dup_check2 m on m.vc_pa_company_id = c.company_id --new tmp table for company merge
	where 1=1
	and deleted_timestamp is NULL
	and (external_id ilike 'REC%' or external_id ilike 'DEF%')--9614 rows
	--and c.id = 65314
	)

	
--MERGED CONTACTS WITHIN VC COMPANY
update contact c
set company_id = cc.vc_company_id
from contact_company cc
where cc.id = c.id --VC contact id in both tables


---UPDATE NEW COMPANY ON JOBS
with job_company as (select pd.id, pd.contact_id, pd.company_id, pd.company_id_bkup, pd.contact_id_bkup, pd.external_id
	, pd.contact_id
	, pd.contact_id_bkup
	, c.company_id as new_contact_company_id
	, c.company_id_bkup as old_contact_company_id
	from position_description pd
	join (select id, company_id, company_id_bkup, external_id from contact 
	      	where company_id_bkup <> company_id and company_id_bkup > 0) c on c.id = pd.contact_id
	) --select * from job_company where company_id <> new_contact_company_id


update position_description pd
set company_id = jc.new_contact_company_id
from job_company jc
where jc.id = pd.id --VC job id in both tables | 15915 rows
and jc.company_id <> jc.new_contact_company_id