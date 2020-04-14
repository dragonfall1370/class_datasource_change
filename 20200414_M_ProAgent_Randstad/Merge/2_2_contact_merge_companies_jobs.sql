/* CREATE TABLE mike_tmp_pa_contact_merged
(company_id bigint
, com_ext_id character varying (1000)
, contact_id bigint
, con_ext_id character varying (1000)
, contact_name character varying (1000)
, contact_name_kana character varying (1000)
, contact_email character varying (1000)
, reg_date timestamp
, update_date timestamp
, update_by character varying (1000)
, update_by_user character varying (1000)
)

select * from mike_tmp_pa_contact_merged
*/

 /* ADD backup column for contact
alter table contact
add column company_id_bkup bigint

--BACKUP CONTACTS
update contact
set company_id_bkup = company_id
where deleted_timestamp is NULL
and external_id ilike 'REC%'
*/

/*
---AUDIT CONTACTS BEFORE MERGING
select c.id, c.external_id, c.company_id, c.company_id_bkup
, m.vc_pa_company_id
, m.vc_company_id
from contact c
join mike_tmp_company_dup_check m on m.vc_pa_company_id = c.company_id
where 1=1
and deleted_timestamp is NULL
and external_id ilike 'REC%'

--BACKUP POSITIONS
alter table position_description
	add column company_id_bkup bigint,
    add column contact_id_bkup bigint;
	
update position_description
set company_id_bkup = company_id,
	contact_id_bkup = contact_id
	where deleted_timestamp is NULL
	and external_id ilike 'JOB%' --140960
*/

--MERGED CONTACTS WITHIN VC COMPANY
with contact_company as (select c.id, c.external_id 
	, c.company_id, c.company_id_bkup
	, m.vc_pa_company_id
	, m.vc_company_id
	from contact c
	join mike_tmp_company_dup_check m on m.vc_pa_company_id = c.company_id
	where 1=1
	and deleted_timestamp is NULL
	and external_id ilike 'REC%' --9614 rows
	)
	
update contact c
set company_id = cc.vc_company_id
from contact_company cc
where cc.id = c.id --VC contact id in both tables

/* AUDIT COMPANIES
select *
from mike_tmp_company_dup_check --vc_pa_company_id = 41138 and vc_company_id = 32876
where vc_company_id = 32876
*/

/* AUDIT CONTACTS/COMPANIES AFTER MERGE
select *
from contact
where company_id in (select company_id_bkup from contact where company_id_bkup <> company_id and company_id_bkup > 0)
and company_id_bkup is not NULL
*/

---UPDATE NEW COMPANY ON JOBS
with job_company as (select pd.id, pd.contact_id, pd.company_id, pd.company_id_bkup, pd.contact_id_bkup, pd.external_id
	, c.company_id as new_contact_company_id
	, c.company_id_bkup as old_contact_company_id
	from position_description pd
	join (select id, company_id, company_id_bkup, external_id from contact 
	      	where company_id_bkup <> company_id and company_id_bkup > 0) c on c.id = pd.contact_id
	)

update position_description pd
set company_id = jc.new_contact_company_id
from job_company jc
where jc.id = pd.id --VC job id in both tables | 55718 rows


--->>UPDATE CONTACT ON JOBS
with merged_new as (select pd.id, pd.company_id, pd.contact_id, pd.company_id_bkup, contact_id_bkup
	, m.new_company_id
	, m.merged_contact_id
	from position_description pd
	join mike_tmp_contact_dup_check m on m.contact_id = pd.contact_id --11540 rows
	)

update position_description pd
set contact_id = m.merged_contact_id
from merged_new m
where m.id = pd.id