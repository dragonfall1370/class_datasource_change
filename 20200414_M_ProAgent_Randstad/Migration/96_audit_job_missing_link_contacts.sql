--AUDIT JOB WITH MISSING LINK WITH CONTACTS
with jobcontact as (select distinct [PANO ], [企業 PANO ], [採用担当者ID]
		, concat('DEF', [企業 PANO ]) as default_contactID
		from csv_job
		where concat('REC-',採用担当者ID) not in (select 採用担当者ID from csv_rec))

select j.[PANO ] as [position-externalId]
, j.[企業 PANO ] as original_com
, j.[採用担当者ID] as original_contact
, case when j.[企業 PANO ] not in (select [PANO ] from csv_recf) then 'REC-999999999' --default contact
		when j.[企業 PANO ] in (select [PANO ] from jobcontact) then default_contactID --default contact in each company
		else concat('REC-', j.[採用担当者ID]) end as [position-contactId]
from csv_job j
left join jobcontact jc on jc.[PANO ] = j.[PANO ]
where j.[企業 PANO ] in ('CPY001004', 'CPY018374', 'CPY000714', 'CPY018332', 'CPY018996', 'CPY000921')


--
with jobtype as (select [PANO ] as job_ext_id
	, value as jobtype
	from csv_job
	cross apply string_split([雇用区分], char(10))
	where [雇用区分] <> '')

, jobtype_group as (select job_ext_id, count(*) as counts
	from jobtype
	group by job_ext_id
	having count(*) > 1) 
	
select j.[PANO ]
, j.[雇用区分]
, j.[募集状況]
, case when j.[募集状況] = 'Close' and j.[PANO ] in (select job_ext_id from jobtype_group) then 'PERMANENT'
	else coalesce(jt.jobtype, 'PERMANENT') end as [position-type]
from csv_job j
left join final_jobtype jt on jt.job_ext_id = j.[PANO ]
where j.[PANO ] = 'JOB000800'


--
select vc_fe_id
, vc_fe_name
, vc_sfe_id
, vc_sfe_name
, concat_ws('', '【PP】', vc_new_fe_en, coalesce(' / ' + nullif(vc_new_fe_ja,''), NULL)) as vc_new_fe_en
, vc_new_fe_ja
, note
, replace(trim(value), '[P]', '') as vc_new_sfe_split
, 3043 id_filter
from vc_2_vc_new_fe_sfe
cross apply string_split(vc_new_sfe, char(10))


--
select distinct concat('【PP】', industry_en, coalesce(' / ' +  nullif(industry_ja,''), ''))
from PA_industry


select distinct concat('【PP】', vc_fe_en, coalesce(' / ' +  nullif(vc_fe_ja,''), ''))
from PA_fe_sfe


--
select *
from csv_job
where [企業 PANO ] = 'CPY000714'
and [採用担当者ID] = '78654'


select *
from csv_rec
where [採用担当者ID] in (select concat('REC-', [採用担当者ID]) from csv_job where [企業 PANO ] = 'CPY000714')

select *
from csv_rec
where [採用担当者ID] = 'REC-78654'


---Additional check for job missing contacts links
select *
from mike_tmp_company_dup_check2 --6284

select *
from mike_tmp_company_dup_check --4884

select *
from mike_tmp_company_dup_check2
where com_ext_id = 'CPY000714'

--
with job_company as (select pd.id, pd.contact_id, pd.company_id, pd.company_id_bkup, pd.contact_id_bkup, pd.external_id
	, pd.contact_id
	, pd.contact_id_bkup
	, c.company_id as new_contact_company_id
	, c.company_id_bkup as old_contact_company_id
	from position_description pd
	join (select id, company_id, company_id_bkup, external_id from contact 
	      	where company_id_bkup <> company_id and company_id_bkup > 0) c on c.id = pd.contact_id
	)

update position_description pd
set company_id = jc.new_contact_company_id
from job_company jc
where jc.id = pd.id --VC job id in both tables | 15915 rows
and jc.company_id <> jc.new_contact_company_id

--Check special case
select id, name
from company
where external_id = 'CPY000714' --company_id=40702

select *
from contact
where 1=1
--and company_id = 40702 --contact_id=65314
and company_id = 14657 --merge with com_ext_id = 'CPY000714'

--Check special case for job > default contact = 'DEFCPY000714' | update to correct and merged contact in VC
select id, external_id, company_id, contact_id, company_id_bkup, contact_id_bkup
from position_description
where company_id = 14657
and company_id_bkup = 40702

select id, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where id = 65314

select id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where external_id = 'REC-207239' --id=62813

select *
from mike_tmp_contact_dup_check2
where contact_id = 38204 --merge with contact_id=26586

select id, first_name, last_name, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where id in (26586, 62813, 65314)

select id, external_id, company_id, company_id_bkup, contact_id, contact_id_bkup
from position_description
where company_id = 14657
and contact_id = 65314 --140 jobs

update position_description
set contact_id = 26586 --switch to VC merged contact 26586 via contact_id=62813
where company_id = 14657
and contact_id = 65314 --wrong contact from PA instead of correct contact_id=62813

--
select id, external_id, company_id, company_id_bkup, insert_timestamp, deleted_timestamp
from contact
where 1=1
--and external_id ilike 'DEF%' --6 default contacts
and external_id in ('REC-155683' --id=37729
										, 'REC-253581' --id=42711
										, 'REC-78654' --id=??
										, 'REC-250767' --id=40338
										)

select *
from contact
where external_id = 'REC-78654'

select *
from contact_deleted
order by id desc
