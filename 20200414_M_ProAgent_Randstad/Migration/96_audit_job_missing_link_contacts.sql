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