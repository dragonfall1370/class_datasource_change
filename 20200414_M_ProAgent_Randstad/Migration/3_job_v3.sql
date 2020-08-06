--Contact not existing in Company, Default contact will be mapped
with jobcontact as (select distinct [PANO ], [企業 PANO ], [採用担当者ID]
		, concat('DEF', [企業 PANO ]) as default_contactID
		from csv_job
		where concat('REC-',採用担当者ID) not in (select 採用担当者ID from csv_rec))

, dup as (select [PANO ], [ポジション名]
		, row_number() over(partition by trim(lower([ポジション名])) order by [PANO ] desc) as rn
		from csv_job)


/* PROD temp table added
, open_enddate as (select [Job PANo]
		, coalesce(nullif(convert(date, [Open], 120),''), NULL) as open_date
		, coalesce(nullif(convert(date, [Close], 120),''), NULL) as end_date
		, coalesce(nullif(convert(date, [Other], 120),''), NULL) as other_date
		from csv_Job_Situation
		where coalesce(nullif([Open],''), nullif([Close],''), nullif([Other],'')) is not NULL)


--From REVIEW 2, final_jobtype will be run from temporary table
---Job Type | Employment Type | 雇用区分
, jobtype as (select [PANO ] as job_ext_id
	, value as jobtype
	from csv_job
	cross apply string_split([雇用区分], char(10))
	where [雇用区分] <> '')

--Priority job type / employment type before Randstad cleanse
'正社員' then 'PERMANENT' > 1
'契約社員' then 'CONTRACT' > 3
'紹介予定派遣' then 'TEMPORARY_TO_PERMANENT' > 5
'【新卒】　正社員' then 'PERMANENT' > 2
'【新卒】　契約社員' then 'CONTRACT' > 4
'【新卒】　紹介予定派遣' then 'TEMPORARY_TO_PERMANENT' > 6
else 0

'正社員' then 'FULL_TIME' > 1
'契約社員' then 'FULL_TIME' > 3
'紹介予定派遣' then NULL > 5
'【新卒】　正社員' then 'FULL_TIME' > 2
'【新卒】　契約社員' then 'FULL_TIME' > 4
'【新卒】　紹介予定派遣' then NULL > 6
else 0 
---

, jobtype_employment as (select job_ext_id
	, case jobtype
		when '正社員' then 1
		when '契約社員' then 3
		when '紹介予定派遣' then 5
		when '【新卒】　正社員' then 2
		when '【新卒】　契約社員' then 4
		when '【新卒】　紹介予定派遣' then 6
		else 0 end as jobtype
	, case jobtype 
		when '正社員' then 1
		when '契約社員' then 3
		when '紹介予定派遣' then 5
		when '【新卒】　正社員' then 2
		when '【新卒】　契約社員' then 4
		when '【新卒】　紹介予定派遣' then 6
		else 0 end as employment_type
	from jobtype)

, type_employment_rn as (select job_ext_id
	, jobtype
	, row_number() over(partition by job_ext_id order by jobtype asc) as jobtype_rn
	, employment_type
	, row_number() over(partition by job_ext_id order by employment_type asc) as employmentype_rn
	from jobtype_employment
	where jobtype > 0 and employment_type > 0)

, final_jobtype as (select job_ext_id
	, case 
		when jobtype in (1, 2) then 'PERMANENT'
		when jobtype in (3, 4) then 'CONTRACT'
		when jobtype in (5, 6) then 'TEMPORARY_TO_PERMANENT'
		else 'PERMANENT' end as jobtype
	, case 
		when employment_type in (1, 2) then 'FULL_TIME'
		when employment_type in (3, 4) then 'FULL_TIME'
		else NULL end as employment_type
	from type_employment_rn
	where jobtype_rn = 1)
	
select * 
into final_jobtype
from final_jobtype
*/

--Job type conditions added on 20200306
, jobtype as (select [PANO ] as job_ext_id
	, value as jobtype
	from csv_job
	cross apply string_split([雇用区分], char(10))
	where [雇用区分] <> '')

, jobtype_group as (select job_ext_id, count(*) as counts
	from jobtype
	group by job_ext_id
	having count(*) > 1)

--Documents
, doc as (select seq
	, job_id
	, pano as job_ext_id
	, right(trim([file]), charindex('/', reverse(trim([file]))) - 1) as UploadedName
	, [file]
	from JOB_resume)

, job_doc as (select job_ext_id
	, string_agg(UploadedName, ',') as job_doc
	from doc
	group by job_ext_id)


--MAIN SCRIPT
select j.[PANO ] as [position-externalId]
, j.[企業 PANO ] as original_com
, j.[採用担当者ID] as original_contact
, case when not exists (select 1 from csv_recf where [PANO ] = j.[企業 PANO ]) then 'REC-999999999' --default company&contact
		when exists (select 1 from jobcontact where [PANO ] = j.[PANO ]) then default_contactID --default contact in each company
		else concat('REC-', j.[採用担当者ID]) end as [position-contactId]
, case when j.[PANO ] in (select [PANO ] from dup where rn > 1) then concat_ws(' - ', j.[ポジション名], dup.rn)
		else coalesce(nullif(j.[ポジション名],''), 'No job title') end as [position-title]
, j.募集状況 --check the status of job
, coalesce(nullif(oe.open_date,''), convert(date, j.[登録日], 120)) as [position-startDate]

--New rule since 31-Mar-2020
, dateadd(year, 3, coalesce(nullif(oe.open_date,''), convert(date, j.[登録日], 120))) as [position-endDate]
, convert(date, oe.other_date, 120) as submission_date --#CF
, convert(numeric, coalesce(try_parse(採用人数 as numeric using 'ja-JP'), 2)) as [position-headcount]
, 'JPY' as [position-currency]

--Update owners rule 01-Jul-2020
, case when j.[JOB担当者ユーザID] in ('FPC163', 'FPC207') then NULL --JOB担当者ユーザID
		else trim(u.EmailAddress) end as [position-owners] --updated on 20200224
--Job brief
, concat_ws(concat(char(10),char(13))
	, coalesce('【紹介料（料率or金額）】' + char(10) + nullif([紹介料（料率or金額）], ''), NULL) --Referral fee (rate or amount)
	, coalesce('【採用人数】' + char(10) + nullif([採用人数], ''), NULL) --Headcount
	, coalesce('【選考プロセス 詳細】' + char(10) + nullif([選考プロセス 詳細], ''), NULL)
	) as [position-note]

--Job type / Employment type
, case when j.[募集状況] = 'Close' and j.[PANO ] in (select job_ext_id from jobtype_group) then 'PERMANENT'
	else coalesce(jt.jobtype, 'PERMANENT') end as [position-type]
, case when j.[募集状況] = 'Close' and j.[PANO ] in (select job_ext_id from jobtype_group) then 'PERMANENT'
	else coalesce(jt.employment_type, 'FULL_TIME') end as [position-employmentType]
, jd.job_doc as [position-document]
from csv_job j
left join open_enddate oe on j.[PANO ] = oe.[Job PANo]
left join dup on dup.[PANO ] = j.[PANO ]
left join jobcontact jc on jc.[PANO ] = j.[PANO ]
left join UserMapping u on u.UserID = j.JOB担当者ユーザID
left join job_doc jd on jd.job_ext_id = j.[PANO ]
left join final_jobtype jt on jt.job_ext_id = j.[PANO ]
where j.[雇用区分] not like '%障がい者採用正社員%'
and j.[雇用区分] not like '%障がい者採用契約社員%'
and j.[雇用区分] not like '%障がい者採用紹介予定派遣%' --140960
--and j.[企業 PANO ] in ('CPY001004', 'CPY018374', 'CPY000714', 'CPY018332', 'CPY018996', 'CPY000921')