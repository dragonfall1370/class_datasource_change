--Contact not existing in Company, Default contact will be mapped
with jobcontact as (select distinct [企業 PANO ], [採用担当者ID]
		, concat('DEF', [企業 PANO ]) as default_contactID
		from csv_job
		where concat('REC-',採用担当者ID) not in (select 採用担当者ID from csv_rec))

, dup as (select [PANO ], [ポジション名]
		, row_number() over(partition by trim(lower([ポジション名])) order by [PANO ] desc) as rn
		from csv_job)

, open_enddate as (select JobPANo
		, coalesce(nullif(convert(date, [Open], 120),''), NULL) as open_date
		, coalesce(nullif(convert(date, [Close], 120),''), NULL) as end_date
		, coalesce(nullif(convert(date, [Other], 120),''), NULL) as other_date
		from csv_Job_Situation)

---Job Type | Employment Type | 雇用区分
, jobtype as (select [PANO ] as job_ext_id
	, value as jobtype
	from csv_job
	cross apply string_split([雇用区分], char(10))
	where [雇用区分] <> '')

/* Priority job type / employment type before Randstad cleanse
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
*/

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
, case when j.[企業 PANO ] not in (select [PANO ] from csv_recf) then 'REC-999999999' --default contact
		when jc.採用担当者ID is not NULL then default_contactID --default contact in each company
		else concat('REC-', j.[採用担当者ID]) end as [position-contactId]
, case when j.[PANO ] in (select [PANO ] from dup where rn > 1) then concat_ws(' - ', j.[ポジション名], dup.rn)
		else coalesce(nullif(j.[ポジション名],''), 'No job title') end as [position-title]
, j.募集状況
, coalesce(nullif(oe.open_date,''), convert(date, j.[登録日], 120)) as [position-startDate]
, case when oe.end_date is NULL then dateadd(year, 1, coalesce(oe.open_date, j.[登録日]))
		else convert(date, oe.end_date, 120) end as [position-endDate]
, convert(date, oe.other_date, 120) as submission_date --#CF
, coalesce(try_parse(採用人数 as numeric using 'ja-JP'), 2) as [position-headcount]
, 'JPY' as [position-currency]
, trim(u.EmailAddress) as [position-owners] --JOB担当者ユーザID
--Job brief
, concat_ws(concat(char(10),char(13))
	, coalesce('【紹介料（料率or金額）】' + char(10) + nullif([紹介料（料率or金額）], ''), NULL) --Referral fee (rate or amount)
	, coalesce('【採用人数】' + char(10) + nullif([採用人数], ''), NULL) --Headcount
	, coalesce('【選考プロセス 詳細】' + char(10) + nullif([選考プロセス 詳細], ''), NULL)
	) as [position-note]
--Job type / Employment type
, jt.jobtype as [position-type]
, jt.employment_type as [position-employmentType]
, jd.job_doc as [position-document]
from csv_job j
left join open_enddate oe on j.[PANO ] = oe.JobPANo
left join dup on dup.[PANO ] = j.[PANO ]
left join jobcontact jc on jc.[企業 PANO ] = j.[企業 PANO ]
left join UserMapping u on u.UserID = j.JOB担当者ユーザID
left join job_doc jd on jd.job_ext_id = j.[PANO ]
left join final_jobtype jt on jt.job_ext_id = j.[PANO ]