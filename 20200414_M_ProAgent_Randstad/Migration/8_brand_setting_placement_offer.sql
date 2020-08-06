---Organize in row_number, with highest stage, latest date
with jobapp as (select cand_ext_id
, job_ext_id
, app_status
, action_date
, rejected_date
, concat(cand_ext_id, job_ext_id) as jobapp_rn
, row_number() over(partition by cand_ext_id, job_ext_id
		order by action_date desc,
				case app_status
				when 'Shortlisted - Pending' then 1
				when 'Shortlisted - Rejected' then 1
				when 'Sent - Pending' then 2
				when 'Sent - Rejected' then 2
				when '1st Interview - Pending' then 3
				when '1st Interview - Rejected' then 3
				when '2nd+ Interview - Pending' then 4
				when '2nd+ Interview - Rejected' then 4
				when 'Offered - received' then 5
				when 'Placed - Starting' then 6
				when 'Placed - Active' then 7
				end desc) as rn
, rank() over(partition by cand_ext_id, job_ext_id
		order by --action_date desc,
				case app_status
				when 'Shortlisted - Pending' then 1
				when 'Shortlisted - Rejected' then 1
				when 'Sent - Pending' then 2
				when 'Sent - Rejected' then 2
				when '1st Interview - Pending' then 3
				when '1st Interview - Rejected' then 3
				when '2nd+ Interview - Pending' then 4
				when '2nd+ Interview - Rejected' then 4
				when 'Offered - received' then 5
				when 'Placed - Starting' then 6
				when 'Placed - Active' then 6
				end desc) as rank_rn
, sub_status
from pa_final_jobapp2
where cand_ext_id <> 'CDT000000' and job_ext_id <> 'JOB000000'
)

---Job Type | Employment Type | 雇用区分
, jobtype as (select [PANO ] as job_ext_id
	, value as jobtype
	from csv_job
	cross apply string_split([雇用区分], char(10))
	where [雇用区分] <> '')

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

/* Check reference
select *
from jobapp --CDT183041	JOB117535
*/

, jobappfilter as (select *
	, case 
		when app_status in ('Shortlisted - Pending', 'Shortlisted - Rejected') then 'SHORTLISTED'
		when app_status in ('Sent - Pending', 'Sent - Rejected') then 'SENT'
		when app_status in ('1st Interview - Pending', '1st Interview - Rejected') then 'FIRST_INTERVIEW'
		when app_status in ('2nd+ Interview - Pending', '2nd+ Interview - Rejected') then 'SECOND_INTERVIEW'
		when app_status in ('Offered - received') then 'OFFERED'
		when app_status in ('Placed - Starting', 'Placed - Active') then 'PLACED'
		else NULL end as stage
	from jobapp
	where rn = rank_rn
	--and cand_ext_id = 'CDT183041' and job_ext_id = 'JOB117535' --check conditions if ok
)

, offer as (select [キャンディデイト PANO ]
		, [JOB PANO ]
		, [売上 担当者1]
		, [売上 担当者1ユーザID]
		, [売上 担当者2]
		, [売上 担当者2ユーザID]
		, coalesce(nullif([売上 担当者1],''), nullif([売上 担当者2], '')) as consultant
	from csv_contract
	where coalesce(nullif([売上 担当者1],''), nullif([売上 担当者2], '')) is not NULL --8561
	)

, branch_offer as (select [キャンディデイト PANO ]
		, [JOB PANO ]
		, lp.vc_office_en as branch
		from offer o
		left join (select * from LP_Office where category = 'candidate') lp on lp.pa_office = o.consultant
		) --select * from branch_offer --8561

select ja.cand_ext_id
	, ja.job_ext_id
	, ja.original_stage
	, case when jt.jobtype = 'CONTRACT' then 302
	when jt.jobtype = 'TEMPORARY_TO_PERMANENT' then 303
	else 301 end as position_status --placed_job_type
	, ja.stage
	, ja.sub_status
	, ja.action_date
	, ja.stage5
	, ja.offer_date as offer_date_original
	, ja.stage6
	, ja.placed_date as placed_date_original
	, c.branch
	, 'offer' as record_type
	, 'BRANCH' as group_type
from vc_final_jobapp2 ja
left join (select * from branch_offer where branch is not NULL) c on c.[キャンディデイト PANO ] = ja.cand_ext_id and c.[JOB PANO ] = ja.job_ext_id
left join final_jobtype jt on jt.job_ext_id = ja.job_ext_id
where 1=1
and ja.original_stage in ('Placed - Starting', 'Placed - Active') --for PLACED records
--and ja.original_stage in ('Offered - received') --for OFFERED records
and c.branch is not NULL
order by ja.cand_ext_id, ja.job_ext_id --6633