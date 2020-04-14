--FINAL JOB APPLICATION
/* Table notes
temp [pa_final_jobapp]: job app mapping from PA
temp [vc_final_jobapp]: job app mapping to inject to VC
*/
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
from pa_final_jobapp
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
	, ja.offer_date
	, ja.stage6
	, ja.placed_date
	, coalesce(convert(money, c.[決定年収]), 0) as pay_rate --gross annual salary
	, coalesce('【請求書ID】' + nullif(c.[基本給], ''), NULL) as internal_note
	, c.売上計上日 --Sales recording date | Start Date
	, c.入金日 --Payment day | End Date
	, cs.[内定実施日] --Referral History-Offer Date
	, cs.[入社承諾実施日] --Referral History-Acceptance Date | Placement Date
--Placement Date | Offer Date | Start Date | End Date
	, coalesce(nullif(convert(datetime, cs.入社承諾実施日, 120), '')
		, convert(datetime
				, coalesce(nullif(case when ja.placed_date = '1111-06-01' then convert(datetime, '2018/07/11 12:26:14', 120) 
							else ja.placed_date end, ''), nullif(c.売上計上日,'')), 120)) as placement_date
	, coalesce(nullif(convert(datetime, cs.[内定実施日], 120), '')
		, convert(datetime, coalesce(nullif(ja.offer_date, '')
			, nullif(case when ja.placed_date = '1111-06-01' then convert(datetime, '2018/07/11 12:26:14', 120) 
						else ja.placed_date end, '')), 120)) as offer_date
	, coalesce(nullif(convert(datetime, c.売上計上日, 120), '')
		, convert(datetime
				, nullif(case when ja.placed_date = '1111-06-01' then convert(datetime, '2018/07/11 12:26:14', 120) 
							else ja.placed_date end, ''), 120)) as start_date
	, case when ja.placed_date = '1111-06-01' then convert(datetime, '2018/07/11 12:26:14', 120) --入社更新日 | special case
			else convert(datetime, coalesce(nullif(c.入金日, '')
					, convert(datetime, dateadd(month, 6, convert(datetime, nullif(ja.placed_date, ''), 120)))), 120) end as end_date
--Permanent Type
	, 1 as use_quick_fee_forecast
	, 100 as percentage_of_annual_salary
--Contract Type
	, 1 as placed_valid
	, 1 as pay_calculation_type
	, 'chargeRate' as charge_rate_type
	, 8 as working_hours_per_day
	, 5 as working_days_per_week
	, 40 as working_hours_per_week
	, 22 as working_days_per_month
	, 4.20 as working_weeks_per_month
--Offer personal info
	, 3 as draft_offer --used to move offered to placed in vc [offer]
	, 2 as invoicestatus --used to update invoice status in vc [invoice] as 'active'
	, 1 as renewal_index --default value in vc [invoice]
	, 1 as renewal_flow_status --used to update flow status in VC [invoice] as 'placement_active'
	, 1 as invoice_valid
	, -10 as latest_user_id
	, current_timestamp as latest_update_date
	, 0 as tax_rate
	, 'other' as export_data_to
	, 0 as net_total
	, 0 as other_invoice_items_total
	, 0 as invoice_total
from vc_final_jobapp ja
left join csv_contract c on c.[キャンディデイト PANO ] = ja.cand_ext_id and c.[JOB PANO ] = ja.job_ext_id
left join csv_status cs on cs.[キャンディデイト PANO ] = ja.cand_ext_id and cs.[JOB PANO ] = ja.job_ext_id
left join final_jobtype jt on jt.job_ext_id = ja.job_ext_id
where 1=1
and ja.original_stage in ('Placed - Starting', 'Placed - Active')
--and jt.jobtype = 'PERMANENT'
--and jt.jobtype in ('CONTRACT', 'TEMPORARY_TO_PERMANENT')
--and ja.cand_ext_id = 'CDT222831' and ja.job_ext_id = 'JOB122288' --Special case
--and placed_date = '1111-06-01'
order by ja.cand_ext_id, ja.job_ext_id