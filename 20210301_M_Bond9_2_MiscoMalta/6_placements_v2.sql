--MAIN SCRIPT
with jobapp as (select --distinct c94.description --11 mapping
	--distinct c68.description
	uniqueid
	, "4 candidate xref"
	, "6 job id xref"
	, case 
			when c94.description = 'Acc/Reject' and c68.description = 'Awaiting Approval' then 'SHORTLISTED'
			when c94.description = 'Acc/Reject' and c68.description = 'Offer Declined' then 'SHORTLISTED'
			when c94.description = 'Acc/Reject' and c68.description = 'Withdrawn' then 'SHORTLISTED'
			when c94.description = 'CV Sent' and c68.description = 'Awaiting Approval' then 'SENT'
			when c94.description = 'CV Sent' and c68.description = 'Interview Confirmed' then 'SENT'
			when c94.description = 'CV Sent' and c68.description = 'Offer Declined' then 'SENT'
			when c94.description = 'CV Sent' and c68.description = 'Withdrawn' then 'SENT'
			when c94.description = 'Fill Bkg' and c68.description = 'PLACED' then 'PLACED'
			when c94.description = 'Interview' and c68.description = 'Awaiting Approval' then 'FIRST_INTERVIEW'
			when c94.description = 'Interview' and c68.description = 'Awaiting Approval' then 'FIRST_INTERVIEW'
			when c94.description = 'Interview' and c68.description = 'Awaiting Approval' then 'SECOND_INTERVIEW'
			when c94.description = 'Interview' and c68.description = 'Offer Accepted' then 'SECOND_INTERVIEW'
			when c94.description = 'Interview' and c68.description = 'Offer Declined' then 'SECOND_INTERVIEW'
			when c94.description = 'Interview' and c68.description = 'Offer Declined' then 'FIRST_INTERVIEW'
			when c94.description = 'Interview' and c68.description = 'PLACED' then 'PLACED'
			when c94.description = 'Interview' and c68.description = 'Withdrawn' then 'SECOND_INTERVIEW'
			when c94.description = 'Perm Place' and c68.description = 'PLACED' then 'PLACED'
			when c94.description = 'Reject' and c68.description = 'Awaiting Approval' then 'SHORTLISTED'
			when c94.description = 'Reject' and c68.description = 'CV Approved' then 'SENT'
			when c94.description = 'Reject' and c68.description = 'Offer Accepted' then 'SHORTLISTED'
			when c94.description = 'Reject' and c68.description = 'Offer Declined' then 'FIRST_INTERVIEW'
			when c94.description = 'Reject' and c68.description = 'PLACED' then 'PLACED'
			when c94.description = 'Reject' and c68.description = 'Withdrawn' then 'SHORTLISTED'
			when c94.description = 'Replace' and c68.description = 'PLACED' then 'PLACED'
			when c94.description = 'Shorlist' and c68.description = 'Awaiting Approval' then 'SHORTLISTED'
			when c94.description = 'Shortlist' and c68.description = 'Awaiting Approval' then 'SHORTLISTED'
			when c94.description = 'Shortlist' and c68.description = 'Interview Requested' then 'SHORTLISTED'
			when c94.description = 'Shortlist' and c68.description = 'PLACED' then 'PLACED'
			when c94.description = 'Shortlist' and c68.description = 'Withdrawn' then 'SHORTLISTED'
			end as stage
	, coalesce(to_date("16 lastactnda date", 'DD/MM/YY'), to_date("11 created date", 'DD/MM/YY')) actioned_date
	, case when c94.description = 'Acc/Reject' and c68.description = 'Awaiting Approval' then 'rejected'
			when c94.description = 'Acc/Reject' and c68.description = 'Offer Declined' then 'rejected'
			when c94.description = 'Acc/Reject' and c68.description = 'Withdrawn' then 'rejected'
			when c94.description = 'CV Sent' and c68.description = 'Withdrawn' then 'rejected'
			when c94.description = 'Interview' and c68.description = 'Withdrawn' then 'rejected'
			when c94.description = 'Reject' then 'rejected'
			when c94.description = 'Shortlist' and c68.description = 'Withdrawn' then 'rejected'
			else NULL end as rejected_status
	, c94.description as sub_status --aka. last_stage
	, c68.description as stage_status
	, coalesce(to_date(f13."31 rejectdte date", 'DD/MM/YY'), to_date("16 lastactnda date", 'DD/MM/YY'), to_date("11 created date", 'DD/MM/YY')) as rejected_date
	from f13
	left join (select * from codes where codegroup = '94') c94 on c94.code = "15 last actio codegroup  94"
	left join (select * from codes where codegroup = '68') c68 on c68.code = "19 status codegroup  68"
	where 1=1
	and "4 candidate xref" in (select uniqueid from f01 where "101 candidate codegroup  23" = 'Y')
	and "6 job id xref" in (select uniqueid from f03)
	--and c68.description = 'PLACED' --may include different stages
	--and c94.description = 'Fill Bkg' --change mapping to PLACED
	--order by c94.description
	--total 85861
	)
	
	
, final_jobapp as (select "4 candidate xref" as cand_ext_id
	, "6 job id xref" as job_ext_id
	, uniqueid
	, stage
	, actioned_date
	, sub_status
	, stage_status
	, rejected_status
	, rejected_date
	, row_number() over(partition by "4 candidate xref", "6 job id xref"
							order by case stage
								WHEN 'PLACED' THEN 6
								WHEN 'OFFERED' THEN 5
								WHEN 'SECOND_INTERVIEW' THEN 4
								WHEN 'FIRST_INTERVIEW' THEN 3
								WHEN 'SENT' THEN 2
								WHEN 'SHORTLISTED' THEN 1 end desc
							, actioned_date desc) as rn
	from jobapp) --select * from final_jobapp where rn=1 and stage = 'PLACED'
	
, jobtype as (select j.uniqueid
	, case
		when c3.code = '1' then 'CONTRACT'
		when c3.code = '2' then 'PERMANENT'
		when c3.code = '3' then 'CONTRACT'
		when c3.code = '4' then 'CONTRACT'
		else 'PERMANENT' end as jobtype
	from f03 j
	left join (select * from codes where codegroup = '3') c3 on c3.code = j."4 type codegroup   3" --job type
	)
	
, placement_join as (select pp3."pp3 uniqueid" reference_id
	, to_date("8 start date date", 'DD/MM/YY') as start_date
	, "10 salary xref"::float salary
	, "18 agreed sal xref"::float agree_salary
	, "19 fee  xref"::float as fee
	, "20 fee amount xref"::float as fee_amount
	, "21 vat xref"::float
	, "22 total fee xref"::float
	, to_date("90 1st plc date", 'DD/MM/YY') as placement_date
	--, date_trunc('year', to_date("90 1st plc date", 'DD/MM/YY')) as year
	, "105 salary note"
	, "106 fee note"
	, to_date("74 lst cont date", 'DD/MM/YY') as last_cont_date
	, "28 contact xref" as cand_ext_id
	, a."field 3" as job_ext_id
	from act_pp3 pp3
	join act a on a.uniqueid = pp3."pp3 uniqueid"
	where a."field 3" is not NULL --candidate taken from placement table
	
	
UNION ALL
select pp3."pp6 uniqueid"
	, to_date("8 start date date", 'DD/MM/YY') as start_date
	, "10 salary xref"::float salary
	, "18 agreed sal xref"::float agree_salary
	, "19 fee  xref"::float as fee
	, "20 fee amount xref"::float as fee_amount
	, "21 vat xref"::float
	, "22 total fee xref"::float
	, to_date("90 1st plc date", 'DD/MM/YY') as placement_date
	--, date_trunc('year', to_date("90 1st plc date", 'DD/MM/YY')) as year
	, "105 salary note"
	, "101 fax note"
	, to_date("74 lst cont date", 'DD/MM/YY') as last_cont_date
	, "28 contact xref" as cand_ext_id
	, a."field 3" as job_ext_id
	from act_pp6 pp3
	join act a on a.uniqueid = pp3."pp6 uniqueid"
	where a."field 3" is not NULL --candidate taken from placement table
	)
	
, placement as (select reference_id
	, start_date
	, salary
	, agree_salary
	, fee
	,  fee_amount
	, "21 vat xref"
	, "22 total fee xref"
	, placement_date
	--, date_trunc('year', to_date("90 1st plc date", 'DD/MM/YY')) as year
	, "105 salary note"
	, "106 fee note"
	, last_cont_date
	, cand_ext_id
	, job_ext_id
	, row_number() over(partition by cand_ext_id, job_ext_id order by placement_date desc, last_cont_date desc, start_date desc) as rn
	from placement_join
	) --select * from placement where rn=1
	
--MAIN SCRIPT
select ja.cand_ext_id
, ja.job_ext_id
, ja.stage
, ja.actioned_date
, ja.uniqueid as jobapp_id
, j.jobtype
, case when j.jobtype = 'PERMANENT' then 301
		when j.jobtype = 'CONTRACT' then 302
		else 301 end as position_status
, ja.rn
, coalesce(p.start_date, p.placement_date, ja.actioned_date)::timestamp as hire_date
, coalesce(p.start_date, p.placement_date, ja.actioned_date)::timestamp as start_date
, coalesce(last_cont_date, coalesce(p.start_date, p.placement_date, ja.actioned_date) + interval '1 month') as end_date
, 'MT' country_code
--PERMANENT PLACEMENTS
, 1 as use_quick_fee_forecast
, coalesce(fee, 100) as percentage_of_annual_salary
, coalesce(agree_salary, salary, 0) as pay_rate
, coalesce(fee, 0) as fee
, coalesce(fee_amount, 0) as fee_amount 
, case 
		when fee is NULL and nullif(fee_amount, 0) is NULL then coalesce(agree_salary, salary, 0)
		when fee is not NULL and coalesce(agree_salary, salary) is not NULL then coalesce(fee_amount, 0)
		else 0 end as project_profit --aka BOOKED FEE
--Contract Type
, 'markup' as charge_rate_type
, coalesce(agree_salary, salary, 0) as charge_rate
, 0 as markup_percent
, 0 as margin_percent
, 1 as contract_rate_type
, 1 as contract_length
, 1 as contract_length_type
, 1 as placed_valid
, 1 as pay_calculation_type
, 8 as working_hours_per_day
, 5 as working_days_per_week
, 40 as working_hours_per_week
, 22 as working_days_per_month
, 4.20 as working_weeks_per_month
--GENERAL
, 3 as draft_offer --used to move offered to placed in vc [offer]
, 2 as invoicestatus --used to update invoice status in vc [invoice] as 'active'
, 1 as renewal_index --default value in vc [invoice]
, 1 as renewal_flow_status
, 1 as invoice_valid
, -10 as latest_user_id
, current_timestamp as latest_update_date
, 0 as tax_rate
, 'other' as export_data_to
, 0 as net_total
, 0 as other_invoice_items_total
, 0 as invoice_total
, p.reference_id
, coalesce(nullif(concat_ws(chr(10)
	, coalesce('Filled date: ' || p.placement_date, NULL)
	, coalesce('Agreed Salary: ' || agree_salary, NULL)
	, coalesce('Fee (%): ' || fee, NULL)
	, coalesce('Net Fee: ' || fee_amount, NULL)
	, coalesce('VAT: ' || "21 vat xref", NULL)
	, coalesce('Total fee: ' || "22 total fee xref", NULL)
	, coalesce('Last contact date: ' || last_cont_date, NULL)
	), ''), 'Placement details were migrated as default') as internal_note
from final_jobapp ja
left join jobtype j on j.uniqueid = ja.job_ext_id
left join (select * from placement where rn = 1) p on p.cand_ext_id = ja.cand_ext_id and p.job_ext_id = ja.job_ext_id
where 1=1
and ja.rn=1
and ja.stage = 'PLACED'
--and j.jobtype = 'PERMANENT' --job type PERM
--and j.jobtype = 'CONTRACT' --job type CONTRACT
--and ja.job_ext_id = '80810301D7B38080'
--and ja.cand_ext_id = '80810101D5E08180'
--and p."pp3 uniqueid" is NULL --check if placement is invalid
order by ja.cand_ext_id, ja.job_ext_id