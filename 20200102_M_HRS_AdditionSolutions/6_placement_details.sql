with jobapp as (select e.__pk as job_app_id
		, e._fk_candidate_list
		, e._fk_job
		, e.colour_code
		, case when j.contract_type = 'Permanent' then 301
				when j.contract_type = 'Temp' then 302
				else 301 end job_type
		, convert(datetime, e.stamp_created, 103) as actioned_date
		, row_number() over(partition by e._fk_candidate_list, e._fk_job 
			order by case e.colour_code
				when 'Agency Interview' then '3'
				when 'Candidate Rejected' then '0'
				when 'Candidate Withdrew' then '0'
				when 'Closed' then '1'
				when 'CV Sent' then '2'
				when 'External Interview 1' then '3'
				when 'External Interview 2' then '4'
				when 'Hold/Rejected' then '1'
				when 'Interview Other' then '3'
				when 'Offer Accepted' then '5'
				when 'Offer Made' then '5'
				when 'Offer Rejected' then '5'
				when 'Offer Sent' then '5'
				when 'Other' then '0'
				when 'Placed' then '6'
				when 'Telephone Interview' then '3'
				else 0 end desc, e.stamp_created desc) as rn
		from [20191030_155243_events] e
		left join [20191030_155620_jobs] j on j.__pk = e._fk_job
		where e._fk_candidate_list is not NULL AND e._fk_job is not NULL)

--MAIN SCRIPT
select ja.job_app_id
	, concat('AS', _fk_candidate_list) as cand_ext_id
	, concat('AS', ja._fk_job) as job_ext_id
	, ja.colour_code as sub_stage
	, ja.job_type
	, ja.actioned_date
--Permanent Type | agency_fee_value_exchanged - agency_fee_percentage
	, coalesce(nullif(convert(date, p.date_placed, 103),'')
			, nullif(convert(date, p.date_start_contract, 103),'')
			, nullif(ja.actioned_date,'')) as placement_date
	, coalesce(nullif(convert(date, p.date_start_contract, 103),'')
			, nullif(convert(date, p.date_placed, 103),'')
			, nullif(ja.actioned_date,'')) as start_date
	, coalesce(convert(datetime, p.date_end_contract, 103)
			, dateadd(month, 6, coalesce(nullif(convert(date, p.date_start_contract, 103),'')
									, nullif(convert(date, p.date_placed, 103),'')
									, nullif(ja.actioned_date,'')))) as end_date
	, case when coalesce(p.zc_initial_cost_exchanged, 0) < p.zc_profit_exchanged then p.zc_profit_exchanged
		else coalesce(p.zc_initial_cost_exchanged, 0) end as pay_rate
	, case p.zc_fee_type_display 
		when 'Fixed' then coalesce(nullif(p.zc_profit_exchanged, ''), nullif(p.zc_initial_cost_exchanged, ''), 0)
		when 'Percent' then coalesce(nullif(p.zc_profit_exchanged, ''), nullif(p.zc_initial_cost_exchanged * p.agency_fee_percentage, ''), 0)
		else 0 end as profit --aka projected_profit
	, case p.zc_fee_type_display
		when 'Fixed' then 100
		when 'Percent' then p.agency_fee_percentage * 100
		else 100 end as percentage_of_annual_salary
	, 1 as use_quick_fee_forecast
--Contract Type | 
	, 1 as placed_valid
	, 1 as pay_calculation_type
	, 'markup' as charge_rate_type
	, 2 as contract_rate_type --daily
	, 0 as contract_length
	, 2 as contract_length_type --daily
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
	, p.contract_salary_exchanged
	, p.contract_type
	, p.date_placed
	, p.date_start_contract
	, p.date_end_contract
	, p.zc_fee_type_display
	, p.zc_initial_cost_exchanged
	, p.zc_profit_exchanged
	, p.zc_total_cost_exchanged
from jobapp ja
left join [20191030_160020_placements] p on p._fk_contact_candidate = ja._fk_candidate_list and p._fk_job = ja._fk_job
where ja.rn = 1
--and ja.colour_code in ('Placed') --PLACED
and ja.colour_code in ('Offer Accepted', 'Offer Made', 'Offer Rejected', 'Offer Sent') --OFFERED
--and job_type = 301 --PERMANENT
and job_type = 302 --CONTRACT