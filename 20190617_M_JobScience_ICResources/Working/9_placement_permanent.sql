/*
--CREATE TEMP TABLE FOR IMPORTED JOB APP
select *
into imported_job_app
from ja1
*/

--MAIN SCRIPT
with jobs as (
				select j.id as job_id
				, j.ts2_recruiter_c
				, u1.email
				from ts2_job_c j
				left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u1 on u1.id = j.ts2_recruiter_c
				UNION 
				select j.id as job_id
				, j.ts2_secondary_recruiter_c
				, u2.email
				from ts2_job_c j
				left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u2 on u2.id = j.ts2_secondary_recruiter_c
				UNION 
				select j.id as job_id
				, j.additional_recruiter_c
				, u3.email
				from ts2_job_c j
				left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u3 on u3.id = j.additional_recruiter_c 
			)
			
, jobshare as (select job_id, count(email) as share_count
				from jobs
				where email is not NULL
				--and job_id = 'a0K0Y000005P9ciUAC'
				group by job_id
			)
						
select ija.*
		, p.id
		, p.name
		, case when p.ts2_start_date_c is NULL then ija.createddate::timestamp
			else p.ts2_start_date_c::timestamp end as start_date
		, case when jobtype = 301 then 1
			when jobtype = 302 then 2
			else NULL end as position_type
		, p.ts2_salary_c
		, p.legacy_fee_amount_c
		, p.ts2_fee_pct_c
		, case lower(p.currencyisocode)
					when 'bgn' then 'bgn'
					when 'cad' then 'cad'
					when 'chf' then 'chf'
					when 'cny' then 'yuan'
					when 'czk' then 'czk'
					when 'dkk' then 'dkk'
					when 'eur' then 'euro'
					when 'gbp' then 'pound'
					when 'nok' then 'nok'
					when 'sek' then 'sek'
					when 'sgd' then 'singd'
					when 'usd' then 'usd'
					else 'pound' end as currency_type
		, 1 as use_quick_fee_forecast
		, case when p.legacy_fee_amount_c::float > 0 then 100
					else coalesce(p.ts2_fee_pct_c::float,0) end as percentage_of_annual_salary --use quick free forecast
		, case when p.ts2_salary_c is NULL and p.legacy_fee_amount_c is NULL then 0 --default value for mapping PLACED stage
					when p.legacy_fee_amount_c::float > 0 then p.legacy_fee_amount_c::float
					when p.legacy_fee_amount_c is NULL then p.ts2_salary_c::float 
					else 0 end as pay_rate --pay_rate | annual_salary
		, case when p.ts2_salary_c is NULL and p.legacy_fee_amount_c is NULL then 0
					when p.legacy_fee_amount_c::float > 0 then p.legacy_fee_amount_c::float
					when p.legacy_fee_amount_c is NULL then p.ts2_salary_c::float * p.ts2_fee_pct_c::float / 100
					else 0 end as projected_profit
		, p.primary_recruiter_cost_centre_text_c
		, p.secondary_recruiter_cost_centre_text_c
		, p.additional_recruiter_cost_centre_text_c
		, p.additional_recruiter_c
		, p.ts2_job_c
		, j.ts2_recruiter_c
		, u1.email
		, j.ts2_secondary_recruiter_c
		, u2.email
		, j.additional_recruiter_c
		, u3.email
		, case when js.share_count > 0 then 100/ js.share_count 
						else 0 end share_pct
		, case when js.share_count > 0 and p.ts2_salary_c is NULL and p.legacy_fee_amount_c is NULL then 0
					when js.share_count > 0 and p.legacy_fee_amount_c::float > 0 then p.legacy_fee_amount_c::float / js.share_count
					when js.share_count > 0 and p.legacy_fee_amount_c is NULL then p.ts2_salary_c::float * p.ts2_fee_pct_c::float / (100 * js.share_count)
					else 0 end split_amount
		, 1 as split_mode --1-percent 2-number
, concat_ws( chr(10)
		, coalesce('Placement: ' || nullif(p.name,''),null) -- p.name as "Placement"
		, coalesce('Salary: ' || nullif(p.ts2_salary_c,''),null) -- p.name as "Placement"
		, coalesce('Legacy Fee Amount: ' || nullif(p.legacy_fee_amount_c,''),null) -- p.name as "Placement"
		, coalesce('Fee percentage: ' || nullif(p.ts2_fee_pct_c,''),null) -- p.name as "Placement"
		--, coalesce('Notes / Insurance: ' || nullif(p.abc,''),null)
		, coalesce('Primary Recruiter Cost Centre: ' || nullif(p.primary_recruiter_cost_centre_text_c,''),null) 
		--, p.primary_recruiter_cost_centre_text_c as "Primary Recruiter Cost Centre"
		, coalesce('Secondary Recruiter Cost Centre: ' || nullif(p.secondary_recruiter_cost_centre_text_c,''),null) 
		--, p.secondary_recruiter_cost_centre_text_c as "Secondary Recruiter Cost Centre"
		, coalesce('Additional Recruiter Cost Centre: ' || nullif(p.additional_recruiter_cost_centre_text_c,''),null) 
		--, p.additional_recruiter_cost_centre_text_c
		) as PlacementNote
		, concat_ws( chr(10)
					, coalesce('Payment Terms: ' || nullif(p.payment_terms_c,''),null) --, p.payment_terms_c as "Payment Terms"
				--, coalesce('Invoice Processing Period: ' || nullif(p.contractor_invoice_processing_period_c,''),null) 
				--, p.contractor_invoice_processing_period_c as "Invoice Processing Period"
				--, coalesce('Tax Percent: ' || nullif(p.jstcl_tax_percent_c,''),null) --, p.jstcl_tax_percent_c as "Tax Percent"
				--, coalesce('Primary Recruiter Cost Centre: ' || nullif(p.primary_recruiter_cost_centre_text_c,''),null) 
				--, p.primary_recruiter_cost_centre_text_c as "Primary Recruiter Cost Centre"
				--, coalesce('Secondary Recruiter Cost Centre: ' || nullif(p.secondary_recruiter_cost_centre_text_c,''),null) 
				--, p.secondary_recruiter_cost_centre_text_c as "Secondary Recruiter Cost Centre"
				--, coalesce('Additional Recruiter Cost Centre: ' || nullif(p.additional_recruiter_cost_centre_text_c,''),null) 
				--, p.additional_recruiter_cost_centre_text_c
		) as "InvoiceNote"
		, 1 as placed_valid
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
from imported_job_app ija
left join ts2_placement_c p on p.id = ija.appid
left join ts2_job_c j on j.id = ija.job_id --to get job owners
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u1 on u1.id = j.ts2_recruiter_c
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u2 on u2.id = j.ts2_secondary_recruiter_c
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u3 on u3.id = j.additional_recruiter_c
left join jobshare js on js.job_id = j.id
where 1=1
and ija.rn = 1
and ija.appstage = 'PLACED'
and ija.jobtype = 301 --6437 rows