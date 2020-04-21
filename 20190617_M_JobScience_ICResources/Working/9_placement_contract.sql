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
						
, contract as (select ija.*
		, p.id
		, p.name
		, case when p.ts2_start_date_c is NULL then ija.createddate::timestamp
			else p.ts2_start_date_c::timestamp end as start_date
		, ts2_end_date_c
		, case when p.ts2_end_date_c is NULL then ija.createddate::timestamp + interval '1 year'
			else p.ts2_end_date_c::timestamp end as end_date
		, case when jobtype = 301 then 1
			when jobtype = 302 then 2
			else NULL end as position_type
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
		, case when p.ts2_bill_rate_c is NULL then 0
				else p.ts2_bill_rate_c::float end as charge_rate
		, case when p.ts2_pay_rate_c is NULL then 0
				else p.ts2_pay_rate_c::float end as pay_rate
		, 1 as pay_calculation_type
		, 'chargeRate' as charge_rate_type --profit | margin | markup | chargeRate
		, case p.jstcl_rate_type_c 
			when 'Hourly' then 1
			when 'Daily' then 2
			when 'Weekly' then 3
			when 'Monthly' then 4
			when 'Annually' then 4 --migrated as Monthly
			else 1 end as contract_rate_type --default as Hourly | contract_rate_type
		, case when p.jstcl_est_wkly_hrs_c is NULL then '40'
			else p.jstcl_est_wkly_hrs_c end as working_hours_per_week
		, 5 as working_days_per_week
		, case when p.jstcl_est_wkly_hrs_c is NULL then '8'
				else p.jstcl_est_wkly_hrs_c::float / 5 end as working_hours
		, coalesce((p.ts2_end_date_c::date - p.ts2_start_date_c::date)/7,0) as contract_length --contract_length | migrated as weekly
		--, case pay_interval 
		--	when 'Hourly' then 1
		--	when 'Daily' then 2
		--	when 'Weekly' then 3
		--	when 'Monthly' then 4
		--	when 'Annually' then 4 --migrated as Monthly
		--	else 1 end contract_length_type_default
		, 3 as contract_length_type --weekly
--Margin % = Profit | Margin * 100 / Charge rate
--Markup % = Profit | Margin * 100 / Total pay rate
		, 0 as use_time_temp --Time management: 0 manual - 1 timetemp - 2 astute payroll
		, (coalesce(p.ts2_bill_rate_c::float,0) - coalesce(p.ts2_pay_rate_c::float,0)) as profit --Profit | Margin
		, case when p.ts2_pay_rate_c::float = 0 or p.ts2_pay_rate_c is NULL then 0
					else round(cast ((coalesce(p.ts2_bill_rate_c::float,0) - coalesce(p.ts2_pay_rate_c::float,0)) * 100 / p.ts2_pay_rate_c::float as numeric),2) end as markup_percent
		, case when p.ts2_bill_rate_c::float = 0 or p.ts2_bill_rate_c is NULL then 0
					else round(cast ((coalesce(p.ts2_bill_rate_c::float,0) - coalesce(p.ts2_pay_rate_c::float,0)) * 100 / p.ts2_bill_rate_c::float as numeric),2) end as margin_percent
		--Total Contract Pay = Base pay rate * Contract length
		--Total Contract Charge = Charge rate * Contract length
		--Total Contract Profit = Profit * Contract length
		--, projected_pay_rate --projected_pay_rate (Total Contract Pay) = pay_rate (base pay rate) * contract_length
		--, projected_charge_rate --Total Contract Charge
		--, projected_profit --Total Contract Profit
		, p.ts2_job_c
		, j.ts2_recruiter_c
		, u1.email
		, j.ts2_secondary_recruiter_c
		, u2.email
		, j.additional_recruiter_c
		, u3.email
		, js.share_count
		, case when js.share_count > 0 then 100/ js.share_count 
						else 0 end split_percent
		, a.legal_entity_name_c as trading_name
		, concat_ws( chr(10)
				, coalesce('Placement: ' || nullif(p.name,''),null) -- p.name as "Placement"
				--, coalesce('Salary: ' || nullif(p.ts2_salary_c,''),null) -- p.name as "Placement"
				--, coalesce('Legacy Fee Amount: ' || nullif(p.legacy_fee_amount_c,''),null) -- p.name as "Placement"
				--, coalesce('Fee percentage: ' || nullif(p.ts2_fee_pct_c,''),null) -- p.name as "Placement"
				--, coalesce('Notes / Insurance: ' || nullif(p.abc,''),null)
				, coalesce('Primary Recruiter Cost Centre: ' || nullif(p.primary_recruiter_cost_centre_text_c,''),null) 
				--, p.primary_recruiter_cost_centre_text_c as "Primary Recruiter Cost Centre"
				, coalesce('Secondary Recruiter Cost Centre: ' || nullif(p.secondary_recruiter_cost_centre_text_c,''),null) 
				--, p.secondary_recruiter_cost_centre_text_c as "Secondary Recruiter Cost Centre"
				, coalesce('Additional Recruiter Cost Centre: ' || nullif(p.additional_recruiter_cost_centre_text_c,''),null) 
				--, p.additional_recruiter_cost_centre_text_c
				, coalesce('Site Address Formula: ' || concat_ws(', ', a.shippingstreet, a.shippingcity, a.shippingstate, a.shippingcountry, a.shippingpostalcode), null)
				, coalesce('Hirer Client Address: ' || concat_ws(', ', a.shippingstreet, a.shippingcity, a.shippingstate, a.shippingcountry, a.shippingpostalcode), null)
				, coalesce('Placement Company Legal Entity Name: ' || nullif(a.legal_entity_name_c,''), null)
				, coalesce('Payment frequency: ' || nullif(p.payment_frequency_c,''), null)
				, coalesce('Payment type: ' || nullif(p.payment_type_c,''), null)
				, coalesce('Accounts payable: ' || nullif(con.fullname,'') || ' ' || nullif(con.email,''), null) --ts2_accounts_payable_c
				--, coalesce('Invoice address: ' || nullif(p.invoice_address_c,''), null)
				, coalesce('Total Profit: ' || nullif(p.jstcl_total_profit_c,''), null)
				, coalesce('Forecasted revenue: ' || nullif(p.jstcl_forecasted_revenue_c,''), null)
		) as PlacementNote
		, concat_ws( chr(10)
				, coalesce('Payment Terms: ' || nullif(p.payment_terms_c,''),null) --, p.payment_terms_c as "Payment Terms"
				, coalesce('Invoice Processing Period: ' || nullif(p.contractor_invoice_processing_period_c,''),null) 
				--, p.contractor_invoice_processing_period_c as "Invoice Processing Period"
				, coalesce('Invoice address: ' || nullif(p.invoice_address_c,''), null)
				, coalesce('Tax Percent: ' || nullif(p.jstcl_tax_percent_c,''),null) --, p.jstcl_tax_percent_c as "Tax Percent"
				, coalesce('Primary Recruiter Cost Centre: ' || nullif(p.primary_recruiter_cost_centre_text_c,''),null) 
				--, p.primary_recruiter_cost_centre_text_c as "Primary Recruiter Cost Centre"
				, coalesce('Secondary Recruiter Cost Centre: ' || nullif(p.secondary_recruiter_cost_centre_text_c,''),null) 
				--, p.secondary_recruiter_cost_centre_text_c as "Secondary Recruiter Cost Centre"
				, coalesce('Additional Recruiter Cost Centre: ' || nullif(p.additional_recruiter_cost_centre_text_c,''),null) 
				--, p.additional_recruiter_cost_centre_text_c
		) as "InvoiceNote"
		, p.jstcl_purchase_order_number_c as PO_number
		, 1 as placed_valid
		, 1 as invoice_valid
		, 2 as position_type --Contract placements
		, 3 as draft_offer --used to move OFFERED to PLACED in VC [offer]
		, 2 as invoicestatus --used to update invoice status in VC [invoice] as 'active'
		, 1 as renewal_index --default value in VC [invoice]
		, 1 as renewal_flow_status --aka placement_active
		, -10 as latest_user_id
		, now() as latest_update_date
		, 0 as tax_rate
		, 'other' as export_data_to
		, 0 as net_total
		, 0 as other_invoice_items_total
		, 0 as invoice_total
		, 1 as split_mode --1-percent 2-number
		, round(cast(p.jstcl_total_profit_c as numeric), 2) as jstcl_total_profit_c
from imported_job_app ija
left join ts2_placement_c p on p.id = ija.appid
left join ts2_job_c j on j.id = ija.job_id --to get job owners
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u1 on u1.id = j.ts2_recruiter_c
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u2 on u2.id = j.ts2_secondary_recruiter_c
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u3 on u3.id = j.additional_recruiter_c
left join account a on a.id = j.ts2_account_c
left join ( select id, concat(firstname,' ',lastname) as fullname, email, title 
							from contact where recordtypeid in ('0120Y0000013O5d')) con on con.id = p.ts2_accounts_payable_c --CONTACT
left join jobshare js on js.job_id = j.id
where 1=1
and ija.rn = 1
and ija.appstage = 'PLACED'
and ija.jobtype = 302 --2024 rows
--and p.id = 'a0Q0Y000009FL24UAG'
)

select *
, case when contract_rate_type = 1 then contract_length * pay_rate * working_hours_per_week::float --hours
			when contract_rate_type = 2 then contract_length * pay_rate * 5 --weekdays
			else 0 end as projected_pay_rate
, case when contract_rate_type = 1 then contract_length * charge_rate * working_hours_per_week::float --hours
			when contract_rate_type = 2 then contract_length * charge_rate * 5 --weekdays
			else 0 end as projected_charge_rate
, case when contract_rate_type = 1 then round(cast(contract_length * profit * working_hours_per_week::float as numeric), 2) --hours
			when contract_rate_type = 2 then round(cast(contract_length * profit * 5 as numeric), 2) --weekdays
			else 0 end as projected_profit
, jstcl_total_profit_c
, case when share_count > 0 then 
			(case when contract_rate_type = 1 then round(cast(contract_length * profit * working_hours_per_week::float / share_count as numeric), 2) --hours
			when contract_rate_type = 2 then round(cast(contract_length * profit * 5 / share_count as numeric), 2) --weekdays
			else 0 end)
			else 0 end as split_amount
from contract
--where contract_rate_type = 1