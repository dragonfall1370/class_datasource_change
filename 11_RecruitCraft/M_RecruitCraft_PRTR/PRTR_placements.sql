--PLACEMENTS INFORMATION
/* 
- Company fee models will be populated with 6 ones as following
1. Candidate Placement
2. Deposit Fee
3. Background Check
4. Management Fee
5. Multiple Placement
6. Engagement Fee

- Default value when migrated to Vincere
+ % base
+ Fee rate: 20% Gross annual salary
+ Comment: added for more info

- Compensation: they only want to get the final amount of each placement i.e. Total Fee Due (amount after Fee %, Deductions, Taxes)
+ The final amount will be popped into [Annual salary]
+ Use [Quick Fee Forecast ]
+ [100%] Annual salary
+ Profit split will be based on Profit

*/

--MAIN SCRIPT
with Deductions as (select pl_id
	, sum(details_total) as TotalDeductions
	, string_agg (concat_ws(char(10)
		, coalesce('Details type: ' + nullif(trim(details_type),''),NULL)
		, coalesce('Details amount: ' + nullif(trim(details_amount),''),NULL)
		, coalesce('Details total: ' + nullif(trim(details_total),''),NULL)
		), char(10)
	) within group (order by details_id asc) as DeductionsDetails
	from placements.PlacementDetails 
	where pl_det_group = 'Deduction'
	and IsDeleted = 0 
	group by pl_id)

select p.pl_id
, p.vac_id
, p.company_id
, p.can_id
, pl_add_alt --used for company address
, p.usr_id --used for consultant in profit split
, 100 as shared --used for [offer_revenue_split] > [shared] percentage
, pl_sub_total_fee + d.TotalDeductions as SubTotal --used for [offer_revenue_split] > [amount] profit split
, invoice_date --used for [offer_personal_info] > [invoice_date]
, case when p.placement_date is NULL then p.pl_added_on
	else p.placement_date end  --used for [offer_personal_info] > [placed_date]
, case when p.pl_date_start is NULL then p.pl_added_on
	else p.pl_date_start end --used for [offer_personal_info] > [start_date]
, p.pl_date_due --used for [offer_personal_info] > [invoice_due_date]
, p.pl_credit_term --used for [offer_personal_info] > [invoice_due_date]
, p.pl_po_no as ContractNo --used for [offer_personal_info] > [note]
, 'thb' as currency_type --used for [offer] > [currency_type] -->> ONLY VALID PER CLIENT CURRENCY
, pl_sub_total_fee + d.TotalDeductions as AnnualSalary --used for [offer] > [pay_rate] -->> ONLY VALID AS PRTR wants to reflect the final profit || SubTotal from RecruitCraft
, 100 as percentage_of_annual_salary --used for [offer] > [percentage_of_annual_salary] as quick fee forecast %
, 1 as use_quick_fee_forecast --used to check on [use_quick_fee_forecast]
, pl_total_fee --populated to Internal Note after taxes (PRTR's requirements) | Total Fee Due
, t.team as division --used for [offer_personal_info] > [division]
, s.sub_team as billing_group --used for [offer_personal_info] > [billing_group]
, p.pl_id --appended to [offer_personal_info] > [invoice_message]
, concat_ws(char(10), '[The placement details were migrated from PRTR]'
	, coalesce('Placement ID: ' + convert(varchar(max),pl_id),NULL)
	, coalesce('Placement status: ' + nullif(trim(p.pl_status),''),NULL)
	, coalesce('Contact number: ' + nullif(trim(p.pl_po_no),''),NULL)
)
from placements.Placements p
left join Users.Users u on u.usr_id = p.usr_id
left join Users.Teams t on t.team_id = u.team_id
left join Users.SubTeams s on s.sub_team_id = u.sub_team_id
left join Deductions d on d.pl_id = p.pl_id
where 1=1
and p.vac_id > 0 and p.can_id > 0
and p.pl_id = 52479