--THIS SCRIPT USED FOR PRTR PRODUCTION: RC change the way to calculate the placement fee
--GET TOTAL AMOUNT BASED ON PLACEMENT ITEMS
with TotalAmount as (select pl_id
	/* , sum(details_total * pl_det_pct) as TotalAmount --incorrect in some case */
	, sum(details_total) as TotalAmount
	from placements.PlacementDetails pd
	where pl_det_group = 'Placement'
	and IsDeleted = 0 
	group by pl_id)

--select * from TotalAmount where pl_id = 52505
--GET APPLICABLE FEE BASED ON FEE PERCENTAGE OF TOTAL AMOUNT
, TotalApplicableFee as (select ta.pl_id
	, case when TotalAmount is NULL then 0 else TotalAmount end * pl_fee_percent / 100 as TotalApplicableFee
	from TotalAmount ta
	left join placements.Placements p on p.pl_id = ta.pl_id)

--select * from TotalApplicableFee where pl_id = 16

--GET TOTAL DEDUCTIONS BASED ON DEDUCTION ITEMS
, Deductions as (select pd.pl_id
	, sum(pd.details_total * pd.pl_det_pct) as ReviewDeductions --used in Review Site
	, sum(TotalApplicableFee * pd.pl_det_pct) as TotalDeductions
	/* Used in Review PRTR database
	, string_agg (concat_ws(char(10)
		, coalesce('Details type: ' + nullif(trim(pd.details_type),''),NULL)
		, coalesce('Details amount: ' + nullif(convert(nvarchar(max),pd.details_amount),''),NULL)
		, coalesce('Details total: ' + nullif(convert(nvarchar(max),pd.details_total),''),NULL)
		, coalesce('Deduction percentage: ' + nullif(convert(nvarchar(max),pd.pl_det_pct * 100),''),NULL)
		), char(10)
	) within group (order by details_id asc) as DeductionsDetails */
	, string_agg (concat_ws(char(10)
		, coalesce('Details type: ' + nullif(trim(pd.details_type),''),NULL)
		, coalesce('Deduction percentage (%): ' + nullif(convert(nvarchar(max),pd.pl_det_pct * 100),''),NULL)
		, coalesce('Total deductions: ' + nullif(convert(nvarchar(max),TotalApplicableFee * pd.pl_det_pct),''),NULL)
		), char(10)
	) within group (order by details_id asc) as DeductionsDetails
	from placements.PlacementDetails pd
	left join TotalApplicableFee tf on tf.pl_id = pd.pl_id
	where pl_det_group = 'Deduction'
	and pd.IsDeleted = 0 
	group by pd.pl_id)

--select * from Deductions where pl_id = 16

--MAIN SCRIPT
select p.pl_id
, concat('PRTR',p.vac_id) as JobExtID
, concat('PRTR',p.can_id) as CandidateExtID
, p.pl_type
, p.company_id
, pl_add_alt --used for company address
, p.usr_id --used for consultant in profit split
, 100 as shared --used for [offer_revenue_split] > [shared] percentage
, ta.TotalApplicableFee --for COMPARISON purpose
, d.TotalDeductions --for COMPARISON purpose
, case when trim(p.pl_type) in ('Multiple Placement', 'Management Fee', 'Engagement Fee') then p.pl_sub_total_fee
	when trim(p.pl_type) = 'Candidate Placement' then (case when ta.TotalApplicableFee is NULL then 0 else ta.TotalApplicableFee end) 
												- (case when d.TotalDeductions is NULL then 0 else d.TotalDeductions end) 
	else 0 end as SubTotal --used for [offer_revenue_split] > [amount] profit split
, invoice_date --used for [offer_personal_info] > [invoice_date]
, case when p.placement_date is NULL then p.pl_added_on
	else p.placement_date end placed_date --used for [offer_personal_info] > [placed_date]
, case when p.pl_date_start is NULL then p.pl_added_on
	else p.pl_date_start end as Placement_start_date--used for [offer_personal_info] > [start_date]
, p.pl_date_due --used for [offer_personal_info] > [invoice_due_date]
, case when p.pl_credit_term is NULL then 0
	else p.pl_credit_term end as terms --used for [offer_personal_info] > [terms]
, p.pl_po_no as ContractNo --used for [offer_personal_info] > [note]
, 'thb' as currency_type --used for [offer] > [currency_type] -->> ONLY VALID PER CLIENT CURRENCY
, case when trim(p.pl_type) in ('Multiple Placement', 'Management Fee', 'Engagement Fee') then p.pl_sub_total_fee
	when trim(p.pl_type) = 'Candidate Placement' then (case when ta.TotalApplicableFee is NULL then 0 else ta.TotalApplicableFee end) 
												- (case when d.TotalDeductions is NULL then 0 else d.TotalDeductions end) 
	else 0 end as AnnualSalary --used for [offer] > [pay_rate] -->> ONLY VALID AS PRTR wants to reflect the final profit || SubTotal from RecruitCraft
, 100 as percentage_of_annual_salary --used for [offer] > [percentage_of_annual_salary] as quick fee forecast %
, 1 as use_quick_fee_forecast --used to check on [use_quick_fee_forecast]
, pl_total_fee --populated to Internal Note after taxes (PRTR's requirements) | Total Fee Due
, t.team as division --used for [offer_personal_info] > [division]
, s.sub_team as billing_group --used for [offer_personal_info] > [billing_group]
, coalesce('Placement ID: ' + convert(varchar(max),p.pl_id),NULL) as invoice_message --appended to [offer_personal_info] > [invoice_message]
, concat_ws(char(10), '[The placement details were migrated from PRTR]'
	, coalesce('Placement status: ' + nullif(trim(p.pl_status),''),NULL)
	, coalesce('Placement type: ' + nullif(trim(p.pl_type),''),NULL)
	, coalesce('Contact number: ' + nullif(trim(p.pl_po_no),''),NULL)
	, coalesce('Placement ID: ' + convert(varchar(max),
						case when trim(p.pl_type) in ('Multiple Placement', 'Management Fee', 'Engagement Fee') then p.pl_sub_total_fee
						when trim(p.pl_type) = 'Candidate Placement' then 
							(case when ta.TotalApplicableFee is NULL then 0 else ta.TotalApplicableFee end) 
							- (case when d.TotalDeductions is NULL then 0 else d.TotalDeductions end) 
						else 0 end),NULL)
	, coalesce('[Total deductions]' + char(10) + nullif(trim(d.DeductionsDetails),''),NULL)
) as PlacementNote --appended to [offer_personal_info] > [note]
	, -10 as Latest_user_id
	, getdate() as Latest_update_date
	, 0 as Tax_rate
	, 'other' as Export_data_to
	, 0 as Net_total
	, 0 as Other_invoice_items_total
	, 0 as Invoice_total
, case when trim(pl_status) in ('Client Cancelled','Cancelled') then getdate() - 7 
	else NULL end as Placement_end_date --used for [offer_personal_info] > [end_date] | update placement to rejected status | updated on 05/12/2018
from placements.Placements p
left join Users.Users u on u.usr_id = p.usr_id
left join Users.Teams t on t.team_id = u.team_id
left join Users.SubTeams s on s.sub_team_id = u.sub_team_id
left join Deductions d on d.pl_id = p.pl_id
left join TotalApplicableFee ta on ta.pl_id = p.pl_id
where p.pl_id in (select max(pl_id) from placements.Placements p1 group by vac_id, can_id)
and p.vac_id > 0 and p.can_id > 0
--and p.pl_id = 53813 --testing 1 placement detail
order by p.pl_id desc