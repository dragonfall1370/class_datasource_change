with jobApp as (select concat('AA', aa.ApplicantActionId) as AppID
, aa.ApplicantId
, aa.JobId
, j.EmploymentTypeId
, case 
    when j.EmploymentTypeId in (5, 6, 7, 8, 10, 11, 13) then 2 --CONTRACT
    when j.EmploymentTypeId in (4, 9, 12) then 1 --PERMANENT
    else 2 end as position_type
, aa.ClientContactId
, aa.ClientId
--, aa.CVSentDate
, aa.StatusId
, case aas.Description
	when 'Hold' then 1 --"SHORTLISTED > REJECTED"
	when 'Delete' then 1 --"SHORTLISTED > REJECTED"
	when 'Client Withdrew Job' then 1 --"SHORTLISTED > REJECTED"
	when '1-SendCV' then 1
	when '2-CVSent' then 2
	when 'Withdrew' then 0 --"SENT > REJECTED"
	when '3-Reject' then 2 --"SENT > REJECTED"
	when '4-Interview' then 3
	when '5-ReInterview' then 4
	when '6-IntOffer' then 5
	when '7-OfferAccept' then 6
	--when '8-IntReject' then 5
	when '9-OfferReject' then 5
	when 'aitv' then 0
	when 'stilwekk' then 0
	else 0 end as stage
, aas.Description as substatus
, case --when aa.StatusId in (27, 40, 41) then coalesce(aa.StatusDate, aa.UpdatedOn)
	when aas.Description in ('Hold', 'Delete', 'Client Withdrew Job') then coalesce(aa.StatusDate, aa.UpdatedOn)
	else NULL end as rejected_date
, aa.StatusDate
, aa.PlacementId
, aa.CreatedOn
, aa.UpdatedOn
, aa.Archived as Reference--check if to remove
--PLACEMENT DETAILS
, coalesce(aa.StatusDate, aa.CreatedOn) as StartDate
, NULL as EndDate
from ApplicantActions aa
left join ApplicantActionStatus aas on aas.ApplicantActionStatusId = aa.StatusId --Description
left join Jobs j on j.JobId = aa.JobId --get position type
where aa.JobID is not NULL and aa.ApplicantId is not NULL
and aa.ApplicantId in (select ObjectId from cand_2sector)
and j.ClientId not in (select ObjectId from SectorObjects where SectorId = 49)

UNION ALL

/* NO NEED INTERVIEWS
select aa.ApplicantActionId
, aa.ApplicantId
, aa.JobId
, aa.ClientContactId
, aa.ClientId
, aa.CVSentDate
, aa.StatusId
, aas.Description
, aas.Description as substatus
, aa.StatusDate
, i.InterviewId
, i.CreatedOn
, convert(datetime, i.InterviewDate + i.InterviewTime, 120) as InterviewDT
, i.InterviewTypeId
, it.Description as InterviewStatus
from Interviews i
left join InterviewTypes it on it.InterviewTypeId = i.InterviewTypeId --Interview mapping
left join ApplicantActions aa on aa.ApplicantActionId = i.ApplicantActionId --trace job/applicant info
left join ApplicantActionStatus aas on aas.ApplicantActionStatusId = aa.StatusId
where aa.ApplicantId is not NULL and aa.JobId is not NULL
*/

select concat('PL', p.PlacementID) as AppID
, p.ApplicantId
, p.JobId
, p.PlacementTypeId
, case 
    when p.PlacementTypeId in (5, 6, 7, 8, 10, 11, 13) then 2
    when p.PlacementTypeId in (4, 9, 12) then 1
    else 2 end as position_type
, p.ClientContactId
, p.ClientId
, 0 as StatusId
, 6 as stage --'PLACED'
, NULL as substatus
, NULL as rejected_date
, NULL as StatusDate
, p.PlacementID
, p.CreatedOn
, p.UpdatedOn
, p.StartCheckOK as Reference--check if to remove
, coalesce(p.StartDate, p.CreatedOn) as StartDate
, EndDate
from Placements p
left join Jobs j on j.JobId = p.JobId
where p.JobID is not NULL and p.ApplicantId is not NULL
and p.ApplicantId in (select ObjectId from cand_2sector)
and j.ClientId not in (select ObjectId from SectorObjects where SectorId = 49)
)

, higheststage as (select AppID
	, ApplicantId
	, JobId
	, position_type
	, ClientContactId
	, ClientId
	, stage
	, substatus
	, rejected_date
	, PlacementID
	, CreatedOn
	, row_number() over (partition by ApplicantId, JobId order by stage desc, CreatedOn desc) as rn --highest stage, latest date
	, StartDate
	, EndDate
	from jobApp
	where stage > 0
	)

, userinfo as (select u.UserId,u.LoginName,u.UserName,u.UserFullName,u.JobTitle,u.Inactive
				from Users u)

, contractdetails as (select concat('NP', hs.JobId) as job_ext_id
	, concat('NP', hs.ApplicantId) as cand_ext_id
	, AppID
	, case when position_type = 1 and stage = 6 then 'PLACED_PERMANENT'
		when position_type = 2 and stage = 6 then 'PLACED_CONTRACT'
		else NULL end as PlacedStage
	, case when position_type = 1 and stage = 6 then 301
		when position_type = 2 and stage = 6 then 302
		else NULL end as position_status
	, position_type
	, substatus
	, hs.CreatedOn
	, rejected_date
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
	, hs.StartDate --Offered date / Start Date / Placement Date
	, coalesce(hs.EndDate, dateadd(month,3,hs.StartDate)) as EndDate --End Date
	, coalesce(nullif(concat_ws(char(10)
		, coalesce('--Description--' + char(10) + nullif(p.Description,'') + char(10),NULL)
		, coalesce('Start Check OK: ' + nullif(p.StartCheckOK,''), NULL)
		, coalesce('Consultants: ' + nullif(u.UserFullName,''), NULL)
		, coalesce('Updated by: ' + nullif(u.UserFullName,''), NULL)
		, coalesce('Updated on: ' + convert(varchar(max),nullif(p.UpdatedOn,''),120), NULL)
		, coalesce('Purchase Order No: ' + nullif(p.PurchaseOrderNo,''),NULL)           
        , coalesce('Invoice Contact ID: ' + nullif(convert(varchar(max),p.InvoiceContactID),''),NULL)
        , coalesce('--Work Address--' + char(10) + nullif(p.WorkAddress,'') + char(10),NULL)
		, coalesce('Salary: ' + nullif(convert(varchar(max),p.Salary),''),NULL)
        , coalesce('CommissionPerc: ' + nullif(convert(varchar(max),p.CommissionPerc),''),NULL)
        , coalesce('Placement Fee: ' + nullif(convert(varchar(max),p.PlacementFee),''),NULL)
        , coalesce('--Notes--' + char(10) + nullif(p.Notes,''),NULL)
		),'')
			, concat_ws(char(10), 'This offer details was imported as default'
					, coalesce('[Status] ' + substatus, NULL))
		) as PlacementNote
	, coalesce(convert(float,cp.ChargeRate), 0) as charge_rate
	, coalesce(convert(float,cp.PayRate), 0) as pay_rate
	, coalesce(convert(float,cp.ChargeRate) - convert(float,cp.PayRate), 0) as profit
	, case when convert(float,cp.PayRate) = 0 or convert(float,cp.PayRate) is NULL then 0
	else round((convert(float,cp.ChargeRate) - convert(float,cp.PayRate)) * 100 / convert(float,cp.PayRate), 2) end as markup_percent
	, case when convert(float,cp.ChargeRate) = 0 or convert(float,cp.ChargeRate) is NULL then 0
	else round((convert(float,cp.ChargeRate) - convert(float,cp.PayRate)) * 100 / convert(float,cp.ChargeRate), 2) end as margin_percent
	, cast(datediff(wk, hs.StartDate, coalesce(hs.EndDate, dateadd(month,3,hs.StartDate))) as numeric) as weeks
	, cast(datediff(month, hs.StartDate, coalesce(hs.EndDate, dateadd(month,3,hs.StartDate))) as numeric) as months
	, cast((DATEDIFF(dd, hs.StartDate, coalesce(hs.EndDate, dateadd(month,3,hs.StartDate))) + 1)
		- (DATEDIFF(wk, hs.StartDate, coalesce(hs.EndDate, dateadd(month,3,hs.StartDate))) * 2)
		- (CASE WHEN DATENAME(dw, hs.StartDate) = 'Sunday' THEN 1 ELSE 0 END)
		- (CASE WHEN DATENAME(dw, coalesce(hs.EndDate, dateadd(month,3,hs.StartDate))) = 'Saturday' THEN 1 ELSE 0 END) as numeric) as workdays
	, lv.ValueName as charge_rate_unit
	, cast(cp.HoursPerWeek as float) as HoursPerWeek
from higheststage hs
left join Placements p on p.PlacementId = hs.PlacementID
left join ContractPlacements cp on cp.PlacementId = hs.PlacementId
left join userinfo u on u.UserId = p.CreatedUserId
left join userinfo u2 on u2.UserId = p.UpdatedUserId
left join ListValues lv on lv.ListValueId = cp.ChargeUnitValueId
where rn = 1
and stage = 6
and position_type = 2 --CONTRACT PLACEMENTS
)

select *
, case when charge_rate_unit = 'Hour' then round(profit * (case when weeks = 0 then workdays/5 else weeks end) * HoursPerWeek, 2)
	when charge_rate_unit = 'Day' then round(profit * workdays, 2)
	when charge_rate_unit = 'Week' then round(profit * weeks, 2)
	when charge_rate_unit = 'Month' then round( profit * months, 2)
	when charge_rate_unit is NULL and HoursPerWeek is not NULL then round(profit * weeks * HoursPerWeek, 2)
	else 0 end as projected_profit
, case when charge_rate_unit = 'Hour' then round(pay_rate * (case when weeks = 0 then workdays/5 else weeks end) * HoursPerWeek, 2)
	when charge_rate_unit = 'Day' then round(pay_rate * workdays, 2)
	when charge_rate_unit = 'Week' then round(pay_rate * weeks, 2)
	when charge_rate_unit = 'Month' then round( pay_rate * months, 2)
	when charge_rate_unit is NULL and HoursPerWeek is not NULL then round(pay_rate * weeks * HoursPerWeek, 2)
	else 0 end as projected_pay_rate
, case when charge_rate_unit = 'Hour' then round(charge_rate * (case when weeks = 0 then workdays/5 else weeks end) * HoursPerWeek, 2)
	when charge_rate_unit = 'Day' then round(charge_rate * workdays, 2)
	when charge_rate_unit = 'Week' then round(charge_rate * weeks, 2)
	when charge_rate_unit = 'Month' then round( charge_rate * months, 2)
	when charge_rate_unit is NULL and HoursPerWeek is not NULL then round(charge_rate * weeks * HoursPerWeek, 2)
	else 0 end as projected_charge_rate
, case when HoursPerWeek is NULL then 8
	else HoursPerWeek / 5 end as working_hours_per_day
, coalesce(HoursPerWeek, 40) as working_hours_per_week
, 5 as working_days_per_week
, 22 as DaysPerMonth
, 4.2 as WeeksperMonth
, case when charge_rate_unit = 'Hour' then 1
	when charge_rate_unit = 'Day' then 2
	when charge_rate_unit = 'Week' then 3
	when charge_rate_unit = 'Month' then 4
	when charge_rate_unit is NULL or HoursPerWeek is not NULL then 4
	else 4 end as contract_rate_type
, case when charge_rate_unit = 'Hour' then 1
	when charge_rate_unit = 'Day' then 2
	when charge_rate_unit = 'Week' then 3
	when charge_rate_unit = 'Month' then 4
	when charge_rate_unit is NULL and HoursPerWeek is not NULL then 4
	else 4 end as contract_length_type
, case when charge_rate_unit = 'Hour' then workdays / 5 * coalesce(HoursPerWeek, 0)
	when charge_rate_unit = 'Day' then workdays
	when charge_rate_unit = 'Week' then weeks
	when charge_rate_unit = 'Month' then months
	when charge_rate_unit is NULL and HoursPerWeek is not NULL then 0
	else 4 end as contract_length
, 1 as placed_valid
, 1 as pay_calculation_type
, 'chargeRate' as charge_rate_type --profit | margin | markup | chargeRate
from contractdetails
where 1=1
--and appID = 'PL4007' --check reference
--and charge_rate_unit = 'Day'
--and job_ext_id = 'NP14002'
--and cand_ext_id = 'NP108829'