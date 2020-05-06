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
and aa.ApplicantId not in (select ObjectId from SectorObjects where SectorId = 48)
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
and p.ApplicantId not in (select ObjectId from SectorObjects where SectorId = 48)
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

select concat('NP', hs.JobId) as job_ext_id
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
	, hs.StartDate --Offered date / Start Date / Placement Date
	, hs.EndDate --End Date
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
	, coalesce(convert(float,p.Salary),0) as pay_rate --gross_annual_salary
	, 1 as use_quick_fee_forecast
	, coalesce(convert(float,p.CommissionPerc),0) as percentage_of_annual_salary
	, coalesce(convert(float,p.PlacementFee),0) as projected_profit
from higheststage hs
left join Placements p on p.PlacementId = hs.PlacementID
left join userinfo u on u.UserId = p.CreatedUserId
left join userinfo u2 on u2.UserId = p.UpdatedUserId
where rn = 1
and stage = 6
and position_type = 1 --PERMANENT PLACEMENTS