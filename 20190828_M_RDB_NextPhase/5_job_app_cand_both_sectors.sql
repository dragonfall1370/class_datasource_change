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
	, CreatedOn
	, row_number() over (partition by ApplicantId, JobId order by stage desc, CreatedOn desc) as rn --highest stage, latest date
	from jobApp
	where stage > 0
	)

select concat('NP', JobId) as [application-positionExternalId]
	, concat('NP', ApplicantId) as [application-candidateExternalId]
	, AppID
	, case when position_type = 1 and stage = 6 then 'PLACED_PERMANENT'
		when position_type = 2 and stage = 6 then 'PLACED_CONTRACT'
		else NULL end as PlacedStage
	, position_type
	, substatus
	, case 
			when Stage = 6 then 'OFFERED' --to be updated as PLACED afterward
			when Stage = 5 then 'OFFERED'
			when Stage = 4 then 'SECOND_INTERVIEW'
			when Stage = 3 then 'FIRST_INTERVIEW'
			when Stage = 2 then 'SENT'
			when Stage = 1 then 'SHORTLISTED'
			end as [application-stage]
	, CreatedOn as [application-actionedDate]
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
from higheststage
where rn = 1 --28361 rows || remove deleted records: 20242 rows
--and stage = 6 --2971 rows || remove deleted records: 2256 rows
--total 36 job apps