select
[application-positionExternalId] as JobExtId
, [application-candidateExternalId] as CanExtId
, [application-Stage]
, ActionedDate as AssociatedDate
, Rejected
, isnull(RejectedDate, DATEADD(day, -1, getdate())) as RejectedDate
, case(upper(FinalStage))
	when 'PLACEMENT_PERMANENT' then 301
	when 'PLACEMENT_CONTRACT' then 302
	when 'PLACEMENT_TEMP' then 303
end as position_candidate_status
, 3 as offer_draft_offer
, 1 as offer_valid
, upper(right(FinalStage, len(FinalStage) - len('PLACEMENT_'))) employmentType
, case(upper(right(FinalStage, len(FinalStage) - len('PLACEMENT_')))) 
	when 'PERMANENT' then 1
	when 'CONTRACT' then 2
	when 'TEMP' then 3
end as offer_position_type
, 1 as invoice_valid
, 1 as invoice_renewal_index
, 1 as invoice_renewal_flow_status
, 2 as invoice_status
from VC_App
where left(FinalStage, len('PLACEMENT_')) = 'PLACEMENT_'