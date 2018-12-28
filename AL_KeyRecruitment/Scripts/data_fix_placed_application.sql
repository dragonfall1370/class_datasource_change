--select * from VCApplications
--where
-- left(ApplicationFinalStage, len('PLACEMENT_')) = 'PLACEMENT_'
----left([application-Stage], len('OFFERED')) = 'OFFERED'

select
[application-positionExternalId] as JobExtId
, [application-candidateExternalId] as CanExtId
, [application-Stage]
, ApplicationFinalStage
, dateAdded as AssociatedDate
, employmentType
, Rejected
, isnull(RejectedDate, DATEADD(day, -1, getdate())) as RejectedDate
, case(upper(ApplicationFinalStage))
	when 'PLACEMENT_PERMANENT' then 301
	when 'PLACEMENT_CONTRACT' then 302
	when 'PLACEMENT_TEMP' then 303
end as position_candidate_status
, 3 as offer_draft_offer
, 1 as offer_valid
, case(upper(employmentType)) 
	when 'PERMANENT' then 1
	when 'CONTRACT' then 2
	when 'TEMPORARY' then 3
end as offer_position_type
, 1 as invoice_valid
, 1 as invoice_renewal_index
, 1 as invoice_renewal_flow_status
, 2 as invoice_status
from VCApplications
where
Rejected = 1
 --left(ApplicationFinalStage, len('PLACEMENT_')) = 'PLACEMENT_'
--left([application-Stage], len('OFFERED')) = 'OFFERED'