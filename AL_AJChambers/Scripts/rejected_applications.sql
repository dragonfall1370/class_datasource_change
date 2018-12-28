select
[application-positionExternalId] as JobExtId
, [application-candidateExternalId] as CanExtId
, isnull(RejectedDate, DATEADD(day, -1, getdate())) as RejectedDate
from VC_App
where Rejected = 1