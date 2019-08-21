declare @chars4trim varchar(20) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + N'%?*,. '
declare @NewLineChar as char(1) = char(10);
drop table if exists VC_App

;with
jobInfo as (
	select
		[position-externalId] as JobId
		, case(lower(trim(isnull(x.[position-type], 'Permanent'))))
			when lower('Permanent') then 'PERMANENT'
			when lower('Contract') then 'CONTRACT'
			when lower('Temporary') then 'TEMPORARY'
			else 'PERMANENT'
		end as JobType
	from
	VC_Job x
)

, AppTmp1 as (
	select
		x.ApplicantId as [application-candidateExternalId]
		, x.JobId as [application-positionExternalId]
		, case(upper(trim(isnull(x.LatestStage, ''))))
			when upper('1-SendCV') then 'SHORTLISTED'
			when upper('2-CVSent') then 'SENT'
			when upper('4-Interview') then 'FIRST_INTERVIEW'
			when upper('5-ReInterview') then 'SECOND_INTERVIEW'
			when upper('7-OfferAccept') then 'OFFERED'
		end as [application-stage]
		, case(upper(trim(isnull(x.LatestStage, ''))))
			when upper('1-SendCV') then 'SHORTLISTED'
			when upper('2-CVSent') then 'SENT'
			when upper('4-Interview') then 'FIRST_INTERVIEW'
			when upper('5-ReInterview') then 'SECOND_INTERVIEW'
			when upper('7-OfferAccept') then
					case(y.JobType)
						when 'PERMANENT' then 'PLACEMENT_PERMANENT'
						when 'CONTRACT' then 'PLACEMENT_CONTRACT'
						when 'TEMPORARY' then 'PLACEMENT_TEMP'
					end
		end as FinalStage
		, isnull(cast(x.CreatedOn as datetime), getdate()) as ActionedDate
		, 0 as Rejected
		, null as RejectedDate
		, x.Users_ConsultantEmailAddress as ownerEmail
		, null as PlacementNote
	from VC_Applications3 x
	inner join jobInfo y on x.JobId = y.JobId
	where x.ApplicantId is not null and x.JobId is not null
	and x.ApplicantId in (select [candidate-externalId] from VC_Can)
	and x.JobId in (select [position-externalId] from VC_Job)
)

, AppTmp2 as (
select
	[application-positionExternalId]
	, [application-candidateExternalId]
	, [application-stage]
	, FinalStage
	, ActionedDate
	, Rejected
	, RejectedDate
	, row_number() over (
		partition by [application-positionExternalId], [application-candidateExternalId], [application-stage]
		order by ActionedDate desc, Rejected desc, RejectedDate desc
	) as rn
	, ownerEmail
	, PlacementNote
from AppTmp1
)

--select * from AppTmp2

, AppTmp3 as (
	select
		[application-positionExternalId]
		, [application-candidateExternalId]
		, [application-stage]
		, case([application-stage])
			--when 'PLACEMENT_PERMANENT' THEN 1
			--when 'PLACEMENT_CONTRACT' THEN 1
			--when 'PLACEMENT_TEMP' THEN 1
			when 'OFFERED' then 2
			when 'SECOND_INTERVIEW' then 3
			when 'FIRST_INTERVIEW' then 4
			when 'SENT' then 5
			when 'SHORTLISTED' then 6
		end as AppStage
		, FinalStage
		, ActionedDate
		, Rejected
		, RejectedDate
		, ownerEmail
		, PlacementNote
	from AppTmp2
	where rn = 1
)

--select * from AppTmp3

, AppTmp4 as (
select
	[application-positionExternalId]
	, [application-candidateExternalId]
	, [application-stage]
	, row_number() over (
		partition by [application-positionExternalId], [application-candidateExternalId]
		order by AppStage asc
	) as rn
	, FinalStage
	, ActionedDate
	, Rejected
	, RejectedDate
	, ownerEmail
	, PlacementNote
from AppTmp3
)

select
	cast([application-positionExternalId] as varchar(10)) as [application-positionExternalId]
	, cast([application-candidateExternalId] as varchar(10)) as [application-candidateExternalId]
	, [application-stage]
	, FinalStage
	, ActionedDate
	, Rejected
	, RejectedDate
	, ownerEmail
	, PlacementNote

into VC_App

from AppTmp4
where rn = 1

select
--top 100
[application-positionExternalId]
, [application-candidateExternalId]
, [application-stage]
, convert(varchar(20), ActionedDate, 111) as ActionedDate
from VC_App -- 698577
--where [application-positionExternalId] not in (select [position-externalID] from VC_Job)
--where [application-candidateExternalId] not in (select [candidate-externalId] from VC_Can)
--where Rejected = 1
--where [application-stage] = 'SECOND_INTERVIEW'
--where [application-positionExternalId] = '890802'
order by
ActionedDate
--[application-positionExternalId]
--, [application-candidateExternalId]


--select distinct [application-positionExternalId]
--, [application-candidateExternalId] from VC_App