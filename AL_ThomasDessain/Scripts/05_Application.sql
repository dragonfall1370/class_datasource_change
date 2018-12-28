declare @chars4trim varchar(20) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + N'%?*,. '
declare @NewLineChar as char(1) = char(10);
drop table if exists VC_App

;with
jobInfo as (
	select
		JOBS_ID as JobId
		, case(lower(trim(isnull(x.type, 'Permanent'))))
			when lower('Permanent') then 'PERMANENT'
			when lower('Contract') then 'CONTRACT'
			when lower('Permanent Part-time') then 'PERMANENT'
			when lower('Temp and Perm') then 'PERMANENT'
			when lower('Temporary') then 'TEMPORARY'
			when lower('Temporary Part-time') then 'TEMPORARY'
			else 'PERMANENT'
		end as JobType
	from
	JOBS_DATA_TABLE x
)

, AppTmp1 as (
select
	x.CAND_ID as [application-candidateExternalId]
	, x.JOBS_ID as [application-positionExternalId]
	, 'SENT' as [application-stage]
	, 'SENT' as FinalStage
	, isnull(dateadd(day, -2, cast(x.SENTON as datetime)), getdate()) as ActionedDate
	, iif(trim(isnull(x.STATUS, '')) = 'DROP', 1, 0) as Rejected
	, iif(iif(trim(isnull(x.STATUS, '')) = 'DROP', 1, 0) = 1, isnull(dateadd(day, -2, cast(x.[WHEN] as datetime)), getdate()), null) as RejectedDate
	, lower(iif(
		dbo.ufn_CheckEmailAddress(trim(@chars4trim from isnull(x.USER_ID, ''))) = 1
		, trim(@chars4trim from isnull(x.USER_ID, ''))
		, ''
	)) as ownerEmail
	, null as PlacementNote
from SENDOUTS_DATA_TABLE x
where x.CAND_ID is not null and x.JOBS_ID is not null
and x.CAND_ID in (select CAND_ID from CANDINFO_DATA_TABLE)
and x.JOBS_ID in (select JOBS_ID from JOBS_DATA_TABLE)

union all

select
	x.CAND_ID as [application-candidateExternalId]
	, x.JOBS_ID as [application-positionExternalId]
	, case(upper(trim(isnull(x.TYPE, ''))))
		when 'FIRST' then 'FIRST_INTERVIEW'
		when 'SECOND' then 'SECOND_INTERVIEW'
		when 'THIRD' then 'SECOND_INTERVIEW'
	end as [application-stage]
	, case(upper(trim(isnull(x.TYPE, ''))))
		when 'FIRST' then 'FIRST_INTERVIEW'
		when 'SECOND' then 'SECOND_INTERVIEW'
		when 'THIRD' then 'SECOND_INTERVIEW'
	end as FinalStage
	, isnull(dateadd(day, -2, cast(x.ENTERED as datetime)), getdate()) as ActionedDate
	, iif(trim(isnull(x.STATUS, '')) = 'DROP', 1, 0) as Rejected
	, iif(iif(trim(isnull(x.STATUS, '')) = 'DROP', 1, 0) = 1, isnull(dateadd(day, -2, cast(x.[WHEN] as datetime)), getdate()), null) as RejectedDate
	, lower(iif(
		dbo.ufn_CheckEmailAddress(trim(@chars4trim from isnull(x.USER_ID, ''))) = 1
		, trim(@chars4trim from isnull(x.USER_ID, ''))
		, ''
	)) as ownerEmail
	, null as PlacementNote
from INTERVIEWS_DATA_TABLE x
where x.CAND_ID is not null and x.JOBS_ID is not null
and x.CAND_ID in (select CAND_ID from CANDINFO_DATA_TABLE)
and x.JOBS_ID in (select JOBS_ID from JOBS_DATA_TABLE)

union all

select
	x.CAND_ID as [application-candidateExternalId]
	, x.JOBS_ID as [application-positionExternalId]
	, 'OFFERED' as [application-stage]
	, case(y.JobType)
		when 'PERMANENT' then 'PLACEMENT_PERMANENT'
		when 'CONTRACT' then 'PLACEMENT_CONTRACT'
		when 'TEMPORARY' then 'PLACEMENT_TEMP'
	end as FinalStage
	, isnull(dateadd(day, -2, cast(x.[WHEN] as datetime)), getdate()) as ActionedDate
	, 0 as Rejected
	, null as RejectedDate
	, lower(iif(
		dbo.ufn_CheckEmailAddress(trim(@chars4trim from isnull(x.USER_ID, ''))) = 1
		, trim(@chars4trim from isnull(x.USER_ID, ''))
		, ''
	)) as ownerEmail
	, concat(
		nullif(concat('Consultant: ', x.user_id, @NewLineChar), concat('Consultant: ', @NewLineChar))
		, nullif(concat('Salary: ', FORMAT(x.salary, '#,#', 'en-gb'), ' GBP', @NewLineChar), concat('Salary: ', ' GBP', @NewLineChar))
		, nullif(concat('Fee: ', FORMAT(x.fee, '#,#.####', 'en-gb'), '%', @NewLineChar), concat('Fee: ', '%', @NewLineChar))
		, nullif(concat('Start Date: ', FORMAT(dateadd(day, -2, cast(x.start_date as datetime)), 'dd-MMM-yyyy', 'en-gb'), @NewLineChar), concat('Start Date: ', @NewLineChar))
		, nullif(concat('Candidate Owner ID: ', x.cand_owner_id, @NewLineChar), concat('Candidate Owner ID: ', @NewLineChar))
		, nullif(concat('Fee Value: ', FORMAT(cast(x.fee_value as decimal), '#,#', 'en-gb'), ' GBP', @NewLineChar), concat('Fee Value: ', ' GBP', @NewLineChar))
		, nullif(concat('Candidate Owner Value: ', x.cand_owner_value, @NewLineChar), concat('Candidate Owner Value: ', @NewLineChar))
		, nullif(concat('Placement Owner Value: ', x.plac_owner_value, @NewLineChar), concat('Placement Owner Value: ', @NewLineChar))
		, nullif(concat('Research ID: ', x.research_id), concat('Research ID: ', ''))
	) as PlacementNote
from PLACEMENTS_DATA_TABLE x
inner join jobInfo y on x.JOBS_ID = y.JobId
where x.CAND_ID is not null and x.JOBS_ID is not null
and x.CAND_ID in (select CAND_ID from CANDINFO_DATA_TABLE)
and x.JOBS_ID in (select JOBS_ID from JOBS_DATA_TABLE)

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

select * from VC_App
--where [application-positionExternalId] not in (select [position-externalID] from VC_Job)
--where [application-candidateExternalId] not in (select [candidate-externalId] from VC_Can)
--where Rejected = 1
order by
ActionedDate
--[application-positionExternalId]
--, [application-candidateExternalId]


--select distinct [application-positionExternalId]
--, [application-candidateExternalId] from VC_App