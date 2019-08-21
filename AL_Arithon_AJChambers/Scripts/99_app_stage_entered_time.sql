with TmpTab1 as (
	select
	x.JOBS_ID as JobExtId
	, x.CAND_ID as CanExtId
	, cast(x.SENTON as datetime) as SentDate
	, null as FirstInterviewDate
	, null as SecondInterviewDate
	, null as PlacedDate
	from SENDOUTS_DATA_TABLE x
	where x.CAND_ID is not null and x.JOBS_ID is not null
	and x.CAND_ID in (select CAND_ID from CANDINFO_DATA_TABLE)
	and x.JOBS_ID in (select JOBS_ID from JOBS_DATA_TABLE)

	union all

	select
	x.JOBS_ID as JobExtId
	,x.CAND_ID as CanExtId
	, null as SentDate
	, iif(upper(trim(isnull(x.TYPE, ''))) = 'FIRST', cast(x.[WHEN] as datetime), null) as FirstInterviewDate
	, iif(upper(trim(isnull(x.TYPE, ''))) = 'SECOND' or upper(trim(isnull(x.TYPE, ''))) = 'THIRD', cast(x.[WHEN] as datetime), null) as SecondInterviewDate
	, null as PlacedDate
	from INTERVIEWS_DATA_TABLE x
	where x.CAND_ID is not null and x.JOBS_ID is not null
	and x.CAND_ID in (select CAND_ID from CANDINFO_DATA_TABLE)
	and x.JOBS_ID in (select JOBS_ID from JOBS_DATA_TABLE)

	union all

	select
	x.JOBS_ID as JobExtId
	, x.CAND_ID as CanExtId
	, null as SentDate
	, null as FirstInterviewDate
	, null as SecondInterviewDate
	, cast(x.[WHEN] as datetime) as PlacedDate
	from PLACEMENTS_DATA_TABLE x
	where x.CAND_ID is not null and x.JOBS_ID is not null
	and x.CAND_ID in (select CAND_ID from CANDINFO_DATA_TABLE)
	and x.JOBS_ID in (select JOBS_ID from JOBS_DATA_TABLE)
)

, TmpTab2 as (
select
cast(JobExtId as varchar) JobExtId
, cast(CanExtId as varchar) CanExtId
, Max(SentDate) SentDate
, Max(FirstInterviewDate) FirstInterviewDate
, Max(SecondInterviewDate) SecondInterviewDate
, Max(PlacedDate) PlacedDate
from TmpTab1
--where CanExtId = 22769 and JobExtId = 16381
group by JobExtId, CanExtId
)

select * from TmpTab2
where SentDate is not null or FirstInterviewDate is not null or SecondInterviewDate is not null or PlacedDate is not null