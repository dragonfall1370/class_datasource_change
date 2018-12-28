--USE AJChambersProd
select
CLNT_ID as ExternalId
, case upper(trim(isnull(INDUSTRY, '')))
	when 'LEGAL' then 'Nelson Chambers'
	when 'INSURANCE' then 'A&F Chambers'
	when 'A&F' then 'A&F Chambers'
	when 'PROPERTY' then 'A&F Chambers'
	when 'PUBLIC PRACTICE' then 'AJ Chambers'
	when 'INTERNAL' then 'AJ Chambers'
	when 'LOCAL AUTHORITY' then 'A&F Chambers'
	else NULL
end as indName
from CLNTINFO_DATA_TABLE
where INDUSTRY is not null
--select distinct INDUSTRY from CLNTINFO_DATA_TABLE

select distinct
case upper(trim(isnull(INDUSTRY, '')))
	when 'LEGAL' then 'Nelson Chambers'
	when 'INSURANCE' then 'A&F Chambers'
	when 'A&F' then 'A&F Chambers'
	when 'PROPERTY' then 'A&F Chambers'
	when 'PUBLIC PRACTICE' then 'AJ Chambers'
	when 'INTERNAL' then 'AJ Chambers'
	when 'LOCAL AUTHORITY' then 'A&F Chambers'
	else NULL
end as indName
, current_timestamp as insert_timestamp
from CLNTINFO_DATA_TABLE
where INDUSTRY is not null
order by indName

select distinct INDUSTRY from CLNTINFO_DATA_TABLE

;with
TmpTab1 as (
	select
	cast(JOBS_ID as int) as entityExtId
	, case upper(trim(isnull(sector, '')))
		when 'LEGAL' then 'Nelson Chambers'
		when 'INSURANCE' then 'A&F Chambers'
		when 'A&F' then 'A&F Chambers'
		when 'PROPERTY' then 'A&F Chambers'
		when 'PUBLIC PRACTICE' then 'AJ Chambers'
		when 'INTERNAL' then 'AJ Chambers'
		when 'LOCAL AUTHORITY' then 'A&F Chambers'
		else NULL
	end as indName
	from JOBS_DATA_TABLE
	where sector is not null
)

--, inds1 as (
--	select
--	trim(' ;' from isnull(cast(businessSectorList as nvarchar(255)), '')) as indNames
--	, clientCorporationID as entityExtId
--	from bullhorn1.BH_ClientCorporation
--	where len(trim(' ;' from isnull(cast(businessSectorList as nvarchar(255)), ''))) > 0 
--)

, TmpTab2 as (
	select
	distinct trim(' ;' from isnull(value, '')) as indName
	, entityExtId
	from TmpTab1
	cross apply string_split(indNames, ';')
)

select
x.entityExtId
, x.indName
from inds2 x
join inds y on lower(x.indName) = lower(trim(isnull(y.indName, '')))
order by x.entityExtId

select
industr
from CONT_DATA_TABLE

select
cast(x.CONT_ID as int) as entityExtId
, case upper(trim(isnull(y.INDUSTRY, '')))
	when 'LEGAL' then 'Nelson Chambers'
	when 'INSURANCE' then 'A&F Chambers'
	when 'A&F' then 'A&F Chambers'
	when 'PROPERTY' then 'A&F Chambers'
	when 'PUBLIC PRACTICE' then 'AJ Chambers'
	when 'INTERNAL' then 'AJ Chambers'
	when 'LOCAL AUTHORITY' then 'A&F Chambers'
	else NULL
end as indName
from
CONT_DATA_TABLE x
left join CLNTINFO_DATA_TABLE y on x.CLNT_ID = y.CLNT_ID
where INDUSTRY is not null
order by x.CONT_ID


select
sector
from JOBS_DATA_TABLE


select
cast(CAND_ID as int) as entityExtId
, LATITUDE
, LONGITUDE
from CANDINFO_DATA_TABLE
where LATITUDE is not null or LONGITUDE is not null

select
cast(CAND_ID as int) as entityExtId
, dbo.ufn_TrimSpecialCharacters_V2(isnull(SOURCE, ''), '') as SourceName
from CANDINFO_DATA_TABLE
where dbo.ufn_TrimSpecialCharacters_V2(isnull(SOURCE, ''), '') <> ''



select distinct
dbo.ufn_TrimSpecialCharacters_V2(isnull(SOURCE, ''), '') as SourceName
from CANDINFO_DATA_TABLE
where dbo.ufn_TrimSpecialCharacters_V2(isnull(SOURCE, ''), '') <> ''
order by dbo.ufn_TrimSpecialCharacters_V2(isnull(SOURCE, ''), '')

;with TmpTab1 as (
select
cast(CAND_ID as int) as entityExtId
, case(OPTOUT)
	when NULL then NULL
	when 1 then 1
	when 0 then 2
end as consent_level
, case(lower(trim(isnull(GDPR_ACCEPT, ''))))
	when '' then NULL
	when lower('Yes') then 1
	when lower('No') then 0
end as explicit_consent
--, GDPR_TYPE
, case(lower(trim(isnull(GDPR_TYPE, ''))))
	when '' then NULL
	when lower('Email') then 1
	when lower('') then 5
	else 6
end as request_through
--, GDPR_DATE
, dateadd(day, -2, cast(cast(GDPR_DATE as float) as datetime)) as request_through_date
--, EXPIRY_DATE
from CANDINFO_DATA_TABLE
--where GDPR_ACCEPT is not null or GDPR_TYPE is not null or GDPR_DATE is not null
)

select
*
, request_through as obtained_through
, request_through_date as obtained_through_date
, 1 as exercise_right
, isnull(request_through_date, getdate()) as insert_timestamp
, iif(explicit_consent = 1, request_through_date, null) as activated_date
from TmpTab1
where request_through_date is not null