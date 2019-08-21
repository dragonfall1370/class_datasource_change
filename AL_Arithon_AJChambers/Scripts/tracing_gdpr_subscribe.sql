
select count(*) from 
(
select
cast(cast(CAND_ID as int) as varchar(20)) as entityExtId
, iif(cast(OPTOUT as int) = 1, 0, 1) as Subscribed
, cast('2018/12/01' as datetime) as SubscribedDate
from CANDINFO_DATA_TABLE
where OPTOUT is not null
--order by CAND_ID
) x
where x.Subscribed = 0


;with TmpTab1 as (
select
cast(CAND_ID as int) as entityExtId
--, case(OPTOUT)
--	when NULL then NULL
--	when 1 then 1
--	when 0 then 2
--end as consent_level
, iif(lower(trim(isnull(GDPR_ACCEPT, ''))) = lower('Yes'), 1, null) as consent_level
, case(lower(trim(isnull(GDPR_ACCEPT, ''))))
	when '' then NULL
	when lower('Yes') then 1
	when lower('No') then 0
end as explicit_consent
, iif(lower(trim(isnull(GDPR_ACCEPT, ''))) = lower('Yes'), 1, null) as portal_status
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

, TmpTab2 as (
select
*
, request_through as obtained_through
, request_through_date as obtained_through_date
, 3 as exercise_right
, isnull(request_through_date, getdate()) as insert_timestamp
, iif(explicit_consent = 1, request_through_date, null) as activated_date
from TmpTab1
--where explicit_consent is not null
)

select count(*) from TmpTab2
where explicit_consent = 1