;with
TmpTab1 as (
	select distinct CONT_ID, '_DefCom000' as CLNT_ID from JOBS_DATA_TABLE
	where CONT_ID in (
		-- num of unique contact that need to be created
		select distinct CONT_ID from JOBS_DATA_TABLE
		where CONT_ID is not null and CONT_ID not in (
			select CONT_ID from CONT_DATA_TABLE
		)
	)
	and CLNT_ID not in (
		select CLNT_ID from CLNTINFO_DATA_TABLE
	) -- 86
)

, TmpTab2 as (
	select
	*
	, row_number() over(partition by CLNT_ID order by CONT_ID) as rn
	from TmpTab1
)

--select * from TmpTab2

--insert into VC_Con

select
CLNT_ID as companyId
, concat('', CONT_ID) as externalId
, concat('Contact ', replicate('0', 2 - len(cast(rn as varchar))), rn, ']') as lastName
, '[Default' as firstName
, '' as email
, '' as phone
, '' as jobTitle
, '' as document
, '' as owners
, concat('ExternalID: ', CONT_ID) as note
from TmpTab2
--contact-companyId	contact-externalId	contact-lastName	contact-firstName	contact-email	contact-phone	contact-jobTitle	contact-document	contact-owners	contact-Note

--insert into VC_Con

--select
--'DefCom000' as companyId
--, 'DefCon000' as externalId
--, 'Contact 00]' as lastName
--, '[Default' as firstName
--, '' as email
--, '' as phone
--, '' as jobTitle
--, '' as document
--, '' as owners
--, 'ExternalID: DefCon000' as note