drop table if exists #EmailTmp1

select
UC.userID
, lower(iif([dbo].ufn_CheckEmailAddress([dbo].ufn_TrimSpecialCharacters_V2(UC.email, '')) = 1, [dbo].ufn_TrimSpecialCharacters_V2(UC.email, ''), '')) as email
, lower(iif([dbo].ufn_CheckEmailAddress([dbo].ufn_TrimSpecialCharacters_V2(UC.email2, '')) = 1, [dbo].ufn_TrimSpecialCharacters_V2(UC.email, ''), '')) as email2
, lower(iif([dbo].ufn_CheckEmailAddress([dbo].ufn_TrimSpecialCharacters_V2(UC.email3, '')) = 1, [dbo].ufn_TrimSpecialCharacters_V2(UC.email, ''), '')) as email3

into #EmailTmp1

from bullhorn1.BH_UserContact UC
left join bullhorn1.BH_Client Cl on Cl.userID = UC.userID
where Cl.isPrimaryOwner = 1
--and UC.externalEmail is not null
--and len(trim(isnull(UC.email_old, ''))) > 0
--and len(trim(isnull(email21, ''))) > 0

drop table if exists #EmailTmp2

select
userID
, iif(charindex('[', email) > 0, right(email, len(email) - charindex('[', email)), email) as email
, email2
, email3

into #EmailTmp2

from #EmailTmp1
--where len(trim(isnull(email2, ''))) > 0 or len(trim(isnull(email3, ''))) > 0
--where email like '%]%'

drop table if exists #EmailTmp1

--select * from #EmailTmp2
--where email like '%]%'

drop table if exists #EmailTmp3

select
userID
, coalesce(
	nullif(email, '')
	, nullif(email2, '')
	, nullif(email3, '')
	, ''
) as email

into #EmailTmp3

from #EmailTmp2

--select * from #EmailTmp3

drop table if exists #EmailTmp2
drop table if exists #EmailTmp4

select userID, email
, Row_number() over(partition by email order by userId desc) as RowNum

into #EmailTmp4

from #EmailTmp3

--select * from #VCCanIdxsTemp3

drop table if exists #EmailTmp3;

drop table if exists VCConEmails

select
userID
, iif(
	len(trim(email)) = 0
	, ''
	, iif(
		RowNum > 1
		, concat('', cast(RowNum as varchar(10)), '_', trim(email))
		, trim(email)
	)
) as Email

into VCConEmails

from #EmailTmp4

drop table if exists #EmailTmp4

select * from VCConEmails
--where charindex(',', Email) > 0