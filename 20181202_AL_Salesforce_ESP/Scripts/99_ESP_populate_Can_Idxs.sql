drop table if exists #VCConIdxsTemp1

;with

ConNoWorkHistory as (
	select Id from Contact
	where
	(Company_Name__c is null and Date_Joined__c is null)
	and (Compamy_Name__c is null and Date_Joined_2__c is null)
	and (Company_Name_3__c is null and (Date_joined_3__c is null or ISDATE(Date_joined_3__c) = 0))
)

select
c.Id
, cast(c.CreatedDate as datetime) as CreatedDate
, [dbo].[ufn_PopulateEmailAddress3] (c.Email, Email_2__c, Assist_email__c) as Emails

into #VCConIdxsTemp1

from Contact c
where c.Id not in (
	select Id from ConNoWorkHistory
)

--select * from #VCConIdxsTemp1

-- split the concated email to multiple rows:
drop table if exists #VCConIdxsTemp2

SELECT vccit1.Id, vccit1.CreatedDate, isnull(nullif(value, ''), 'no-email@no-email.com') as Email

into #VCConIdxsTemp2

FROM #VCConIdxsTemp1 vccit1
    CROSS APPLY STRING_SPLIT(Emails, ',');

--select * from #VCConIdxsTemp2

-- add row number column to check duplicate
drop table if exists #VCConIdxsTemp3

select Id, Email, Row_number() over(partition by Email order by cast(CreatedDate as datetime) desc) as RowNum

into #VCConIdxsTemp3

from #VCConIdxsTemp2

--select * from #VCConIdxsTemp3
-- finalize
drop table if exists #VCConIdxsTemp4;

select Id
, iif(len(trim(Email)) = 0, '', iif(RowNum > 1, '(' + cast(RowNum as varchar(10)) + ')' + trim(Email), trim(Email))) as Email

into #VCConIdxsTemp4

from #VCConIdxsTemp3

--select * from #VCConIdxsTemp4

-- finalize
drop table if exists [dbo].[VCCanIdxs]

select Id, trim(', ' from string_agg(Email, ',')) as Emails

into [dbo].[VCCanIdxs]

from #VCConIdxsTemp4
group by Id

select * from [dbo].[VCCanIdxs]

--select * from Account
--where [Name] like '%candidate%'

--select * from Contact
--where AccountId in (
--	'00120000005Pxf8AAC'
--	--'00120000003tIRHAA2'
--)

--select count(*) from Contact
--joi


--drop table if exists [dbo].[VCCanIdxs];

--select
--c.Id
--, [dbo].[ufn_PopulateEmailAddress3] (c.Email, Email_2__c, Assist_email__c) as Emails
--, row_number() over(
--	partition by [dbo].[ufn_PopulateEmailAddress3] (c.Email, Email_2__c, Assist_email__c)
--	order by cast(c.CreatedDate as datetime) desc) as RowNum

--into [dbo].[VCCanIdxs]

--from Contact c
--join [dbo].[VCAccIdxs] ais on c.AccountId = ais.Id
--where ais.Name like '%candidate%'