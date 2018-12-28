drop table if exists #VCConIdxsTemp1

;with

ConNoComLink as (
	select Id from Contact
	where AccountId is null
	or AccountId = '000000000000000AAA'
	--or AccountId not in (
	--	select Id from VCAccIdxs
	--)
)

--select * from ConNoComLink

select
c.AccountId
, c.Id
, cast(c.CreatedDate as datetime) as CreatedDate
, trim(concat(trim(isnull(c.FirstName, '')), ' ', trim(isnull(c.LastName, '')))) as FullName
, [dbo].[ufn_PopulateEmailAddress3] (c.Email, c.Email_2__c, c.Assist_email__c) as Emails

into #VCConIdxsTemp1

from Contact c
where c.Id not in (
	select Id from ConNoComLink
)

--select * from #VCConIdxsTemp1

-- split the concated email to multiple rows:
drop table if exists #VCConIdxsTemp2

SELECT vccit1.AccountId, vccit1.Id, vccit1.CreatedDate, FullName, value as Email

into #VCConIdxsTemp2

FROM #VCConIdxsTemp1 vccit1
    CROSS APPLY STRING_SPLIT(Emails, ',');

--select * from #VCConIdxsTemp2

-- add row number column to check duplicate
drop table if exists #VCConIdxsTemp3

select AccountId, Id, FullName, Email, Row_number() over(partition by Email order by cast(CreatedDate as datetime) desc) as RowNum

into #VCConIdxsTemp3

from #VCConIdxsTemp2

--select * from #VCConIdxsTemp3
-- finalize
drop table if exists #VCConIdxsTemp4;

select
AccountId
, Id
, FullName
, iif(len(trim(Email)) = 0, '', iif(RowNum > 1, '(' + cast(RowNum as varchar(10))+ ')' + trim(Email), trim(Email))) as Email

into #VCConIdxsTemp4

from #VCConIdxsTemp3

--select * from #VCConIdxsTemp4

-- finalize
drop table if exists VCConIdxs

select AccountId, Id, FullName, trim(', ' from string_agg(Email, ',')) as Emails

into VCConIdxs

from #VCConIdxsTemp4
group by AccountId, Id, FullName

select * from VCConIdxs

--select * from Account
--where [Name] like '%candidate%'

--select * from Contact
--where AccountId in (
--	'00120000005Pxf8AAC'
--	--'00120000003tIRHAA2'
--)

--select count(*) from VCConIdxs
--where Id like '%DefCon'
--joi