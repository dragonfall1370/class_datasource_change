drop table if exists #VCConIdxsTemp1

select 
c.ContactID as ConId
, c.CreationDate as CreatedDate
, [dbo].[ufn_PopulateEmailAddress3] (c.Email, c.Email2, c.PersonalEmail) as Emails
into #VCConIdxsTemp1

from Contacts c

--select * from #VCConIdxsTemp1

drop table if exists #VCConIdxsTemp2

select cit1.ConId, cit1.CreatedDate, value as Email

into #VCConIdxsTemp2

from #VCConIdxsTemp1 cit1
    cross apply string_split(cit1.Emails, ',');

--select * from #VCConIdxsTemp2

drop table if exists #VCConIdxsTemp1
-- add row number column to check duplicate
drop table if exists #VCConIdxsTemp3

select cit2.ConId, cit2.Email
, Row_number() over(partition by cit2.Email order by cit2.CreatedDate desc) as RowNum

into #VCConIdxsTemp3

from #VCConIdxsTemp2 cit2

--select * from #VCConIdxsTemp3

drop table if exists #VCConIdxsTemp2;

drop table if exists #VCConIdxsTemp4

select
cit3.ConId
, iif(
	len(trim(cit3.Email)) = 0
	, ''
	, iif(
		cit3.RowNum > 1
		, '(' + cast(cit3.RowNum as varchar(10))+ ')' + trim(cit3.Email)
		, trim(cit3.Email)
	)
) as Email

into #VCConIdxsTemp4

from #VCConIdxsTemp3 cit3

--select * from #VCConIdxsTemp4
drop table if exists #VCConIdxsTemp3

drop table if exists #VCConIdxsTemp5

select cit4.ConId
, trim(', ' from string_agg(cit4.Email, ',')) as Emails

into #VCConIdxsTemp5

from #VCConIdxsTemp4 cit4
group by ConId

--select * from #VCConIdxsTemp5

drop table if exists #VCComIdxsTemp4

drop table if exists #VCComIdxsTemp6

select
cit5.ConId
, string_agg(u.Email, ',') as OwnerEmails

into #VCComIdxsTemp6

from #VCConIdxsTemp5 cit5
left join [Ownership] o on cit5.ConId = o.RecordID
left join RecordTypes rt on o.RecordTypeID = rt.RecordTypeID
left join Users u on o.OwnerID = u.UserID
where lower(trim(isnull(rt.Description, ''))) = lower('Contacts')
group by cit5.ConId

--select * from #VCComIdxsTemp6

drop table if exists #VCComIdxsTemp7

select
cit5.ConId, cit5.Emails, cit6.OwnerEmails

into #VCComIdxsTemp7

from #VCConIdxsTemp5 cit5
left join #VCComIdxsTemp6 cit6 on cit5.ConId = cit6.ConId

--select * from #VCComIdxsTemp7

drop table if exists #VCConIdxsTemp5
drop table if exists #VCComIdxsTemp6

drop table if exists #VCComIdxsTemp8

select
c.ContactID as ConId
, c.ParentContactID as ParentId
, concat(trim(isnull(c.FirstName, '')), ' ', trim(isnull(c.Surname, ''))) as FullName

into #VCComIdxsTemp8

from Contacts c

drop table if exists #VCComIdxsTemp9

select
cit8.*
, concat(trim(isnull(c.FirstName, '')), ' ', trim(isnull(c.Surname, ''))) as ParentFullName
, trim(coalesce(c.Email, c.Email2, c.PersonalEmail, '')) as ParentEmail

into #VCComIdxsTemp9

from #VCComIdxsTemp8 cit8
left join Contacts c on c.ContactID = cit8.ParentId

drop table if exists #VCComIdxsTemp8

-- finalize
drop table if exists VCConIdxs

select
cit7.ConId, cit7.Emails, cit7.OwnerEmails
, cit9.FullName, cit9.ParentEmail, cit9.ParentFullName

into VCConIdxs

from #VCComIdxsTemp7 cit7
left join #VCComIdxsTemp9 cit9 on cit7.ConId = cit9.ConId

drop table if exists #VCConIdxsTemp7
drop table if exists #VCConIdxsTemp9

select * from VCConIdxs

--select * from Account
--where [Name] like '%candidate%'

--select * from Contact
--where AccountId in (
--	'00120000005Pxf8AAC'
--	--'00120000003tIRHAA2'
--)

--select count(*) from Contacts
--joi

--DECLARE @VarString NVARCHAR(400) = 'Mike,John,Miko,Matt';
--SELECT value
--FROM STRING_SPLIT(@VarString, ',');

--ALTER DATABASE [YorkStreetProd]
--SET COMPATIBILITY_LEVEL = 130 -- For SQL Server 2016
--GO

--select * from Contacts
--where ContactID in (602, 704, 723)