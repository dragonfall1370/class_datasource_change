drop table if exists #VCCanIdxsTemp1

select 
c.CandidateID as CanId
, c.CreationDate as CreatedDate
, iif(
	len([dbo].[ufn_PopulateEmailAddress3] (c.Email, c.Email2, '')) = 0
	, concat('no-email-', c.CandidateID, '@noemail.com')
	, [dbo].[ufn_PopulateEmailAddress3] (c.Email, c.Email2, '')
) as Emails
into #VCCanIdxsTemp1

from Candidates c

--select * from #VCCanIdxsTemp1

drop table if exists #VCCanIdxsTemp2

select cit1.CanId, cit1.CreatedDate, value as Email

into #VCCanIdxsTemp2

from #VCCanIdxsTemp1 cit1
    cross apply string_split(cit1.Emails, ',');

--select * from #VCCanIdxsTemp2

drop table if exists #VCCanIdxsTemp1
-- add row number column to check duplicate
drop table if exists #VCCanIdxsTemp3

select cit2.CanId, cit2.Email
, Row_number() over(partition by cit2.Email order by cit2.CreatedDate desc) as RowNum

into #VCCanIdxsTemp3

from #VCCanIdxsTemp2 cit2

--select * from #VCCanIdxsTemp3

drop table if exists #VCCanIdxsTemp2;

drop table if exists #VCCanIdxsTemp4

select
cit3.CanId
, iif(
	len(trim(cit3.Email)) = 0
	, ''
	, iif(
		cit3.RowNum > 1
		, '(' + cast(cit3.RowNum as varchar(10))+ ')' + trim(cit3.Email)
		, trim(cit3.Email)
	)
) as Email

into #VCCanIdxsTemp4

from #VCCanIdxsTemp3 cit3

--select * from #VCCanIdxsTemp4
drop table if exists #VCCanIdxsTemp3

drop table if exists #VCCanIdxsTemp5

select cit4.CanId
, trim(', ' from string_agg(cit4.Email, ',')) as Emails

into #VCCanIdxsTemp5

from #VCCanIdxsTemp4 cit4
group by CanId

--select * from #VCCanIdxsTemp5

drop table if exists #VCComIdxsTemp4

drop table if exists #VCComIdxsTemp6

select
cit5.CanId
, string_agg(u.Email, ',') as OwnerEmails

into #VCComIdxsTemp6

from #VCCanIdxsTemp5 cit5
left join [Ownership] o on cit5.CanId = o.RecordID
left join RecordTypes rt on o.RecordTypeID = rt.RecordTypeID
left join Users u on o.OwnerID = u.UserID
where lower(trim(isnull(rt.Description, ''))) = lower('Candidates')
group by cit5.CanId

--select * from #VCComIdxsTemp6

drop table if exists #VCCanIdxsTemp7

select
cit5.CanId
, concat(trim(isnull(x.FirstName, '')), ' ', trim(isnull(x.Surname, ''))) as FullName
, cit5.Emails
, cit6.OwnerEmails
, [dbo].[ufn_PopulateLocationAddressUK](
	x.Address1,
	x.Address2,
	x.Address3,
	x.Town,
	x.County,
	x.PostCode,
	trim(isnull(c.Description, 'UK')),
	'., '
)
as FullAddress
, trim(isnull(c.Description, 'UK')) as Country
, trim(isnull(n.Description, '')) as Nationality

into #VCCanIdxsTemp7

from #VCCanIdxsTemp5 cit5
left join #VCComIdxsTemp6 cit6 on cit5.CanId = cit6.CanId
left join Candidates x on cit5.CanId = x.CandidateID
left join Countries c on x.CountryID = c.CountryID
left join Nationality n on x.Nationality = n.NationalityID

drop table if exists #VCCanIdxsTemp5
drop table if exists #VCCanIdxsTemp6

drop table if exists #VCCanIdxsTemp8

select
cvt.CandidateID
, string_agg(
	case lower(trim(isnull(vt.Description, '')))
		when lower('Permanent') then 'PERMANENT'
		when lower('Contract') then 'CONTRACT'
		when lower('Part Time') then 'TEMPORARY'
		when lower('Temp') then 'TEMPORARY'
	end
	, ','
) as JobTypes

into #VCCanIdxsTemp8

from CandidateVacancyTypes cvt
left join VacancyTypes vt on cvt.VacancyTypeID = vt.VacancyTypeID
group by cvt.CandidateID

--select * from #VCCanIdxsTemp8

drop table if exists VCCanIdxs

select
vcti7.*
, isnull(vcti8.JobTypes, 'PERMANENT,CONTRACT,TEMPORARY') as JobTypes

into VCCanIdxs

from #VCCanIdxsTemp7 vcti7
left join #VCCanIdxsTemp8 vcti8 on vcti7.CanId = vcti8.CandidateID

drop table if exists #VCCanIdxsTemp5
drop table if exists #VCCanIdxsTemp6

select * from VCCanIdxs

--select * from Account
--where [Name] like '%candidate%'

--select * from Contact
--where AccountId in (
--	'00120000005Pxf8AAC'
--	--'00120000003tIRHAA2'
--)

--select count(*) from Candidates
--joi

--DECLARE @VarString NVARCHAR(400) = 'Mike,John,Miko,Matt';
--SELECT value
--FROM STRING_SPLIT(@VarString, ',');

--ALTER DATABASE [YorkStreetProd]
--SET COMPATIBILITY_LEVEL = 130 -- For SQL Server 2016
--GO

--select * from Candidates
--where ContactID in (602, 704, 723)