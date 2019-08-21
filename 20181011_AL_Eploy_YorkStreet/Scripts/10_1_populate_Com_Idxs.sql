drop table if exists #VCComIdxsTemp1

select
x.CompanyID
, x.[Name]
, row_number() over(partition by lower(trim(isnull(x.[Name], ''))) order by x.CompanyID desc) as RowNum

into #VCComIdxsTemp1

from CompanyDetails x

--select CompanyID from #VCComIdxsTemp

drop table if exists #VCComIdxsTemp2

select
CompanyID as ComId
, iif(
	RowNum = 1
	, trim(isnull([Name], ''))
	, concat(
		trim(isnull([Name], ''))
		, '('
		, RowNum
		, ')'
	)
) as ComName

into #VCComIdxsTemp2

from #VCComIdxsTemp1

drop table if exists #VCComIdxsTemp1

drop table if exists #VCComIdxsTemp3

select

ci2.*

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

, x.ParentCompanyID
, x.HeadOffice
, trim(isnull(c.Description, 'UK')) as Country

into #VCComIdxsTemp3

from #VCComIdxsTemp2 ci2
left join CompanyDetails x on ci2.ComId = x.CompanyID
left join Countries c on x.CountryID = c.CountryID

drop table if exists #VCComIdxsTemp2

drop table if exists #VCComIdxsTemp4

select 
ci3.ComId
, ci3.ComName
, ci3.FullAddress
, ci3.Country
, iif(
	ci3.HeadOffice = 1-- or ci3.ParentCompanyID = 0
	, ci3.FullAddress
	, iif(
		ci3.ParentCompanyID = 0
		, ci3.FullAddress
		, [dbo].[ufn_PopulateLocationAddressUK](
			x.Address1,
			x.Address2,
			x.Address3,
			x.Town,
			x.County,
			x.PostCode,
			c.Description,
			'., '
		)
	)
)
as HeadQuater

, x.Name as ParentName

into #VCComIdxsTemp4

from #VCComIdxsTemp3 ci3
left join CompanyDetails x on ci3.ParentCompanyID = x.CompanyID
left join Countries c on x.CountryID = c.CountryID

drop table if exists #VCComIdxsTemp3

drop table if exists #VCComIdxsTemp5

select
ci4.ComId
, string_agg(u.Email, ',') as OwnerEmails

into #VCComIdxsTemp5

from #VCComIdxsTemp4 ci4
left join [Ownership] o on ci4.ComId = o.RecordID
left join RecordTypes rt on o.RecordTypeID = rt.RecordTypeID
left join Users u on o.OwnerID = u.UserID
where lower(trim(isnull(rt.Description, ''))) = lower('Companies')
group by ci4.ComId

--drop table if exists #VCComIdxsTemp4

drop table if exists #VCComIdxsTemp6

select
ci4.*
, ci5.OwnerEmails

into #VCComIdxsTemp6

from #VCComIdxsTemp4 ci4
left join #VCComIdxsTemp5 ci5 on ci4.ComId = ci5.ComId

drop table if exists #VCComIdxsTemp4
drop table if exists #VCComIdxsTemp5

drop table if exists [dbo].[VCComIdxs]

select *

into [dbo].[VCComIdxs]

from #VCComIdxsTemp6 ci6

order by ci6.ComId

drop table if exists #VCComIdxsTemp6

--select * from [dbo].[VCComIdxs]
--where Country <> 'UK'-- or Country is null

--select * from Countries where Description like '%UK%'