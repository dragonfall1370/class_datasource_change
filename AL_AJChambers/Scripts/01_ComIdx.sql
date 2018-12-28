drop table if exists #TabTmp1

select CLNT_ID as ID
, [dbo].[ufn_TrimSpecialCharacters_V2]((isnull(company, 'Company without name')), '') as Name

into #TabTmp1

from [dbo].[CLNTINFO_DATA_TABLE]

--select * from #TabTmp1
--where ID = 86489

drop table if exists #TabTmp2

select
*
, row_number() over(partition by [Name] order by ID) as rn

into #TabTmp2

from #TabTmp1

drop table if exists #TabTmp1

--select * from #TabTmp2
---where Name like 'Company%'

drop table if exists #TabTmp3

select
ID
--, len(cast(rn as varchar)) IDLen
, iif(
	rn = 1
	, iif(
		left(Name, len('Company without name')) = 'Company without name'
		, concat('[', Name, ' ', replicate('0', 3 - len(cast(rn as varchar))), rn, ']')
		, Name
	)
	, iif(
		left(Name, len('Company without name')) = 'Company without name'
		, concat('[', Name, ' ', replicate('0', 3 - len(cast(rn as varchar))), rn, ']')
		, concat(Name, ' [Dup ', replicate('0', 2 - len(cast(rn as varchar))), rn, ']')
	)
) as Name

into #TabTmp3

from #TabTmp2

drop table if exists #TabTmp2

drop table if exists #TabTmp4

select
ComID
, string_agg(Name, ',') as Docs

into #TabTmp4

from VC_DocsIdx
where ComID is not null
group by ComID

--select * from #TabTmp4

drop table if exists #TabTmp5

select
x.*
, isnull(y.Docs, '') as Docs

into #TabTmp5

from #TabTmp3 x
left join #TabTmp4 y on x.ID = y.ComID

drop table if exists #TabTmp4

drop table if exists #TabTmp6

select
CLNT_ID as ComId

, replace(
	dbo.ufn_PopulateLocationAddress(
		concat(ADDRESS1, ', ', ADDRESS2, ', ', ADDRESS3)
		, CITY
		, COUNTY
		, ZIPCODE
		, isnull(COUNTRY, 'UK')
		, ', '
	)
	, ',,'
	, ','
) as Address

, replace(
	dbo.ufn_PopulateLocationAddress(
		''
		, CITY
		, COUNTY
		, ZIPCODE
		, isnull(COUNTRY, 'UK')
		, ', '
	)
	, ',,'
	, ','
) as LocName

into #TabTmp6

from CLNTINFO_DATA_TABLE

drop table if exists #TabTmp7

select
x.*
, y.Address
, y.LocName

into #TabTmp7

from #TabTmp5 x
left join #TabTmp6 y on x.ID = y.ComID

drop table if exists #TabTmp5
drop table if exists #TabTmp6

drop table if exists VC_ComIdx

select *

into VC_ComIdx

from #TabTmp7

drop table if exists #TabTmp7

select * from VC_ComIdx
--where Name like '%]'-- escape '\'
--where Docs is not null
--order by ID
--where ID = 86489
order by Name
