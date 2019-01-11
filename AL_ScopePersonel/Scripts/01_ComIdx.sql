drop table if exists #TabTmp1

select clientid as ID
, iif(
	isnull(nullif([dbo].[ufn_TrimSpecialCharacters_V2](isnull(company, 'Company without name'), ''), ''), 'Company without name') like '[.]%[.]'
	, 'Company without name'
	, isnull(nullif([dbo].[ufn_TrimSpecialCharacters_V2](isnull(company, 'Company without name'), ''), ''), 'Company without name')
) as Name
--...........................................................................
into #TabTmp1

from [dbo].[RF_Clients_Complete]

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
ClientId as ComID
, Docs

into #TabTmp4

from VCComDocs

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
clientid as ComId

, replace(
	dbo.ufn_PopulateLocationAddress(
		locations_description
		, ''
		, ''
		, locations_code
		, 'UK'
		, ', '
	)
	, ',,'
	, ','
) as Address

, replace(
	dbo.ufn_PopulateLocationAddress(
		''
		, ''
		, ''
		, locations_code
		, 'UK'
		, ', '
	)
	, ',,'
	, ','
) as LocName

into #TabTmp6

from [RF_Clients_Complete]

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
