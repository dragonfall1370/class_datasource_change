drop table if exists #TabTmp1

select [Client ID ] as ID
, [dbo].[ufn_TrimSpecialCharacters_V2]((trim(isnull([Client ], 'Company without name'))), '') as Name

into #TabTmp1

from [Clients List with ID]

--select * from #TabTmp1
--where ID = 86489

drop table if exists #TabTmp2

select
*
, row_number() over(partition by lower([Name]) order by ID) as rn

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

--select * from #TabTmp3

drop table if exists #TabTmp2

drop table if exists #TabTmp4

select
[Client ID ] as ComId

, replace(
	dbo.ufn_PopulateLocationAddress(
		concat(trim(isnull([Address Line 1], '')), ', ', trim(isnull([Address Line 2], '')))
		, [Locality]
		, [City/Town]
		, [Post Code]
		, isnull([Country], 'UK')
		, ', '
	)
	, ',,'
	, ','
) as Address

, replace(
	dbo.ufn_PopulateLocationAddress(
		''
		, [Locality]
		, [City/Town]
		, [Post Code]
		, isnull([Country], 'UK')
		, ', '
	)
	, ',,'
	, ','
) as LocName

into #TabTmp4

from [Clients List with ID] x

--select * from #TabTmp4

drop table if exists #TabTmp5

select
x.ID
, x.Name
, y.Address
, y.LocName

into #TabTmp5

from #TabTmp3 x
left join #TabTmp4 y on x.ID = y.ComID

drop table if exists #TabTmp3
drop table if exists #TabTmp4

--select * from #TabTmp5

drop table if exists VC_ComIdx

--select replace('P.O. Box 250, St. Paul?s Gate, New Street, St. Helier, JE4 5PU, Jersey', '?', ' ') as Name
--select 'P.O. Box 250, St. Paul?s Gate, New Street, St. Helier, JE4 5PU, Jersey' COLLATE Latin1_General_100_CS_AS_SC as Name
--'3076 Sir Francis Drake?s Highway , P.O. Box 3463, Road Town, Tortola, UK'
--PO Box 264, , Forum 4, Grenville Street, St Helier, JE4 8TQ, Jersey
select
ID
, replace(replace([Name], ', ,', ','), '�', ' ') as [Name]
, replace(replace([Address], ', ,', ','), '�', ' ') as [Address]
, replace(replace([LocName], ', ,', ','), '�', ' ') as [LocName]
--P.O. Box 250, St. Paul?s Gate, New Street, St. Helier, JE4 5PU, Jersey
into VC_ComIdx

from #TabTmp5

drop table if exists #TabTmp5

select * from VC_ComIdx
--where Name like '%]'-- escape '\'
--where Docs is not null
--order by ID
--where ID = 86489
order by [Name]
--3076 Sir Francis Drake�s Highway , P.O. Box 3463, Road Town, Tortola, UK