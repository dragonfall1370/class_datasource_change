;with
Cans as (
	select row_number() over(partition by '' order by Comments) as CanId
	, *
	from [All Candidates]
)

select

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
from Cans x
[All Candidates] y on x.CanId