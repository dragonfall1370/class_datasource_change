drop table if exists [dbo].[VCAccIdxs]

select
Id
, [Name]
, row_number() over(partition by [Name] order by cast(CreatedDate as datetime) desc) as RowNum

into [dbo].[VCAccIdxs]

from Account