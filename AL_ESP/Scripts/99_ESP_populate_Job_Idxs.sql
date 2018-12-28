drop table if exists #VCJobIdxsTemp

select AccountId, Id, trim(isnull([Name], 'No Job Title')) as [Name], row_number() over(partition by AccountId, lower(trim(isnull([Name], 'No Job Title'))) order by cast(CreatedDate as datetime) desc) as RowNum

into #VCJobIdxsTemp

from Opportunity

drop table if exists [dbo].[VCJobIdxs]

select AccountId, Id, iif(RowNum = 1, [Name], concat([Name], ' (', RowNum, ')')) as JobTitle

into [dbo].[VCJobIdxs]

from #VCJobIdxsTemp

-- delete temp table
drop table if exists #VCJobIdxsTemp

select * from VCJobIdxs

--select * from Opportunity where AccountId is null or AccountId = '000000000000000AAA'
--select * from Opportunity where [Name] like 'BDM'
