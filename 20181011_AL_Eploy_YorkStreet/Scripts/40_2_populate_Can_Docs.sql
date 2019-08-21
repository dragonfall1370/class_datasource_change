drop table if exists #VCCanDocsTemp

select

x.OwnerId as CanId,

STRING_AGG(x.DocFileName, ',') as Docs

into #VCCanDocsTemp

from VCFilesMapping x

where x.OwnerType = 'Candidates'

group by x.OwnerId

--select * from #VCCanDocsTemp

drop table if exists [dbo].[VCCanDocs];

--select max(len(Docs)) from (
select
x.CanId,
x.Docs

into [dbo].[VCCanDocs]

from #VCCanDocsTemp x
join VCCanIdxs cis on x.CanId = cis.CanId

drop table if exists #VCCanDocsTemp

select * from  [dbo].[VCCanDocs]

--select * from VCFilesMapping
--where OwnerId not in (
--	select Cand
--)