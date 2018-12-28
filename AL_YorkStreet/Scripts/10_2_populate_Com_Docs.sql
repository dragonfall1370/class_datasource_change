drop table if exists #VCComDocsTemp

select

x.OwnerId as ComId,

STRING_AGG(x.DocFileName, ',') as Docs

into #VCComDocsTemp

from VCFilesMapping x

where x.OwnerType = 'Companies'

group by x.OwnerId

--select * from #VCComDocsTemp

drop table if exists [dbo].[VCComDocs];

--select max(len(Docs)) from (
select
x.ComId,
x.Docs

into [dbo].[VCComDocs]

from #VCComDocsTemp x
join VCComIdxs cis on x.ComId = cis.ComId

drop table if exists #VCComDocsTemp

--select * from  [dbo].[VCComDocs]

--select * from VCFilesMapping