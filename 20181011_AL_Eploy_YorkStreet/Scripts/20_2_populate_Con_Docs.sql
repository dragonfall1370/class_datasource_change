drop table if exists #VCConDocsTemp

select

x.OwnerId as ConId,

STRING_AGG(x.DocFileName, ',') as Docs

into #VCConDocsTemp

from VCFilesMapping x

where x.OwnerType = 'Contacts'

group by x.OwnerId

--select * from #VCConDocsTemp

drop table if exists [dbo].[VCConDocs];

--select max(len(Docs)) from (
select
x.ConId,
x.Docs

into [dbo].[VCConDocs]

from #VCConDocsTemp x
join VCConIdxs cis on x.ConId = cis.ConId

drop table if exists #VCConDocsTemp

--select * from  [dbo].[VCComDocs]

--select * from VCFilesMapping