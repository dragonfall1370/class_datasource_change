drop table if exists #VCJobDocsTemp

select

x.OwnerId as JobId,

STRING_AGG(x.DocFileName, ',') as Docs

into #VCJobDocsTemp

from VCFilesMapping x

where x.OwnerType = 'Vacancies'

group by x.OwnerId

--select * from #VCJobDocsTemp

drop table if exists [dbo].[VCJobDocs];

--select max(len(Docs)) from (
select
x.JobId,
x.Docs

into [dbo].[VCJobDocs]

from #VCJobDocsTemp x
join VCJobIdxs cis on x.JobId = cis.JobId

drop table if exists #VCJobDocsTemp

select * from  [dbo].[VCJobDocs]

--select * from VCFilesMapping
--where OwnerId not in (
--	select Cand
--)