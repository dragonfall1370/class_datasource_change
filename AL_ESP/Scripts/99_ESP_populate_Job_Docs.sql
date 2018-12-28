drop table if exists [dbo].[VCJobDocs];

select
cis.Id as JobId,
STRING_AGG(
	[dbo].[ufn_PopulateFileName2](a.[Name], a.Id)
	, ','
) as Docs

into [dbo].[VCJobDocs]

from Attachment a -- 16409
--left join TaskIdxs tis on a.ParentId = tis.Id
join VCJobIdxs cis on a.ParentId = cis.Id
--join AccountIdxs ais on a.AccountId = ais.Id
--where ais.Id <> '001b00000044tF3AAI'
group by cis.Id

select * from [VCJobDocs]