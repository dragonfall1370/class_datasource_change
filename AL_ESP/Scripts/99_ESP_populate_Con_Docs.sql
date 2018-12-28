drop table if exists [dbo].[VCConDocs];

select
cis.Id as ContactId,
STRING_AGG(
	[dbo].[ufn_PopulateFileName2](a.[Name], a.Id)
	, ','
) as Docs

into [dbo].[VCConDocs]

from Attachment a -- 16409
--left join TaskIdxs tis on a.ParentId = tis.Id
join VCConIdxs cis on a.ParentId = cis.Id
--join AccountIdxs ais on a.AccountId = ais.Id
--where ais.Id <> '001b00000044tF3AAI'
group by cis.Id