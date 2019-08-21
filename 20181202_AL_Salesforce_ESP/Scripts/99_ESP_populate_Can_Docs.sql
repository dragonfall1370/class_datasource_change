drop table if exists [dbo].[VCCanDocs];

select
cis.Id as ContactId,
STRING_AGG(
	[dbo].[ufn_PopulateFileName2](a.[Name], a.Id)
	, ','
) as Docs

into [dbo].[VCCanDocs]

from Attachment a -- 16409
--left join TaskIdxs tis on a.ParentId = tis.Id
join VCCanIdxs cis on a.ParentId = cis.Id
--join AccountIdxs ais on a.AccountId = ais.Id
--where ais.Id <> '001b00000044tF3AAI'
group by cis.Id