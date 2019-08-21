drop table if exists [dbo].[VCComDocs];

--select max(len(Docs)) from (
select
a.AccountId,
STRING_AGG(
	[dbo].[ufn_PopulateFileName2](a.[Name], a.Id)
	, ','
) as Docs

into [dbo].[VCComDocs]

from Attachment a -- 16409
--left join TaskIdxs tis on a.ParentId = tis.Id
--left join ContactIdxs cis on a.ParentId = cis.Id
join [dbo].VCAccIdxs ais on a.AccountId = ais.Id
--where ais.Id <> '001b00000044tF3AAI'
group by AccountId
--) abc
--tis.Id is null and
--cis.Id is null and
--len(trim(isnull(AccountId, ''))) > 0
--and trim(isnull(AccountId, '')) <> '000000000000000AAA'

--select * from Account
--where Id = '001b00000044tF3AAI'

--select * from Attachment
--where [Name] = 'image001.png'

--select * from AVTRRT__Job__c where Id = '00T0X00002qrPGqUAM'

--select * from ContentVersion

--select * from Task where Id = '00T0X00002ezqlAUAQ'

--select * from ContentVersion