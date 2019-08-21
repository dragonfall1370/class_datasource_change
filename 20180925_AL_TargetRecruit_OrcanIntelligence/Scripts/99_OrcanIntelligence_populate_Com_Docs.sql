with
--TaskIdxs as (
--	select Id from Task
--)
--, ContactIdxs as (
--	select Id from Contact
--)
AccountIdxs as (
	select id from Account
)

select
a.AccountId,
STRING_AGG(
	concat(a.Id, '-',
		replace(
			replace(trim(isnull(a.[Name], '')), ',', '_')
			, ' ', '_'
		)
	)
	, ',') as Docs
from Attachment a -- 16409
--left join TaskIdxs tis on a.ParentId = tis.Id
--left join ContactIdxs cis on a.ParentId = cis.Id
join AccountIdxs ais on a.AccountId = ais.Id
where ais.Id <> '001b00000044tF3AAI'
group by AccountId

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
