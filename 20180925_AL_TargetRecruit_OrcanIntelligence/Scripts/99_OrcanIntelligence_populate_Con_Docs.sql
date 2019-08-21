with
--TaskIdxs as (
--	select Id from Task
--)
ContactIdxs as (
	select Id from Contact
	where RecordTypeId =
	--'012b0000000J2RE' -- Contact
	'012b0000000J2RD'
)

select
cis.Id as ContactId,
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
join ContactIdxs cis on a.ParentId = cis.Id
--join AccountIdxs ais on a.AccountId = ais.Id
--where ais.Id <> '001b00000044tF3AAI'
group by cis.Id