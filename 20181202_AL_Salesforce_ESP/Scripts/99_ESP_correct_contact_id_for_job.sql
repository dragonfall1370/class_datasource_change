update js

set js.[position-contactId] =
	(select top 1 c.Id
		from Contact c
		join [dbo].[VCAccIdxs] ais on c.AccountId = ais.Id
		where ais.Name not like '%candidate%' and jci.AccountId = c.AccountId
	)

from VCJobs js
join VCJobContactIdxs jci on js.[position-externalId] = jci.JobId

where js.[position-externalId] in (
	select [position-externalId] from [VCJobs]
	where [position-contactId] not in (select Id from VCConIdxs)
)