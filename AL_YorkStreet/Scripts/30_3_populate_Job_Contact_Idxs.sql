drop table if exists #JobTemp1

select
o.Id
, o.AccountId
, oc.ContactId

into #JobTemp1

from Opportunity o
left join [OpportunityContactRole] oc on o.Id = oc.OpportunityId

drop table if exists [dbo].[VCJobContactIdxs]

select j1.Id as JobId
, j1.AccountId
, iif(len(trim(isnull(ContactId, ''))) = 0
, (select top 1 c.Id
	from Contact c
	join [dbo].[VCAccIdxs] ais on c.AccountId = ais.Id
	where ais.Name not like '%candidate%' and j1.AccountId = c.AccountId)
, trim(isnull(ContactId, ''))) as ContactId

into [dbo].[VCJobContactIdxs]

from #JobTemp1 j1

drop table if exists #JobTemp1

--select * from [dbo].[VCJobContactIdxs]

  --  select distinct [OpportunityId]
--  from [OpportunityContactRole]

--  select count(*) from [OpportunityContactRole]
--  select count(*) from [Opportunity]