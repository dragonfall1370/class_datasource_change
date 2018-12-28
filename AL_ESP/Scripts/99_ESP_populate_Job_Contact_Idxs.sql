drop table if exists #JobTemp1

select
o.Id
, o.AccountId
, oc.ContactId

into #JobTemp1

from Opportunity o
left join [OpportunityContactRole] oc on o.Id = oc.OpportunityId

--select * from #JobTemp1
--where ContactId is null

drop table if exists [dbo].[VCJobContactIdxs]

select j1.Id as JobId
, j1.AccountId
, iif(len(trim(isnull(j1.ContactId, ''))) = 0
	, (select top 1 c.Id
		from VCConIdxs c
		where j1.AccountId = c.AccountId and c.Id like '%DefCon')
	, trim(isnull(ContactId, ''))
) as ContactId

into [dbo].[VCJobContactIdxs]

from #JobTemp1 j1
--where ContactId is not null

drop table if exists #JobTemp1

select * from [dbo].[VCJobContactIdxs]

  --  select distinct [OpportunityId]
--  from [OpportunityContactRole]

--  select count(*) from [OpportunityContactRole]
--  select count(*) from [Opportunity]

--select * from Opportunity
----where Contact_name_position__c
--where id = '0062000000hwKhpAAE'

--select * from Contact where AccountId = '0012000001YoLvSAAV'