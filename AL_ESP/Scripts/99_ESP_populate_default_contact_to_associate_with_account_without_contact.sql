;with

TabTmp1 as (
select
o.Id
, o.AccountId
, oc.ContactId
from Opportunity o
left join [OpportunityContactRole] oc on o.Id = oc.OpportunityId
)


--insert into Contact (Id, IsDeleted, AccountId, FirstName, LastName, OwnerId, CreatedById, CreatedDate
--	, LastModifiedDate, LastModifiedById, SystemModstamp)

select
a.Id + 'DefCon' as Id
, 0 as IsDeleted
, a.Id as AccountId
, 'Default Contact' as FirstName
, [Name] as LastName
--, replace(trim(isnull([Name], '')), ' ', '-')  + '@no-email.com' as Email
--, '012b0000000J2RE' as RecordTypeId
, u.Id as OwnerId
, u.Id as CreatedById
, a.CreatedDate as CreatedDate
, a.LastModifiedDate as LastModifiedDate
, u.Id as LastModifiedById
, a.SystemModstamp as SystemModstamp

from Account a
join [User] u on a.OwnerId = u.Id 
where a.Id in (
  select AccountId from TabTmp1
  where ContactId is null
)

--select * from Contact where Id like '%DefCon'

--delete from Contact where Id like '%DefCon'