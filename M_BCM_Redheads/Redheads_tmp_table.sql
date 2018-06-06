--TEMP TABLE FOR REDHEADS
create table ContactEmailFinal
(ContactID int PRIMARY KEY,
ContactEmailFinal nvarchar(max)
)
go

with CombinedEmail as (
select ContactServiceID, AddressBookEmailAddress1 as ContactEmail from ContactDetailsTable
UNION ALL
select ContactServiceID, AddressBookEmailAddress2 from ContactDetailsTable
UNION ALL
select ContactServiceID, AddressBookEmailAddress3 from ContactDetailsTable) 

, DistinctEmail as (SELECT distinct ContactServiceID, ContactEmail from CombinedEmail where ContactEmail is not NULL)

, EmailDupRegconition as (SELECT distinct ContactServiceID, ContactEmail, ROW_NUMBER() OVER(PARTITION BY ContactEmail ORDER BY ContactServiceID ASC) AS rn 
from DistinctEmail)

, ContactEmail as (select ContactServiceID
, case	when rn = 1 then ContactEmail
		else concat(ContactServiceID,'-',ContactEmail) end as ContactEmail
, rn
from EmailDupRegconition)

insert into ContactEmailFinal SELECT
     ContactServiceID,
     STUFF(
         (SELECT ',' + ContactEmail
          from  ContactEmail
          WHERE ContactServiceID = a.ContactServiceID
          FOR XML PATH (''))
          , 1, 1, '')  AS ContactEmailFinal
FROM ContactEmail as a
GROUP BY a.ContactServiceID

select * from ContactEmailFinal