
select top 200 *

from email;

----

select    c.ID as 'contact-externalId'
        , c.FirstName as 'contact-firstName'
        , c.LastName as 'contact-Lastname'
        , c.Salutation as "Title"
        , c.Title as 'contact-jobTitle'
        , a.*
-- select count(*)
from Contact c
left join email a on a.AccountId = c.AccountId
where a.AccountId  is not null
;


----

select
          l.ID as 'candidate-externalId'
        , Coalesce(NULLIF(replace(l.FirstName ,'?',''), ''), 'No Firstname') as 'candidate-firstName'
        , Coalesce(NULLIF(replace(l.LastName,'?',''), ''), concat('Lastname-',l.ID) ) as 'candidate-lastName'
        , l.Title__c 'candidate-jobTitle1'
        , a.*
-- select * -- select count(*)
from Lead l --4725
left join email a on a.WhoId = l.id
where a.WhoId  is not null
;
