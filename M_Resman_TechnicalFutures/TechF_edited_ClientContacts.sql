--REMOVE DUPLICATE CONTACTID FROM CLIENTCONTACT
with dup as (
select *
, row_number() over (partition by ContactID order by ClientID desc) as rn
from ClientContacts
where Status = 'A')

select *
into EditedContact
from dup where rn = 1