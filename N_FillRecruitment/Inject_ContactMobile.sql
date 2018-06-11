--Contact Mobile
with tempContactMobile as (select cc.ClientContactId, cc.ContactPersonId, p.CommunicationTypeId, replace(p.Num,' ','') as Num
from ClientContacts cc left join Phones p on cc.ContactPersonId = p.ObjectID
where p.CommunicationTypeId = 83)

SELECT concat('FR',ClientContactId) as ContactExternalId,
     STUFF(
         (SELECT ',' + Num
          from  tempContactMobile
          WHERE ClientContactId = tcm.ClientContactId
    order by ClientContactId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS ContactMobile
FROM tempContactMobile as tcm
GROUP BY tcm.ClientContactId
