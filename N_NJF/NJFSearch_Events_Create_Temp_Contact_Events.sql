CREATE TABLE tempEventContact (
    contactEventId int,
    contactName varchar(max)
);

with temp as (select ectc.intEventId contactEventId, 
	ct.vchCompanyTierName, ct.intCompanyId as companyid, vchCompanyName,
	coalesce(NULLIF(p.vchForename, ''), '') + Coalesce(' ' + NULLIF(p.vchSurname, ''), '') as contactName
from lEventCompanyTierContact ectc left join dEvent e on ectc.intEventId = e.intEventId
				left join dContact c on ectc.intContactId = c.intContactId
				left join dPerson p on c.intPersonId = p.intPersonId
				left join lCompanyTierContact ctc on ectc.intContactId = ctc.intContactId and ectc.intCompanyTierId = ctc.intCompanyTierId
				left join dCompanyTier ct on ectc.intCompanyTierId = ct.intCompanyTierId
				left join dCompany comp on ct.intCompanyId = comp.intCompanyId)
--select *, ROW_NUMBER() OVER(PARTITION BY contactEventId ORDER BY intCompanyId ASC) AS rn from tempEventContact
, temp1 as (select *, 
 iif(contactName = '' or contactName is null, '', concat(contactName, iif(vchCompanyTierName = '','',concat(' (',vchCompanyTierName,iif(vchCompanyName = '' or vchCompanyName is null,')',concat(' - ',ltrim(rtrim(vchCompanyName)),')')))))) as contactName2
from temp
)
insert into tempEventContact SELECT contactEventId, 
     STUFF(
         (SELECT char(10) + '- ' + contactName2
          from  temp1
          WHERE contactEventId =ca.contactEventId
    order by contactEventId asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          ,1,3, '')  AS contactName
FROM temp1 as ca
GROUP BY ca.contactEventId

