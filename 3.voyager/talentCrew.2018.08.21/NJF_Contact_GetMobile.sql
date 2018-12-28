with tempContacts as (select c.intContactId, c.intPersonId, intCompanyTierContactId, ctc.intCompanyTierId, ct.intCompanyId, ctc.vchJobTitle, c.datLastContacted
		, ctc.vchNote, ctc.bitActive, ctc.intPreferredTelecomId, ROW_NUMBER() OVER(PARTITION BY ctc.intContactId ORDER BY intCompanyId ASC) AS rn
from dContact c left join lCompanyTierContact ctc on c.intContactId = ctc.intContactId
				left join dCompanyTier ct on ctc.intCompanyTierId = ct.intCompanyTierId
where ctc.intContactId is not null)
--GET CONTACT MOBILES FROM DCOMPANYTIERCONTACTTELECOM
, tempMobile1 as (select intContactId, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as mobile
from dCompanyTierContactTelecom
where vchDescription = 'Mobile Phone')

--GET CONTACT MOBILES FROM dContactTelecom
, tempMobile2 as(
select intContactId, iif(vchExtension <> '', concat(vchValue,vchExtension), vchValue) as mobile
from dContactTelecom
where tintTelecomId = 5 and intContactId not in (select intContactId from tempMobile1)
)

--UNION 2 TABLES
, tempMobile3 as (select * from tempMobile1 union all select * from tempMobile2)

--LINK MOBILE WITH companytiercontactId
, ContactMobile as (select tc.intCompanyTierContactId,tc.intCompanyId, tce.*
from tempMobile3 tce left join tempContacts tc on tce.intContactId = tc.intContactId)

--select * from ContactMobile where intCompanyId in (2,455)
----select * from dContact where intContactId = 65068
--select * from dPerson where intPersonId = 104376
--select * from dJob