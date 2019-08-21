with 
tmp1 as (
select clientCorporationID
, case when (CC.dateAdded = '' OR CC.dateAdded is NULL) THEN '' ELSE concat('Date Added: ',left(convert(varchar,CC.dateAdded,110),10),char(10)) END as DateAdded
, case when (cast(CC.companyDescription as varchar(max)) = '' OR CC.companyDescription is NULL) THEN '' ELSE concat('Company Description:', ltrim(rtrim([dbo].[udf_StripHTML](CC.companyDescription))),char(10)) END as CompanyDescription
, case when (CC.dateFounded = '' OR CC.dateFounded is NULL) THEN '' ELSE concat('Date Founded: ',left(convert(varchar,CC.dateFounded,120),4),char(10)) END as YearFounded
, case when (CC.customText1 = '' OR CC.customText1 is NULL) THEN '' ELSE concat('Industry: ',CC.customText1,char(10)) END as Industry
, case when (cast(CC.competitors as varchar(max)) = '' OR CC.competitors is NULL) THEN '' ELSE concat('Competitors: ',CC.competitors,char(10)) END as Competitors
, case when (cast(CC.businessSectorList as varchar(max)) = '' OR CC.businessSectorList is NULL) THEN '' ELSE concat('Business Sector: ',CC.businessSectorList,char(10)) END as BusinessSector
, case when (CC.status = '' OR CC.status is NULL) THEN '' ELSE concat('Status: ',CC.status,char(10)) END as Status
, iif (CC.customText5 = '' OR CC.customText5 is NULL,'',concat('Company Coverage: ',CC.customText5,char(10))) as CompanyCoverage
, iif (CC.customText6 = '' OR CC.customText6 is NULL,'',concat('No. of Employees: ',CC.customText6,char(10))) as NoEmployees
, iif (CC.ownerShip = '' OR CC.ownerShip is NULL,'',concat('Ownership: ',CC.ownerShip,char(10))) as Ownerships
, case when (cast(CC.notes as varchar(max)) = '' OR CC.notes is NULL) THEN '' ELSE concat('Company Overview: ',CC.notes,char(10)) END as CompanyNotes
, iif (CC.customText2 = '' OR CC.customText2 is NULL,'',concat('Twitter: ',CC.customText2,char(10))) as Twitter
, iif (CC.customText3 = '' OR CC.customText3 is NULL,'',concat('Facebook: ',CC.customText3,char(10))) as Facebook
, iif (CC.customText4 = '' OR CC.customText4 is NULL,'',concat('LinkedIn: ',CC.customText4,char(10))) as LinkedIn
, iif (CC.customText7 = '' OR CC.customText6 is NULL,'',concat('Instagram: ',CC.customText7,char(10))) as Instagram
, iif (cast(CC.culture as varchar(max)) = '' OR CC.culture is NULL,'',concat('Culture: ',CC.customText7,char(10))) as Culture
, case when (CC.feeArrangement = '' OR CC.feeArrangement is NULL) THEN '' ELSE concat ('Fee Arrangement: ',CC.feeArrangement,char(10)) END as 'feeArrangement'
, case when (CC.billingContact = '' OR CC.billingContact is NULL) THEN '' ELSE concat ('Billing Contact: ',CC.billingContact,char(10)) END as 'billingContact'
--, CC.feeArrangement
--, cc.billingContact as BillingContact
from bullhorn1.BH_ClientCorporation CC)
--select * from tmp1

, tmp2 as (select clientCorporationID, concat(DateAdded,CompanyDescription,YearFounded,Industry,Competitors,BusinessSector,Status,CompanyCoverage,NoEmployees,Ownerships,CompanyNotes,Twitter,Facebook,LinkedIn,Instagram,Culture,feeArrangement,billingContact)
as CombinedNote from tmp1)
--select * from tmp2

select
CC.clientCorporationID as 'company-externalId'
, CC.name as 'company-name'
, concat(CC.address1,char(10),CC.address2,char(10)) as 'company-locationAddress'
, CC.city as 'company-locationCity'
, CC.state as 'company-locationState'
, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL') THEN '' ELSE tc.abbreviation	END as 'company-locationCountry'
, CC.zip as 'company-locationZipCode'
, concat(city,char(10),state,char(10)) as 'company-locationName'
, CC.phone as 'company-phone'
, CC.fax as 'company-fax'
, CC.companyURL as 'company-website'
, tmp2.CombinedNote as 'company-note'

from bullhorn1.BH_ClientCorporation CC
left join tmp_country tc ON CC.countryID = tc.code
left join tmp2 on CC.clientCorporationID = tmp2.clientCorporationID
where CC.name not like '%Imported Contacts%' and CC.status not like '%Archive%'
--and CC.clientCorporationID = 2438
--order by name

/*
----select * from bullhorn1.BH_ClientCorporation CC

select cc.name from bullhorn1.BH_ClientCorporation CC
where CC.name not like '%Imported Contacts%' and CC.status not like '%Archive%'
--where cc.name like '%Deloitte%' or cc.name like '%Manhattan Chamber of Commerce%'
group by cc.name having count(*) > 1
order by name
*/


