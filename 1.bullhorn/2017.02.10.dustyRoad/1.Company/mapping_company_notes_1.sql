with 
tmp1 as (
select CC.clientCorporationID
--, case when (CC.dateAdded = '' OR CC.dateAdded is NULL) THEN '' ELSE concat('Date Added: ',left(convert(varchar,CC.dateAdded,110),10)) END as DateAdded
, case when (cast(CC.notes as varchar(max)) = '' OR CC.notes is NULL) THEN '' ELSE concat('Company Notes: ',CC.notes,char(10)) END as CompanyNotes
, case when (cast(CC.companyDescription as varchar(max)) = '' OR CC.companyDescription is NULL) THEN '' ELSE concat('Company Description:', ltrim(rtrim([dbo].[udf_StripHTML](CC.companyDescription))),char(10)) END as CompanyDescription
, case when (CC.status = '' OR CC.status is NULL) THEN '' ELSE concat('Status: ',CC.status,char(10)) END as Status1
, case when (cast(CC.businessSectorList as varchar(max)) = '' OR CC.businessSectorList is NULL) THEN '' ELSE concat('BusinessSectorList: ',CC.businessSectorList,char(10)) END as BusinessSectorList
--, CC.feeArrangement
--, CC.culture as Culture
--, cc.billingContact as BillingContact
--, case when (CC.customText2 = '' OR CC.customText2 is NULL) THEN '' ELSE concat('CustomText2: ',CC.customText2) END as customText2
--, cc.customText3 as customText3
from bullhorn1.BH_ClientCorporation CC)
--select * from tmp1

--, tmp2 as (select clientCorporationID,concat(CompanyNotes,char(10),CompanyDescription,char(10),Status1,char(10),Culture,char(10),BillingContact,char(10),customText2,char(10),customText3,char(10)) as CombinedNote from tmp1)
--, tmp2 as (select clientCorporationID,concat(CompanyNotes,char(10),CompanyDescription,char(10),Status1,char(10),BusinessSectorList,char(10)) as CombinedNote from tmp1)
, tmp2 as (select clientCorporationID,concat(CompanyNotes,CompanyDescription,Status1,BusinessSectorList) as CombinedNote from tmp1)
--select * from tmp2

select CC.name as 'company-name'
, CC.clientCorporationID as 'company-externalId'
, CC.countryID
, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL') THEN '' ELSE tc.abbreviation END as 'company-locationCountry'
--, CC.status
, CC.parentClientCorporationID
, CC.companyURL as 'company-URL'
--, CC.businessSectorList
, CC.culture
--, CC.notes
--, CC.companyDescription
, CC.feeArrangement
--, concat(CC.phone,case when (CC.customText2 = '' OR CC.customText2 is NULL) THEN '' ELSE concat(', ',CC.customText2) END) as 'company-phone'
, CC.customText2
, CC.phone as 'company-phone'
, concat(CC.address1,char(10),CC.address2,char(10)) as 'company-locationAddress'
, concat(city,char(10),state,char(10)) as 'company-locationName'
, CC.city as 'company-locationCity'
, CC.state as 'company-locationState'
, CC.zip as 'company-locationZipCode'
--, cc.billingContact
--, cc.customText3
, tmp2.CombinedNote as 'company-note'
from bullhorn1.BH_ClientCorporation CC
left join tmp_country tc ON CC.countryID = tc.code
left join tmp2 on CC.clientCorporationID = tmp2.clientCorporationID
where CC.name not like '%Imported Contacts%' and CC.status not like '%Archive%'

--and CC.clientCorporationID = 2438
--order by name

/*
select cc.name from bullhorn1.BH_ClientCorporation CC
where CC.name not like '%Imported Contacts%' and CC.status not like '%Archive%'
--where cc.name like '%Deloitte%' or cc.name like '%Manhattan Chamber of Commerce%'
group by cc.name having count(*) > 1
order by name
*/