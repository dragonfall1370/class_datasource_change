with 
tmp1 as (
select clientCorporationID
,case when (CC.address2 = '' OR CC.address2 is NULL) THEN '' ELSE concat('Address 2: ',CC.address2) END as Address2
, case when (CC.dateAdded = '' OR CC.dateAdded is NULL) THEN '' ELSE concat('Date Added: ',left(convert(varchar,CC.dateAdded,110),10)) END as DateAdded
, case when (cast(CC.companyDescription as varchar(max)) = '' OR CC.companyDescription is NULL) THEN '' ELSE concat('Company Description:', ltrim(rtrim([dbo].[udf_StripHTML](CC.companyDescription)))) END as CompanyDescription
, case when (CC.dateFounded = '' OR CC.dateFounded is NULL) THEN '' ELSE concat('Date Founded: ',left(convert(varchar,CC.dateFounded,120),4)) END as YearFounded
, case when (CC.customText1 = '' OR CC.customText1 is NULL) THEN '' ELSE concat('Industry: ',CC.customText1) END as Industry
,case when (cast(CC.competitors as varchar(max)) = '' OR CC.competitors is NULL) THEN '' ELSE concat('Competitors: ',CC.competitors) END as Competitors
, case when (cast(CC.businessSectorList as varchar(max)) = '' OR CC.businessSectorList is NULL) THEN '' ELSE concat('Business Sector: ',CC.businessSectorList) END as BusinessSector
, case when (CC.status = '' OR CC.status is NULL) THEN '' ELSE concat('Status: ',CC.status) END as Status1
, iif (CC.customText5 = '' OR CC.customText5 is NULL,'',concat('Company Coverage: ',CC.customText5)) as CompanyCoverage
, iif (CC.customText6 = '' OR CC.customText6 is NULL,'',concat('No. of Employees: ',CC.customText6)) as NoEmployees
, iif (CC.ownerShip = '' OR CC.ownerShip is NULL,'',concat('Ownership: ',CC.ownerShip)) as OwnershipS
, case when (cast(CC.notes as varchar(max)) = '' OR CC.notes is NULL) THEN '' ELSE concat('Company Overview: ',CC.notes) END as CompanyNotes
, iif (CC.customText2 = '' OR CC.customText2 is NULL,'',concat('Twitter: ',CC.customText2)) as Twitter
, iif (CC.customText3 = '' OR CC.customText3 is NULL,'',concat('Facebook: ',CC.customText3)) as Facebook
, iif (CC.customText4 = '' OR CC.customText4 is NULL,'',concat('LinkedIn: ',CC.customText4)) as LinkedIn
, iif (CC.customText7 = '' OR CC.customText6 is NULL,'',concat('Instagram: ',CC.customText7)) as Instagram
, iif (cast(CC.culture as varchar(max)) = '' OR CC.culture is NULL,'',concat('Culture: ',CC.customText7)) as Culture
from bullhorn1.BH_ClientCorporation CC)
--select * from tmp1

, tmp2 as (select clientCorporationID, concat(Address2,char(10),DateAdded,char(10),CompanyDescription,char(10)
,YearFounded,char(10),Industry,char(10),Competitors,char(10),BusinessSector,char(10)
,Status1,char(10),CompanyCoverage,char(10),NoEmployees,char(10),OwnershipS,char(10),CompanyNotes,char(10),Twitter,char(10)
,Facebook,char(10),LinkedIn,char(10),Instagram,char(10)
,Culture) 
as CombinedNote
from tmp1)

--select * from tmp2

select
CC.clientCorporationID as 'company-externalId'
, CC.name as 'company-name'
, CC.address1 as 'company-locationAddress'
, CC.city as 'company-locationCity'
, CC.state as 'company-locationState'
, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL') THEN ''
	ELSE tc.abbreviation
	END as 'company-locationCountry'
, CC.zip as 'company-locationZipCode'
, CC.phone as 'company-phone'
, CC.fax as 'company-fax'
, CC.companyURL as 'company-website'
, tmp2.CombinedNote as 'company-notes'
from bullhorn1.BH_ClientCorporation CC
left join tmp_country tc ON CC.countryID = tc.code
left join tmp2 on CC.clientCorporationID = tmp2.clientCorporationID