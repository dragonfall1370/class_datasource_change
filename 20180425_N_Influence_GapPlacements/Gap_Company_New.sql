---DUPLICATION REGCONITION
with 
loc as (
	select Site_Unique_ID, Site_Address_Hse_No,Site_Address_Line_1,Site_Address_Line_2,Site_Address_Line_3
			,Site_Address_Line_4,Site_Address_Line_5,Site_Address_Line_6,Site_PostCode,Locality
			, coalesce(ltrim(Stuff(
			  Coalesce(' ' + NULLIF(Site_Address_Hse_No, ''), '')
			+ Coalesce(', ' + NULLIF(Site_Address_Line_1, ''), '')
			+ Coalesce(', ' + NULLIF(Site_Address_Line_2, ''), '')
			+ Coalesce(', ' + NULLIF(Site_Address_Line_3, ''), '')
			+ Coalesce(', ' + NULLIF(Site_Address_Line_4, ''), '')
			+ Coalesce(', ' + NULLIF(Site_Address_Line_5, ''), '')
			+ Coalesce(', ' + NULLIF(Site_Address_Line_6, ''), '')
			+ Coalesce(', ' + NULLIF(Site_PostCode, ''), '')
			, 1, 1, '')),Locality) as 'locationName'
	from ContactManagementSites)

--, companyName as (
--	select Site_Unique_ID, ltrim(Stuff(
--			  Coalesce(' ' + NULLIF(Organisation, ''), '')
--			+ Coalesce('_' + NULLIF(Locality, ''), '')
--			, 1, 1, '')) as compName
--	from ContactManagementSites)

--, switchboard as (
--	select Upper_Org_Name, STUFF(
--         (SELECT ', ' + nullif(ltrim(rtrim(Site_Phone_Number)),' ')--ltrim(rtrim(Site_Phone_Number))
--          from  ContactManagementSites
--          WHERE Upper_Org_Name = cms.Upper_Org_Name
--    order by Upper_Org_Name asc
--          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
--          , 1, 2, '')  AS switchboard
--FROM ContactManagementSites as cms
--GROUP BY cms.Upper_Org_Name)

--, loc2 as (
--select c.client_ref, l.description
--from client c left join clientlocation cl on c.client_ref = cl.client_ref
--left join location l on cl.loc_ref = l.loc_ref
--where cl.loc_ref <> 0)

, Users as (select concat(Users_Name,STMP_Account) as UserName, concat(iif(User_Email like '%@%',User_Email,''),Sync_Mailbox_Name) as UserEmail
from UserProfiles)

, dup as (SELECT Site_Unique_ID, Organisation,Locality, ROW_NUMBER() OVER(PARTITION BY Organisation ORDER BY Site_Unique_ID ASC) AS rn 
FROM ContactManagementSites)

----select * from dup
---Main Script---
select
  concat('GP',cms.Site_Unique_ID) as 'company-externalId'
, cms.Organisation as '(OriginalName)'
, iif(cms.Site_Unique_ID in (select Site_Unique_ID from dup where dup.rn > 1)
	, iif(dup.Locality not in('',' '),concat(dup.Organisation,' - ',dup.Locality),concat(dup.Organisation,' - ',dup.rn))
	, iif(cms.Organisation = '' or cms.Organisation is null,concat('No Company Name - ',cms.Site_Unique_ID),cms.Organisation)) as 'company-name'
, u.UserEmail as 'company-owners'
, loc.locationName as 'company-locationName'
, loc.locationName as 'company-locationAddress'
, coalesce(loc.Site_Address_Line_5,loc.Locality) as 'company-locationCity'
--, c.client_county as 'company-locationState'
, loc.Site_Postcode as 'company-locationZipCode'
, case
	when loc.locationName like 'Afghan%' then 'AF'
	when loc.locationName like '%Africa%' then 'ZA'
	when loc.locationName like 'Albani%' then 'AL'
	when loc.locationName like '%Americ%' then 'US'
	when loc.locationName like 'Andorr%' then 'AD'
	when loc.locationName like 'Austra%' then 'AU'
	when loc.locationName like 'AUSTARLIA%' then 'AU'
	when loc.locationName like 'Austri%' then 'AT'
	when loc.locationName like 'Belgi%' then 'BE'
	when loc.locationName like '%BERMUDA%' then 'BM'
	when loc.locationName like 'Brazil%' then 'BR'
	when loc.locationName like 'Britis%' then 'GB'
	when loc.locationName like 'Bucha%' then 'RO'
	when loc.locationName like 'Burmes%' then 'MM'
	when loc.locationName like 'Cambod%' then 'KH'
	when loc.locationName like 'Canad%' then 'CA'
	when loc.locationName like '%Cape%town%' then 'ZA'
	when loc.locationName like 'Cayman%' then 'KY'
	when loc.locationName like 'Chines%' then 'CN'
	when loc.locationName like 'Colombi%' then 'CO'
	when loc.locationName like 'Costa%' then 'CR'
	when loc.locationName like 'Cypr%' then 'CY'
	when loc.locationName like 'Czech%' then 'CZ'
	when loc.locationName like 'Danish%' then 'DK'
	when loc.locationName like 'Denmark%' then 'DK'
	when loc.locationName like 'Dutch%' then 'NL'
	when loc.locationName like '%Dubai%' then 'AE'
	when loc.locationName like '%England%' then 'GB'
	when loc.locationName like 'East%' then 'ZA'
	when loc.locationName like 'Emiria%' then 'AE'
	when loc.locationName like 'Eritre%' then 'ER'
	when loc.locationName like 'Estoni%' then 'EE'
	when loc.locationName like 'Ethiop%' then 'ET'
	when loc.locationName like 'Europe%' then 'TR'
	when loc.locationName like '%FRANCE%' then 'FR'
	when loc.locationName like 'Fijian%' then 'FJ'
	when loc.locationName like 'Filipi%' then 'PH'
	when loc.locationName like 'fili%' then 'PH'
	when loc.locationName like 'Finnish%' then 'FI'
	when loc.locationName like 'Flemish%' then 'BE'
	when loc.locationName like 'French%' then 'FR'
	when loc.locationName like 'Gabone%' then 'GA'
	when loc.locationName like 'German%' then 'DE'
	when loc.locationName like 'Ghanai%' then 'GH'
	when loc.locationName like 'Gree%' then 'GR'
	when loc.locationName like 'Hong Kong%' then 'HK'
	when loc.locationName like '%Ho%land%' then 'NL'
	when loc.locationName like 'Hunga%' then 'HU'
	when loc.locationName like 'Indian%' then 'IN'
	when loc.locationName like 'Indone%' then 'ID'
	when loc.locationName like 'Irania%' then 'IR'
	when loc.locationName like 'Irish%' then 'IE'
	when loc.locationName like 'Isra%' then 'IL'
	when loc.locationName like 'Ital%' then 'IT'
	when loc.locationName like 'Jamaic%' then 'JM'
	when loc.locationName like 'Japane%' then 'JP'
	when loc.locationName like 'Keny%' then 'KE'
	when loc.locationName like 'Leban%' then 'LB'
	when loc.locationName like 'Lithua%' then 'LT'
	when loc.locationName like 'LUXEMBOURG%' then 'LU'
	when loc.locationName like 'Malaga%' then 'MG'
	when loc.locationName like 'Malays%' then 'MY'
	when loc.locationName like 'Malt%' then 'MT'
	when loc.locationName like 'Mauritian%' then 'MU'
	when loc.locationName like 'Mexi%' then 'MX'
	when loc.locationName like 'Namibi%' then 'NA'
	when loc.locationName like '%NETHERLANDS%' then 'NL'
	when loc.locationName like '%New%Zea%' then 'NZ'
	when loc.locationName like 'Nigeri%' then 'NG'
	when loc.locationName like 'Northern Irish' then 'IE'
	when loc.locationName like 'Norwe%' then 'NO'
	when loc.locationName like 'Norway%' then 'NO'
	when loc.locationName like '%QATAR%' then 'QA'
	when loc.locationName like 'Pakist%' then 'PK'
	when loc.locationName like '%Paris%' then 'FR'
	when loc.locationName like 'Philip%' then 'PH'
	when loc.locationName like 'Phili%' then 'PH'
	when loc.locationName like 'Polish%' then 'PL'
	when loc.locationName like 'Portu%' then 'PT'
	when loc.locationName like 'Russia%' then 'RU'
	when loc.locationName like '%SCOTLAND%' then 'GB'
	when loc.locationName like 'Serbia%' then 'RS'
	when loc.locationName like 'Singap%' then 'SG'
	when loc.locationName like 'Sri%' then 'LK'
	when loc.locationName like '%Africa%' then 'ZA'
	when loc.locationName like 'South Africa%' then 'ZA'
	when loc.locationName like 'Spanish%' then 'ES'
	when loc.locationName like 'Sri Lankan%' then 'LK'
	when loc.locationName like 'Sri lankan%' then 'LK'
	when loc.locationName like 'Swedish%' then 'SE'
	when loc.locationName like 'Swiss%' then 'CH'
	when loc.locationName like '%SWITZERLAND%' then 'CH'
	when loc.locationName like 'Taiwan%' then 'TW'
	when loc.locationName like 'Thai%' then 'TH'
	when loc.locationName like 'Trinida%' then 'TT'
	when loc.locationName like 'Turk%' then 'TR'
	when loc.locationName like 'London%' then 'GB'
	when loc.locationName like '%UNITED%ARAB%' then 'AE'
	when loc.locationName like '%UAE%' then 'AE'
	when loc.locationName like '%UGANDA%' then 'UG'
	when loc.locationName like '%UNITED%KINGDOM%' then 'GB'
	when loc.locationName like '%UNITED%STATES%' then 'US'
	when loc.locationName like '%USA%' then 'US'
	when loc.locationName like '%ZIMBABWE%' then 'ZW'
 else '' end as 'company-locationCountry'
--, cms.Site_Phone_Number as 'company-switchBoard1'
, left(ltrim(cms.Site_Phone_Number),99) as 'company-switchBoard'
, cms.Site_Fax_Number as 'company-fax'
--, lc.localities
, iif(cms.Web_Address like '%.%',cms.Web_Address,'')as 'company-website'
, coalesce(c.Documents_Names_001 + ',' + c.Documents_Names_002 + ',' + c.Documents_Names_003,'') as 'company-document'
, left(Concat(
			'Company External ID: GP', cms.Site_Unique_ID,char(10)
			, iif(Account_Code = '' or Account_Code is NULL,'',Concat(char(10), 'Account Code: ', Account_Code, char(10)))
			, iif(Main_Site_Unique = '' or Main_Site_Unique is NULL,'',Concat(char(10), 'Main Site Unique: ', Main_Site_Unique, char(10)))
			, iif(cms.Upper_Org_Name = '' or cms.Upper_Org_Name is NULL,'',Concat(char(10), 'Upper Org Name: ', cms.Upper_Org_Name, char(10)))
			, iif(CHARINDEX(':',cms.Creation_Date)<>0,concat(char(10), 'Creation Date: ', substring(cms.Creation_Date,6,2),'/',substring(cms.Creation_Date,9,2),'/',left(cms.Creation_Date,4),char(10)),concat(char(10), 'Creation Date: ',cms.Creation_Date,char(10)))
			--, iif(cms.Creation_Date = '' or cms.Creation_Date is NULL,'',Concat(char(10), 'Creation Date: ', cms.Creation_Date, char(10)))
			, iif(cms.Creating_User = '' or cms.Creating_User is NULL,'',Concat(char(10), 'Creating User: ', cms.Creating_User, char(10)))
			, iif(CHARINDEX(':',cms.Amendment_Date)<>0,concat(char(10), 'Amendment Date: ', substring(cms.Amendment_Date,6,2),'/',substring(cms.Amendment_Date,9,2),'/',left(cms.Amendment_Date,4),char(10)),concat(char(10), 'Amendment Date: ',cms.Amendment_Date,char(10)))
			--, iif(c.Amendment_Date = '' or c.Amendment_Date is NULL,'',Concat(char(10), 'Amendment Date: ', c.Amendment_Date, char(10)))
			, iif(cms.Amending_User = '' or cms.Amending_User is NULL,'',Concat(char(10), 'Amending User: ', cms.Amending_User, char(10)))
			, iif(c.Client_Importance = '' or c.Client_Importance is NULL,'',Concat(char(10), 'Client Importance: ', c.Client_Importance, char(10)))
			, iif(c.Client_Status = '' or c.Client_Status is NULL,'',Concat(char(10), 'Client Status: ', c.Client_Status, char(10)))
			, iif(cms.Locality = '' or cms.Locality is NULL,'',Concat(char(10), 'Locality: ', cms.Locality, char(10)))
			--, iif(sw.switchboard = '' or sw.switchboard is NULL,'',Concat(char(10), 'Phone Number: ', sw.switchboard, char(10)))
			, iif(c.Credit_Limit = '' or c.Credit_Limit is NULL,'',Concat(char(10), 'Credit Limit: ', c.Credit_Limit, char(10)))
			, iif(c.Business_Area_001 = '' or c.Business_Area_001 is NULL,'',Concat(char(10), 'Business Area: ', c.Business_Area_001, char(10)))
			, iif(c.Business_Type_001 = '' or c.Business_Type_001 is NULL,'',Concat(char(10), 'Business Type: ', c.Business_Type_001, char(10)))
			, iif(c.Enquiry_Source = '' or c.Enquiry_Source is NULL,'',Concat(char(10), 'Enquiry Source: ', c.Enquiry_Source, char(10)))
			, iif(cms.Compressed_Name = '' or cms.Compressed_Name is NULL,'',Concat(char(10), 'Compressed Name: ', cms.Compressed_Name, char(10)))
			, iif(cms.Email = '' or cms.Email is NULL,'',Concat(char(10), 'Email: ', cms.Email, char(10)))
			, iif(cms.Sales_Representative = '' or cms.Sales_Representative is NULL,'',Concat(char(10), 'Sales Representative: ', cms.Sales_Representative, char(10)))
			, iif(cms.Short_Name = '' or cms.Short_Name is NULL,'',Concat(char(10), 'Short Name: ', cms.Short_Name, char(10)))
			, iif(cms.Country_Code = '' or cms.Country_Code is NULL,'',Concat(char(10), 'Country Code: ', cms.Country_Code, char(10)))
			, iif(c.ClientProfile = '' or c.ClientProfile is NULL,'',Concat(char(10), 'Profile: ',char(10), c.ClientProfile, char(10)))
			, iif(c.ClientNotes = '' or c.ClientNotes is NULL,'',Concat(char(10),'Notes: ',char(10),c.ClientNotes))),32000)
			as 'company-note'
FROM ContactManagementSites cms
			left join clients1 c on cms.Site_Unique_ID = c.Main_Site_Unique
			left join dup on cms.Site_Unique_ID = dup.Site_Unique_ID
			left join Users u on cms.Account_Manager = u.UserName
			left join loc on cms.Site_Unique_ID = loc.Site_Unique_ID
			--left join locality lc on c.Account_Name = lc.Upper_Org_Name
			--left join switchboard sw on c.Account_Name = sw.Upper_Org_Name
--			where dup.rn>1
--where cms.Site_Unique_id = 1071
UNION ALL
select 'GP9999999','','Default Company','','','','','','','','','','','This is Default Company from Data Import'

