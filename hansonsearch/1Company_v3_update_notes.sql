/*
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

, tmp2 as (select clientCorporationID, 
concat(iif(Address2 = '' or Address2 is NULL,'',concat(Address2,char(10)))
,iif(DateAdded = '' or DateAdded is NULL,'',concat(DateAdded,char(10)))
,iif(CompanyDescription = '' or CompanyDescription is NULL,'',concat(CompanyDescription,char(10)))
,iif(YearFounded = '' or YearFounded is NULL,'',concat(YearFounded,char(10)))
,iif(Industry = '' or Industry is NULL,'',concat(Industry,char(10)))
,iif(Competitors = '' or Competitors is NULL,'',concat(Competitors,char(10)))
,iif(BusinessSector = '' or BusinessSector is NULL,'',concat(BusinessSector,char(10)))
,iif(Status1 = '' or Status1 is NULL,'',concat(Status1,char(10)))
,iif(CompanyCoverage = '' or CompanyCoverage is NULL,'',concat(CompanyCoverage,char(10)))
,iif(NoEmployees = '' or NoEmployees is NULL,'',concat(NoEmployees,char(10)))
,iif(OwnershipS = '' or OwnershipS is NULL,'',concat(OwnershipS,char(10)))
,iif(CompanyNotes = '' or CompanyNotes is NULL,'',concat(CompanyNotes,char(10)))
,iif(Twitter = '' or Twitter is NULL,'',concat(Twitter,char(10)))
,iif(Facebook = '' or Facebook is NULL,'',concat(Facebook,char(10)))
,iif(LinkedIn = '' or LinkedIn is NULL,'',concat(LinkedIn,char(10)))
,iif(Instagram = '' or Instagram is NULL,'',concat(Instagram,char(10)))
,iif(Culture = '' or Culture is NULL,'',Culture))
as CombinedNote
from tmp1)

--select * from tmp2

-- Get candidates files
,  tmp_5 (clientCorporationID, name) as (select a.clientCorporationID, concat(a.clientCorporationFileID,'-co-doc', a.fileExtension) from bullhorn1.BH_ClientCorporationFile a)
 --select * from tmp_5
--where a.type = 'Resume') ==> get all candidates files
, tmp_6 (clientCorporationID, ResumeId) as (SELECT clientCorporationID, STUFF((SELECT DISTINCT ',' + name from tmp_5 WHERE clientCorporationID = a.clientCorporationID FOR XML PATH ('')), 1, 1, '')  AS URLList FROM tmp_5 AS a GROUP BY a.clientCorporationID)
--select * from tmp_6 order by clientCorporationID
*/

--select top 100
SELECT
	CC.CompanyId as 'company-externalId'
	, case when CC.CompanyName in (select CC.CompanyName from dbo.Companies CC group by CC.CompanyName having count(*) > 1) then concat (dup.CompanyName,' ',dup.rn)
                when (CC.CompanyName = '' or CC.CompanyName is null) then 'no company name'
                else CC.CompanyName end as 'company-name'
	, iif(CC.Location != '' or CC.Location is not NULL, concat(CC.Location,case when (CC.Country = '' or CC.Country is null) then '' else concat (' - ',CC.Country) end), CC.Country) as 'company-locationName'
	--, as 'company-locationAddress'
	--, as 'company-locationCity'
	--, as 'company-locationState'
	
	--, CASE WHEN (CC.Country = '' OR CC.Country = 'NULL') THEN '' ELSE CC.Country END as 'company-locationCountry'
	, case
		when CC.Country like '%Allemagne%' then 'DE'
		when CC.Country like '%Argentina%' then 'AR'
		when CC.Country like '%Australia%' then 'AU'
		when CC.Country like '%Austria%' then 'AT'
		when CC.Country like '%Bahrain%' then 'BH'
		when CC.Country like '%Belgium%' then 'BE'
		when CC.Country like '%Belguim%' then 'BE'
		when CC.Country like '%BERLIN%' then 'DE'
		when CC.Country like '%Brazil%' then 'BR'
		when CC.Country like '%Brussel%' then 'BE'
		when CC.Country like '%Cambodia%' then 'KH'
		when CC.Country like '%Canada%' then 'CA'
		when CC.Country like '%Chile%' then 'CL'
		when CC.Country like '%China%' then 'CN'
		when CC.Country like '%Clichy%' then 'FR'
		when CC.Country like '%Colombia%' then 'CO'
		when CC.Country like '%Czech%' then 'CZ'
		when CC.Country like '%DUBAI%' then 'AE'
		when CC.Country like '%England%' then 'GB'
		when CC.Country like '%États-Unis%' then 'US'
		when CC.Country like '%France%' then 'FR'
		when CC.Country like '%Germany%' then 'DE'
		when CC.Country like '%Great%' then 'GB'
		when CC.Country like '%Hamburg%' then 'DE'
		when CC.Country like '%Hong%' then 'CN'
		when CC.Country like '%India%' then 'IN'
		when CC.Country like '%INDONESIA%' then 'ID'
		when CC.Country like '%Ireland%' then 'IE'
		when CC.Country like '%Israel%' then 'IL'
		when CC.Country like '%Italy%' then 'IT'
		when CC.Country like '%Japan%' then 'JP'
		when CC.Country like '%Jordan%' then 'JO'
		when CC.Country like '%Kuwait%' then 'KW'
		when CC.Country like '%Lebanon%' then 'LB'
		when CC.Country like '%london%' then 'GB'
		when CC.Country like '%Louvain-la-Neuve%' then 'BE'
		when CC.Country like '%Luxembourg%' then 'LU'
		when CC.Country like '%Malaysia%' then 'MY'
		when CC.Country like '%Mexico%' then 'MX'
		when CC.Country like '%Morocco%' then 'MA'
		when CC.Country like '%Netherlands%' then 'NL'
		when CC.Country like '%New%' then 'NZ'
		when CC.Country like '%Norway%' then 'NO'
		when CC.Country like '%Oman%' then 'OM'
		when CC.Country like '%Paris%' then 'FR'
		when CC.Country like '%Peru%' then 'PE'
		when CC.Country like '%Poland%' then 'PL'
		when CC.Country like '%Portugal%' then 'PT'
		when CC.Country like '%Qatar%' then 'QA'
		when CC.Country like '%Romania%' then 'RO'
		when CC.Country like '%RSA%' then 'RU'
		when CC.Country like '%Russia%' then 'RU'
		when CC.Country like '%Saint-Ouen%' then 'FR'
		when CC.Country like '%Saudi%' then 'SA'
		when CC.Country like '%Scotland%' then 'GB'
		when CC.Country like '%Singapore%' then 'SG'
		when CC.Country like '%South%' then 'ZA'
		when CC.Country like '%Spain%' then 'ES'
		when CC.Country like '%Suède%' then 'SE'
		when CC.Country like '%Sweden%' then 'SE'
		when CC.Country like '%Switzerland%' then 'CH'
		when CC.Country like '%Thailand%' then 'TH'
		when CC.Country like '%Turkey%' then 'TR'
		when CC.Country like '%UAE%' then 'AE'
		when CC.Country like '%UKraine%' then 'UA'
		when CC.Country like '%UKr%' then 'UA'
		when CC.Country like '%U.K.%' then 'GB'
		when CC.Country like '%UK%' then 'UA'
		when CC.Country like '%United Arab Emirates%' then 'AE'
		when CC.Country like '%United Kingdom%' then 'GB'
		when CC.Country like '%United States%' then 'US'
		when CC.Country like '%USA%' then 'US'
		when CC.Country like '%Wales%' then 'GB'
	end as 'company-locationCountry'
	
	, CC.Postcode as 'company-locationZipCode'
	, CC.TelNo as 'company-phone'
	--, as 'company-fax'
	, CC.WebSite as 'company-website'
	--, CC.Owner as 'company-owner'
	, at.Filename as 'company-document'
	, concat(
		-- case when (CC.Email = '' or CC.Email is null) then '' else concat('Email: ',CC.Email,char(10)) end
		--,case when (CC.SubLocation = '' or CC.SubLocation is null) then '' else concat('Sub Location: ',CC.SubLocation,char(10)) end
		 case when (CC.CompanySkills = '' or CC.CompanySkills is null) then '' else concat('Company Skills: ',CC.CompanySkills,char(10)) end
		,case when (CC.ParentCompany = '' or CC.ParentCompany is null) then '' else concat('Parent Company: ',CC.ParentCompany,char(10)) end
		,case when (CC.companystatus = '' or CC.companystatus is null) then '' else concat('Status: ',CC.companystatus,char(10)) end
		--,case when (CC.companysource = '' or CC.companysource is null) then '' else concat('Source: ',CC.companysource,char(10)) end
		--,case when (CC.Owner = '' or CC.Owner is null) then '' else concat('Owner: ',CC.Owner,char(10)) end
		--,case when (CC.Sector = '' or CC.Sector is null) then '' else concat('Sector: ',CC.Sector,char(10)) end
		--,case when (CC.Segment = '' or CC.Segment is null) then '' else concat('Segment: ',CC.Segment,char(10)) end
		--,case when (CC.RegDate = '' or CC.RegDate is null) then '' else concat('Reg Date: ',CC.RegDate,char(10)) end
		--,case when (CC.Lastupdate = '' or CC.Lastupdate is null) then '' else concat('Last Update: ',CC.Lastupdate,char(10)) end
		--,case when (CC.LastUser = '' or CC.LastUser is null) then '' else concat('Last User: ',CC.LastUser,char(10)) end
		--,case when CC.restricted = 'false' then concat('Restricted: No',char(10)) when CC.restricted = 'true' then concat('Restricted: Yes',char(10)) end
		,case when (CC.Description = '' or CC.Description is null) then '' else concat('Description: ',CC.Description,char(10)) end
		,case when (CC.CompanyLog = '' or CC.CompanyLog is null) then '' else concat('Company Log: ',CC.CompanyLog,char(10)) end
	) as 'company-note'

-- select count(*) --17221 (17277)
-- select distinct hotlist
from Companies CC --where CC.CompanyName like '%Hanson Search%' --userid != '' or userid is not null
left join (SELECT id, filename = STUFF((SELECT DISTINCT ',' + 'com_' + replace(filename,',','') from Attachments WHERE id = a.id FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') FROM Attachments a GROUP BY id) at on cc.CompanyId = at.Id
left join (SELECT CompanyId,CompanyName,ROW_NUMBER() OVER(PARTITION BY CC.CompanyName ORDER BY CC.CompanyId DESC) AS rn FROM Companies CC) dup on CC.CompanyId = dup.CompanyId
--where CC.CompanyName like '%Hanson Search%'
--CC.Location is null and CC.Country is not null
--at.Filename is not null
--and CC.country is not NULL and CC.country != ''

-- select companystatus hotlist from Companies CC where companystatus is not null and companystatus <> ''
-- select companysource from Companies CC where companysource is not null and companysource <> ''
-- select hotlist from Companies CC where hotlist is not null and hotlist <> ''
-- SELECT DISTINCT restricted , count (*) from Companies CC where restricted is not null group by CC.Restricted

/*
select at.*
from Companies CC
left join ( SELECT id, filename, ref, replace(ref,',','') as newref from Attachments ) at on cc.CompanyId = at.Id
where at.filename is not null and at.filename <> ''
*/
