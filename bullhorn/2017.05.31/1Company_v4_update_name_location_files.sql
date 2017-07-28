
with 
tmp1 as (
	select clientCorporationID
	, case when (CC.address2 = '' OR CC.address2 is NULL) THEN '' ELSE concat('Address 2: ',CC.address2) END as Address2
	, case when (CC.dateAdded = '' OR CC.dateAdded is NULL) THEN '' ELSE concat('Date Added: ',convert(varchar(10),CC.dateAdded,120)) END as DateAdded
	, case when (cast(CC.companyDescription as varchar(max)) = '' OR CC.companyDescription is NULL) THEN '' ELSE concat('Company Description: ', ltrim(rtrim([dbo].[udf_StripHTML](CC.companyDescription)))) END as CompanyDescription
	, case when (CC.dateFounded = '' OR CC.dateFounded is NULL) THEN '' ELSE concat('Year Founded: ',convert(varchar(4),CC.dateFounded,120)) END as YearFounded
	, case when (cast(CC.industryList as varchar(max)) = '' OR CC.industryList is NULL) THEN '' ELSE concat('Industry: ',CC.industryList) END as Industry
	, case when (cast(CC.competitors as varchar(max)) = '' OR CC.competitors is NULL) THEN '' ELSE concat('Competitors: ',CC.competitors) END as Competitors
	, case when (cast(CC.businessSectorList as varchar(max)) = '' OR CC.businessSectorList is NULL) THEN '' ELSE concat('Business Sector: ',CC.businessSectorList) END as BusinessSector
	, case when (CC.status = '' OR CC.status is NULL) THEN '' ELSE concat('Status: ',CC.status) END as Status1
	--, iif (CC.customText5 = '' OR CC.customText5 is NULL,'',concat('Company Coverage: ',CC.customText5)) as CompanyCoverage
	, iif (CC.numEmployees = '' OR CC.numEmployees is NULL,'',concat('No. of Employees: ',CC.numEmployees)) as NoEmployees
	, iif (CC.ownership = '' OR CC.ownership is NULL,'',concat('Ownership: ',CC.ownership)) as OwnershipS
	, case when (cast(CC.notes as varchar(max)) = '' OR CC.notes is NULL) THEN '' ELSE concat('Company Overview: ',[dbo].[udf_StripHTML](CC.notes)) END as CompanyNotes
	, iif (CC.twitterHandle = '' OR CC.twitterHandle is NULL,'',concat('Twitter: ',CC.twitterHandle)) as Twitter
	, iif (CC.facebookProfileName = '' OR CC.facebookProfileName is NULL,'',concat('Facebook: ',CC.facebookProfileName)) as Facebook
	, iif (CC.linkedinProfileName = '' OR CC.linkedinProfileName is NULL,'',concat('LinkedIn: ',CC.linkedinProfileName)) as LinkedIn
	, iif (cast(CC.culture as varchar(max)) = '' OR CC.culture is NULL,'',concat('Culture: ',CC.culture)) as Culture
	, iif (CC.feeArrangement = '' OR CC.feeArrangement is NULL,'',concat('Standard Perm Fee (%): ',CC.feeArrangement)) as StandardPermFee
	from bullhorn1.BH_ClientCorporation CC)

, tmp2 as (
	select clientCorporationID, 
	concat(iif(Address2 = '' or Address2 is NULL,'',concat(Address2,char(10)))
		,iif(DateAdded = '' or DateAdded is NULL,'',concat(DateAdded,char(10)))
		,iif(CompanyDescription = '' or CompanyDescription is NULL,'',concat(CompanyDescription,char(10)))
		,iif(YearFounded = '' or YearFounded is NULL,'',concat(YearFounded,char(10)))
		,iif(Industry = '' or Industry is NULL,'',concat(Industry,char(10)))
		,iif(Competitors = '' or Competitors is NULL,'',concat(Competitors,char(10)))
		,iif(BusinessSector = '' or BusinessSector is NULL,'',concat(BusinessSector,char(10)))
		,iif(Status1 = '' or Status1 is NULL,'',concat(Status1,char(10)))
		--,iif(CompanyCoverage = '' or CompanyCoverage is NULL,'',concat(CompanyCoverage,char(10)))
		,iif(NoEmployees = '' or NoEmployees is NULL,'',concat(NoEmployees,char(10)))
		,iif(OwnershipS = '' or OwnershipS is NULL,'',concat(OwnershipS,char(10)))
		,iif(CompanyNotes = '' or CompanyNotes is NULL,'',concat(CompanyNotes,char(10)))
		,iif(Twitter = '' or Twitter is NULL,'',concat(Twitter,char(10)))
		,iif(Facebook = '' or Facebook is NULL,'',concat(Facebook,char(10)))
		,iif(LinkedIn = '' or LinkedIn is NULL,'',concat(LinkedIn,char(10)))
		--,iif(Instagram = '' or Instagram is NULL,'',concat(Instagram,char(10)))
		,iif(Culture = '' or Culture is NULL,'',concat(Culture,char(10)))
		,iif(StandardPermFee = '' or StandardPermFee is NULL,'',StandardPermFee))
	as CombinedNote
	from tmp1)

/* Get candidates files  */
, with tmp_5 (clientCorporationID, name) as (select a.clientCorporationID, concat('company-',a.clientCorporationFileID,a.fileExtension) from bullhorn1.BH_ClientCorporationFile a where fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf')) --where a.type = 'Resume' ==> get all candidates files
-- select * from bullhorn1.BH_ClientCorporationFile
, tmp_6 (clientCorporationID,ResumeId) as (SELECT clientCorporationID, STUFF((SELECT DISTINCT ',' + name from tmp_5 WHERE clientCorporationID = a.clientCorporationID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS string  FROM tmp_5 as a GROUP BY a.clientCorporationID)
--select CC.NAME, tmp_6.ResumeId as 'company-document' from bullhorn1.BH_ClientCorporation CC left join tmp_6 on CC.clientCorporationID = tmp_6.clientCorporationID where tmp_6.ResumeId is not null


, dup as (SELECT clientCorporationID,name,ROW_NUMBER() OVER(PARTITION BY CC.name ORDER BY CC.clientCorporationID ASC) AS rn FROM bullhorn1.BH_ClientCorporation CC 
        --where name like 'Azurance'
        )

select CC.clientCorporationID as 'company-externalId'
, iif(CC.clientCorporationID in (select clientCorporationID from dup where dup.rn > 1),concat(dup.name,' ',dup.rn)
	, iif(CC.NAME = '' or CC.name is null,'No CompanyName',CC.NAME)) as 'company-name'
	, iif(CC.address1 = '' or CC.address1 is NULL,concat(CC.name,' - ',tc.country)
	, concat(CC.address1,iif(CC.address2 = '' or CC.address2 is NULL,'',concat(', ',CC.address2))
	, iif(CC.city = '' or CC.city is NULL,'',concat(', ',CC.city))
	, iif(CC.state = '' or CC.state is NULL,'',concat(', ',CC.state,', ')),tc.country)) as 'company-locationName'
	, iif(CC.address1 = '' or CC.address1 is NULL,concat(CC.name,' - ',tc.country)
	, concat(CC.address1,iif(CC.address2 = '' or CC.address2 is NULL,'',concat(', ',CC.address2))
	, iif(CC.city = '' or CC.city is NULL,'',concat(', ',CC.city))
	, iif(CC.state = '' or CC.state is NULL,'',concat(', ',CC.state,', ')),tc.country)) as 'company-locationAddress'
	, CC.city as 'company-locationCity'
	, CC.state as 'company-locationState'
	, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL') THEN ''
		ELSE tc.abbreviation
		END as 'company-locationCountry'
	, CC.zip as 'company-locationZipCode'
	, CC.phone as 'company-phone'
	, CC.fax as 'company-fax'
	, CC.companyURL as 'company-website'
	, tmp_6.ResumeId as 'company-document'
	, replace(replace(replace(replace(replace(
	       concat('BH Company ID: ',CC.clientCorporationID,char(10)
	       ,ltrim(rtrim([dbo].[udf_StripHTML](tmp2.CombinedNote))))
	       ,'&nbsp;',''),'&amp;','&'),'&#39;',''''),'&ldquo;','"'),'&rdquo;','"') as 'company-note'
-- select count (*) --4763
from bullhorn1.BH_ClientCorporation CC
left join tmp_country tc ON CC.countryID = tc.code
left join tmp2 on CC.clientCorporationID = tmp2.clientCorporationID
left join tmp_6 on CC.clientCorporationID = tmp_6.clientCorporationID
left join dup on CC.clientCorporationID = dup.clientCorporationID