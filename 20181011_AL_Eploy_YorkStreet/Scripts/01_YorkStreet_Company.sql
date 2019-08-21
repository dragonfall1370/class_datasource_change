declare @NewLineChar as char(2) = char(13) + char(10);
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar;

drop table if exists VCCompanies

select

cis.ComId as [company-externalId]

, trim(isnull(cis.ComName, '')) as [company-name]

, trim(isnull(l.Description, '')) as [company-locationName]

, cis.FullAddress as [company-locationAddress]

, [dbo].[ufn_TrimSpecifiedCharacters](x.Town, '., ') as [company-locationCity]

, [dbo].[ufn_TrimSpecifiedCharacters](x.County, '., ') as [company-locationState]

, [dbo].[ufn_TrimSpecifiedCharacters](x.PostCode, '., ') as [company-locationZipCode]

, cs.Code as [company-locationCountry]

, cis.HeadQuater as [company-headQuarter]

, [dbo].[ufn_RefinePhoneNumber](x.Telephone) as [company-phone]

, [dbo].[ufn_RefineWebAddress](x.WebsiteAddress) as [company-website]

, concat(
	concat('External ID: ', cis.ComId)
	, iif(len(trim(isnull(cis.ParentName, ''))) = 0
		, ''
		, concat(@DoubleNewLine, 'Parent Company: ', cis.ParentName)
	)
	, iif(len(trim(isnull(x.NatureOfBusiness, ''))) = 0
		, ''
		, concat(@DoubleNewLine, 'Nature of Business:', @NewLineChar, x.NatureOfBusiness)
	)
	, iif(len(trim(isnull(i.Description, ''))) = 0
		, ''
		, concat(@DoubleNewLine, 'Industry: ', i.Description)
	)
	, iif(len(trim(isnull(cst.Description, ''))) = 0
		, ''
		, concat(@DoubleNewLine, 'Company Status: ', cst.Description)
	)
	, iif(len(trim(isnull(x.SpecialistArea, ''))) = 0
		, ''
		, concat(@DoubleNewLine, 'Specialist Area:', @NewLineChar, x.SpecialistArea)
	)
	, iif(len(trim(isnull(cots.Description, ''))) = 0
		, ''
		, concat(@DoubleNewLine, 'Contact Type: ', cots.Description)
	)
	, iif(len(trim(isnull(x.Email, ''))) = 0
		, ''
		, concat(@DoubleNewLine, 'Email: ', x.Email)
	)
	, iif(len(trim(isnull(cast(x.InvoiceTerms as nvarchar(max)), ''))) = 0
		, ''
		, concat(@DoubleNewLine, 'Specialist Area:', @NewLineChar
			, trim(isnull(cast(x.InvoiceTerms as nvarchar(max)), '')))
	)
	, iif(x.VerificationDate is null
		, ''
		, concat(@DoubleNewLine, 'Verification Date: ', x.VerificationDate)
	)
	, iif(x.ContactByTypeId = 0
		, ''
		, concat(@DoubleNewLine, 'Contact By: ', iif(x.ContactByTypeId = 1, 'Email', ''))
	)
	, iif(x.Turnover is null
		, ''
		, concat(@DoubleNewLine, 'Turnover: ', x.Turnover)
	)
	, iif(x.NoOfPremises is null
		, ''
		, concat(@DoubleNewLine, 'No Of Premises: ', x.NoOfPremises)
	)
	, concat(@DoubleNewLine, 'Sales Rank: ', iif(x.SalesRank is null, 'Not Ranked' , x.SalesRank))
	, iif(len(trim(isnull(cast(x.Comments as nvarchar(max)), ''))) = 0
		, ''
		, concat(@DoubleNewLine, 'Specialist Area:', @NewLineChar
			, trim(isnull(cast(x.Comments as nvarchar(max)), '')))
	)
)
as [company-note]

, cis.OwnerEmails as [company-owners]
 
, cd.Docs as [company-document]

into VCCompanies

from
VCComIdxs cis
left join CompanyDetails x on cis.ComId = x.CompanyID
left join Locations l on x.LocationID = l.LocationID
left join VCComDocs cd on cis.ComId = cd.ComId
left join Industries i on x.IndustryID = i.IndustryID
left join CompanyStatus cst on x.CompanyStatusID = cst.CompanyStatusID
left join ContactTypes cots on x.ContactType = cots.ContactTypeID
left join VCCountries cs on
	iif(lower(trim(isnull(cis.Country, ''))) = 'uk', 'gb', lower(trim(isnull(cis.Country, '')))) = lower(trim(cs.Name))
	or iif(lower(trim(isnull(cis.Country, ''))) = 'uk', 'gb', lower(trim(isnull(cis.Country, '')))) = lower(trim(cs.Code))

select * from VCCompanies

--where [company-note] like '%reference%'

--select distinct
--c.CompanyID AS [company-externalId]
--, isnull(c.Name, '') as [company-name]
--, iif(len(trim(isnull(c.Telephone, ''))) = 0
--	, iif(len(trim(isnull(c.Telephone2, ''))) = 0
--		, iif(len(trim(isnull(c.Mobile, ''))) = 0
--			, ''
--			, trim(isnull(c.Mobile, ''))
--		)
--		, trim(isnull(c.Telephone2, ''))
--	)
--	, trim(isnull(c.Telephone, ''))
--) as [company-phone]
--, COALESCE(c.Fax, '') AS [company-fax]
--, COALESCE(c.WebsiteAddress, '')  AS [company-website]
--, IIF(c.HeadOffice = 1,
--	TRIM(COALESCE(c.Address1, '')
--	+ ' ' + COALESCE(c.Address2, '')
--	+ ' ' + COALESCE(c.Address3, '')
--	+ ' ' + COALESCE(c.Town, '')
--	+ ' ' + COALESCE(c.County, '')
--	+ ' ' + COALESCE((SELECT TOP 1 co.Description FROM [Countries] co WHERE co.CountryID = c.CountryID), '')
--	+ ' ' + COALESCE(c.PostCode, '')),
--	IIF(c.ParentCompanyID <> 0,
--		TRIM(COALESCE(c2.Address1, '')
--		+ ' ' + COALESCE(c2.Address2, '')
--		+ ' ' + COALESCE(c2.Address3, '')
--		+ ' ' + COALESCE(c2.Town, '')
--		+ ' ' + COALESCE(c2.County, '')
--		+ ' ' + COALESCE((SELECT TOP 1 co.Description FROM [Countries] co WHERE co.CountryID = c2.CountryID), '')
--		+ ' ' + COALESCE(c2.PostCode, '')), '')
--) AS [company-headQuarter]
--, COALESCE(l.Description, '') AS [company-locationName]
--, COALESCE(TRIM(COALESCE(c.Address1, '')
--		+ ' ' + COALESCE(c.Address2, '')
--		+ ' ' + COALESCE(c.Address3, '')), '') AS [company-locationAddress]
--, COALESCE(TRIM(COALESCE(c.County, '')), '') AS [company-locationCity]
--, COALESCE(TRIM(COALESCE(c.Town, '')), '') AS [company-locationState]
--,COALESCE(
--	(SELECT TOP(1) [Code] FROM [dbo].[VincereCountryCodeDic] vcc
--		WHERE UPPER(TRIM(COALESCE(iif((SELECT TOP 1 co.Description FROM [Countries] co WHERE co.CountryID = c.CountryID)='UK', 'United Kingdom', (SELECT TOP 1 co.Description FROM [Countries] co WHERE co.CountryID = c.CountryID)), ''))) = UPPER(vcc.[Name])
--	), ''
--) AS [company-locationCountry]

--, COALESCE(TRIM(COALESCE(c.PostCode, '')), '') AS [company-locationZipCode]
--, iif(len(trim(isnull(c2.Name, ''))) > 0, '' + trim(isnull(c2.Name, '')), '')
--, trim(@NewLineChar from 
--	concat(
--		ufn_GenerateNVarcharString('Parent Company: ', c2.Name)
--		, ufn_GenerateNVarcharString('Nature of Business: ', c.NatureOfBusiness)
--		, ufn_GenerateNVarcharString('Industry: ', i.Description)
--		, ufn_GenerateNVarcharString('Company Status: ', cs.Description)
--		, ufn_GenerateNVarcharString('Telephone2: ', c.Telephone2)
--		, ufn_GenerateNVarcharString('Fax: ', c.Fax)
--		, ufn_GenerateNVarcharString('Mobile: ', c.Mobile)
--		, ufn_GenerateNVarcharString('Reference: ', c.Reference)
--		, ufn_GenerateNVarcharString('Specialist Area: ', c.SpecialistArea)
--		, ufn_GenerateNVarcharString('Contact Type: ', ct.Description)
--		, ufn_GenerateNVarcharString('Email: ', COALESCE(c.Email, c.Email2, ''))
--		, ufn_GenerateNVarcharStringFromInt('Invoice Contact: ', c.InvoiceContactID)
--		, @NewLineChar + 'Invoice Address: ' +
--			iif(c.InvoiceAddressID is not null and c.InvoiceAddressID <> 0
--				, (select top 1 
--					TRIM(COALESCE(ad.Address1, '')
--					+ ' ' + COALESCE(ad.Address2, '')
--					+ ' ' + COALESCE(ad.Address3, '')
--					+ ' ' + COALESCE(ad.Town, '')
--					+ ' ' + COALESCE(ad.County, '')
--					+ ' ' + COALESCE((SELECT TOP 1 co.Description FROM [Countries] co WHERE co.CountryID = ad.CountryID), '')
--					+ ' ' + COALESCE(c2.PostCode, ''))
--					from Addresses ad where c.InvoiceAddressID = ad.AddressID)
--				, ''
--			)
--		, @NewLineChar + 'Tax Code:' + 
--			iif(c.TaxCodeID is not null and c.TaxCodeID <> 0
--				, (select top 1 isnull(tc.Reference, '') + '-' + isnull(tc.Description, '') from TaxCodes tc where c.TaxCodeID = tc.TaxCodeID)
--				, ''
--			)
--		, ufn_GenerateNVarcharStringFromNText('Invoice Terms: ', c.InvoiceTerms)
--		, ufn_GenerateNVarcharStringFromDateTime('Verification Date: ', c.VerificationDate) 
--		, ufn_GenerateNVarcharString('Contact By: ', cbt.Description)
--		, ufn_GenerateNVarcharStringFromFloat('Turnover: ', c.Turnover)
--		, ufn_GenerateNVarcharStringFromInt('No of Premises: ', c.NoOfPremises)
--		, ufn_GenerateNVarcharStringFromInt('Sales Rank: ', c.SalesRank)
--		, ufn_GenerateNVarcharStringFromNText('Comments: ', c.Comments)
--	)
--) AS [company-note]

--, coalesce(
--	(
--		select top 1 string_agg(sfp.FileName, ',')
--		from
--		StoredFilePaths sfp
--		join RecordTypes rt on sfp.RecordTypeID = rt.RecordTypeID
--		where rt.Description like '%Companies%' and sfp.RecordID = c.CompanyID
--		group by sfp.RecordID
--	)
--	, ''
--) as [company-document]
--FROM
--[CompanyDetails] c
--left join [CompanyDetails] c2 ON c.ParentCompanyID = c2.CompanyID
--left join [Industries] i ON c.IndustryID = i.IndustryID
--left join [CompanyStatus] cs ON c.CompanyStatusID = cs.CompanyStatusID
--left join [Locations] l ON c.LocationID = l.LocationID
--left join [ContactTypes] ct ON c.ContactType = ct.ContactTypeID
--left join [ContactByTypes] cbt ON c.ContactByTypeID = cbt.ContactByTypeID