declare @NewLineChar as char(2) = char(13) + char(10);
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar;

drop table if exists VCJobs

select

iif(x.ContactID <> 0
	, x.ContactID
	, (select top 1 ContactId from Contacts c where c.CompanyID = x.CompanyId)
) as [position-contactId]

, cis.JobId as [position-externalId]

, cis.JobTitle as [position-title]

, trim(isnull(cast(x.PositionsAvailable as varchar(10)), '')) as [position-headcount]

, cis.OwnerEmails as [position-owners]

, (case lower(trim(isnull(vts.Description, '')))
	when lower('Permanent') then 'PERMANENT'
	when lower('Contract') then 'CONTRACT'
	when lower('Part Time') then 'TEMPORARY'
	when lower('Temp') then 'TEMPORARY'
	else 'PERMANENT'
end) as [position-type]

, 'GBP' as [position-currency]

, trim(coalesce(cast(x.Salary as varchar(20)), '')) as [position-actualSalary]
--, db-field-not-found as [position-payRate]
--, db-field-not-found as [position-contractLength]

, concat(
	'<div><b>Job Advert:</b><br><br></div>'
	, trim(isnull(cast(x.Description as nvarchar(max)), ''))
	, '<div>------------------------------<br>'
	, '<b>Job Specification:</b><br><br><div/>'
	, trim(isnull(cast(x.Qualifications as nvarchar(max)), ''))
	, '<div>------------------------------<br>'
	, '<b>Benefits:</b><br><br><div/>'
	, trim(isnull(cast(x.Benefits as nvarchar(max)), ''))
) as [position-publicDescription]

--, coalesce(x.PrivateDescription, '') as [position-internalDescription]

, iif(x.StartDate is not null
	, trim(isnull(convert(varchar(50), x.StartDate, 111), ''))
	, trim(isnull(convert(varchar(50), x.DatePosted, 111), ''))
) as [position-startDate]

, trim(coalesce(convert(varchar(50), x.EndDate, 111), '')) as [position-endDate]

, concat(
	concat('External Id: ', cis.JobId)
	, concat(@DoubleNewLine, 'Reference: ', trim(isnull(x.Reference, '')))
	, concat(@DoubleNewLine, 'Recruitment Workflow: ', x.ApplicationWorkflowID)
	, concat(@DoubleNewLine, 'Automatically send workflow emails for this vacancy: '
		, iif(x.AutoSendWorkflowEmails = 1, 'Yes', 'No')
	)
	, concat(@DoubleNewLine, 'Status: ', vs.Description)

	, concat(@DoubleNewLine, 'Location: ', l.Description)
	
	, concat(@DoubleNewLine, 'Industry: ', i.Description)
	
	, concat(@DoubleNewLine, 'Position: ', p.Description)

	, concat(@DoubleNewLine, 'Company Name: ', comIdxs.ComName)

	, concat(@DoubleNewLine, 'Logo URL: ', x.LogoURL)

	, concat(@DoubleNewLine, 'Display Logo: ', iif(x.DisplayLogo is null, '', iif(x.DisplayLogo = 1, 'YES', 'NO')))

	, concat(@DoubleNewLine, 'Hours Per Week: ', x.HoursPerWeek)
	
	, concat(@DoubleNewLine, 'Charge (One off): ', x.ChargeToClient)
	
	, concat(@DoubleNewLine, 'Working Hours: ', x.WorkingHours)
	
	, concat(@DoubleNewLine, 'Report To: ', x.ReportTo)
	
	, concat(@DoubleNewLine, 'Number of References Required: ', x.CandidateReferenceCount)
	
	, concat(@DoubleNewLine, 'Company Address: ', comIdxs.FullAddress)

	, concat(@DoubleNewLine, 'Vacancy Address: '
		, [dbo].[ufn_PopulateLocationAddressUK](
			x.Address1,
			x.Address2,
			x.Address3,
			x.Town,
			x.County,
			x.PostCode,
			trim(isnull(cj.Description, 'UK')),
			'., '
		)
	)

	, concat(@DoubleNewLine, 'Telephone Number: ', x.Telephone)
	, concat(@DoubleNewLine, 'Fax: ', x.Fax)
	, concat(@DoubleNewLine, 'Mobile: ', x.Mobile)
	, concat(@DoubleNewLine, 'Email: ', x.Email)
	, concat(@DoubleNewLine, 'Website Addres: ', x.WebsiteAddress)

	, concat(@DoubleNewLine, 'Invoice Contact Name: ', x.InvoiceContactName)
	, concat(@DoubleNewLine, 'Invoice Address: '
		, [dbo].[ufn_PopulateLocationAddressUK](
			x.InvoiceAddress1,
			x.InvoiceAddress2,
			x.InvoiceAddress3,
			x.InvoiceTown,
			x.InvoiceCounty,
			x.InvoicePostcode,
			trim(isnull(ci.Description, 'UK')),
			'., '
		)
	)
	
	, concat(@DoubleNewLine, 'Invoice Telephone Number: ', x.InvoiceTelephone)
	, concat(@DoubleNewLine, 'Invoice Fax: ', x.InvoiceFax)
	, concat(@DoubleNewLine, 'Invoice Mobile: ', x.InvoiceMobile)
	, concat(@DoubleNewLine, 'Invoice Email: ', x.InvoiceEmail)
	, concat(@DoubleNewLine, 'Invoice Website Addres: ', x.InvoiceWebsiteAddress)
) as [position-note]

into VCJobs

from VCJobIdxs cis
left join Vacancies x on cis.JobId = x.VacancyID
left join VacancyTypes vts on x.VacancyTypeID = vts.VacancyTypeID
left join Currency c on c.CurrencyID = x.RateCurrencyID
left join VacancyStatus vs on vs.VacancyStatusID = x.VacancyStatus
left join Industries i on x.IndustryID = i.IndustryID
left join Positions p on x.PositionID = p.PositionID
left join Locations l on x.LocationID = l.LocationID
left join VCComIdxs comIdxs on x.CompanyID = comIdxs.ComId
left join Countries cj on x.CountryID = cj.CountryID
left join Countries ci on x.InvoiceCountryID = ci.CountryID
order by cis.JobId

select * from VCJobs

--with
--VacanciesSub as (
--	select
--	v.ContactID
--	, v.VacancyID
--	, trim(coalesce(v.Title, '')) as Title
--	from Vacancies v
--	--where v.ContactID is not null and v.ContactID <> 0
--)


--select

--iif(v.contactID is null or v.contactID = 0
--	, cast((select top 1 ContactID from Contacts where CompanyID = v.CompanyID and ContactID not in (select contactID from VacanciesSub where trim(coalesce(v.Title, '')) = Title)) as varchar(20))
--	, cast(v.ContactID as varchar(20))
--) as [position-contactId]

--, trim(coalesce(cast(v.VacancyID as varchar(20)), '')) as [position-externalId]

--, trim(coalesce(v.Title, '')) as [position-title]

--, trim(coalesce(cast(v.PositionsAvailable as varchar(10)), '')) as [position-headcount]

----, db-field-not-found as [position-owners]

--, isnull([dbo].ufn_ConvertJobTypeYS2VC(v.VacancyTypeID), 'PERMANENT') as [position-type]

--, iif(v.RateCurrencyID is not null and v.RateCurrencyID <> 0
--	, UPPER((select top 1 c.Reference from Currency c where c.CurrencyID = v.RateCurrencyID))
--	, 'GBP'
--) as [position-currency]

--, trim(coalesce(cast(v.Salary as varchar(20)), '')) as [position-actualSalary]
----, db-field-not-found as [position-payRate]
----, db-field-not-found as [position-contractLength]

--, coalesce(v.Description, '') as [position-publicDescription]

--, coalesce(v.PrivateDescription, '') as [position-internalDescription]

--, trim(coalesce(convert(varchar(50), v.StartDate, 111), '')) as [position-startDate]

--, trim(coalesce(convert(varchar(50), v.EndDate, 111), '')) as [position-endDate]

--, concat_ws(
--	@NewLineChar
--	, 'Reference: ' + isnull(v.Reference, '')
--	, 'Recruitment Workflow: ' + cast(isnull(v.ApplicationWorkflowID, '') as varchar(20))
--	, 'Automatically send workflow emails for this vacancy: ' + iif(v.AutoSendWorkflowEmails is not null, iif(v.AutoSendWorkflowEmails = 1, 'YES', 'NO'), '')
--	, 'Status: ' + iif(v.VacancyStatus is not null and v.VacancyStatus <> 0
--		, (select top 1 vs.Description from VacancyStatus vs where vs.VacancyStatusID = v.VacancyStatus)
--		, '')
--	, 'Location: ' + iif(v.LocationID is not null and v.LocationID <> 0
--		, (select top 1 l.Description from Locations l where l.LocationID = v.LocationID)
--		, '')
--	, 'Industry: ' + iif(v.IndustryID is not null and v.IndustryID <> 0
--		, (select top 1 i.Description from Industries i where i.IndustryID = v.IndustryID)
--		, '')
--	, 'Position: ' + iif(v.PositionID is not null and v.PositionID <> 0
--		, (select top 1 p.Description from Positions p where p.PositionID = v.PositionID)
--		, '')
--	, 'Company Display Name: ' + isnull(v.CompanyDisplayName, '')
--	, 'Logo URL: ' + cast(isnull(v.LogoURL, '') as nvarchar(max))
--	, 'Display Logo: ' + iif(v.DisplayLogo is not null, iif(v.DisplayLogo = 1, 'YES', 'NO'), '')
--	, 'Nominal Code: ' + iif(v.NominalCodeID is not null and v.NominalCodeID <> 0
--		, (select top 1 nc.Description from NominalCodes nc where nc.NominalCodeID = v.NominalCodeID)
--		, '')
--	, 'Hours Per Week: ' + cast(isnull(v.HoursPerWeek, '') as varchar(20))
--	, 'Charge (One off): ' + cast(isnull(v.ChargeToClient, '') as varchar(20))
--	, 'Working Hours: ' + cast(isnull(v.WorkingHours, '') as varchar(20))
--	, 'Report To: ' + isnull(v.ReportTo, '')
--	, 'Number of References Required: ' + cast(isnull(v.CandidateReferenceCount, '') as varchar(20))
--	, 'Company Address: ' + (
--		select top 1
--			trim(concat_ws(' '
--					, trim(isnull(cd.Address1, ''))
--					, trim(isnull(cd.Address2, ''))
--					, trim(isnull(cd.Address3, ''))
--					, trim(isnull(cd.Town, ''))
--					, trim(isnull(cd.County, ''))
--					, trim(isnull((select top 1 co.Description from Countries co where cd.CountryID = co.CountryID), ''))
--					, trim(isnull(cd.PostCode, ''))
--				)
--			)
--		from CompanyDetails cd
--		where cd.CompanyID = v.CompanyID
--	)
--	, 'Address 1: ' + isnull(v.Address1, '')
--	, 'Address 2: ' + isnull(v.Address2, '')
--	, 'Address 3: ' + isnull(v.Address3, '')
--	, 'Town: ' + isnull(v.Town, '')
--	, 'County: ' + isnull(v.County, '')
--	, 'Postcode: ' + isnull(v.Postcode, '')
--	, 'Country: ' + isnull((select top 1 co.Description from Countries co where v.CountryID = co.CountryID), '')
--	, 'Telephone Number: ' + isnull(v.Telephone, '')
--	, 'Fax: ' + isnull(v.Fax, '')
--	, 'Mobile: ' + isnull(v.Mobile, '')
--	, 'Email: ' + isnull(v.Email, '')
--	, 'Website Addres: ' + isnull(v.WebsiteAddress, '')
--	, 'Invoice Contact Name: ' + isnull(v.InvoiceContactName, '')
--	, 'Invoice Address 1: ' + isnull(v.InvoiceAddress1, '')
--	, 'Invoice Address 2: ' + isnull(v.InvoiceAddress2, '')
--	, 'Invoice Address 3: ' + isnull(v.InvoiceAddress3, '')
--	, 'Invoice Town: ' + isnull(v.InvoiceTown, '')
--	, 'Invoice County: ' + isnull(v.InvoiceCounty, '')
--	, 'Invoice Postcode: ' + isnull(v.InvoicePostcode, '')
--	, 'Invoice Country: ' + isnull((select top 1 co.Description from Countries co where v.InvoiceCountryID = co.CountryID), '')
--	, 'Invoice Telephone Number: ' + isnull(v.InvoiceTelephone, '')
--	, 'Invoice Fax: ' + isnull(v.InvoiceFax, '')
--	, 'Invoice Mobile: ' + isnull(v.InvoiceMobile, '')
--	, 'Invoice Email: ' + isnull(v.InvoiceEmail, '')
--	, 'Invoice Website Addres: ' + isnull(v.InvoiceWebsiteAddress, '')
--) as [position-note]

----select cast(null as varchar(20))

--, coalesce(
--	(
--		select top 1 string_agg(sfp.FileName, ',')
--		from
--		StoredFilePaths sfp
--		join RecordTypes rt on sfp.RecordTypeID = rt.RecordTypeID
--		where rt.Description like '%Vacancies%' and sfp.RecordID = v.VacancyID
--		group by sfp.RecordID
--	)
--	, ''
--)
--as [position-document]
----, db-field-not-found as [position-otherDocument]

--from
--Vacancies v
--order by
----[position-contactId],
--[position-externalId]