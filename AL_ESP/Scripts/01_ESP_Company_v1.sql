-----------------------------------------------------
-- ESP - Company
------------------------------------------------------
drop table if exists [dbo].[VCCompanies]

declare @NewLineChar as char(1) = char(10);
declare @chars4trimming nvarchar(255) = '., '

select

trim(isnull(x.Id, '')) as [company-externalId]

, iif(
	ais.RowNum > 1
	, concat(isnull(x.Name, ''), ' (' + cast(ais.RowNum as varchar(10)), ')')
	, isnull(x.Name, '')
) as [company-name]

-- populate location name from state and country if location name not found
, [dbo].[ufn_PopulateLocationName](
	x.BillingState,
	x.BillingCountry,
	@chars4trimming
) as [company-locationName]

-- populate full address
, [dbo].[ufn_PopulateLocationAddress](
	x.BillingStreet,
	x.BillingCity,
	x.BillingState,
	x.BillingPostalCode,
	x.BillingCountry,
	@chars4trimming
) as [company-locationAddress]

, [dbo].[ufn_TrimSpecifiedCharacters](x.BillingCity, @chars4trimming) as [company-locationCity]
--, db-field-not-found as [company-locationDistrict]

, [dbo].[ufn_TrimSpecifiedCharacters](x.BillingState, @chars4trimming) as [company-locationState]

, iif(len([dbo].[ufn_TrimSpecifiedCharacters](x.BillingCountry, @chars4trimming)) > 0
	, case([dbo].[ufn_TrimSpecifiedCharacters](x.BillingCountry, @chars4trimming))
		when 'Canada' then 'CA'
		when 'USA' then 'US'
		when 'England' then 'GB'
		when 'Scotland' then 'GB'
		when 'United Kingdom' then 'GB'
	end
	--, isnull((select top 1 [Code] from [VCCountries] ccd
	--	where lower(ccd.Name) = lower(trim(isnull(x.BillingCountry, '')))
	--		or lower(ccd.Code) = lower(trim(isnull(x.BillingCountry, '')))), '')
	, 'GB'
) as [company-locationCountry]

, [dbo].[ufn_TrimSpecifiedCharacters](x.BillingPostalCode, @chars4trimming) as [company-locationZipCode]
--, db-field-not-found as [company-nearestTrainStation]
--, db-field-not-found as [company-headQuarter]

-- get all phone
, [dbo].[ufn_RefinePhoneNumber](x.phone) as [company-phone]

, [dbo].[ufn_RefinePhoneNumber](x.Fax) as [company-fax]
--, db-field-not-found as [company-switchBoard]

, [dbo].ufn_RefineWebAddress(x.website) as [company-website]

, trim(@NewLineChar from 'External ID: ' + x.Id
	+ iif(len(trim(isnull(x.NumberOfEmployees, ''))) > 0, @NewLineChar + 'Number Of Employees: ' + trim(isnull(x.NumberOfEmployees, '')), '')
	+ iif(len(trim(isnull(x.Description, ''))) > 0, @NewLineChar + 'Description:' + @NewLineChar + trim(isnull(x.Description, '')), '')
	--+ iif(len(trim(isnull(x.LastModifiedById, ''))) > 0 and u.Id is not null
	--	, @NewLineChar + 'Last Modified By: ' + trim(isnull(u.FirstName, '') + ' ' + isnull(u.LastName, '') + ' - ' + isnull(u.Email, ''))
	--	, '')
)
as [company-note]

, u.Username as [company-owners]

, isnull(cds.Docs, '') as [company-document]

, cast(x.CreatedDate as datetime) CreatedDate

into [dbo].[VCCompanies]

from
Account x
left join VCAccIdxs ais on x.Id = ais.Id
left join [User] u on x.OwnerId = u.Id
left join VCComDocs cds on x.Id = cds.AccountId
where x.IsDeleted = 0

--select count(*) from Account

--select count(*) from VCAccIdxs

--select count(*) from VCCompanies

select * from VCCompanies
order by CreatedDate

--select count(distinct [company-name]) from VCCompanies