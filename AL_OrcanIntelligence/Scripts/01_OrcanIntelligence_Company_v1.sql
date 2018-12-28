-----------------------------------------------------
-- OrcanIntelligence - Company
------------------------------------------------------
declare @NewLineChar as char(2) = char(13) + char(10);

with AccountDupCheck as (
	select Id, Name, row_number() over(partition by Name order by CreatedDate) as RowNum
	from Account
)

select

trim(isnull(x.Id, '')) as [company-externalId]

, (select top 1 iif(adc.RowNum > 1, isnull(x.Name, '') + ' (' + cast(adc.RowNum as varchar(10)) + ')', isnull(x.Name, '')) from AccountDupCheck adc where adc.Id = x.Id and adc.Name = x.Name) as [company-name]

-- populate location name from state and country if location name not found
, trim('., ' from concat(
		iif(len(trim('., ' from isnull(x.BillingState, ''))) > 0, trim('., ' from isnull(BillingState, '')), '')
		, iif(len(trim('., ' from isnull(x.BillingCountry, ''))) > 0, '., ' + trim(', ' from isnull(BillingCountry, '')), '')
		)
) as [company-locationName]

-- populate full address
, trim('., ' from concat(
		trim(isnull(x.BillingStreet, ''))
		, iif(len(trim('., ' from isnull(x.BillingCity, ''))) > 0, ', ' + trim('., ' from isnull(BillingCity, '')), '')
		, iif(len(trim('., ' from isnull(x.BillingState, ''))) > 0, ', ' + trim('., ' from isnull(BillingState, '')), '')
		, iif(len(trim('., ' from isnull(x.BillingPostalCode, ''))) > 0, ', ' + trim('., ' from isnull(BillingPostalCode, '')), '')
		, iif(len(trim('., ' from isnull(x.BillingCountry, ''))) > 0, ', ' + trim('., ' from isnull(BillingCountry, '')), '')
		)
)  as [company-locationAddress]

, trim('., ' from isnull(x.BillingCity, '')) as [company-locationCity]
--, db-field-not-found as [company-locationDistrict]

, trim('., ' from isnull(x.BillingState, '')) as [company-locationState]

, iif(len(trim(isnull(x.BillingCountry, ''))) > 0
	, isnull((select top 1 [Code] from [VC_Countries] ccd
		where lower(ccd.Name) = lower(trim(isnull(x.BillingCountry, '')))
			or lower(ccd.Code) = lower(trim(isnull(x.BillingCountry, '')))), '')
	, 'GB'
) as [company-locationCountry]

, trim('., ' from isnull(x.BillingPostalCode, '')) as [company-locationZipCode]
--, db-field-not-found as [company-nearestTrainStation]
--, db-field-not-found as [company-headQuarter]

-- get all phone
, replace(replace(trim('.,!/ '  from x.phone), ' ', ''), '/', ',') as [company-phone]

, replace(replace(trim('.,!/ '  from x.Fax), ' ', ''), '/', ',') as [company-fax]
--, db-field-not-found as [company-switchBoard]

, [dbo].ufn_RefineWebAddress(x.website) as [company-website]

, trim(@NewLineChar from 'External ID: ' + x.Id
	+ iif(len(trim(isnull(x.Type, ''))) > 0, @NewLineChar + 'Type: ' + trim(isnull(x.Type, '')), '')
	+ iif(len(trim(isnull(x.Description, ''))) > 0, @NewLineChar + 'Description:' + @NewLineChar + trim(isnull(x.Description, '')), '')
	+ iif(len(trim(isnull(x.LastModifiedById, ''))) > 0 and u.Id is not null
		, @NewLineChar + 'Last Modified By: ' + trim(isnull(u.FirstName, '') + ' ' + isnull(u.LastName, '') + ' - ' + isnull(u.Email, ''))
		, '')
)
as [company-note]
--, db-field-not-found as [company-owners]
, isnull(cds.Docs, '') as [company-document]

from
Account x
left join [User] u on x.LastModifiedById = u.Id
left join VC_Com_Docs cds on x.Id = cds.AccountId
where x.IsDeleted = 0
order by x.CreatedDate