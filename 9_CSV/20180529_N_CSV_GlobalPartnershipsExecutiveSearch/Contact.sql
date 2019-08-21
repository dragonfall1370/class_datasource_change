select contactcompanyId 'contact-companyId'
	,contactexternalId 'contact-externalId'
	, contactfirstName 'contact-firstName'
	, contactlastName 'contact-lastName'
	,contactjobTitle 'contact-jobTitle'
	, replace(ltrim(Stuff(
			  Coalesce(' ' + NULLIF(companylocationAddress, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationCity, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationState, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationZipCode, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationCountry, ''), '')
			, 1, 1, '')),' ,',',') as 'company-locationAddress'
	, replace(ltrim(Stuff(
			  Coalesce(' ' + NULLIF(companylocationCity, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationState, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationCountry, ''), '')
			, 1, 1, '')),' ,',',') as 'company-locationName'
	, companylocationCity as 'company-locationCity'
	, companylocationCountry as 'company-locationCountry'
	, companylocationState as 'company-locationState'
	, companylocationZipCode as 'company-locationZipCode'
	, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(contactphone1, ''), '')
			+ Coalesce(',' + NULLIF(contactphone2, ''), '')
			, 1, 1, '')) as 'contact-phone'
	, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(contactemail1, ''), '')
			+ Coalesce(',' + NULLIF(contactemail2, ''), '')
			, 1, 1, '')) as 'contact-email'
	, Stuff( Coalesce('Contact External ID: '+ contactexternalId + char(10),'')
			+ Coalesce('Contact Notes: ' + char(10) + nullif(contactNote,'') + char(10), '')
            , 1, 0, '') as 'contact-note'
from contact
--WHERE contactcompanyId in ('B0397')
--select * from contact