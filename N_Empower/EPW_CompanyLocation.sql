with
temp as (select ca.CompanyId, ca.HeadOffice,ca.AddressTypeId,a.*
, replace(replace(replace(replace(ltrim(Stuff(
			  Coalesce(' ' + NULLIF(ltrim(rtrim(Street)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(City)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(County)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(PostCode)), ''), '')
			+ Coalesce(', ' + NULLIF(ltrim(rtrim(Country)), ''), '')
			+ Coalesce(' (TelNo. ' + NULLIF(ltrim(rtrim(TelNo)), '') + ')', '')
			, 1, 1, '')),char(10),', '),char(13),''),'  ',''),' ,',',') as 'locationName'
from CompanyAddress ca left join Address a on ca.AddressId = a.AddressId)

select concat('EPW',CompanyId) as externalID, City, County, PostCode, locationName, AddressId as AddressExternalId
		, case 
			when locationName like '%Australia%' then 'AU'
			when locationName like '%United Kingdom%' then 'GB'
			when locationName like '%Bahrain%' then 'BH'
			when locationName like '%Sydney%' then 'AU'
			when locationName like '%Brisbane%' then 'AU'
			when locationName like '%NSW%' then 'AU'
			when locationName like '%QLD%' then 'AU'
			when locationName like '%New Zealand%' then 'NZ'
  else 'AU' end as CountryCode
from temp
where locationName is not null and locationName <> '?, ?'

--select * from address

