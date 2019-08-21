with
temp as (
select contactexternalId,contactcompanyId-- as externalid
	, replace(ltrim(Stuff(
			  Coalesce(' ' + NULLIF(companylocationAddress, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationCity, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationState, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationZipCode, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationCountry, ''), '')
			, 1, 1, '')),' ,',',') as locationAddress
	, replace(ltrim(Stuff(
			  Coalesce(' ' + NULLIF(companylocationCity, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationState, ''), '')
			+ Coalesce(', ' + NULLIF(companylocationCountry, ''), '')
			, 1, 1, '')),' ,',',') as locationName
	, companylocationCity as City
	, companylocationCountry as CountryCode
	, companylocationState as State
	, companylocationZipCode as ZipCode
	--, CURRENT_TIMESTAMP as insert_timestamp
from contact
)
, temp2 as(
select distinct contactcompanyId,locationAddress,locationName,city,CountryCode,State,ZipCode--, ROW_NUMBER() OVER(PARTITION BY contactcompanyId ORDER BY locationaddress ASC) AS rn 
from temp where locationName is not null)
--order by contactcompanyId

,temp3 as (
select *, ROW_NUMBER() OVER(PARTITION BY contactcompanyId ORDER BY locationaddress ASC) AS rn
	, concat(contactcompanyId,ROW_NUMBER() OVER(PARTITION BY contactcompanyId ORDER BY locationaddress ASC)) as addressExternalId
from temp2)

--select * from temp3

--select temp.*,temp3.addressExternalId
--from temp left join temp3 on temp.contactcompanyId = temp3.contactcompanyId and temp.locationAddress = temp3.locationAddress

select temp.contactexternalId, convert(varchar(20),temp3.addressExternalId) addressExternalId,CURRENT_TIMESTAMP as insertTimeStamp
from temp left join temp3 on temp.contactcompanyId = temp3.contactcompanyId and temp.locationAddress = temp3.locationAddress
where addressExternalId is not null and addressExternalId like 'B%' and temp.contactexternalId like 'C%'

--select concat('EPW',ContactId) as contactExternalId, AddressId as AddressExternalId, CURRENT_TIMESTAMP as insertTimeStamp
--from CompanyContactAddress