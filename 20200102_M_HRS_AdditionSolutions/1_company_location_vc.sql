--Delete empty company location from VC
delete from company_location
where coalesce(nullif(address, ''), nullif(location_name, ''), nullif(city, ''), nullif(country_code, '')) is NULL --183 rows