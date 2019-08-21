/*--UPDATE COMPANY LOCATION as 'HEADQUARTER'
update company_location set location_type = 'HEADQUARTER'
*/

--INSERT COMPANY LOCATION AND MARK 'HEADQUARTER' INSTEAD (used with company ver2)
with CompanyAddress as (select c.address_id, c.company_id
	, case when company_add1 in ('','x','xxx','-','NA','n/a','tbc') then NULL
	else trim(company_add1) end as company_add1
	, case when company_add2 in ('','x','xxx','-','NA','n/a','tbc') then NULL
	else trim(company_add2) end as company_add2
	, case when company_city in ('','x','xxx','-','NA','n/a','tbc') then NULL
	else trim(company_city) end as company_city
	, case when company_zip in ('','x','xxx','-','NA','n/a','tbc') then NULL
	else trim(company_zip) end as company_zip
	, case when company_country in ('','x','xxx','-','NA','n/a','tbc') then NULL 
	else trim(company_country) end as company_country
	, add_default
	, company_tax_ID
	, case 
	when c.company_country = 'Antigua and Barbuda' then 'AG'
	when c.company_country = 'Siggapore' then 'SG'
	when c.company_country = 'Finland' then 'FI'
	when c.company_country = 'U.S.A' then 'US'
	when c.company_country = 'Thailnd' then 'TH'
	when c.company_country = 'South Korea' then 'KR'
	when c.company_country = 'Thialand' then 'TH'
	when c.company_country = 'Thailang' then 'TH'
	when c.company_country = 'Vietnam' then 'VN'
	when c.company_country = 'USA' then 'US'
	when c.company_country = 'Thailnad' then 'TH'
	when c.company_country = 'Angola' then 'AO'
	when c.company_country = 'Tahiland' then 'TH'
	when c.company_country = 'Italy' then 'IT'
	when c.company_country = 'TH1' then 'TH'
	when c.company_country = 'Netherlands' then 'NL'
	when c.company_country = 'Hong Kong, ' then 'HK'
	when c.company_country = 'Hong Kong' then 'HK'
	when c.company_country = 'Malaysia' then 'MY'
	when c.company_country = ' THAILAND' then 'TH'
	when c.company_country = 'Indonesia' then 'ID'
	when c.company_country = 'Thailand.' then 'TH'
	when c.company_country = 'Germany' then 'DE'
	when c.company_country = 'Philippines' then 'PH'
	when c.company_country = 'England' then 'GB'
	when c.company_country = 'Lithuania' then 'LT'
	when c.company_country = 'Singapore (SEA HQ)' then 'SG'
	when c.company_country = 'Thaniland' then 'TH'
	when c.company_country = 'Singapore 609914' then 'SG'
	when c.company_country = 'Thailans' then 'TH'
	when c.company_country = 'SAMUTPRAKARN  THAILAND.' then 'TH'
	when c.company_country = 'Switzerland' then 'CH'
	when c.company_country = 'United States' then 'US'
	when c.company_country = 'Korea' then 'KR'
	when c.company_country = 'The People’s Republic of China' then 'CN'
	when c.company_country = 'Thai' then 'TH'
	when c.company_country = 'Hongkong ' then 'HK'
	when c.company_country = ' PRC ' then 'CN'
	when c.company_country = 'Falkland Islands (UK)' then 'GB'
	when c.company_country = 'Laos' then 'LA'
	when c.company_country = 'Pathumwan, Bangkok' then 'TH'
	when c.company_country = 'Thaiiland' then 'TH'
	when c.company_country = 'Australia' then 'AU'
	when c.company_country = 'The Netheland ' then 'NL'
	when c.company_country = 'Bangkok, Thailand' then 'TH'
	when c.company_country = ' China' then 'CN'
	when c.company_country = 'Thaialnd' then 'TH'
	when c.company_country = 'Thailland' then 'TH'
	when c.company_country = 'Anguilla (UK)' then 'GB'
	when c.company_country = 'Bangkok 10250' then 'TH'
	when c.company_country = 'United Kingdom' then 'GB'
	when c.company_country = 'Mexico' then 'MX'
	when c.company_country = 'HK' then 'HK'
	when c.company_country = 'Albania' then 'AL'
	when c.company_country = 'Myanmar' then 'MM'
	when c.company_country = 'Khlongtoey Nue, Wattana, Bangkok 10110' then 'TH'
	when c.company_country = 'Sweden' then 'SE'
	when c.company_country = 'Netherland' then 'NL'
	when c.company_country = 'Thaliand' then 'TH'
	when c.company_country = 'Thailanad' then 'TH'
	when c.company_country = 'Viet Nam ' then 'VN'
	when c.company_country = ' Indonesia' then 'ID'
	when c.company_country = 'Wattana, Bangkok 10110, Thailand ' then 'TH'
	when c.company_country = 'Britain, Australia' then 'AU'
	when c.company_country = 'Singapre' then 'SG'
	when c.company_country = 'China' then 'CN'
	when c.company_country = 'Argentina' then 'AR'
	when c.company_country = 'People''s Republic of China ' then 'CN'
	when c.company_country = 'CA' then 'CA'
	when c.company_country = 'Armenia' then 'AM'
	when c.company_country = 'PR. China, Hongkong' then 'HK'
	when c.company_country = 'German' then 'DE'
	when c.company_country = '21140 Thailand' then 'TH'
	when c.company_country = 'Bangna, Bangkok 10260' then 'TH'
	when c.company_country = 'Bangkok 10110 Thailand' then 'TH'
	when c.company_country = 'India' then 'IN'
	when c.company_country = 'Hong Kong (China)' then 'HK'
	when c.company_country = 'Austria' then 'AT'
	when c.company_country = 'Greece' then 'GR'
	when c.company_country = 'United Arab Emirates' then 'AE'
	when c.company_country = 'Samutprakarn 10270, Thailand.' then 'TH'
	when c.company_country = 'UK' then 'GB'
	when c.company_country = 'Israel' then 'IL'
	when c.company_country = 'Bangrak Bangkok 10500' then 'TH'
	when c.company_country = 'Kuala Lumpur' then 'ML'
	when c.company_country = 'Thailandd' then 'TH'
	when c.company_country = 'West Malaysia' then 'ML'
	when c.company_country = 'Poland' then 'PL'
	when c.company_country = 'Canada' then 'CA'
	when c.company_country = 'TH' then 'TH'
	when c.company_country = 'Afghanistan' then 'AF'
	when c.company_country = ' Australia ' then 'AU'
	when c.company_country = 'Aruba (Netherlands)' then 'NL'
	when c.company_country = 'Wattana, Bangkok 10110 Thailand' then 'TH'
	when c.company_country = 'IRELAND' then 'IE'
	when c.company_country = 'Wantonglang, Bangkok 10110, Thailand' then 'TH'
	when c.company_country = 'U.S.A.' then 'US'
	when c.company_country = 'Thnailand' then 'TH'
	when c.company_country = 'Bangkok 10110' then 'TH'
	when c.company_country = 'Thailand' then 'TH'
	when c.company_country = 'Norway' then 'NO'
	when c.company_country = 'France' then 'FR'
	when c.company_country = 'THE NETHERLANDS' then 'NL'
	when c.company_country = ' Singapore' then 'SG'
	when c.company_country = 'Belgium' then 'BE'
	when c.company_country = 'Japan' then 'JP'
	when c.company_country = 'Thaoland' then 'TH'
	when c.company_country = 'Thaland' then 'TH'
	when c.company_country = 'Thailand,' then 'TH'
	when c.company_country = 'Singpapore' then 'SG'
	when c.company_country = 'Spain' then 'ES'
	when c.company_country = 'Thaiand' then 'TH'
	when c.company_country = ' India ' then 'IN'
	when c.company_country = 'Turkey' then 'TR'
	when c.company_country = '  Australia' then 'AU'
	when c.company_country = 'Thailamd' then 'TH'
	when c.company_country = 'Bangladesh' then 'BD'
	when c.company_country = 'Denmark' then 'DK'
	when c.company_country = 'Pakistan' then 'PK'
	when c.company_country = 'Singapore' then 'SG'
	when c.company_country = 'Thailamnd' then 'TH'
	when c.company_country = 'Cambodia' then 'KH'
	when c.company_country = 'singapore (head office)' then 'SG'
	when c.company_country = 'HQ - UK' then 'GB'
	when c.company_country = 'bangkok Thailand 10600' then 'TH'
	when c.company_country = 'Czech Republic' then 'CZ'
	when c.company_country = 'Bangkok 10330' then 'TH'
	when c.company_country = 'Taiwan' then 'TW'
	when c.company_country = 'Italian' then 'IT'
	when c.company_country = 'Phuket' then 'TH'
	when c.company_country = 'SG' then 'SG'
	when c.company_country = 'Thailan d' then 'TH'
	else NULL end as countryCode
	, json_value(custom_field,'$.Address.dxTextBox[0].Value') as AddressTel
	, row_number() over (partition by company_id order by address_id desc) as rn
	from company.Addresses c
	)

select concat('PRTR',company_id) as ComExtID
, concat_ws(', '
		, nullif(company_add1,'')
		, nullif(trim(company_city),'')
		, nullif(trim(' ' from company_zip),'')
		, nullif(trim(company_country),'')) as locationName
, concat_ws(', ', nullif(company_add1,'')
		, nullif(company_add2,'')
		, nullif(trim(company_city),'')
		, nullif(trim(' ' from company_zip),'')
		, nullif(company_country,'')) as locationAddress
, trim(company_city) as company_city
, trim(' ' from company_zip) as company_zip
, trim(company_country) as country
, countryCode
, company_add1
, company_add2
, AddressTel as phone_number
, case when add_default = 1 then 'HEADQUARTER'
else NULL end as location_type
, address_id as geo_name_id --used [company_location] > [geo_name_id] for reference
, concat('PRTR',address_id) as AddressID --to be appened to Note
, getdate() as  insert_timestamp
from CompanyAddress
where (company_add1 is not NULL or company_add2 is not NULL or company_city is not NULL or company_country is not NULL)
--where rn > 1 --filter to get the latest address | use default address as Headquarters instead
--where AddressTel is not NULL
order by company_id desc