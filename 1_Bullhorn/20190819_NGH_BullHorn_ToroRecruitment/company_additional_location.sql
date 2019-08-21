--Company additional location
select concat('TR', CC.clientCorporationID) as COMPANY_ID
	, left(concat_ws(', ', nullif(billingAddress1,''), nullif(billingAddress2,''), nullif(cc.billingCity,''), nullif(cc.billingState,'')
			, nullif(cc.billingZip,''), nullif(tc.COUNTRY,'')),300) as locationName
	, left(concat_ws(', ', nullif(billingAddress1,''), nullif(billingAddress2,''), nullif(cc.billingCity,''), nullif(cc.billingState,'')
			, nullif(cc.billingZip,''), nullif(tc.COUNTRY,'')),300) as locationAddress
	, 'BILLING_ADDRESS' as location_type
	, getdate() as insert_timestamp
	, cc.billingState as [state]
	, cc.billingZip as post_code
	, cc.billingCity as city
from bullhorn1.BH_ClientCorporation cc
left join tmp_country tc ON CC.billingCountryID = tc.code
where billingAddress1 <> '' or billingAddress2 <> '' or billingCountryID <> '';