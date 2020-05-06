with contactloc as (select clientID
		, cl.clientCorporationID
		, uc.address1
		, uc.address2
		, uc.city
		, uc.state
		, uc.zip
		, uc.countryID
		, tc.COUNTRY
		, tc.ABBREVIATION
		, row_number() over(partition by cl.clientCorporationID order by lower(concat(uc.address1, uc.address2, city, state, zip, tc.COUNTRY))) as rn
from bullhorn1.BH_Client cl
left join bullhorn1.BH_UserContact uc ON cl.userID = uc.userID
left join tmp_country tc on tc.CODE = uc.countryID
where 1=1
--and (uc.address1 is not NULL or uc.address2 is not NULL)
and (uc.address1 <> '' or uc.address2 <> ''))

--CONTACT LOCATION AS COMPANY LOCATION
select concat('PR', clientID) as con_ext_id
	, concat('PR', clientCorporationID) as com_ext_id
	, left(concat_ws(', ', nullif(address1,''), nullif(address2,''), nullif(city,''), nullif(state,'')
			, nullif(zip,''), nullif(COUNTRY,'')),300) as locationName
	, left(concat_ws(', ', nullif(address1,''), nullif(address2,''), nullif(city,''), nullif(state,'')
			, nullif(zip,''), nullif(COUNTRY,'')),300) as locationAddress
	, city as city
	, state as [state]
	, zip as post_code
	, ABBREVIATION as country_code
	, 'PERSONAL_ADDRESS' as location_type
	, getdate() as insert_timestamp
	, concat_ws('-', 'PR', clientCorporationID, clientID) as note --combination of company-contact IDs
from contactloc
where 1=1
and rn = 1 --249 rows
and lower(concat(address1, address2, city, state, zip, countryID)) not in (select lower(concat(address1, address2, city, state, zip, countryID)) from bullhorn1.BH_ClientCorporation) --70 rows