with alladdress as (
        select ad.ObjectId,ad.AddressId,adt.Description,ad.Building,ad.Street,ad.District,ad.City,ad.PostCode
		, lvct.ValueName as 'County'
		, lvc.ValueName as 'Country'
		, lvc.SystemCode as CountryCode
		, row_number() OVER(partition by ad.ObjectID order by ad.AddressID desc) AS rn --,ad.CountyValueId,ad.CountryValueId
        from dbo.Address ad
        left join dbo.AddressTypes adt on ad.AddressTypeId=adt.AddressTypeId
        left join dbo.ListValues lvc on lvc.ListValueId=ad.CountryValueId --country
        left join dbo.ListValues lvct on lvct.ListValueId=ad.CountyValueId --county
)

, add_address as (select concat('NP', aad.ObjectId) as com_ext_id
      , nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.Building),'') as Building
      , nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.Street),'') as Street
      , nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.District),'') as District
      , nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.City),'') as city
      , nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.PostCode),'') as post_code
      , nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.County),'') as [state]
      , nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.Country),'') as Country
			, nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.CountryCode),'') as CountryCode
      , left(concat_ws(', ',
					nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.Building),''),
					nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.Street),''),
					nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.District),''),
					nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.City),''),
					nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.PostCode),''),
					nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.County),''),
					nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.Country),'')),300) as 'company_address'
			, getdate() as insert_timestamp
from alladdress aad
where rn > 1)

select *
from add_address
where Building is not NULL or city is not NULL or Country is not NULL or CountryCode is not NULL