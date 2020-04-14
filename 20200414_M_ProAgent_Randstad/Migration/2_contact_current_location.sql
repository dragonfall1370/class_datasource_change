--Contact current location
select [採用担当者ID] as con_ext_id --added contact_external_id in case
--, concat_ws(', '
--	, coalesce('〒' + nullif([所在地 〒], ''), NULL)
--	, coalesce(nullif([所在地 都道府県], ''), NULL)
--	, coalesce(nullif([所在地詳細], ''), NULL)
--	) as location_name
, '担当者住所' as location_name --updated 20200212
, concat_ws(', '
	--, coalesce('〒' + nullif([所在地 〒], ''), NULL)
	, coalesce(nullif([所在地 都道府県], ''), NULL)
	, coalesce(nullif([所在地詳細], ''), NULL)
	) as location_address
, [所在地 都道府県] as location_state
, [所在地 〒] as post_code
, current_timestamp as insert_timestamp
from csv_rec
where coalesce(nullif([所在地 都道府県], ''), nullif([所在地 〒], ''), nullif([所在地詳細], '')) is not NULL

/* UPDATE CONTACT CURRENT LOCATION
select address, right(address, length(address) - length(concat(post_code)) - 3) as address
, *
from common_location
where contact_id is not NULL
and nullif(post_code, '') is not NULL

update common_location
set address = right(address, length(address) - length(concat(post_code)) - 3)
where contact_id is not NULL
and nullif(post_code, '') is not NULL

*/