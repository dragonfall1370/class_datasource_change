--Candidate current address
select [PANO ] as cand_ext_id
, concat_ws(', '
	, coalesce(nullif([住所 都道府県], ''), NULL) --prefecture
	, coalesce(nullif([住所 住所], ''), NULL) --remaining address
	) as location_name
, concat_ws(', '
	--, coalesce('〒' + nullif([住所 〒], ''), NULL) --postal code
	, coalesce(nullif([住所 都道府県], ''), NULL) --prefecture
	, coalesce(nullif([住所 住所], ''), NULL) --remaining address
	) as location_address
, [住所 都道府県] as location_state
, [住所 〒] as post_code
, current_timestamp as insert_timestamp
from csv_can
where coalesce(nullif([住所 都道府県], ''), nullif([住所 〒], ''), nullif([住所 住所], '')) is not NULL