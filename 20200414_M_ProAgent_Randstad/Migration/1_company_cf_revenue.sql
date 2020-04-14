--#CF | Established Year/Month | Free Text
select [PANO ] as com_ext_id
	, 'add_com_info' as additional_type
	, 1001 as form_id
	, 1004 as field_id
	, concat_ws('/', nullif([設立 年], ''), nullif([設立 月], '')) as field_value
	--, [設立 年]
	--, [設立 月]
	, current_timestamp as insert_timestamp
from csv_recf
where coalesce(nullif([設立 月], ''), nullif([設立 年], '')) is not NULL --17150 rows


UNION ALL
--#CF | Capital | Free Text
select [PANO ] as com_ext_id
	, 'add_com_info' as additional_type
	, 1001 as form_id
	, 1114 as field_id
	, concat('JPY ', [資本金]) as field_value
	, current_timestamp as insert_timestamp
from csv_recf
where coalesce(nullif([資本金], ''), NULL) is not NULL


UNION ALL
--#CF | Amount of sales: Yen1 | Free Text
select [PANO ] as com_ext_id
	, 'add_com_info' as additional_type
	, 1001 as form_id
	, 1007 as field_id
	, [売上高 円1] as field_value
	, current_timestamp as insert_timestamp
from csv_recf
where coalesce(nullif([売上高 円1], ''), NULL) is not NULL


UNION ALL
--#CF | Amount of sales: Year1 | Amount of sales: Month1 | Free Text
select [PANO ] as com_ext_id
	, 'add_com_info' as additional_type
	, 1001 as form_id
	, 1008 as field_id
	, concat_ws('/', coalesce(nullif([売上高 年1],''), '0000'), nullif([売上高 月1],'')) as field_value
	--, [売上高 年1]
	--, [売上高 月1]
	, current_timestamp as insert_timestamp
from csv_recf
where coalesce(nullif([売上高 年1], ''), nullif([売上高 月1],'')) is not NULL


UNION ALL
--#CF | Amount of sales: Yen2 | Free Text
select [PANO ] as com_ext_id
	, 'add_com_info' as additional_type
	, 1001 as form_id
	, 1010 as field_id
	, concat('JPY ', [売上高 円2]) as field_value
	, current_timestamp as insert_timestamp
from csv_recf
where coalesce(nullif([売上高 円2], ''), NULL) is not NULL


UNION ALL
--#CF | Amount of sales: Year2 | Amount of sales: Month2 | Free Text
select [PANO ] as com_ext_id
	, 'add_com_info' as additional_type
	, 1001 as form_id
	, 1011 as field_id
	, concat_ws('/', coalesce(nullif([売上高 年2],''), '0000'), nullif([売上高 月2],'')) as field_value
	--, [売上高 年1]
	--, [売上高 月1]
	, current_timestamp as insert_timestamp
from csv_recf
where coalesce(nullif([売上高 年2], ''), nullif([売上高 月2],'')) is not NULL