--Company CF
---#CF PANO No.
select [PANO ] as com_ext_id
	, 'add_com_info' as additional_type
	, 1001 as form_id
	, 9999 as field_id
	, [PANO ] as field_value
	, current_timestamp as insert_timestamp
from csv_recf


---#CF Name of representative
select [PANO ] as com_ext_id
	, 'add_com_info' as additional_type
	, 1001 as form_id
	, 9999 as field_id
	, trim(代表者氏名) as field_value
	, current_timestamp as insert_timestamp
from csv_recf
where nullif(代表者氏名, '') is not NULL


--#CF Reg Date
select [PANO ] as com_ext_id
, convert(datetime, 登録日, 120) as reg_date
from csv_recf


--Number of employees (FINAL)
select [PANO ] as com_ext_id
	, 'add_com_info' as additional_type
	, 1001 as form_id
	, 9999 as field_id
	, trim([従業員]) as field_value
	, try_parse([従業員] collate SQL_Latin1_General_CP1_CI_AS as int) as employees
	, current_timestamp as insert_timestamp
from csv_recf
where nullif([従業員], '') is not NULL
--and [PANO] = 'CPY015169'
and try_parse([従業員] collate SQL_Latin1_General_CP1_CI_AS as int) is not NULL