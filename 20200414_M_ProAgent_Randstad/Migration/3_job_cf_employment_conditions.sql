--#CF | Salary form | Free Text
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, 給与形態 as field_value --salary form
	, current_timestamp as insert_timestamp
from csv_job
where nullif(給与形態, '') is not NULL


--#CF | Annual bonus | Text field
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1066 as field_id
	, [賞与 回数] as field_value --Annual bonus number
	, current_timestamp as insert_timestamp
from csv_job
where nullif([賞与 回数], '') is not NULL


--#CF | Annual Bonus Previous Year Record | Text field
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1067 as field_id
	, [賞与 昨年実績] as field_value --Annual bonus previous year
	, current_timestamp as insert_timestamp
from csv_job
where nullif([賞与 昨年実績], '') is not NULL


--#CF | Annual Bonus (Details) | Change to Text Area
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1068 as field_id
	, concat_ws(concat(char(10),char(13))
		, coalesce('【賞与 メモ】' + nullif([賞与 メモ], ''), NULL) --annual bonus memo
		, coalesce('【賞与月・賞与メモ】' + nullif([賞与月・賞与メモ], ''), NULL) --bonus month, memo
		) as field_value 
	, current_timestamp as insert_timestamp
from csv_job
where coalesce(nullif([賞与 メモ], ''), nullif([賞与月・賞与メモ], '')) is not NULL


--#CF | Type of introduction | Radio | 1261
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1261 as field_id
	,case [紹介区分] 
		when '紹介' then 'Permanent'
		when '紹介予定派遣' then 'Temp to Perm'
		when '派遣後紹介' then 'Perm after Temp'
		end as field_value --Position Type
	, current_timestamp as insert_timestamp
from csv_job
where nullif([紹介区分], '') is not NULL


--#CF Benefit Details | Text Area | 1080
select [PANO ] as job_ext_id
, 'add_job_info' as additional_type
, 1003 as form_id
, 1080 as field_id
, current_timestamp as insert_timestamp
, concat_ws(concat(char(10),char(13))
	, coalesce('【諸手当】' + char(10) + '■' + nullif(replace([諸手当], char(10), concat(char(10),'■')),''), NULL) --various benefits
	, coalesce('【諸手当メモ】' + nullif([諸手当メモ], ''), NULL) --various benefits memo
	, coalesce('【待遇・福利厚生】' + char(10) + '■' + nullif(replace([待遇・福利厚生], char(10), concat(char(10),'■')),''), NULL) --Reward and welfare benefits
	, coalesce('【待遇・福利厚生メモ】' + nullif([待遇・福利厚生メモ], ''), NULL) --Reward and welfare benefits memo
	) as benefit_memo
from csv_job
where coalesce(nullif([諸手当], ''), nullif([諸手当メモ], ''), nullif([待遇・福利厚生], ''), nullif([待遇・福利厚生メモ], '')) is not NULL


--#CF Travel Expense | Free Text | 1077
select [PANO ] as job_ext_id
, 'add_job_info' as additional_type
, 1003 as form_id
, 1077 as field_id
, current_timestamp as insert_timestamp
, trim(通勤交通費) as field_value --Commuter traffic expenses
from csv_job
where nullif([通勤交通費], '') is not NULL


--#CF Travel Expenses (Details) | Text Area | 1078
select [PANO ] as job_ext_id
, 'add_job_info' as additional_type
, 1003 as form_id
, 1078 as field_id
, current_timestamp as insert_timestamp
, trim(通勤交通費メモ) as field_value --Commuter traffic expenses memo
from csv_job
where nullif([通勤交通費メモ], '') is not NULL


--#CF Insurance (Details) | Text Area | 1079
select [PANO ] as job_ext_id
, 'add_job_info' as additional_type
, 1003 as form_id
, 1079 as field_id
, current_timestamp as insert_timestamp
, concat_ws(concat(char(10),char(13))
	, coalesce('【保険】' + char(10) + '■' + nullif(replace([保険], char(10), concat(char(10),'■')),'') + char(10), NULL) --insurance
	, coalesce('【保険メモ】' + nullif([保険メモ], ''), NULL) --insurance memo
	) as field_value
from csv_job
where coalesce(nullif([保険], ''), nullif([保険メモ], '')) is not NULL


--#CF Retirement Age | Text Area | 11296
select [PANO ] as job_ext_id
, 'add_job_info' as additional_type
, 1003 as form_id
, 11296 as field_id
, current_timestamp as insert_timestamp
, concat_ws(concat(char(10),char(13))
	, coalesce('【定年制】' + char(10) + nullif([定年制], '') , NULL) --retirement age
	, coalesce('【定年制メモ】' + char(10) + nullif([定年制メモ], ''), NULL) --retirement memo
	) as field_value
from csv_job
where coalesce(nullif([定年制], ''), nullif([定年制メモ], '')) is not NULL