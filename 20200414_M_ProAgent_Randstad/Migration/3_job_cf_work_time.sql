--#CF | Work start time | Free Text
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, concat_ws(':', coalesce(nullif([勤務時間 開始時],''), '00') , coalesce(nullif([勤務時間 開始分],''), '00')) as field_value --work start time
	, current_timestamp as insert_timestamp
from csv_job
where coalesce(nullif([勤務時間 開始時], ''), nullif([勤務時間 開始分], '')) is not NULL


--#CF | Work end time | Free Text
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, concat_ws(':', coalesce(nullif([勤務時間 終了時],''), '00') , coalesce(nullif([勤務時間 終了分],''), '00')) as field_value --work end time
	, current_timestamp as insert_timestamp
from csv_job
where coalesce(nullif([勤務時間 終了時], ''), nullif([勤務時間 終了分], '')) is not NULL


--#CF | Overtime work | Free text
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, concat_ws(' - '
		, coalesce('【下限】' + nullif([残業時間 下限], ''), NULL)
		, coalesce('【上限】' + nullif([残業時間 上限], ''), NULL)
		) as field_value --overtime_work
	, current_timestamp as insert_timestamp
from csv_job
where coalesce(nullif([残業時間 下限], ''), nullif([残業時間 上限], '')) is not NULL


--#CF Break time | Text Area
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, [休憩時間] as field_value --Break time
	, current_timestamp as insert_timestamp
from csv_job
where nullif([休憩時間], '') is not NULL


--#CF Employment Period | Text Area
/* --Split for new custom fields
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, [雇用期間] --Employment Period
	, [雇用期間（事業報告用）] --Employment Period (business report)
	, [１人あたりの雇用日数（臨時・日雇の場合は必須）] --employment days per person
	, [試用期間] --trial period
	, [試用期間メモ] --trial period memo
	, concat_ws(char(10)
		, coalesce('[雇用期間]' + nullif([雇用期間], ''), NULL) --employment period
		, coalesce('[雇用期間（事業報告用）]' + nullif([雇用期間（事業報告用）], ''), NULL) --Employment Period (business report)
		, coalesce('[１人あたりの雇用日数（臨時・日雇の場合は必須）]' + nullif([１人あたりの雇用日数（臨時・日雇の場合は必須）], ''), NULL) --employment days per person
		, coalesce('[試用期間]' + nullif([試用期間], ''), NULL) --trial period
		, coalesce('[試用期間メモ]' + nullif([試用期間メモ], ''), NULL) --trial memo
		) as field_value
	, current_timestamp as insert_timestamp
from csv_job
where coalesce(nullif([雇用期間], ''), nullif([雇用期間（事業報告用）], ''), nullif([１人あたりの雇用日数（臨時・日雇の場合は必須）], '')
				, nullif([試用期間], ''), nullif([試用期間メモ],'')) is not NULL
*/

--#CF | Employment Period | Text Area
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, [雇用期間] as field_value --Employment Period
	, current_timestamp as insert_timestamp
from csv_job
where nullif([雇用期間], '') is not NULL


--#CF | Employment Period (business report) | Multiple selection
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, [雇用期間（事業報告用）] --Employment Period (business report)
	, current_timestamp as insert_timestamp
from csv_job
where nullif([雇用期間（事業報告用）], '') is not NULL


--#CF | Employment days per person | Free Text
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, [１人あたりの雇用日数（臨時・日雇の場合は必須）] as field_value --employment days per person
	, current_timestamp as insert_timestamp
from csv_job
where nullif([１人あたりの雇用日数（臨時・日雇の場合は必須）], '') is not NULL


--#CF | Trial period | Text Area
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, concat_ws(char(10)
		, coalesce('【試用期間】' + nullif([試用期間], ''), NULL) --trial period
		, coalesce('【試用期間メモ】' + nullif([試用期間メモ], ''), NULL) --trial memo
		) as field_value
	, current_timestamp as insert_timestamp
from csv_job
where coalesce(nullif([試用期間], ''), nullif([試用期間メモ], '')) is not NULL


--#CF | Salary raise | Text Area
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, [昇給] as field_value --Salary raise
	, current_timestamp as insert_timestamp
from csv_job
where nullif([昇給], '') is not NULL