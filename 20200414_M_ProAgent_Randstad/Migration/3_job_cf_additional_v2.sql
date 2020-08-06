---#CF | Open to HP (step 1) | RADIO_BUTTON
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1048 as field_id
	, case when [HP公開] = '公開' then 'Open' --'Release'
		when [HP公開] = '非公開' then 'Closed' --'Private'
		--else NULL 
		end as field_value
	, current_timestamp as insert_timestamp
from csv_job
where nullif([HP公開], '') is not NULL


--#CF | Open to HP (step 2) | Drop down
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1048 as field_id
	, case when [募集状況] = 'Close' then 'Closed' --'Private' | 非公開 <> 'Open' | '公開'
		--else NULL 
		end as field_value
	, current_timestamp as insert_timestamp
from csv_job
where [募集状況] = 'Close'


---Trial term / trial memo (ver 2)
--#CF | Trial term | Drop down
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, case [試用期間] --trial term
		when '有' then 'Yes'
		when '無' then 'No'
		else 'Not confirmed' end as field_value --(未選択)
	, current_timestamp as insert_timestamp
from csv_job
where nullif([試用期間], '') is not NULL


--#CF | Trial term memo | Text Area| 20200205 | Length > 265 chars
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, nullif([試用期間メモ], '') as field_value --trial memo
	, current_timestamp as insert_timestamp
from csv_job
where nullif([試用期間メモ], '') is not NULL


--#CF | Annual bonus | Text field | 20200205
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 1066 as field_id
	, case when isnumeric(try_parse([賞与 回数] as int)) = 1 then concat('年', [賞与 回数], '回')
		else NULL end as field_value --Annual bonus number
	, current_timestamp as insert_timestamp
from csv_job
where nullif([賞与 回数], '') is not NULL


--#CF | Salary form memo | Free Text | 20200205
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, 給与形態 as field_value --salary form
	, current_timestamp as insert_timestamp
from csv_job
where nullif(給与形態, '') is not NULL


--#CF | Overtime work | Free text | 20200205
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, concat('月', nullif([残業時間 下限], ''), '～', nullif([残業時間 上限], ''), '時間') as field_value --overtime_work
	, current_timestamp as insert_timestamp
from csv_job
where coalesce(nullif([残業時間 下限], ''), nullif([残業時間 上限], '')) is not NULL


---#CF | HP public approval flag | RADIO_BUTTON | 20200206
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, HP公開承認フラグ --HP public approval flag
	, case [HP公開承認フラグ]
		when 'ＨＰ公開申請' then 'Currently applying' --ja: 申請中
		when '却下' then 'Rejection'
		when '承認' then 'Approval'
		else 'Currently applying' end as field_value
	, current_timestamp as insert_timestamp
from csv_job
where 1=1


---#CF | RS office | Multiple selection
/* Added 125 values 
select distinct [RS担当オフィス]
from csv_job
where [RS担当オフィス] <> ''
order by [RS担当オフィス] --125 values */

select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id --RS office
	, [RS担当オフィス] as field_value --free text
	, current_timestamp as insert_timestamp
from csv_job
where nullif([RS担当オフィス], '') is not NULL


--#CF Holidays (Details) | Text Area | 1070
with holiday_details as (select [PANO ] as job_ext_id
, 'add_job_info' as additional_type
, 1003 as form_id
, 1070 as field_id
, concat_ws(concat(char(10), char(13))
	, coalesce('【有給休暇 初年度】' + char(10) + nullif([有給休暇 初年度], ''), NULL) --paid holidays first year
	, coalesce('【有給休暇 発生月】' + char(10) + '入社' + nullif([有給休暇 発生月] + 'ヶ月後', ''), NULL) --paid holidays (after joining)
	, coalesce('【年間休日】' + char(10) + nullif([年間休日], ''), NULL) --annual holiday
	, coalesce('【休日メモ】' + char(10) + nullif([休日メモ], ''), NULL) --holiday note
	, coalesce('【休暇】' + char(10) + '■' + nullif(replace([休暇], char(10), concat(char(10),'■')),''), NULL) --vacation
	, coalesce('【休暇メモ】' + char(10) + nullif([休暇メモ], ''), NULL) --vacation memo
	, coalesce('【有給休暇メモ】' + char(10) + nullif([有給休暇メモ], ''), NULL) --paid vacation note
	) as holiday_details
, current_timestamp as insert_timestamp
from csv_job)

select *
from holiday_details
where holiday_details <> ''


--#CF | Salary form memo | Text Area
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 9999 as field_id
	, 給与形態メモ as field_value --salary form memo
	, current_timestamp as insert_timestamp
from csv_job
where nullif(給与形態メモ, '') is not NULL


--#CF | Division in charge | Radio button | 20200224
select [PANO ] as job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 11325 as field_id
	, case when [JOB担当者ユーザID] = 'FPC326' then 'ENG'
		else 'PP' end as field_value --user ID
	, current_timestamp as insert_timestamp
from csv_job
where nullif([JOB担当者ユーザID], '') is not NULL