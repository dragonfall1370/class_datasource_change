--#CF | Job Check item | Multiple selection
with job_checkitem as (select [PANO ] as job_ext_id
	, [チェック項目]
	, value as job_checkitem
	from csv_job
	cross apply string_split([チェック項目], char(10))
	where [チェック項目] <> '')

, final_checkitem as (select job_ext_id
	, job_checkitem
	, case job_checkitem
		when '外資系企業' then 'Foreign-affiliated company'
		when '上場企業' then 'Listed company'
		when 'ベンチャー企業' then 'Venture company'
		when '海外勤務あり' then 'Work abroad'
		when '転勤なし' then 'No transfer'
		when '女性活躍中' then 'Women works active'
		when '女性比率4割以上' then 'Female is more than 40%'
		when '服装自由' then 'No dress code'
		when '面接1回' then 'One interview'
		when '土日祝休み' then 'Weekends and holidays'
		when '完全週休2日制' then 'Two days off a week'
		when '年間休日120日以上' then 'More than 120 days holiday'
		when '月平均残業時間20時間以内' then 'Overtime hours within 20 hours'
		when 'フレックスタイム制' then 'Flex time system'
		when '語学力を活かす' then 'Use language skills'
		when '未経験可' then 'Inexperienced'
		when 'Ｕ・Ｉターン歓迎' then 'UI turn welcome' --edited | can be modified on PROD
		when 'グローバル人材歓迎' then 'Global human resources welcome'
		when '新規立上げメンバー' then 'Newly launched member'
		else NULL end as final_checkitem
	from job_checkitem
	where job_checkitem is not NULL)


select job_ext_id
	, 'add_job_info' as additional_type
	, 1003 as form_id
	, 11300 as field_id
	, job_checkitem
	, final_checkitem as field_value
	, current_timestamp as insert_timestamp
from final_checkitem
where final_checkitem is not NULL
--and job_ext_id = 'JOB075873'