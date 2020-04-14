with --Candidate work history only take 1 Industry / 1 FE / 1 SFE
all_wh as (select [PANO ] as cand_ext_id
	, [勤務歴 勤務期間 開始年1] --period year from
	, [勤務歴 勤務期間 開始月1] --period month from
	, [勤務歴 勤務期間 終了年1] --period year to
	, [勤務歴 勤務期間 終了月1] --period month to
	, [勤務歴 会社名1] as origin_employer1 --company name
	, '勤務歴 会社名1' as employer1 --company name
	, [勤務歴 役職名1] as job_title1 --job title
	, [勤務歴 雇用形態1] as employmentstatus1 --employment status
	, [勤務歴 担当職務1] as experience1 --experience
	, iif(len([勤務歴 勤務期間 開始年1]) < 4, NULL, convert(date
			, coalesce(nullif(nullif([勤務歴 勤務期間 開始年1],''), '0') 
				+ coalesce(nullif([勤務歴 勤務期間 開始月1],''), '01') + '01', NULL), 120)) as startdate1
	, iif(len([勤務歴 勤務期間 終了年1]) < 4, NULL, eomonth(convert(date
			, coalesce(nullif(nullif([勤務歴 勤務期間 終了年1],''), '0') 
				+ coalesce(nullif([勤務歴 勤務期間 終了月1],''), '01') + '01', NULL), 120))) as enddate1
	, [勤務歴 勤務期間 開始年2] --period year from
	, [勤務歴 勤務期間 開始月2] --period month from
	, [勤務歴 勤務期間 終了年2] --period year to
	, [勤務歴 勤務期間 終了月2] --period month to
	, [勤務歴 会社名2] as origin_employer2 --company name
	, '勤務歴 会社名2' as employer2 --company name
	, [勤務歴 役職名2] as job_title2 --job title
	, [勤務歴 雇用形態2] as employmentstatus2 --employment status
	, [勤務歴 担当職務2] as experience2 --experience
	, iif(len([勤務歴 勤務期間 開始年2]) < 4, NULL, convert(date
			, coalesce(nullif(nullif([勤務歴 勤務期間 開始年2],''), '0') 
				+ coalesce(nullif(nullif([勤務歴 勤務期間 開始月2],''), '00'), '01') + '01', NULL), 120)) as startdate2
	, iif(len([勤務歴 勤務期間 終了年2]) < 4, NULL, eomonth(convert(date
			, coalesce(nullif(nullif([勤務歴 勤務期間 終了年2],''), '0') 
				+ coalesce(nullif(nullif([勤務歴 勤務期間 終了月2],''), '00'), '01') + '01', NULL), 120))) as enddate2
from csv_can c
--where [PANO ] = 'CDT154379'
)

, work_history as (
	select cand_ext_id
	, employer1 as currentEmployer
	, origin_employer1 as origin_employer
	, job_title1 as jobTitle
	, startdate1 as dateRangeFrom
	, enddate1 as dateRangeTo
	, experience1 as experience
	, concat_ws('<br/>'
		, coalesce('【会社名】' + '<br/>' + origin_employer1, NULL)
		, coalesce('【役職名】' + '<br/>' + job_title1, NULL)
		, coalesce('【雇用形態】' + '<br/>' + employmentstatus1, NULL)
		, coalesce('【担当職務】' + '<br/>' + experience1, NULL)
		) as company
	, 1 as employer
	from all_wh
	where coalesce(nullif(origin_employer1, ''), nullif(job_title1, ''), nullif(experience1, '')) is not NULL
	
	UNION ALL
	
	select cand_ext_id
	, employer2 as currentEmployer
	, origin_employer2 as origin_employer
	, job_title2 as jobTitle
	, startdate2 as dateRangeFrom
	, enddate2 as dateRangeTo
	, experience2 as experience
	, concat_ws('<br/>'
		, coalesce('【会社名】' + '<br/>' + origin_employer2, NULL)
		, coalesce('【役職名】' + '<br/>' + job_title2, NULL)
		, coalesce('【雇用形態】' + '<br/>' + employmentstatus2, NULL)
		, coalesce('【担当職務】' + '<br/>' + experience2, NULL)
		) as company
	, 2 as employer
	from all_wh
	where coalesce(nullif(origin_employer2, ''), nullif(job_title2, ''), nullif(experience2, '')) is not NULL)

--TEMP TABLE
select cand_ext_id
, currentEmployer
, origin_employer
, jobTitle
, dateRangeFrom
, dateRangeTo
, experience
, company
, row_number() over(partition by cand_ext_id order by coalesce( dateRangeTo, dateRangeFrom) desc, employer asc) as rn
--into cand_work_history --174850 rows
from work_history
where coalesce(nullif(currentEmployer, ''), nullif(jobTitle, ''), nullif(experience, '')) is not NULL
--and cand_ext_id = 'CDT001620' --e.g. CDT154379