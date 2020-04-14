--Candidate education
with education as (select [PANO ] as cand_ext_id
	, [入学年月 年1] as yearfrom--period year from
	, [入学年月 月1] as monthfrom --period month from
	, [卒業年月 年1] as yearto--period year to
	, [卒業年月 月1] as monthto --period month to
	, [卒退区分1] as educlass --classification 1
	, [学歴区分1] educategory --education category 1
	, [学校名 学部名 学科名1] as schoolname --school name / department name 1
	, [学歴メモ1] as edunote --educational note
from csv_can
where coalesce(nullif([入学年月 年1], ''), nullif([卒退区分1], '')
		, nullif([学歴区分1], ''), nullif([学校名 学部名 学科名1], ''), nullif([学歴メモ1], '')) is not NULL

UNION ALL

select [PANO ]
	, [卒業年月 年2] --period year from
	, [卒業年月 月2] --period month from
	, [卒業年月 年2] --period year to
	, [卒業年月 月2] --period month to
	, [卒退区分2] --classification 2
	, [学歴区分2] --education category 1
	, [学校名 学部名 学科名2] --school name / department name 2
	, [学歴メモ2] --educational note
from csv_can
where coalesce(nullif([入学年月 年2], ''), nullif([卒退区分2], '')
		, nullif([学歴区分2], ''), nullif([学校名 学部名 学科名2], ''), nullif([学歴メモ2], '')) is not NULL)

, cand_education as (select cand_ext_id
	, yearfrom
	, monthfrom
	, yearto
	, monthto
	, educlass
	, educategory
	, schoolname
	, edunote
	, row_number() over(partition by cand_ext_id
							order by case educategory
								when '大学院' then 1
								when '大学' then 2
								when '短大・高専・専門・各種学校' then 3
								when '高校' then 4
								when '中学' then 5
								else 0 end asc) as rn
	from education)

--MAIN SCRIPT
select cand_ext_id
	, 'add_cand_info' as additional_type
	, 1139 as form_id
	, 11305 as parent_id
	, 11306 as children_id
	, yearfrom as text_data --TEXT FIELD
	, current_timestamp as insert_timestamp
	, '11306_11305' as constraint_id
from cand_education
where 1=1
and rn = 1
and nullif(yearfrom, '') is not NULL
--order by cand_ext_id

UNION ALL

select cand_ext_id
	, 'add_cand_info' as additional_type
	, 1139 as form_id
	, 11305 as parent_id
	, 11307 as children_id
	, monthfrom as text_data --TEXT FIELD
	, current_timestamp as insert_timestamp
	, '11307_11305' as constraint_id
from cand_education
where 1=1
and rn = 1
and nullif(monthfrom, '') is not NULL
--order by cand_ext_id

UNION ALL

select cand_ext_id
	, 'add_cand_info' as additional_type
	, 1139 as form_id
	, 11305 as parent_id
	, 11308 as children_id
	, yearto as text_data --TEXT FIELD
	, current_timestamp as insert_timestamp
	, '11308_11305' as constraint_id
from cand_education
where 1=1
and rn = 1
and nullif(yearto, '') is not NULL
--order by cand_ext_id

UNION ALL

select cand_ext_id
	, 'add_cand_info' as additional_type
	, 1139 as form_id
	, 11305 as parent_id
	, 11309 as children_id
	, monthto as text_data --TEXT FIELD
	, current_timestamp as insert_timestamp
	, '11309_11305' as constraint_id
from cand_education
where 1=1
and rn = 1
and nullif(monthto, '') is not NULL
--order by cand_ext_id

UNION ALL

select cand_ext_id
	, 'add_cand_info' as additional_type
	, 1139 as form_id
	, 11305 as parent_id
	, 11310 as children_id
	, educategory as text_data --DROP DOWN
	, current_timestamp as insert_timestamp
	, '11310_11305' as constraint_id
from cand_education
where 1=1
and rn = 1
and nullif(educategory, '') is not NULL
--order by cand_ext_id

UNION ALL

select cand_ext_id
	, 'add_cand_info' as additional_type
	, 1139 as form_id
	, 11306 as parent_id
	, 11310 as children_id
	, case educlass 
		when '卒業' then 'Graduate'
		when '卒業見込み' then 'Expected graduation'
		when '中退' then 'Dropout'
		when '修了' then 'Completion'
		when '不明' then 'unknown' else NULL end as text_data --DROP DOWN
	, current_timestamp as insert_timestamp
	, '11310_11306' as constraint_id
from cand_education
where 1=1
and rn = 1
and nullif(educlass, '') is not NULL
--order by cand_ext_id

UNION ALL

select cand_ext_id
	, 'add_cand_info' as additional_type
	, 1139 as form_id
	, 11307 as parent_id
	, 11310 as children_id
	, schoolname as text_data --TEXT FIELD
	, current_timestamp as insert_timestamp
	, '11310_11307' as constraint_id
from cand_education
where 1=1
and rn = 1
and nullif(schoolname, '') is not NULL
--order by cand_ext_id