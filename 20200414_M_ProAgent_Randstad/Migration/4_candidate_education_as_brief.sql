--Academic background as Brief
select [PANO ] as cand_ext_id
, concat_ws(char(10)
	, coalesce('【学歴1】' + char(10) + nullif(concat_ws(char(10)
		, coalesce('(Period) ' + nullif(concat_ws(' - '
				, coalesce(nullif([入学年月 年1], '')  + '/' + nullif([入学年月 月1], ''), NULL) --period from
				, coalesce(nullif([卒業年月 年1], '')  + '/' + nullif([卒業年月 月1], ''), NULL)), ''), NULL) --period to
		, coalesce('(Classification of withdrawal) ' + nullif([卒退区分1], ''), NULL) --classification 1
		, coalesce('(Education category) ' + nullif([学歴区分1], ''), NULL) --education category 1
		, coalesce('(School name) ' + nullif([学校名 学部名 学科名1], ''), NULL) --school name / department name 1
		, coalesce('(Educational note) ' + nullif([学歴メモ1], ''), NULL)--educational note	
		), ''), NULL)
	, coalesce(char(10) + '【学歴2】' + char(10) + nullif(concat_ws(char(10)
		, coalesce('(Period) ' + nullif(concat_ws(' - '
				, coalesce(nullif([入学年月 年2], '')  + '/' + nullif([入学年月 月2], ''), NULL) --period from
				, coalesce(nullif([卒業年月 年2], '')  + '/' + nullif([卒業年月 月2], ''), NULL)), ''), NULL) --period to
		, coalesce('(Classification of withdrawal) ' + nullif([卒退区分2], ''), NULL) --classification 1
		, coalesce('(Education category) ' + nullif([学歴区分2], ''), NULL) --education category 1
		, coalesce('(School name) ' + nullif([学校名 学部名 学科名2], ''), NULL) --school name / department name 1
		, coalesce('(Educational note) ' + nullif([学歴メモ2], ''), NULL)--educational note	
		), ''), NULL)
	, coalesce(char(10) + '【その他学歴】' + nullif([その他学歴], ''), NULL)
	) as education
from csv_can