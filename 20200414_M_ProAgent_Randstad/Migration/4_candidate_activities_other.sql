declare 
	@string1 nvarchar(50) = N'[日時]', --date
	@string2 nvarchar(50) = N'[対応者ID]', --user id
	@string3 nvarchar(50) = N'[対応者]', --user name
	@string4 nvarchar(50) = N'[メモ]',
	@splitvalue nchar(1) = nchar(9999);

with cand_activity as (select [キャンディデイト PANO ] as cand_ext_id
	, [対応履歴メモ]
	, value as cand_activity
	from csv_can_history
	cross apply string_split(replace([対応履歴メモ], @string1, @splitvalue), @splitvalue)
	where [対応履歴メモ] <> '')

, cand_act_trim as (select cand_ext_id
	, cand_activity
	, left(cand_activity, charindex(@string2, cand_activity)- 1) as activity_date
	, substring(replace(cand_activity, @string2, @splitvalue)
			, charindex(@string2, cand_activity) + 1
			, charindex(@string3, cand_activity) - charindex(@string2, cand_activity) - len(@string2)) as user_account_id
	, substring(replace(cand_activity, @string3, @splitvalue)
			, charindex(@string3, cand_activity) + 1
			, charindex(@string4, cand_activity) - charindex(@string3, cand_activity) - len(@string3)) as username
	, case when len(cand_activity) - charindex(@string4, cand_activity) - len(@string4) + 1 > 0
			then right(cand_activity, len(cand_activity) - charindex(@string4, cand_activity) - len(@string4) + 1)
			else NULL end as memo
	from cand_activity
	where cand_activity <> '')

select cand_ext_id
, user_account_id
, dateadd(hour, 9, convert(datetime, activity_date, 120)) as insert_timestamp
, concat_ws(concat(char(10),char(13)), '【対応履歴メモ】', trim(memo)
		, coalesce('【対応者】' + c.username, NULL)
		, coalesce('【対応者ID】' + c.user_account_id, NULL)
		) as comment_activities
, 'comment' as category
, 'candidate' as type
, lower(coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io')) as user_email
from cand_act_trim c
left join UserMapping u on u.UserID = c.user_account_id
where memo is not NULL --130302 rows