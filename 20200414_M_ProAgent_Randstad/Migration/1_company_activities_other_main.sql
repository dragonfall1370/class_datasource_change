declare 
	@string1 nvarchar(50) = N'[日時]', --date
	@string2 nvarchar(50) = N'[対応者ID]', --user id
	@string3 nvarchar(50) = N'[対応者]', --user name
	@string4 nvarchar(50) = N'[メモ]',
	@splitvalue nchar(1) = nchar(9999);

with com_activity as (select [企業 PANO ] as com_ext_id
	, [対応履歴メモ]
	, value as com_activity
	from csv_recf_history
	cross apply string_split(replace([対応履歴メモ], @string1, @splitvalue), @splitvalue)
	where [対応履歴メモ] <> '')

, com_act_trim as (select com_ext_id
	, com_activity
	, left(com_activity, charindex(@string2, com_activity)- 1) as activity_date
	, substring(replace(com_activity, @string2, @splitvalue)
			, charindex(@string2, com_activity) + 1
			, charindex(@string3, com_activity) - charindex(@string2, com_activity) - len(@string2)) as user_account_id
	, substring(replace(com_activity, @string3, @splitvalue)
			, charindex(@string3, com_activity) + 1
			, charindex(@string4, com_activity) - charindex(@string3, com_activity) - len(@string3)) as username
	, case when len(com_activity) - charindex(@string4, com_activity) - len(@string4) + 1 > 0
			then right(com_activity, len(com_activity) - charindex(@string4, com_activity) - len(@string4) + 1)
			else NULL end as memo
	from com_activity
	where com_activity <> '')

--MAIN SCRIPT
select c.com_ext_id
, c.user_account_id
, c.activity_date
, dateadd(hour, -9, convert(datetime, c.activity_date, 120)) as insert_timestamp
, concat_ws(char(10), '【対応履歴メモ】'
		, coalesce(trim(memo), NULL)
		, coalesce(char(10) + '【対応者】' + char(10) + c.username, NULL)
		, coalesce(char(10) + '【対応者ID】' + char(10) + c.user_account_id, NULL)
		) as comment_activities
, 'comment' as category
, 'company' as type
, coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io') as user_email
from com_act_trim c
left join UserMapping u on u.UserID = c.user_account_id
where memo is not NULL --39063 rows