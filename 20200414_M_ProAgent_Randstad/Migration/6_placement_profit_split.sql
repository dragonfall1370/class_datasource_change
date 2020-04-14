--Profit split name
declare 
	@string1 nvarchar(50) = N'[担当者ID]', --user id
	@string2 nvarchar(50) = N'[担当者]', --user name
	@string3 nvarchar(50) = N'[売上]', --amount split
	@splitvalue nchar(1) = nchar(9999);

with profit_split as (select [キャンディデイト PANO ] as cand_ext_id
	, [JOB PANO ] as job_ext_id
	, [その他売上]
	, value as profit_split
	from csv_contract
	cross apply string_split(replace([その他売上], @string1, @splitvalue), @splitvalue)
	where [その他売上] <> '')

/* Audit
select * from profit_split
where cand_ext_id = 'CDT156466' and job_ext_id = 'JOB057451'
*/

, profit_split_trim as (select cand_ext_id
	, job_ext_id
	, case when charindex(@string2, profit_split) > 1 then left(profit_split, charindex(@string2, profit_split) - 1)
		else '' end as profit_split_user_id
	, substring(profit_split
				, charindex(@string2, profit_split) + len(@string2)
				, charindex(@string3, profit_split) - charindex(@string2, profit_split) - len(@string2)) as profit_split_user_name
	, case when len(profit_split) - charindex(@string3, profit_split) - len(@string3) + 1 > 0 then 
			nullif(right(profit_split, len(profit_split) - charindex(@string3, profit_split) - len(@string3) + 1), '')
		else 0 end as split_amount
	from profit_split
	where profit_split <> '')

select cand_ext_id
, job_ext_id
, profit_split_user_id
, profit_split_user_name
, u.EmailAddress as user_email
, convert(float, split_amount) as split_amount
, 2 as profit_split_mode --amount mode
from profit_split_trim p
left join UserMapping u on u.UserID = p.profit_split_user_id
where 1=1
and profit_split_user_id <> ''
and split_amount <> 0
and u.EmailAddress is not NULL

UNION ALL
select [キャンディデイト PANO ] as cand_ext_id
	, [JOB PANO ] as job_ext_id
	, [売上 担当者1ユーザID]
	, [売上 担当者1]
	, u.EmailAddress as user_email
	, [売上 担当別売上1]
	, 2 as profit_split_mode
from csv_contract p
left join UserMapping u on u.UserID = p.[売上 担当者1ユーザID]
where nullif([売上 担当者1ユーザID], '') is not NULL
and nullif([売上 担当別売上1], '') is not NULL
and u.EmailAddress is not NULL

UNION ALL
select [キャンディデイト PANO ] as cand_ext_id
	, [JOB PANO ] as job_ext_id
	, [売上 担当者2ユーザID]
	, [売上 担当者2]
	, u.EmailAddress as user_email
	, [売上 担当別売上2]
	, 2 as profit_split_mode
from csv_contract p
left join UserMapping u on u.UserID = p.[売上 担当者2ユーザID]
where nullif([売上 担当者2ユーザID], '') is not NULL
and nullif([売上 担当別売上2], '') is not NULL
and u.EmailAddress is not NULL
