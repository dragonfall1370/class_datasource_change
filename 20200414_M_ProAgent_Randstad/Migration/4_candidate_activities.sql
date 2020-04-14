--Candidate activities | KPI action: NCAD | ID: 34
select [キャンディデイト PANO ] as cand_ext_id
	, concat_ws(concat(char(10),char(13)), '【本登録】'
		, coalesce('【本登録実施日】' + char(10) + nullif(本登録実施日, ''), NULL)
		, coalesce('【本登録対応者】' + char(10) + nullif(本登録対応者 , ''), NULL)
		--, coalesce('[本登録対応者ユーザID]' + nullif(本登録対応者ユーザID , ''), NULL)
	) as comment_activities
	, 'comment' as category
	, 'candidate' as type
	, 34 as action_id --NCAD category
	, 34 as kpi_action --NCAD category
	, dateadd(hour, -9, convert(datetime, [本登録実施日], 120)) as insert_timestamp
	, [本登録対応者ユーザID] as user_account_id --linked with user mapping
	, lower(coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io')) as user_email
from csv_can_history c
left join UserMapping u on u.UserID = c.[本登録対応者ユーザID]
where nullif(本登録実施日, '') is not NULL


--->> Different candidate activities <<---
--Activities | 呼び込みメール・TEL実施日 --Invitation email
with pa_activity as (select [キャンディデイト PANO ] as cand_ext_id
	, concat_ws(char(10), '【呼び込みメール・TEL】'
		, coalesce('【呼び込みメール・TEL実施日】' + char(10) + nullif([呼び込みメール・TEL実施日], ''), NULL)
		, coalesce('【呼び込みメール・TEL対応者】' + char(10) + nullif([呼び込みメール・TEL対応者], ''), NULL)
	) as comment_activities
	, 'comment' as category
	, 'candidate' as type
	, dateadd(hour, -9, convert(datetime, [呼び込みメール・TEL実施日], 120)) as insert_timestamp
	, [呼び込みメール・TEL対応者ユーザID] as user_account_id --linked with user mapping
	, lower(coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io')) as user_email
from csv_can_history c
left join UserMapping u on u.UserID = c.[呼び込みメール・TEL対応者ユーザID]
where nullif(呼び込みメール・TEL実施日, '') is not NULL
--and [キャンディデイト PANO ] = 'CDT121823'

UNION ALL
--Activities | レジュメサンプル送信メール（アドミのみ使用）実施日 --resume sample sent
select [キャンディデイト PANO ] as cand_ext_id
	, concat_ws(char(10), '【レジュメサンプル送信メール（アドミのみ使用）】'
		, coalesce('【レジュメサンプル送信メール（アドミのみ使用）実施日】' + char(10) + nullif([レジュメサンプル送信メール（アドミのみ使用）実施日], ''), NULL)
		, coalesce('【レジュメサンプル送信メール（アドミのみ使用）対応者】' + char(10) + nullif([レジュメサンプル送信メール（アドミのみ使用）対応者], ''), NULL)
	) as comment_activities
	, 'comment' as category
	, 'candidate' as type
	, dateadd(hour, -9, convert(datetime, [レジュメサンプル送信メール（アドミのみ使用）実施日], 120)) as insert_timestamp
	, [レジュメサンプル送信メール（アドミのみ使用）対応者ユーザID] as user_account_id --linked with user mapping
	, lower(coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io')) as user_email
from csv_can_history c
left join UserMapping u on u.UserID = c.[レジュメサンプル送信メール（アドミのみ使用）対応者ユーザID]
where nullif([レジュメサンプル送信メール（アドミのみ使用）実施日], '') is not NULL

UNION ALL
--Activities | 面談日時確定実施日 --interview confirmation
select [キャンディデイト PANO ] as cand_ext_id
	, concat_ws(char(10), '【面談日時確定】'
		, coalesce('【面談日時確定実施日】' + char(10) + nullif([面談日時確定実施日], ''), NULL)
		, coalesce('【面談日時確定対応者】' + char(10) + nullif([面談日時確定対応者], ''), NULL)
	) as comment_activities
	, 'comment' as category
	, 'candidate' as type
	, dateadd(hour, -9, convert(datetime, [面談日時確定実施日], 120)) as insert_timestamp
	, [面談日時確定対応者ユーザID] as user_account_id --linked with user mapping
	, lower(coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io')) as user_email
from csv_can_history c
left join UserMapping u on u.UserID = c.[面談日時確定対応者ユーザID]
where nullif([面談日時確定実施日], '') is not NULL

UNION ALL
--Activities | 面談実施実施日 --interview date
select [キャンディデイト PANO ] as cand_ext_id
	, concat_ws(char(10), '【面談実施】'
		, coalesce('【面談実施実施日】' + char(10) + nullif([面談実施実施日], ''), NULL)
		, coalesce('【面談実施対応者】' + char(10) + nullif([面談実施対応者], ''), NULL)
	) as comment_activities
	, 'comment' as category
	, 'candidate' as type
	, dateadd(hour, -9, convert(datetime, [面談実施実施日], 120)) as insert_timestamp
	, [面談実施対応者ユーザID] as user_account_id --linked with user mapping
	, lower(coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io')) as user_email
from csv_can_history c
left join UserMapping u on u.UserID = c.[面談実施対応者ユーザID]
where nullif([面談実施実施日], '') is not NULL

UNION ALL
--Activities | 面談お礼メール実施日 --interview thank you
select [キャンディデイト PANO ] as cand_ext_id
	, concat_ws(char(10), '【面談お礼メール】'
		, coalesce('【面談お礼メール実施日】' + char(10) + nullif([面談お礼メール実施日], ''), NULL)
		, coalesce('【面談お礼メール対応者】' + char(10) + nullif([面談お礼メール対応者], ''), NULL)
	) as comment_activities
	, 'comment' as category
	, 'candidate' as type
	, dateadd(hour, -9, convert(datetime, [面談お礼メール実施日], 120)) as insert_timestamp
	, [面談お礼メール対応者ユーザID] as user_account_id --linked with user mapping
	, lower(coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io')) as user_email
from csv_can_history c
left join UserMapping u on u.UserID = c.[面談お礼メール対応者ユーザID]
where nullif([面談お礼メール実施日], '') is not NULL

UNION ALL
--Activities | ホールド人材実施日 --holder implementation date
select [キャンディデイト PANO ] as cand_ext_id
	, concat_ws(char(10), '【ホールド人材】'
		, coalesce('【ホールド人材実施日】' + char(10) + nullif([ホールド人材実施日], ''), NULL)
		, coalesce('【ホールド人材対応者】' + char(10) + nullif([ホールド人材対応者], ''), NULL)
	) as comment_activities
	, 'comment' as category
	, 'candidate' as type
	, dateadd(hour, -9, convert(datetime, [ホールド人材実施日], 120)) as insert_timestamp
	, [ホールド人材対応者ユーザID] as user_account_id --linked with user mapping
	, lower(coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io')) as user_email
from csv_can_history c
left join UserMapping u on u.UserID = c.[ホールド人材対応者ユーザID]
where nullif([ホールド人材実施日], '') is not NULL
)

select *
from pa_activity