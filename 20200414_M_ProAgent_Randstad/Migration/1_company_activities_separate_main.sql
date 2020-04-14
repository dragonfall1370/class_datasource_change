--Company activities
select [企業 PANO ] as com_ext_id
, concat_ws(char(10), '【新規開拓】'
	, coalesce(char(10) + '【新規開拓実施日】' + char(10) + nullif(新規開拓実施日 , ''), NULL)
	, coalesce(char(10) + '【新規開拓対応者】' + char(10) + nullif(新規開拓対応者 , ''), NULL)
	, coalesce(char(10) + '【新規開拓対応者ユーザID】' + char(10) + nullif(新規開拓対応者ユーザID , ''), NULL)
) as comment_activities
, 'comment' as category
, 'company' as type
, dateadd(hour, -9, convert(datetime, [新規開拓実施日], 120)) as insert_timestamp
, [登録者ユーザID] as user_account_id --linked with user mapping
, coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io') as user_email
from csv_recf_history c
left join UserMapping u on u.UserID = c.[新規開拓対応者ユーザID]
where nullif(新規開拓実施日, '') is not NULL


UNION ALL
select [企業 PANO ] as com_ext_id
, concat_ws(char(10), '【情報受付】'
	, coalesce(char(10) + '【情報受付実施日】' + char(10) + nullif(情報受付実施日 , ''), NULL)
	, coalesce(char(10) + '【情報受付対応者ユーザID】' + char(10) + nullif(情報受付対応者ユーザID , ''), NULL)
	, coalesce(char(10) + '【情報受付対応者】' + char(10) + nullif(情報受付対応者 , ''), NULL)
) as comment_activities
, 'comment' as category
, 'company' as type
, dateadd(hour, -9, convert(datetime, [情報受付実施日], 120)) as insert_timestamp
, [情報受付対応者ユーザID] as user_account_id --linked with user mapping
, coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io') as user_email
from csv_recf_history c
left join UserMapping u on u.UserID = c.[情報受付対応者ユーザID]
where nullif([情報受付実施日], '') is not NULL


UNION ALL
select [企業 PANO ] as com_ext_id
, concat_ws(char(10), '【訪問】'
	, coalesce(char(10) + '【訪問実施日】' + char(10) + nullif(訪問実施日 , ''), NULL)
	, coalesce(char(10) + '【訪問対応者ユーザID】' + char(10) + nullif(訪問対応者ユーザID , ''), NULL)
	, coalesce(char(10) + '【訪問対応者】' + char(10) + nullif(訪問対応者 , ''), NULL)
) as comment_activities
, 'comment' as category
, 'company' as type
, dateadd(hour, -9, convert(datetime, [訪問実施日], 120)) as insert_timestamp
, [訪問対応者ユーザID] as user_account_id --linked with user mapping
, coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io') as user_email
from csv_recf_history c
left join UserMapping u on u.UserID = c.[訪問対応者ユーザID]
where nullif([訪問実施日], '') is not NULL


UNION ALL
select [企業 PANO ] as com_ext_id
, concat_ws(char(10), '【来訪】'
	, coalesce(char(10) + '【来訪実施日】' + char(10) + nullif(来訪実施日 , ''), NULL)
	, coalesce(char(10) + '【来訪対応者ユーザID】' + char(10) + nullif(来訪対応者ユーザID , ''), NULL)
	, coalesce(char(10) + '【来訪対応者】' + char(10) + nullif(来訪対応者 , ''), NULL)
) as comment_activities
, 'comment' as category
, 'company' as type
, dateadd(hour, -9, convert(datetime, [来訪実施日], 120)) as insert_timestamp
, [来訪対応者ユーザID] as user_account_id --linked with user mapping
, coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io') as user_email
from csv_recf_history c
left join UserMapping u on u.UserID = c.[来訪対応者ユーザID]
where nullif([来訪実施日], '') is not NULL


UNION ALL
select [企業 PANO ] as com_ext_id
, concat_ws(char(10), '【電話】'
	, coalesce(char(10) + '【電話実施日】' + char(10) + nullif(電話実施日 , ''), NULL)
	, coalesce(char(10) + '【電話対応者ユーザID】' + char(10) + nullif(電話対応者ユーザID , ''), NULL)
	, coalesce(char(10) + '【電話対応者】' + char(10) + nullif(電話対応者 , ''), NULL)
) as comment_activities
, 'comment' as category
, 'company' as type
, dateadd(hour, -9, convert(datetime, [電話実施日], 120)) as insert_timestamp
, [電話対応者ユーザID] as user_account_id --linked with user mapping
, coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io') as user_email
from csv_recf_history c
left join UserMapping u on u.UserID = c.[電話対応者ユーザID]
where nullif([電話実施日], '') is not NULL


UNION ALL
select [企業 PANO ] as com_ext_id
, concat_ws(char(10), '【メール】'
	, coalesce(char(10) + '【メール実施日】' + char(10) + nullif(メール実施日 , ''), NULL)
	, coalesce(char(10) + '【メール対応者ユーザID】' + char(10) + nullif(メール対応者ユーザID , ''), NULL)
	, coalesce(char(10) + '【メール対応者】' + char(10) + nullif(メール対応者 , ''), NULL)
) as comment_activities
, 'comment' as category
, 'company' as type
, dateadd(hour, -9, convert(datetime, [メール実施日], 120)) as insert_timestamp
, [メール対応者ユーザID] as user_account_id --linked with user mapping
, coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io') as user_email
from csv_recf_history c
left join UserMapping u on u.UserID = c.[メール対応者ユーザID]
where nullif([メール実施日], '') is not NULL


UNION ALL
select [企業 PANO ] as com_ext_id
, concat_ws(char(10), '【ＲＰ担当変更】'
	, coalesce(char(10) + '【ＲＰ担当変更実施日】' + char(10) + nullif(ＲＰ担当変更実施日 , ''), NULL)
	, coalesce(char(10) + '【ＲＰ担当変更対応者ユーザID】' + char(10) + nullif(ＲＰ担当変更対応者ユーザID , ''), NULL)
	, coalesce(char(10) + '【ＲＰ担当変更対応者】' + char(10) + nullif(ＲＰ担当変更対応者 , ''), NULL)
) as comment_activities
, 'comment' as category
, 'company' as type
, dateadd(hour, -9, convert(datetime, [ＲＰ担当変更実施日], 120)) as insert_timestamp
, [ＲＰ担当変更対応者ユーザID] as user_account_id --linked with user mapping
, coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io') as user_email
from csv_recf_history c
left join UserMapping u on u.UserID = c.[ＲＰ担当変更対応者ユーザID]
where nullif([ＲＰ担当変更実施日], '') is not NULL


UNION ALL
select [企業 PANO ] as com_ext_id
, concat_ws(char(10), '【取引中止】'
	, coalesce(char(10) + '【取引中止実施日】' + char(10) + nullif(取引中止実施日 , ''), NULL)
	, coalesce(char(10) + '【取引中止対応者ユーザID】' + char(10) + nullif(取引中止対応者ユーザID , ''), NULL)
	, coalesce(char(10) + '【取引中止対応者】' + char(10) + nullif(取引中止対応者 , ''), NULL)
) as comment_activities
, 'comment' as category
, 'company' as type
, dateadd(hour, -9, convert(datetime, [取引中止実施日], 120)) as insert_timestamp
, [取引中止対応者ユーザID] as user_account_id --linked with user mapping
, coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io') as user_email
from csv_recf_history c
left join UserMapping u on u.UserID = c.[取引中止対応者ユーザID]
where nullif([取引中止実施日], '') is not NULL

UNION ALL
select [企業 PANO ] as com_ext_id
, concat_ws(char(10), '【接触不可】'
	, coalesce(char(10) + '【接触不可実施日】' + char(10) + nullif(接触不可実施日 , ''), NULL)
	, coalesce(char(10) + '【接触不可対応者ユーザID】' + char(10) + nullif(接触不可対応者ユーザID , ''), NULL)
	, coalesce(char(10) + '【接触不可対応者】' + char(10) + nullif(接触不可対応者 , ''), NULL)
) as comment_activities
, 'comment' as category
, 'company' as type
, dateadd(hour, -9, convert(datetime, [接触不可実施日], 120)) as insert_timestamp
, [接触不可対応者ユーザID] as user_account_id --linked with user mapping
, coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io') as user_email
from csv_recf_history c
left join UserMapping u on u.UserID = c.[接触不可対応者ユーザID]
where nullif([接触不可実施日], '') is not NULL


UNION ALL
select [企業 PANO ] as com_ext_id
, concat_ws(char(10), '【その他】'
	, coalesce(char(10) + '【その他実施日】' + char(10) + nullif(その他実施日 , ''), NULL)
	, coalesce(char(10) + '【その他対応者ユーザID】' + char(10) + nullif(その他対応者ユーザID , ''), NULL)
	, coalesce(char(10) + '【その他対応者】' + char(10) + nullif(その他対応者 , ''), NULL)
) as comment_activities
, 'comment' as category
, 'company' as type
, dateadd(hour, -9, convert(datetime, [その他実施日], 120)) as insert_timestamp
, [その他対応者ユーザID] as user_account_id --linked with user mapping
, coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io') as user_email
from csv_recf_history c
left join UserMapping u on u.UserID = c.[その他対応者ユーザID]
where nullif([その他実施日], '') is not NULL


UNION ALL
select [企業 PANO ] as com_ext_id
, concat_ws(char(10), '【面接】'
	, coalesce(char(10) + '【面接実施日】' + char(10) + nullif(面接実施日 , ''), NULL)
	, coalesce(char(10) + '【面接対応者ユーザID】' + char(10) + nullif(面接対応者ユーザID , ''), NULL)
	, coalesce(char(10) + '【面接対応者】' + char(10) + nullif(面接対応者 , ''), NULL)
) as comment_activities
, 'comment' as category
, 'company' as type
, dateadd(hour, -9, convert(datetime, [面接実施日], 120)) as insert_timestamp
, [面接対応者ユーザID] as user_account_id --linked with user mapping
, coalesce(nullif(u.EmailAddress, ''), 'sysadmin@vincere.io') as user_email
from csv_recf_history c
left join UserMapping u on u.UserID = c.[面接対応者ユーザID]
where nullif([面接実施日], '') is not NULL