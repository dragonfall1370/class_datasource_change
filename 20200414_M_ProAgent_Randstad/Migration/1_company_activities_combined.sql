--Company activities
select [企業 PANO ] as com_ext_id
, concat_ws(char(10), '[対応履歴]'
	, coalesce('[新規開拓実施日]' + nullif(新規開拓実施日 , ''), NULL)
	, coalesce('[新規開拓対応者ユーザID]' + nullif(新規開拓対応者ユーザID , ''), NULL)
	, coalesce('[新規開拓対応者]' + nullif(新規開拓対応者 , ''), NULL)
	, coalesce('[情報受付実施日]' + nullif(情報受付実施日 , ''), NULL)
	, coalesce('[情報受付対応者ユーザID]' + nullif(情報受付対応者ユーザID , ''), NULL)
	, coalesce('[情報受付対応者]' + nullif(情報受付対応者 , ''), NULL)
	, coalesce('[訪問実施日]' + nullif(訪問実施日 , ''), NULL)
	, coalesce('[訪問対応者ユーザID]' + nullif(訪問対応者ユーザID , ''), NULL)
	, coalesce('[訪問対応者]' + nullif(訪問対応者 , ''), NULL)
	, coalesce('[来訪実施日]' + nullif(来訪実施日 , ''), NULL)
	, coalesce('[来訪対応者ユーザID]' + nullif(来訪対応者ユーザID , ''), NULL)
	, coalesce('[来訪対応者]' + nullif(来訪対応者 , ''), NULL)
	, coalesce('[電話実施日]' + nullif(電話実施日 , ''), NULL)
	, coalesce('[電話対応者ユーザID]' + nullif(電話対応者ユーザID , ''), NULL)
	, coalesce('[電話対応者]' + nullif(電話対応者 , ''), NULL)
	, coalesce('[メール実施日]' + nullif(メール実施日 , ''), NULL)
	, coalesce('[メール対応者ユーザID]' + nullif(メール対応者ユーザID , ''), NULL)
	, coalesce('[メール対応者]' + nullif(メール対応者 , ''), NULL)
	, coalesce('[ＲＰ担当変更実施日]' + nullif(ＲＰ担当変更実施日 , ''), NULL)
	, coalesce('[ＲＰ担当変更対応者ユーザID]' + nullif(ＲＰ担当変更対応者ユーザID , ''), NULL)
	, coalesce('[ＲＰ担当変更対応者]' + nullif(ＲＰ担当変更対応者 , ''), NULL)
	, coalesce('[取引中止実施日]' + nullif(取引中止実施日 , ''), NULL)
	, coalesce('[取引中止対応者ユーザID]' + nullif(取引中止対応者ユーザID , ''), NULL)
	, coalesce('[取引中止対応者]' + nullif(取引中止対応者 , ''), NULL)
	, coalesce('[接触不可実施日]' + nullif(接触不可実施日 , ''), NULL)
	, coalesce('[接触不可対応者ユーザID]' + nullif(接触不可対応者ユーザID , ''), NULL)
	, coalesce('[接触不可対応者]' + nullif(接触不可対応者 , ''), NULL)
	, coalesce('[その他実施日]' + nullif(その他実施日 , ''), NULL)
	, coalesce('[その他対応者ユーザID]' + nullif(その他対応者ユーザID , ''), NULL)
	, coalesce('[その他対応者]' + nullif(その他対応者 , ''), NULL)
	, coalesce('[面接実施日]' + nullif(面接実施日 , ''), NULL)
	, coalesce('[面接対応者ユーザID]' + nullif(面接対応者ユーザID , ''), NULL)
	, coalesce('[面接対応者]' + nullif(面接対応者 , ''), NULL)
) as comment_activities
, 'comment' as category
, 'company' as type
, convert(datetime, [登録日], 120) as insert_timestamp--created date
, [登録者ユーザID] as user_account_id --linked with user table then
from csv_recf_history
where coalesce(nullif(新規開拓実施日, '')
			, nullif(情報受付実施日, '')
			, nullif(訪問実施日, '')
			, nullif(来訪実施日, '')
			, nullif(電話実施日, '')
			, nullif(メール実施日, '')
			, nullif(ＲＰ担当変更実施日, '')
			, nullif(取引中止実施日, '')
			, nullif(接触不可実施日, '')
			, nullif(その他実施日, '')
			, nullif(面接実施日, ''), NULL) is not NULL