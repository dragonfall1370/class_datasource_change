with work_email as (select [PANO ], 携帯メール as work_email
	from csv_can
	where nullif(携帯メール, '') is not NULL
	and 携帯メール like '%_@_%.__%')

, photo as (select pano
		, [file]
		, right([file], len([file]) - len('can/picture/')) as photo
		from CAN_picture)

, cand_status as (select [PANO] as cand_id
		, string_agg(concat_ws(char(10)
			, coalesce('[Open (紹介中)] ' + nullif([Open (紹介中)], ''), NULL)
			, coalesce('[Placement (決定)] ' + nullif([Placement (決定)], ''), NULL)
			, coalesce('[Close (紹介終了)] ' + nullif([Close (紹介終了)], ''), NULL)
			, coalesce('[Other (その他)] ' + nullif([Other (その他)], ''), NULL)
			), concat(char(10),char(13))) 
			within group (order by coalesce([Open (紹介中)], [Placement (決定)], [Close (紹介終了)], [Other (その他)]) desc) as cand_status
		from csv_status_my_can
		where [PANO] is not NULL
		and coalesce(nullif([Open (紹介中)],''), nullif([Placement (決定)],''), nullif([Close (紹介終了)],''), nullif([Other (その他)],'')) is not NULL
		group by [PANO])

, status_note as (select [PANO] as cand_id
		, string_agg(状況メモ, char(10)) within group (order by 登録日 desc) as status_note
		from csv_status_my_can
		where nullif([PANO],'') is not NULL
		and nullif(状況メモ, '') is not NULL
		group by [PANO])

--Primary Phone
, allphone as (select [PANO ] as cand_ext_id
	, trim(':' from trim(連絡先1)) as phone
	from csv_can
	where 連絡先種別1 in ('電話') and nullif(連絡先1, '') is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim(':' from trim(連絡先2)) as phone
	from csv_can
	where 連絡先種別2 in ('電話') and nullif(連絡先2, '') is not NULL
	)

, cand_phone as (select cand_ext_id
	, string_agg(phone, ',') as cand_phone
	from allphone
	where phone is not NULL
	group by cand_ext_id)

--Mobile
, allmobile as (select [PANO ] as cand_ext_id
	, trim(':' from trim(連絡先1)) as mobile
	from csv_can
	where 連絡先種別1 in ('携帯') and nullif(連絡先1, '') is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim(':' from trim(連絡先2)) as mobile
	from csv_can
	where 連絡先種別2 in ('携帯') and nullif(連絡先2, '') is not NULL
	)

, cand_mobile as (select cand_ext_id
	, string_agg(mobile, ',') as cand_mobile
	from allmobile
	where mobile is not NULL
	group by cand_ext_id)

--Documents --added extension from memo | updated 20200311
, doc as (select seq
	, can_id
	, pano as cand_ext_id
	, charindex('.', [file]) as ext
	--extension should be reversed in case of wrong file extension
	, case when charindex('.', [file]) = 0 and charindex('.', reverse([memo])) > 1 then --starting position for extension
		reverse(substring(reverse([memo]), patindex('%[a-zA-Z]%', reverse([memo]))
				, 1 + charindex('.', reverse([memo])) - patindex('%[a-zA-Z]%', reverse([memo])))) 
		else NULL end as memo_ext
	, left(memo, case when charindex('.', memo) > 0 then charindex('.', memo) - 1 else len(memo) end) as memo_filename
	, right(trim([file]), charindex('/', reverse(trim([file]))) - 1) as UploadedName
	, [file]
	, memo
from CAN_resume
where 1=1
--and seq = '58587' --wrong extension and invalid case
--and seq = '58796'
--and pano = 'CDT152336' --invalid extension
--and pano = 'CDT076813' --wrong extension position
--and pano = 'CDT078522' --wrong extension
)

, doc_final as (select cand_ext_id
	, ext
	, memo_ext
	, memo_filename
	, case when ext > 0 then UploadedName
		else concat(UploadedName, memo_ext) end as UploadedName
	from doc) --select * from doc_final where ext = 0

, can_doc as (select cand_ext_id
	, string_agg(UploadedName, ',') as can_doc
	from doc_final
	group by cand_ext_id)

--MAIN SCRIPT
select --top 1000 
	c.[PANO ] as [candidate-externalId] --#CF inject then
	, convert(datetime, c.[登録日], 120) as reg_date
	, '氏名' as [candidate-firstName] --replace with N'　' (double byte blank later) | identified dup records with 【Duplicate】
	, coalesce(nullif([氏名],''), 'No Last name') as [candidate-Lastname]
	, 'フリガナ' as [candidate-firstNameKana] --replace with N'　' (double byte blank later)
	, [フリガナ] as [candidate-lastNameKana]
	, convert(date, c.[生年月日], 120) as [candidate-dob]
	, c.[人材担当ユーザID] --candidate owners
	, case when c.[人材担当ユーザID] in ('FPC163', 'FPC207') then NULL
		else u.EmailAddress end as [candidate-owners] --updated on 20200224
	, case when c.[メール] like '%_@_%.__%' then concat_ws('_', c.[PANO ], c.[メール])
		when nullif(c.[メール],'') is NULL and c.[携帯メール] like '%_@_%.__%' then concat_ws('_', c.[PANO ], c.[携帯メール])
		else concat_ws('_', c.[PANO ], 'candidate@noemail.com') end as [candidate-email]
	, we.work_email as [candidate-workEmail]
--Brief --to be injected later
	, p.photo as [candidate-photo]
	, coalesce(nullif(cp.cand_phone, ''), nullif(cm.cand_mobile, '')) as [candidate-phone]
	, nullif(cm.cand_mobile, '') as [candidate-mobile]
	, 'JPY' as [candidate-currency]
	, cd.can_doc as [candidate-resume]
from csv_can c
left join UserMapping u on u.UserID = c.[人材担当ユーザID]
left join cand_status cs on cs.cand_id = c.[PANO ]
left join status_note sn on sn.cand_id = c.[PANO ]
left join photo p on p.pano = c.[PANO ]
left join cand_phone cp on cp.cand_ext_id = c.[PANO ]
left join cand_mobile cm on cm.cand_ext_id = c.[PANO ]
left join work_email we on we.[PANO ] = c.[PANO ]
left join can_doc cd on cd.cand_ext_id = c.[PANO ]
where c.[チェック項目] not like '%チャレンジド人材%' --171801