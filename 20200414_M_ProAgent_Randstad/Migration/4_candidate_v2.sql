/* 
--USING FOR CORRECTING CANDIDATE EMAIL IF ANY
with dup as (select [PANO ], [メール]
	, row_number() over(partition by trim(' ' from lower([メール])) order by [PANO ] asc) as rn --distinct email if emails exist more than once
	from csv_can
	where [メール] like '%_@_%.__%'
	and not exists (select email from vc_prod_candidate where trim(email) = trim([メール]))
	) --1624 rows

, vc_dup as (select [PANO ], [メール]
	, concat_ws('_', [PANO ], [メール]) as cand_vc_email
	from csv_can
	where [メール] like '%_@_%.__%'
	and exists (select email from vc_prod_candidate where trim(email) = trim([メール]))
	)

, cand_email as (select [PANO ], [メール]
	, case when rn > 1 then concat(rn, '_', [メール])
		else [メール] end as cand_email
	from dup

	UNION ALL
	select [PANO ], [メール]
	, cand_vc_email
	from vc_dup)

, */
with
work_email as (select [PANO ], 携帯メール as work_email
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
	, trim(連絡先1) as phone
	from csv_can
	where 連絡先種別1 in ('電話') and nullif(連絡先1, '') is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim(連絡先2) as phone
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
	, trim(連絡先1) as mobile
	from csv_can
	where 連絡先種別1 in ('携帯') and nullif(連絡先1, '') is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim(連絡先2) as mobile
	from csv_can
	where 連絡先種別2 in ('携帯') and nullif(連絡先2, '') is not NULL
	)

, cand_mobile as (select cand_ext_id
	, string_agg(mobile, ',') as cand_mobile
	from allmobile
	where mobile is not NULL
	group by cand_ext_id)

--Documents
, doc as (select seq
	, can_id
	, pano as can_ext_id
	, right(trim([file]), charindex('/', reverse(trim([file]))) - 1) as UploadedName
	, [file]
	from CAN_resume)

, can_doc as (select can_ext_id
	, string_agg(UploadedName, ',') as can_doc
	from doc
	group by can_ext_id)

--MAIN SCRIPT
select --top 1000 
	c.[PANO ] as [candidate-externalId] --#CF inject then
	, convert(datetime, c.[登録日], 120) as reg_date
	--, N'　' as [candidate-firstName]
	, '氏名' as [candidate-firstName]
	, coalesce(nullif([氏名],''), 'No Last name') as [candidate-Lastname]
	--, N'　' as [candidate-firstNameKana]
	, 'フリガナ' as [candidate-firstNameKana]
	, [フリガナ] as [candidate-lastNameKana]
	, convert(date, c.[生年月日], 120) as [candidate-dob]
	, c.[人材担当ユーザID] --candidate owners
	, case when c.[人材担当ユーザID] in ('FPC163', 'FPC207') then NULL
		else u.EmailAddress end as [candidate-owners] --updated on 20200224

/* THIS IMPACTS THE PERFORMANCE WHEN LOADING RESULTS
	, coalesce(nullif(ce.cand_email, ''), nullif(we.work_email, ''), concat_ws('_', c.[PANO ], 'candidate@noemail.com')) as [candidate-email]
--VERSION 2
	, case when dup.rn > 1 then concat_ws('_', dup.[PANO ], dup.rn, dup.[メール])
			when dup.rn = 1 then concat_ws('_', dup.[PANO ], dup.[メール])
			else coalesce(nullif(c.[メール],''), nullif(we.work_email, ''), concat_ws('_', c.[PANO ], 'candidate@noemail.com'))
			end as [candidate-email]
*/
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
--left join cand_email ce on ce.[PANO ] = c.[PANO ]
--left join dup on dup.[PANO ] = c.[PANO ]
left join work_email we on we.[PANO ] = c.[PANO ]
left join can_doc cd on cd.can_ext_id = c.[PANO ]
where c.[チェック項目] not like '%チャレンジド人材%' --171801
--order by convert(datetime, c.[登録日], 120) desc --added for test search