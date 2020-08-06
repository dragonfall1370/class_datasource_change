--duplicates within Pro Agent but not in comparison with VC
with dup as (select [PANO ], [会社名], row_number() over(partition by lower([会社名]) order by [PANO] desc) as rn
		from csv_recf
		--where trim([会社名]) not in (select trim(name) from [vc_company])
		) --29734 rows

, allphone as (select [PANO ] as com_ext_id
	, trim(連絡先1) as phone
	from csv_recf
	where 連絡先種別1 in ('電話', '携帯') and nullif(連絡先1, '') is not NULL
	
	UNION ALL
	select [PANO ] as com_ext_id
	, trim(連絡先2) as phone
	from csv_recf
	where 連絡先種別2 in ('電話', '携帯') and nullif(連絡先2, '') is not NULL
	)

, com_phone as (select com_ext_id
	, string_agg(phone, ',') as com_phone
	from allphone
	where phone is not NULL
	group by com_ext_id)

, allfax as (select [PANO ] as com_ext_id
	, trim(連絡先1) as fax
	from csv_recf
	where 連絡先種別1 in ('FAX') and nullif(連絡先1, '') is not NULL
	
	UNION ALL
	select [PANO ] as com_ext_id
	, trim(連絡先2) as fax
	from csv_recf
	where 連絡先種別2 in ('FAX') and nullif(連絡先2, '') is not NULL
	)

, com_fax as (select com_ext_id
	, string_agg(fax, ',') as com_fax
	from allfax
	where fax is not NULL
	group by com_ext_id)

--Company documents	| checking final backup if file without extension
, doc as (select seq
	, recf_id as company_id
	, pano as com_ext_id
	, right(trim([file]), charindex('/', reverse(trim([file]))) - 1) as UploadedName
	, memo
	, [file]
	, concat(coalesce(nullif(memo,''), concat_ws('_', convert(nvarchar,seq), pano))
			, right(trim([file]), charindex('.', reverse(trim([file]))))) as RealName
	from RECF_resume)

, companydoc as (select com_ext_id
	, string_agg(UploadedName, ',') as companydoc
	from doc
	where nullif(UploadedName,'') is not NULL
	group by com_ext_id)

--MAIN SCRIPT
select c.[PANO ] as [company-externalId] --#CF
, c.[会社名] as com_orginal_name
, concat('【', c.[PANO ], '】', c.[会社名]) as [company-name]
, case when len(c.[URL]) > 100 then left(c.[URL], patindex('%/%/%', replace(c.[URL], '//', '')) + 2)
	else trim(c.[URL]) end as [company-website]
--Company Owners
, case when c.[企業担当ユーザID] in ('FPC163', 'FPC207') then NULL --updated on 20200224 --c.[企業担当ユーザID] --userID | c.[企業担当] --user
		else trim(u.EmailAddress) end as [company-owners]
, cp.com_phone as [company-phone]
, cf.com_fax as [company-fax]
--Notes
, concat_ws(char(10)
	--, coalesce('[PA No.]' + c.[PANO ], NULL)
	, coalesce(char(10) + '【社内データメモ】' + char(10) + nullif(c.[社内データメモ],''), NULL) --internal memo
	, coalesce(char(10) + '【代表者氏名】' + char(10) + nullif(c.代表者氏名,''), NULL) --name of representative
	, coalesce(char(10) + '【従業員】' + char(10) + nullif(c.従業員,''), NULL) --number of employees
	, coalesce(char(10) + '【契約内容メモ】' + char(10) + nullif(c.契約内容メモ,''), NULL) --contract memo
	, coalesce(char(10) + '【その他メモ】' + char(10) + nullif(c.[その他メモ],''), NULL) --other notes
	) as [company-note]
, cd.companydoc as [company-document]
from csv_recf c
left join dup on dup.[PANO ] = c.[PANO ]
left join com_phone cp on cp.com_ext_id = c.[PANO ]
left join com_fax cf on cf.com_ext_id = c.[PANO ]
left join UserMapping u on u.UserID = c.[企業担当ユーザID]
left join companydoc cd on cd.com_ext_id = c.[PANO ]