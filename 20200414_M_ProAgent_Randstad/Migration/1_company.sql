--duplicates within Pro Agent but not in comparison with VC
with dup as (select [PANO ], [会社名], row_number() over(partition by lower([会社名]) order by [PANO] desc) as rn
		from csv_recf
		where trim([会社名]) not in (select trim(name) from [vc_company])) --29734 rows

/* HIDE THIS RULE
--duplicates in comparison with VC
, dup_vc as (select [PANO ], [会社名], [連絡先1], row_number() over(partition by lower([会社名]) order by [PANO] desc) as rn
		from csv_recf
		where trim([会社名]) in (select trim(name) from [vc_company])) --570 rows
*/
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

/*
select [会社名], count(*) from dup_vc
group by [会社名]
having count(*) > 1

select [PANO ], [会社名], [連絡先1]
, id as VC_company_id, name as VC_company_name, phone as VC_phone
from csv_recf com
inner join [20190903_vc_company] c on trim(c.name) = trim([会社名])
where [会社名] in (select name from [20190903_vc_company]) --VC has duplicate companies so using inner join may not be correct
*/

select c.[PANO ] as [company-externalId] --#CF
, c.[会社名] as com_orginal_name
, concat('【', c.[PANO ], '】', c.[会社名]) as [company-name]
/* Hide this rule for Review Site
, case 
	when c.[PANO ] in (select [PANO ] from dup_vc) then concat_ws(' - ', dup_vc.[会社名], '[vc]', dup_vc.rn)
	when c.[PANO ] in (select [PANO ] from dup) and dup.rn > 1 then concat_ws(' - ', dup.[会社名], dup.rn)
	else trim(c.[会社名]) end as [company-name]
*/
--, c.[URL]
, case when len(c.[URL]) > 100 then left(c.[URL], patindex('%/%/%', replace(c.[URL], '//', '')) + 2) 
	else trim(c.[URL]) end as [company-website]
--Company Owners
, trim(u.EmailAddress) as [company-owners] --c.[企業担当ユーザID] --userID | c.[企業担当] --user
, cp.com_phone as [company-phone]
, cf.com_fax as [company-fax]
--Notes
, concat_ws(char(10)
	--, coalesce('[PA No.]' + c.[PANO ], NULL)
	, coalesce('【社内データメモ】' + char(10) + nullif(c.[社内データメモ],''), NULL) --internal memo | removed on 20200205
	, coalesce('【代表者氏名】' + char(10) + nullif(c.代表者氏名,''), NULL) --name of representative
	, coalesce('【従業員】' + char(10) + nullif(c.従業員,''), NULL) --number of employees
	, coalesce('【契約内容メモ】' + char(10) + nullif(c.契約内容メモ,''), NULL) --contract memo
	, coalesce('【その他メモ】' + char(10) + nullif(c.[その他メモ],''), NULL) --other notes
	) as [company-note]
, cd.companydoc as [company-document]
from csv_recf c
left join dup on dup.[PANO ] = c.[PANO ]
--left join dup_vc on dup_vc.[PANO ] = c.[PANO ] --Hide this rule
left join com_phone cp on cp.com_ext_id = c.[PANO ]
left join com_fax cf on cf.com_ext_id = c.[PANO ]
left join UserMapping u on u.UserID = c.[企業担当ユーザID]
left join companydoc cd on cd.com_ext_id = c.[PANO ]