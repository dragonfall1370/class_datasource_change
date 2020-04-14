--Check if Pro Agent company name contains in VC company name
select top 100 A.[PANO ]
, [会社名]
, B.name
from csv_recf A
cross join [20190903_vc_company] B
where patindex('%' + B.name + '%', A.[会社名]) > 1

--Check duplicated companies in Vincere
select * from [20190903_vc_company] where name in (
	select name
	from [20190903_vc_company]
	group by name
	having count(*) > 1) --186 rows
	
--Company owners
select distinct  [企業担当ユーザID]
, [企業担当]
from csv_recf
order by [企業担当ユーザID]

--Company info check records
select CompanyPANo
from csv_Company_Situation
group by CompanyPANo
having count(*) > 1

select CompanyPANo
, concat_ws(char(10)
	, coalesce('List: ' + coalesce(nullif(List, ''), 'NA'), NULL)
	, coalesce('Entry: ' + coalesce(nullif(Entry, ''), 'NA'), NULL)
	, coalesce('Contact: ' + coalesce(nullif(Contact, ''), 'NA'), NULL)
	, coalesce('Open: ' + coalesce(nullif([Open], ''), 'NA'), NULL)
	, coalesce('Close: ' + coalesce(nullif([Close], ''), 'NA'), NULL)
	, coalesce('Other: ' + coalesce(nullif(Other, ','), 'NA'), NULL)
	) as company_situation
, Other
from csv_Company_Situation

--Website trim
select [PANO ]
, [会社名]
, [代表者氏名]
, [URL]
, case when len([URL]) > 100 then left([URL], patindex('%/%/%', replace([URL], '//', '')) + 2) 
	else trim([URL]) end as website
, *
from csv_recf

--Company Industry
select [業種1]
, [業種2]
, [その他業種]
from csv_recf
where coalesce(nullif([業種1], ''), nullif([業種2], ''), nullif([その他業種], '')) is not NULL

select distinct [業種1]
from csv_recf
where [業種1] is not NULL

select distinct [業種2]
from csv_recf
where [業種2] is not NULL

select distinct [その他業種]
from csv_recf
where [その他業種] is not NULL

--Billing details
select [企業 PANO ]
, [その他請求先] --added in location note (if any)
, [請求先 〒1] --post code > General PO number
, [請求先 都道府県1] --prefecture > General PO number
, [請求先 住所詳細1] --street > General PO number
, [請求先 部署名1] --department name > Billing group name
, [請求先 担当者名1] --contact name > Trading name
, [請求先 TEL1]
, concat_ws( ' | '
	, concat_ws(', '
		, coalesce('[〒1]' + nullif([請求先 〒1],''), NULL)
		, coalesce(nullif([請求先 都道府県1],''), NULL)
		, coalesce(nullif([請求先 住所詳細1],''), NULL))
	, coalesce(nullif(concat_ws(', '
		, coalesce('[〒2]' + nullif([請求先 〒2],''), NULL)
		, coalesce(nullif([請求先 都道府県2],''), NULL)
		, coalesce(nullif([請求先 住所詳細2],''), NULL)) 
		, ''), NULL)
	) as general_po_number --to be confirmed
, concat_ws(' | '
	, coalesce(nullif([請求先 部署名1],''), NULL)
	, coalesce(nullif([請求先 部署名2],''), NULL)
	) as billing_group_name
, concat_ws(' | '
	, coalesce(nullif([請求先 担当者名1],''), NULL)
	, coalesce(nullif([請求先 担当者名2],''), NULL)
	) as trading_name
, concat_ws(' | '
	, coalesce(nullif([請求先 TEL1],''), NULL)
	, coalesce(nullif([請求先 TEL1],''), NULL)
	) as company_number
, [請求先 〒2]
, [請求先 都道府県2]
, [請求先 住所詳細2]
, [請求先 部署名2]
, [請求先 担当者名2]
, [請求先 TEL2]
from csv_recf_claim

--Company activities
select [企業 PANO ]
, [最新対応状況]
, [登録日]
, [登録者]
, [対応履歴メモ]
from csv_recf_history

--Job work location
select max(len([勤務地 詳細])) --town/city: 1745
--max(len([勤務地（部署名・所在地）※求人管理簿用])) --address: 5827
from csv_job

