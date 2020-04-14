--VC COMPANY WITH MERGING CONDITIONS
with company_name as (select id, name
	, charindex('(',name) begin_string
	, charindex(')',name) end_string
	, insert_timestamp
	, last_activity_date
	from vc_company)

--MAIN SCRIPT --3 cases with wrong company name ID (37906,15424,21300)
, vc_company_name as (select id, name
	, substring(name, begin_string + 1
		, case when end_string < begin_string then len(name) - begin_string - 1
			else end_string - begin_string - 1 end) as vc_company_name
	, insert_timestamp
	, last_activity_date
	from company_name
	where 1=1
	and begin_string > 0 and end_string > 0
	--and end_string - begin_string < 1
	--and id = 37906
	)

select [PANO ]
, [会社名]
, convert(datetime2, [登録日], 120) as created_date
, convert(datetime2, [更新日], 120) as modified_date
, vc.id
, vc.name
, vc.insert_timestamp
, vc.last_activity_date
from csv_recf c
join vc_company_name vc on replace(replace([会社名], N'　', ''), ' ', '') = replace(replace(vc_company_name, N'　', ''), ' ', '')
where vc.id is not NULL
--order by id
--6377 rows