--COMPANY INDUSTRY
with split_ind as (select pa_name
	, industry_en
	, industry_ja
	, concat_ws('', '【PP】', industry_en, coalesce(' / ' + nullif(industry_ja, ''), NULL)) as vc_industry
	, sub_industry
	, trim(value) as vc_sub_industry
	from PA_industry
	cross apply string_split(sub_industry, char(10)))

--MAIN SCRIPT
, ind_sub as (select pa_name
	, vc_industry
	, sub_industry
	, vc_sub_industry
	, row_number() over(partition by vc_industry order by case when charindex('[P]', vc_sub_industry) = 1 then 1 else 2 end asc, vc_sub_industry asc) as rn
	from split_ind) --select * from ind_sub

, other_ind as (select [PANO ]
	, [その他業種]
	, trim(value) as other_ind
	from csv_recf
	cross apply string_split(replace(replace([その他業種], '[業種]', '|'), char(10), ''), '|')
	where coalesce(nullif([その他業種],''), NULL) is not NULL)

, company_industry as (select [PANO ]
	, [業種1] as original
	, trim([業種1]) as pa_industry
	, vc_industry
	, vc_sub_industry
	from csv_recf
	left join ind_sub p on p.pa_name = trim([業種1])
	where coalesce(nullif([業種1],''), NULL) is not NULL
	
	UNION
	
	select [PANO ]
	, [業種2] as original
	, trim([業種2]) as pa_industry
	, vc_industry
	, vc_sub_industry
	from csv_recf
	left join ind_sub p on p.pa_name = trim([業種2])
	where coalesce(nullif([業種2],''), NULL) is not NULL
	
	UNION
	
	select [PANO ]
	, [その他業種] as original
	, other_ind as pa_industry
	, vc_industry
	, vc_sub_industry
	from other_ind
	left join ind_sub p on p.pa_name = trim(other_ind)
	where coalesce(nullif(other_ind,''), NULL) is not NULL
	--total 37622
)

--select distinct vc_industry from company_industry
select distinct [PANO ] as com_ext_id
, trim(vc_industry) as vc_industry
, replace(vc_sub_industry, '[P]', '') as vc_sub_industry
, current_timestamp as insert_timestamp
from company_industry --75904 rows
--where [PANO ] = 'CPY000028'
order by com_ext_id