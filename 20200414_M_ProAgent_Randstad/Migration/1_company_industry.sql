--COMPANY INDUSTRY
with other_ind as (select [PANO ]
	, [その他業種]
	, trim(value) as other_ind
	from csv_recf
	cross apply string_split(replace(replace([その他業種], '[業種]', '|'), char(10), ''), '|')
	where coalesce(nullif([その他業種],''), NULL) is not NULL)

--MAIN SCRIPT
, company_industry as (select [PANO ]
	, [業種1] as original
	, trim([業種1]) as industry
	, concat('【PP】', p.industry_en) as vc_industry
	from csv_recf
	left join PA_industry p on p.pa_name = trim([業種1])
	where coalesce(nullif([業種1],''), NULL) is not NULL
	
	UNION
	
	select [PANO ]
	, [業種2] as original
	, trim([業種2]) as industry
	, concat('【PP】', p.industry_en) as vc_industry
	from csv_recf
	left join PA_industry p on p.pa_name = trim([業種2])
	where coalesce(nullif([業種2],''), NULL) is not NULL
	
	UNION
	
	select [PANO ]
	, [その他業種] as original
	, other_ind as industry
	, concat('【PP】', p.industry_en) as vc_industry
	from other_ind
	left join PA_industry p on p.pa_name = trim(other_ind)
	where coalesce(nullif(other_ind,''), NULL) is not NULL
	--total 37622
)

--select distinct vc_industry from company_industry

select distinct [PANO ] as com_ext_id
, trim(vc_industry) as vc_industry
, current_timestamp as insert_timestamp
from company_industry --35647 rows