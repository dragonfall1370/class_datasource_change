--Desired Industry
with cand_industry as (select [PANO ] as cand_ext_id
	, trim([希望業種1]) as industry
	, coalesce('【PP】'+ nullif( p.industry_en,''), NULL) as vc_industry
	from csv_can c
	left join PA_industry p on p.pa_name = trim([希望業種1])
	where coalesce(nullif([希望業種1],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([希望業種2]) as industry
	, coalesce('【PP】'+ nullif( p.industry_en,''), NULL) as vc_industry
	from csv_can c
	left join PA_industry p on p.pa_name = trim([希望業種2])
	where coalesce(nullif([希望業種2],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([希望業種3]) as industry
	, coalesce('【PP】'+ nullif( p.industry_en,''), NULL) as vc_industry
	from csv_can c
	left join PA_industry p on p.pa_name = trim([希望業種3])
	where coalesce(nullif([希望業種3],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([希望業種4]) as industry
	, coalesce('【PP】'+ nullif( p.industry_en,''), NULL) as vc_industry
	from csv_can c
	left join PA_industry p on p.pa_name = trim([希望業種4])
	where coalesce(nullif([希望業種4],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([希望業種5]) as industry
	, coalesce('【PP】'+ nullif( p.industry_en,''), NULL) as vc_industry
	from csv_can c
	left join PA_industry p on p.pa_name = trim([希望業種5])
	where coalesce(nullif([希望業種5],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([希望業種6]) as industry
	, coalesce('【PP】'+ nullif( p.industry_en,''), NULL) as vc_industry
	from csv_can c
	left join PA_industry p on p.pa_name = trim([希望業種6])
	where coalesce(nullif([希望業種6],''), NULL) is not NULL
)

select distinct cand_ext_id
, vc_industry
, current_timestamp as insert_timestamp
from cand_industry