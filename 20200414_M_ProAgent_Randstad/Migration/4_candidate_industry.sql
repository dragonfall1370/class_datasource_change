with cand_industry as (select [PANO ] as cand_ext_id
	, trim([勤務歴 業種1]) as industry
	, concat('【PP】', p.industry_en) as vc_industry
	from csv_can c
	left join PA_industry p on p.pa_name = trim([勤務歴 業種1])
	where coalesce(nullif([勤務歴 業種1],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([勤務歴 業種2]) as industry
	, concat('【PP】', p.industry_en) as vc_industry
	from csv_can c
	left join PA_industry p on p.pa_name = trim([勤務歴 業種2])
	where coalesce(nullif([勤務歴 業種2],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([勤務歴 業種3]) as industry
	, concat('【PP】', p.industry_en) as vc_industry
	from csv_can c
	left join PA_industry p on p.pa_name = trim([勤務歴 業種3])
	where coalesce(nullif([勤務歴 業種3],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([勤務歴 業種4]) as industry
	, concat('【PP】', p.industry_en) as vc_industry
	from csv_can c
	left join PA_industry p on p.pa_name = trim([勤務歴 業種4])
	where coalesce(nullif([勤務歴 業種4],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([勤務歴 業種5]) as industry
	, concat('【PP】', p.industry_en) as vc_industry
	from csv_can c
	left join PA_industry p on p.pa_name = trim([勤務歴 業種5])
	where coalesce(nullif([勤務歴 業種5],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([勤務歴 業種6]) as industry
	, concat('【PP】', p.industry_en) as vc_industry
	from csv_can c
	left join PA_industry p on p.pa_name = trim([勤務歴 業種6])
	where coalesce(nullif([勤務歴 業種6],''), NULL) is not NULL
)

select distinct cand_ext_id
, vc_industry
, current_timestamp as insert_timestamp
from cand_industry