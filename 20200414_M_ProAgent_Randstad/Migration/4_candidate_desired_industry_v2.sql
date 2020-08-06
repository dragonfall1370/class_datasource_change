--Desired Industry
with split_industry as (select pa_name
	, concat_ws(' / ', industry_en, industry_ja) as industry
	, industry_en
	, trim(value) as vc_sub_ind
	from PA_industry
	cross apply string_split(sub_industry, char(10)))

--using for mapping records
, ind_sub as (select pa_name
	, concat_ws('', '【PP】', industry) as industry
	, industry_en
	, vc_sub_ind
	, replace(vc_sub_ind, '[P]', '') as sub_industry
	, row_number() over(partition by industry order by case when charindex('[P]', vc_sub_ind) = 1 then 1 else 2 end asc, vc_sub_ind asc) as rn
	, row_number() over(partition by pa_name order by case when charindex('[P]', vc_sub_ind) = 1 then 1 else 2 end asc, vc_sub_ind asc) as pa_rn
	--rn using for mapping
	from split_industry) --select * from ind_sub

--MAIN SCRIPT
, cand_industry as (select [PANO ] as cand_ext_id
	, trim([希望業種1]) as industry
	, industry as vc_industry
	, sub_industry as vc_sub_industry
	, pa_rn
	from csv_can c
	left join ind_sub p on p.pa_name = trim([希望業種1])
	where coalesce(nullif([希望業種1],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([希望業種2]) as industry
	, industry as vc_industry
	, sub_industry vc_sub_industry
	, pa_rn
	from csv_can c
	left join ind_sub p on p.pa_name = trim([希望業種2])
	where coalesce(nullif([希望業種2],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([希望業種3]) as industry
	, industry as vc_industry
	, sub_industry vc_sub_industry
	, pa_rn
	from csv_can c
	left join ind_sub p on p.pa_name = trim([希望業種3])
	where coalesce(nullif([希望業種3],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([希望業種4]) as industry
	, industry as vc_industry
	, sub_industry vc_sub_industry
	, pa_rn
	from csv_can c
	left join ind_sub p on p.pa_name = trim([希望業種4])
	where coalesce(nullif([希望業種4],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([希望業種5]) as industry
	, industry as vc_industry
	, sub_industry vc_sub_industry
	, pa_rn
	from csv_can c
	left join ind_sub p on p.pa_name = trim([希望業種5])
	where coalesce(nullif([希望業種5],''), NULL) is not NULL
	
	UNION ALL
	select [PANO ] as cand_ext_id
	, trim([希望業種6]) as industry
	, industry as vc_industry
	, sub_industry vc_sub_industry
	, pa_rn
	from csv_can c
	left join ind_sub p on p.pa_name = trim([希望業種6])
	where coalesce(nullif([希望業種6],''), NULL) is not NULL
)

select *
from cand_industry
--where cand_ext_id = 'CDT061361'