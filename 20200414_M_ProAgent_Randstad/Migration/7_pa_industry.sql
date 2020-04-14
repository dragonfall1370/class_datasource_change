--Industry / Sub Ind from PA mapping
with split_industry as (select pa_name
	, concat_ws(' / ', industry_en, industry_ja) as industry
	, industry_en
	, trim(value) as vc_sub_ind
	from PA_industry
	cross apply string_split(sub_industry, char(10)))

--MAIN SCRIPT --using for mapping records
, ind_sub as (select pa_name
	, industry
	, industry_en
	, vc_sub_ind
	, replace(vc_sub_ind, '[P]', '') as sub_industry
	, row_number() over(partition by industry order by case when charindex('[P]', vc_sub_ind) = 1 then 1 else 2 end asc, vc_sub_ind asc) as rn
	--rn using for mapping
	from split_industry)

--MAIN SCRIPT FOR INDUSTRY --using for ind/ sub ind
select distinct industry
, sub_industry
from ind_sub
order by industry, sub_industry