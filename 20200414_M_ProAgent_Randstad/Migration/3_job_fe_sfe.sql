/* Add new column to track created date

alter table position_description_functional_expertise
add column created_date timestamp

*/

with job_fe_sfe as (select [PANO ] as job_ext_id
	, [職種分類 職種カテゴリー1] as fe
	, [職種分類 職種1] as sfe
	from csv_job j
	where coalesce(nullif([職種分類 職種カテゴリー1],''), nullif([職種分類 職種1],'')) is not NULL
	
	UNION ALL
	
	select [PANO ] as job_ext_id
	, [職種分類 職種カテゴリー2]
	, [職種分類 職種2]
	from csv_job j
	where coalesce(nullif([職種分類 職種カテゴリー2],''), nullif([職種分類 職種2],'')) is not NULL
)

, split_sfe as (select pa_category
	, pa_category_name
	, vc_fe_en
	, vc_fe_ja
	, vc_sfe
	, trim(value) as vc_sfe_split
	from PA_fe_sfe
	cross apply string_split(vc_sfe, char(10)))

, fe_sfe as (select pa_category
	, pa_category_name
	, vc_fe_en
	, vc_fe_ja
	, vc_sfe
	, vc_sfe_split
	, left(vc_sfe_split, charindex('/', vc_sfe_split) - 2) as vc_sfe_en
	, right(vc_sfe_split, len(vc_sfe_split) - charindex('/', vc_sfe_split) - 1) as vc_sfe_ja
	, row_number() over(partition by vc_fe_en order by case when charindex('[P]', vc_sfe_split) = 1 then 1 else 2 end asc, vc_sfe_split asc) as rn
	from split_sfe)

select j.job_ext_id
, j.fe
, concat('【PP】', vc_fe_en, coalesce(' / ' +  nullif(vc_fe_ja,''), '')) as vc_fe
, j.sfe
, replace(trim(fe.vc_sfe_split), '[P]', '') as vc_sfe
from job_fe_sfe j
left join fe_sfe fe on fe.pa_category = j.fe and fe.pa_category_name = j.sfe
--where vc_fe_en like '%Executive%'

--Audit from VC
/*
select max(id)
from position_description_functional_expertise --34985 --19318 rows
*/