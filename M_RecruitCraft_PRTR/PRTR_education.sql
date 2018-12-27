with EducationSummary as 
(select cn_id
, string_agg( cast(concat_ws(char(10),char(13)
		, coalesce('Level of Education: ' + nullif(ltrim(rtrim(edu_degree_parsed)),''),NULL)
		, coalesce('Institution: ' + nullif(ltrim(rtrim(edu_inst_name)),''),NULL)
		, coalesce('Country: ' + nullif(ltrim(rtrim(edu_duration)),''),NULL)
		, coalesce('Program	: ' + nullif(ltrim(rtrim(edu_major)),''),NULL)
		, coalesce('Faculty / Program / Field: ' + nullif(ltrim(rtrim(edu_degree_other)),''),NULL)
		, coalesce('Start Year: ' + nullif(ltrim(rtrim(edu_start)),''),NULL)
		, coalesce('Finish Year: ' + nullif(ltrim(rtrim(edu_finish)),''),NULL)
		, coalesce('GPA: ' + nullif(ltrim(rtrim(edu_gpa)),''),NULL)
		) as nvarchar(max)), char(10)) within group (order by edu_id asc) as Education --edu_id as checked from the oldest to the latest
from candidate.Education
where isDeleted = 0
and (edu_degree_parsed is not NULL or edu_inst_name is not NULL or edu_duration is not NULL or edu_major is not NULL
	or edu_degree_other is not NULL or edu_start is not NULL or edu_finish is not NULL or edu_gpa is not NULL)
group by cn_id)

select concat('PRTR',c.cn_id) as CandidateExtID
, concat_ws(char(10)
	, '[Education Summary]'
	, nullif(e.Education,'')
	, coalesce(char(10) + 'Training & Certification Details: ' 
	+ nullif(replace(replace(replace(replace(
		ltrim(rtrim(cast([dbo].[udf_StripHTML]([dbo].[udf_StripCSS](c.cn_training)) as nvarchar(max))))
		,char(9),''),char(10),''),char(13),''),'.','. '),''),NULL)) as Education
from candidate.Candidates c
left join EducationSummary e on e.cn_id = c.cn_id
where c.can_type = 1
and (e.Education <> '' or cast(c.cn_training as nvarchar(max)) <> '')
order by c.cn_id