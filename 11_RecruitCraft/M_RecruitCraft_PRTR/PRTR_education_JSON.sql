/* SPECIAL NOTES FOR JSON
--Backspace is replaced with \b
--Form feed is replaced with \f
--Newline is replaced with \n
--Carriage return is replaced with \r
--Tab is replaced with \t
--Double quote is replaced with \"
--Backslash is replaced with \\
*/

select distinct concat('PRTR',cn_id) as CandidateExtID
, getdate() as insert_timestamp --removed from json
	, (select edu_inst_name as institutionName
	, edu_duration as institutionAddress
	, edu_major as course
	, edu_degree_other as department
	/* CANNOT CONVERT START or GRADUATE DATE */
	--, convert(date,case when datalength(edu_start) = 4 and isnumeric(edu_start) = 1 then trim(edu_start) else NULL end,120) as startDate
	--, convert(date,case when len(edu_finish) = 4 and then edu_start else NULL end,120) as graduationDate
	, edu_gpa as gpa
	, concat_ws(char(10)
		, coalesce('Level of Education: ' + nullif(ltrim(rtrim(edu_degree_parsed)),''),NULL)
		, coalesce('Institution: ' + nullif(ltrim(rtrim(edu_inst_name)),''),NULL)
		, coalesce('Country: ' + nullif(ltrim(rtrim(edu_duration)),''),NULL)
		, coalesce('Program	: ' + nullif(ltrim(rtrim(edu_major)),''),NULL)
		, coalesce('Faculty / Program / Field: ' + nullif(ltrim(rtrim(edu_degree_other)),''),NULL)
		, coalesce('Start Year: ' + nullif(ltrim(rtrim(edu_start)),''),NULL)
		, coalesce('Finish Year: ' + nullif(ltrim(rtrim(edu_finish)),''),NULL)
		, coalesce('GPA: ' + nullif(ltrim(rtrim(edu_gpa)),''),NULL)
		) as description
	from candidate.Education where cn_id = m.cn_id
	and isDeleted = 0
	order by edu_id asc --edu_id as checked from the oldest to the latest
	for json path
	) as Education
from candidate.Education m -- rows (distinct to get 1 unique json for unique candidate)
where exists (select cn_id from candidate.Candidates where can_type = 1 and candidate.Candidates.cn_id = m.cn_id)
and m.isDeleted = 0
and (edu_degree_parsed is not NULL or edu_inst_name is not NULL or edu_duration is not NULL or edu_major is not NULL
	or edu_degree_other is not NULL or edu_start is not NULL or edu_finish is not NULL or edu_gpa is not NULL)
order by CandidateExtID --126149 rows