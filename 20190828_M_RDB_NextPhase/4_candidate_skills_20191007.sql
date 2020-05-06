/*Audit from VC

select id, first_name, last_name, email, skills, external_id
into mike_candidate_skills_bkup_20191007
from candidate
where 1=1
--and skills is not NULL
and external_id is not NULL --36223 rows
*/

with candidateinIDobject as (
        select ObjectID
        from dbo.Objects
        where ObjectTypeId=1 --candidate

		UNION ALL
		select ObjectID
		from cand_2sector
)

, skills as (select AttributeId
	, case when AttributeMasterId in (432, 434) then 'All Gene Therapy'
		else trim(Description) end as skills
	from Attributes
	where AttributeMasterId in (433, 429, 430, 432, 434) --.Pharma Disciplines, .Pharma LIMS, .Pharma LIMS Skills, Cell and Gene Therapy, Therapy Areas
)

/* Audit 'Bio' values
select *
from ObjectAttributes
where AttributeId in (6981, 6983, 6982, 6984) */

, finalvc as (select distinct oa.ObjectID
		, oa.AttributeId
		, s.skills
		, sf.skills as VCSkills
		from ObjectAttributes oa
		inner join candidateinIDobject c on c.ObjectID = oa.ObjectID
		inner join skills s on s.AttributeId = oa.AttributeId
		left join skills_final sf on sf.Attributes = s.skills)

/* Check example 
select * from finalvc
where skills = 'Bioinformatics Developer' */

, distinctskills as (select distinct concat('NP', ObjectID) as cand_ext_id 
	, VCSkills
	from finalvc
	where VCSkills is not NULL
	--and cand_ext_id = 'NP101551'
	--and ObjectID in (select ObjectId from cand_2sector)
)

select cand_ext_id
, string_agg(VCSkills, ', ') as skills
from distinctskills
group by cand_ext_id --136 rows