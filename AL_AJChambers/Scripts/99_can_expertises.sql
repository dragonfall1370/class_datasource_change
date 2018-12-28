--use AJChambersProd
declare @chars4trim varchar(20) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + N'%?*,. |'

--update ExpertisesSkillMap2
--set Skill = trim(@chars4trim from isnull(Skill, ''))

;with

CanSkills as (
	  select
	  CAND_ID as CanId
	  , trim(@chars4trim from isnull(Skill, '')) as Skill
	  from [SKILLINFO_DATA_TABLE]
	  where CAND_ID is not null
	  
	  --select
	  --CAND_ID as CanId
	  --, STRING_AGG(trim(@chars4trim from isnull(Skill, '')), ',') as Skills
	  --from
	  --[SKILLINFO_DATA_TABLE]
	  --group by CAND_ID
)

--select * from CanSkills order by CanId

, CanExpertises as (
	select
	CanId as entityExtId
	, y.Expertise
	, y.SubExpertise
	from CanSkills x
	join ExpertisesSkillMap2 y on lower(x.Skill) = lower(y.Skill)
)

select * from CanExpertises
order by entityExtId

--select distinct *

--into ExpertisesSkillMap2

--from ExpertisesSkillMap

--select * from ExpertisesSkillMap2
--where Skill like 'Leo%'
--where SubExpertise = 'Leon�s Candidate'

--update ExpertisesSkillMap2
--set SubExpertise = 'Leon''s Candidate'
--where Skill like 'Leo%'

--update ExpertisesSkillMap2
--set SubExpertise = N'Leon�s Candidate'
--, Skill = N'Leon�s Candidate'
--where Skill = 'Leon?s Candidate'

--insert into ExpertisesSkillMap2 values(
--'Tax', 'Leon�s Candidate', 'Leon�s Candidate'
--)