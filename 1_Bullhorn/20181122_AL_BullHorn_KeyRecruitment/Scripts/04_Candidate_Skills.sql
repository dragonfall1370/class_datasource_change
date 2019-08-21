-- SkillName: split by separate rows by comma, then combine them into SkillName
drop table if exists #CanSkillsTmp1
select x.UserId, trim(' ,' from isnull(cast(x.skillIDList as varchar(max)), '')) as skillIds
into #CanSkillsTmp1
from bullhorn1.Candidate x
where x.isPrimaryOwner = 1 and x.isDeleted = 0
--#debug
--select * from #CanSkillsTmp1

drop table if exists #CanSkillsTmp2

select
distinct
userID,
iif(len(trim(isnull(value, ''))) = 0, 0, convert(int, trim(isnull(value, '')))) as skillId

into #CanSkillsTmp2

from #CanSkillsTmp1 x
    cross apply string_split(x.skillIds, ',');

drop table if exists #CanSkillsTmp1

--#debug
--select * from #CanSkillsTmp2

drop table if exists #CanSkillsTmp3

select
x.userID
, trim(isnull(y.name, '')) as skillName

into #CanSkillsTmp3

from #CanSkillsTmp2 x
left join bullhorn1.BH_SkillList y ON x.skillId = y.skillID

drop table if exists #CanSkillsTmp2

--#debug
--select * from #CanSkillsTmp3

drop table if exists #CanSkillsTmp4

select
userID
, string_agg(skillName, ', ') as Skills

into #CanSkillsTmp4

from #CanSkillsTmp3
group by userID

drop table if exists #CanSkillsTmp3

drop table if exists VCCanSkills


select
x.candidateID
, x.userID
, trim(' ,' from concat(y.Skills, ', ', x.skillSet)) as Skills

into VCCanSkills

from
(select CandidateID, userID, skillSet from bullhorn1.Candidate) x
left join #CanSkillsTmp4 y on x.userID = y.userID

drop table if exists #CanSkillsTmp4

select * from VCCanSkills
--where len(Skills) > 0
--order by userID