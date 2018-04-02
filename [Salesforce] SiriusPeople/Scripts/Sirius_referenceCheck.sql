select * from Ads

select PEOPLECLOUD1__VACANCY__C, count(*), max(NAME) 
from Ads where PEOPLECLOUD1__VACANCY__C is not NULL
group by PEOPLECLOUD1__VACANCY__C
having COUNT(*) > 1

--Check duplicate and latest created date
select * from Ads where PEOPLECLOUD1__VACANCY__C = 'a0C9000000bTa5zEAC' --max Name: 12850

select * from Ads where PEOPLECLOUD1__VACANCY__C = 'a0C9000000Sqn8cEAB'

select distinct PEOPLECLOUD1__VACANCY__C from Ads

select PEOPLECLOUD1__END_DATE__C from Jobs

select top 10 * from Candidate

select distinct VACANCY_STATUS__C from Jobs

select distinct JOB_TYPE__C from Jobs

select distinct RECORD_TYPE_NAME__C from Jobs

select * from Jobs where CLIENT_CONTACT__C = '' or CLIENT_CONTACT__C is NULL

select getdate() - 1

select top 10 * from ResumeCompliance

select PEOPLECLOUD1__DOCUMENT_RELATED_TO__C
	, STRING_AGG(NAME, ', ') as ContactEmail
from ResumeCompliance
where NAME like '.%doc' or NAME like '%.pdf' or NAME like '%.rtf' or NAME like '%.xls%' or NAME like '%.html'
group by PEOPLECLOUD1__DOCUMENT_RELATED_TO__C

select distinct PEOPLECLOUD1__STATUS__C from Candidate

-----
select * from Candidate
where PEOPLECLOUD1__STATUS__C = 'Inactive'

select * from Candidate where PEOPLECLOUD1__STATUS__C <> 'Inactive'

select count(*) from Candidate where PEOPLECLOUD1__STATUS__C not in ('Inactive') --240099

select count(*) from Candidate where PEOPLECLOUD1__STATUS__C in ('Inactive') --6868

select count(*) from Candidate --358939

select count(*) from Candidate where PEOPLECLOUD1__STATUS__C is NULL --111972

select distinct SALUTATION from Candidate

select distinct PEOPLECLOUD1__GENDER__C from Candidate

select PEOPLECLOUD1__HOME_EMAIL__C, PEOPLECLOUD1__WORK_EMAIL__C from Candidate

select * from ResumeCompliance
where NAME like '%Industrious%'

select * from ResumeCompliance
where PEOPLECLOUD1__DOCUMENT_TYPE__C = 'Attachment'

select * from CandidateSkills
where PEOPLECLOUD1__CANDIDATE__C ='0039000000ri7MSAAY'
------------
with SkillName as (select PEOPLECLOUD1__CANDIDATE__C, SKILL_GROUP_NAME__C, STRING_AGG(SKILL_NAME__C,', ') as SkillName 
from CandidateSkills
group by PEOPLECLOUD1__CANDIDATE__C, SKILL_GROUP_NAME__C)

, SkillGroup as (select PEOPLECLOUD1__CANDIDATE__C, concat(SKILL_GROUP_NAME__C,': ',SkillName) as SkillGroup
from SkillName)

, CandSkills as (select PEOPLECLOUD1__CANDIDATE__C,  STRING_AGG(SkillGroup,', ') as CandSkills 
from SkillGroup
group by PEOPLECLOUD1__CANDIDATE__C)

select * from CandSkills
---------------

select PEOPLECLOUD1__CANDIDATE__C, SKILL_GROUP_NAME__C, STRING_AGG(concat(SKILL_GROUP_NAME__C,': ',SKILL_NAME__C),', ') as Skills 
from CandidateSkills
group by PEOPLECLOUD1__CANDIDATE__C, SKILL_GROUP_NAME__C

---------------
select * from Tasks
where SUBJECT like '%now working as a solutions architect  open to freelance part time jobs with anything%'

select len(subject) from Tasks
where id = '00T9000003wx9lxEAA'

