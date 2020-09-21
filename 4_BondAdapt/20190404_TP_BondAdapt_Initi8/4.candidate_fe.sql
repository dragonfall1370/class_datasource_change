

with 
skill as (select REFERENCE,DESCRIPTION from PROP_SKILLS SKILL INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SKILL.SKILL where LANGUAGE = 10010)



select --top 123
         pg.REFERENCE as 'candidate-externalId', pg.person_id
	, Coalesce(NULLIF(replace(pg.FIRST_NAME,'?',''), ''), 'No Firstname') as 'candidate-firstName'
	, Coalesce(NULLIF(replace(pg.LAST_NAME,'?',''), ''), 'No Lastname') as 'candidate-lastName'
       , skill.description
from PROP_PERSON_GEN pg --where pg.person_id = 1136371 --44889 rows
left join skill ON skill.REFERENCE = pg.reference -- industry
where skill.REFERENCE is not null
