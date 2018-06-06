---MAIN SCRIPT: INJECT FE & SFE FOR JOB
with JobFE as (
select al.ATTOBJECTUNIQ as Forward_JobExtID
, al.ATTRIBUTEUNIQ as OriginalAttID
, a.*
from AttributesLink al
left join FinalAttributes a on a.SFEID = al.ATTRIBUTEUNIQ
where al.ATTOBJECTUNIQ in (select UniqueID from Vacancies))

select distinct concat('FR',Forward_JobExtID) as Forward_JobExtID
, OriginalAttID
, FEValue
, SFEValue as Forward_SFE
, getdate() as Forward_insert_timestamp
from JobFE
where FEID is not NULL --131392 rows


---INJECT FE ONLY FOR VACANCY
with JobFE as (
select al.ATTOBJECTUNIQ as Forward_JobExtID
, al.ATTRIBUTEUNIQ as OriginalAttID
, a.*
from AttributesLink al
left join FinalAttributes a on a.SFEID = al.ATTRIBUTEUNIQ
where al.ATTOBJECTUNIQ in (select UniqueID from Vacancies))

select distinct concat('FR',jfe.Forward_JobExtID) as Forward_JobExtID
, jfe.OriginalAttID
, a.FEValue as ForwardRole_FE
from JobFE jfe
left join FinalAttributes a on a.FEID = jfe.OriginalAttID
where 1 = 1
and jfe.OriginalAttID in (select AttributeUniq from Attributes where ParentAttUniq not in (7, 37, 70, 542) and AttributeUniq not in (7, 37, 70, 542))
and jfe.FEID is NULL --2044 rows w/ only FE