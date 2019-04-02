with persontype as (select * from person_type where type in ('A','C','D','E'))

,test as (select b.person_ref as 'candidate-externalId',
ROW_NUMBER() over ( partition by b.person_ref order by b.person_ref ) as rn
from candidate a
right join persontype d on a.person_type_ref = d.person_type_ref
left join person b on d.person_ref = b.person_ref)

,test2 as (select * from test where rn = 1)


,test3 as (select person_ref,a.organisation_ref,a.displayname as jobtitle, b.displayname, convert(datetime, CONVERT(float,a.start_date)) as start_date from position a
left join organisation b on a.organisation_ref = b.organisation_ref


where person_ref in (select [candidate-externalId] from test2))


,test4 as (select *, ROW_NUMBER() over (partition by person_ref order by start_date desc) as rn2 from test3)

,test5 as (select person_ref,jobTitle, replace(displayname,'&#x0D','') as currentEmployer from test4 where rn2 <> 1)
--select person_ref,(select jobtitle,currentEmployer, 1 as cb_Employer for json path) as json_detail from test5 where jobtitle is not null and currentEmployer is not null 

--select * from test5

,test6 as (select person_ref, concat('Employer: ',currentEmployer,(char(13)+char(10)),'Job Title: ',jobTitle) as Work_History from test5)

--select * from test6
select person_ref, string_agg(Work_History, char(10)) as workHistory
FROM test6 b
GROUP BY person_ref



