with tmp1 as (select app_id, max(id) as max_id 
from app_education
group by app_id)

---Education
, tmp2 as (select app_id
, school
, degree
from app_education
where id in (select max_id from tmp1))

---Job History
, tmp3 as (select id
, app_id
, company
, title
, ROW_NUMBER() OVER(PARTITION BY app_id ORDER BY id) AS company_row
from app_job_history)

, tmp3_1 as (select id
, app_id
, company as company1
, title as title1
from tmp3 where company_row = 1)

, tmp3_2 as (select id
, app_id
, company as company2
, title as title2
from tmp3 where company_row = 2)

, tmp3_3 as (select id
, app_id
, company as company3
, title as title3
from tmp3 where company_row = 3)

---Skills
, tmp4 (app_id, skill) as (select app_id
, STUFF(
         (SELECT ', ' + parent_skill + ' || ' + child_skill
          from app_skills
          WHERE app_id = a.app_id
          FOR XML PATH (''))
          , 1, 2, '') AS URLList
FROM app_skills AS a
GROUP BY a.app_id)

---Res Skills
, tmp5 (app_id, res_skill) as (select app_id
, STUFF(
         (SELECT ', ' + 'Skill: ' + cast(skill_name as varchar(max)) + ' || ' + 'value: '+ cast(skill_value as varchar(max))
          from app_res_skills
          WHERE app_id = a.app_id
          FOR XML PATH (''))
          , 1, 2, '')  AS URLList
FROM app_res_skills AS a
GROUP BY a.app_id)

, tmp6 as (select tmp4.app_id as app_id
, concat(tmp4.skill,char(10),tmp5.res_skill) as Skill
from tmp4
left join tmp5 on tmp4.app_id = tmp5.app_id)

---Doc_upload
, tmp7 (app_id, doc_upload) as (SELECT
     app_id,
     STUFF(
         (SELECT DISTINCT ',' + file_name
          from app_doc_upload
          WHERE app_id = a.app_id
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM app_doc_upload AS a
GROUP BY a.app_id)

---Resumes
, tmp8 (app_id, resumes) as (SELECT
     app_id,
     STUFF(
         (SELECT DISTINCT ',' + file_name
          from app_upload
          WHERE app_id = a.app_id
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM app_upload AS a
GROUP BY a.app_id)

, tmp9 (app_id, resumes) as 
(
SELECT * from tmp7
UNION
SELECT * from tmp8)

---Combine Doc and Resumes
, tmp10 (app_id, resumes) as (SELECT
     app_id,
     STUFF(
         (SELECT DISTINCT ',' + resumes
          from tmp9
          WHERE app_id = a.app_id
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM tmp9 AS a
GROUP BY a.app_id)
--- LinkedIn and Tweeter
, lktw (app_id, soc_url ) as (SELECT
     app_id,
     STUFF(
         (SELECT DISTINCT ',' + soc_url
          from app_social
          WHERE app_id = a.app_id
          FOR XML PATH (''))
          , 1, 1, '')  AS soc_url
FROM app_social AS a
GROUP BY a.app_id)
--select * from lktw
---
select ap.id as 'candidate-externalId'
, ap.first_name as 'candidate-firstName'
, ap.last_name as 'candidate-Lastname'
, ap.middle_name as 'candidate-middleName'
, iif(ap.email = '' or ap.email is NULL,concat(ap.id,'_candidate@noemail.com'),ap.email) as 'candidate-email'
, iif(ap.email2 = '' or ap.email is NULL,'',ap.email2) as 'candidate-workemail'
--, replace(ap.country,'United Kingdom (Great Britain)','United Kingdom') 'candidate-citizenship'
, cc.Code as 'candidate-citizenship'
, ap.addr1 as 'candidate-address'
, ap.city as 'candidate-city'
, ap.state as 'candidate-state'
, ap.zip as 'candidate-zipCode'
, ap.mobilephone as 'candidate-phone'
, ap.workphone as 'candidate-workPhone'
, ap.homephone as 'candidate-homePhone'
, lk.soc_url as 'candidate-linkedin'
, tmp3_1.company1 as 'candidate-employer1'
, tmp3_1.company1 as 'candidate-employer1'
, tmp3_1.title1 as 'candidate-jobTitle1'
, tmp3_2.company2 as 'candidate-employer2'
, tmp3_2.company2 as 'candidate-employer2'
, tmp3_2.title2 as 'candidate-jobTitle2'
, tmp3_3.company3 as 'candidate-employer3'
, tmp3_3.company3 as 'candidate-employer3'
, tmp3_3.title3 as 'candidate-jobTitle3'
, tmp2.school as 'candidate-schoolName'
, tmp2.degree as 'candidate-degreeName'
, tmp4.skill as 'candidate-keywords'
, tmp5.res_skill as 'candidate-skills'
, tmp10.resumes as 'candidate-resumes'
, concat(
	iif(addr2 = '' OR addr2 is NULL,'',concat('Address 2: ',addr2)),char(13)
	, 'Create date: ',convert(varchar(10),ap.create_date,120),char(13)
	, 'Last update on: ',convert(varchar(10),ap.create_date,120),char(13)
	, 'Headline: ',ap.headline,char(13)
	, 'Social URL: ', lktw.soc_url,char(13)
	) as 'candidate-note'
from applicants ap
left join CountryCode CC on ap.country = CC.Name
left join tmp2 on ap.id = tmp2.app_id
left join tmp3_1 on ap.id = tmp3_1.app_id
left join tmp3_2 on ap.id = tmp3_2.app_id
left join tmp3_3 on ap.id = tmp3_3.app_id
left join tmp4 on ap.id = tmp4.app_id
left join tmp5 on ap.id = tmp5.app_id
left join tmp10 on ap.id = tmp10.app_id
left join (select * from app_social where soc_url like '%linkedin%') lk on ap.id = lk.app_id
left join lktw on ap.id = lktw.app_id

--where ap.id = 8193154
--where ap.first_name like '%Angie%' or ap.first_name like '%Anne%' or ap.first_name like '%Sergio%'

--select * from applicants

--select * from CountryCode where name like 'United Kingdom%'