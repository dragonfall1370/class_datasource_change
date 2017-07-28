with
 tmp1 as (select app_id, max(id) as max_id from app_education group by app_id)

---Education
, tmp2 as (select app_id, [from], [to], school, degree from app_education where id in (select max_id from tmp1))
--select * from tmp2

---Job History
, tmp3 as (select id, app_id, company, [from], title, summary, ROW_NUMBER() OVER(PARTITION BY app_id ORDER BY id) AS company_row from app_job_history)
, tmp3_1 as (select id, app_id, company as company1, [from] as from1, title as title1, summary as summary1 from tmp3 where company_row = 1)
, tmp3_2 as (select id, app_id, company as company2, [from] as from2, title as title2, summary as summary2 from tmp3 where company_row = 2)
, tmp3_3 as (select id, app_id, company as company3, [from] as from3, title as title3, summary as summary3 from tmp3 where company_row = 3)

---Skills
, tmp4 (app_id, skill) as (select app_id
, STUFF((SELECT ', ' + parent_skill + ': ' + child_skill from app_skills WHERE app_id = a.app_id FOR XML PATH ('')), 1, 2, '') AS URLList FROM app_skills AS a GROUP BY a.app_id)

---Res Skills
, tmp5 (app_id, res_skill) as (select app_id
, STUFF((SELECT char(10) + ' Skill: ' + cast(skill_name as varchar(max)) + ' - ' + 'Value: '+ cast(skill_value as varchar(max)) from app_res_skills WHERE app_id = a.app_id FOR XML PATH ('')), 1, 2, '')  AS URLList FROM app_res_skills AS a GROUP BY a.app_id)

, tmp6 as (select tmp4.app_id as app_id, concat(tmp4.skill,char(10),tmp5.res_skill) as Skill from tmp4
left join tmp5 on tmp4.app_id = tmp5.app_id)

---Doc_upload
, tmp7 (app_id, doc_upload) as (SELECT app_id,
     STUFF((SELECT DISTINCT ',' + file_name from app_doc_upload WHERE app_id = a.app_id FOR XML PATH ('')), 1, 1, '')  AS URLList FROM app_doc_upload AS a GROUP BY a.app_id)

---Resumes
, tmp8 (app_id, resumes) as (SELECT app_id,
     STUFF((SELECT DISTINCT ',' + file_name from app_upload WHERE app_id = a.app_id FOR XML PATH ('')), 1, 1, '')  AS URLList FROM app_upload AS a GROUP BY a.app_id)

, tmp9 (app_id, resumes) as (SELECT * from tmp7 UNION SELECT * from tmp8)

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

, tag as (SELECT app_id,STUFF((SELECT char(10)  + 'Date: ' + convert(varchar(10), actdate, 120) + ' ' + convert(varchar(10), acttime, 120) 
	+ char(10) + 'Author: ' + case when ( c.first_name is null) then '(no author)' else concat(c.first_name,' ',c.last_name) end + char(10) 
	+ '- ' + b.act_name + ': ' + cast(actnotes as varchar(max)) + char(10)
	from app_activity 
	left join app_activity_type b on acttype = b.id  
	left join users c on act_auth = c.ID
	WHERE app_id = a.app_id order by actdate FOR XML PATH ('')), 1, 1, '')  AS tag 
FROM  app_activity AS a GROUP BY a.app_id)
--select * from tag where app_id = 8428418

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
, lk.soc_url as 'candidate-linkedinprofile'
--
, tmp3_1.company1 as 'candidate-employer1'
, tmp3_1.title1 as 'candidate-jobTitle1'
--, case when (tmp3_1.from1 like '[A-Z]%' and tmp3_1.from1 != '' and tmp3_1.from1 is not null) then cast(tmp3_1.from1 as date) else tmp3_1.from1 end as 'candidate-startdate1'
--, cast(tmp3_1.from1 as date) as 'candidate-startdate1'
--, convert(datetime,tmp3_1.from1,64) as 'candidate-startdate11'
, concat(case when (tmp3_1.from1 is null) then '' else concat('Candidate Startdate: ',tmp3_1.from1,char(13)) end,char(10),tmp3_1.summary1) as 'candidate-company1'
--
, tmp3_2.company2 as 'candidate-employer2'
, tmp3_2.title2 as 'candidate-jobTitle2'
--, case when (tmp3_2.from2 like '[A-Z]%' and tmp3_2.from2 != '' and tmp3_2.from2 is not null) then cast(tmp3_2.from2 as date) else tmp3_2.from2 end as 'candidate-startdate2'
, concat(case when (tmp3_2.from2 is null) then '' else concat('Candidate Startdate: ',tmp3_2.from2,char(13)) end,char(10),tmp3_2.summary2) as 'candidate-company2'
--
, tmp3_3.company3 as 'candidate-employer3'
, tmp3_3.title3 as 'candidate-jobTitle3'
--, case when (tmp3_3.from3 like '[A-Z]%' and tmp3_3.from3 != '' and tmp3_3.from3 is not null) then cast(tmp3_3.from3 as date) else tmp3_3.from3 end as 'candidate-startdate3'
, concat(case when (tmp3_3.from3 is null) then '' else concat('Candidate Startdate: ',tmp3_3.from3,char(13)) end,char(10),tmp3_3.summary3) as 'candidate-company3'
--
--, ap.salary as 'candidate-salary'
, tmp2.school as 'candidate-schoolName'
, tmp2.degree as 'candidate-degreeName'
--, concat('01 01 ',tmp2.[to]) as 'candidate-graduationdate'
, tmp4.skill as 'candidate-keyword'
, tmp5.res_skill as 'candidate-skills'
, tmp10.resumes as 'candidate-resume'
, concat(
	iif(addr2 = '' OR addr2 is NULL,'',concat('Address 2: ',addr2)),char(13)
	, 'Create date: ',convert(varchar(10),ap.create_date,120),char(13)
	, 'Last update on: ',convert(varchar(10),ap.create_date,120),char(13)
	, case when (ap.headline is null) then '' else concat('Headline: ',ap.headline,char(13)) end
	, case when (lktw.soc_url is null) then '' else concat('Social URL: ',lktw.soc_url,char(13)) end
	--, case when (tag.tags is null) then '' else concat('Tags / Activities Notes: ',tag.tags,char(13)) end
	, case when (tmp2.[to] is null) then '' else concat('Graduation Date: ',tmp2.[to],char(13)) end
	, case when (class.class is null) then '' else concat('The Class: ',class.class,char(13)) end
	, case when (wa.app_source is null) then '' else concat('Source: ',wa.app_source,char(13)) end
	, case when (src.source is null) then '' else concat('Source: ',src.source,char(13)) end
	, case when (ind.industry is null) then '' else concat('Industry: ',ind.industry,char(13)) end
	, case when (tmp3_1.title1 is null) then '' else concat('Position: ',tmp3_1.title1,char(13)) end
	, case when (cast(be.beeeeo as varchar(max)) = '') then '' else concat('BEE/EEO: ',be.beeeeo,char(13)) end
	, concat (
			case when (ap.salary_min is null) then '' else concat('Salary Min: ',ap.salary_min,char(13)) end
			, case when (ap.salary_max is null) then '' else concat('Salary Max: ',ap.salary_max,char(13)) end
			, case when (ap.salary is null) then '' else concat('Salary: ',ap.salary,char(13)) end)
	, concat(
			case when (ap.hourly_min is null) then '' else concat('Hourly Min: ',ap.hourly_min,char(13)) end
			, case when (ap.hourly_max is null) then '' else concat('Hourly Max: ',ap.hourly_max,char(13)) end
			, case when (ap.hourly is null) then '' else concat('Hourly: ',ap.hourly,char(13)) end)
	, case when (cnt.country is null) then '' else concat('Country: ',cnt.country,char(13)) end
	) as 'candidate-note'
	, left(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(case when (tag.tag is null) then '' else concat('Activities: ',char(10),tag.tag,char(10)) end,case when (ap.comments is null) then '' else concat('Comments: ',char(10),ap.comments,char(10)) end)
		,'&amp;',''),'nbsp;',''),'#39;',''),'&#x0D;',''),'&lt;',''),'&gt;',''),'%20',''),'%28','('),'%29',')'),'%2B','+'),29999) as 'candidate-comments'
		--,'&lt;br /&gt;','.'),'&amp;',''),'nbsp;',''),'#39;',''),'&#x0D;',''),'&lt;',''),'&gt;','') as 'candidate-comments'
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
left join (select distinct app_id,app_source from web_applicants) wa on ap.id = wa.app_id 
left join (SELECT app_id,STUFF((SELECT DISTINCT ',' + tag_name from app_tag5 WHERE app_id = a.app_id FOR XML PATH ('')), 1, 1, '')  AS beeeeo FROM app_tag5 AS a GROUP BY a.app_id) be on ap.id = be.app_ID
left join (SELECT app_id,STUFF((SELECT DISTINCT ',' + tag_name from app_tag6 WHERE app_id = a.app_id FOR XML PATH ('')), 1, 1, '')  AS source FROM app_tag6 AS a GROUP BY a.app_id) src on ap.id = src.app_ID
left join (SELECT app_id,STUFF((SELECT DISTINCT ',' + tag_name from app_tag3 WHERE app_id = a.app_id FOR XML PATH ('')), 1, 1, '')  AS industry FROM app_tag3 AS a GROUP BY a.app_id) ind on ap.id = ind.app_ID
left join (SELECT app_id,STUFF((SELECT DISTINCT ',' + tag_name from app_tag2 WHERE app_id = a.app_id FOR XML PATH ('')), 1, 1, '')  AS country FROM app_tag2 AS a GROUP BY a.app_id) cnt on ap.id = cnt.app_ID
left join (SELECT app_id,STUFF((SELECT DISTINCT ',' + detail_tag from applicant_tags WHERE app_id = a.app_id FOR XML PATH ('')), 1, 1, '')  AS class FROM applicant_tags AS a GROUP BY a.app_id) class on ap.id = class.app_ID
left join tag on ap.id = tag.app_id

where ap.id = 8468827 
or ap.id = 9904908 
or ap.id = 9904982
or ap.id = 9904940
or ap.id = 9904924
or ap.id = 9904927
or ap.id = 9904974
or ap.id = 9730509 --
or ap.id = 9904977
or ap.id = 9904993
or ap.id = 9905024
or ap.id = 9728687 --
or ap.id = 9905042
or ap.id = 10111845
or ap.id = 10106815
or ap.id = 10111865

or ap.id = 8626894
or ap.id = 9930002
or ap.id = 9970190

or ap.id = 8543236 --
or ap.id = 8543252

--where tag.tag is not null 
--8468827
--where tmp3_1.from1 != '' and tmp3_1.from1 is not null
--and tmp3_2.from2 != '' and tmp3_2.from2 is not null
--and tmp3_2.from2 != '' and tmp3_2.from2 is not null
--where ap.first_name like '%Angie%' or ap.first_name like '%Anne%' or ap.first_name like '%Sergio%'

--select * from applicants

--select * from CountryCode where name like 'United Kingdom%'