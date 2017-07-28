
with 
tmp1 as (select id
, concat(
(case when (phone1 = '' OR phone1 is NULL) THEN '' ELSE concat(phone1, ',') END),
(case when (phone2 = '' OR phone2 is NULL) THEN '' ELSE concat(phone2, ',') END),
(case when (phone3 = '' OR phone2 is NULL) THEN '' ELSE concat(phone3, ',') END)) as PrimaryPhone
from hiring_manager)

, tmp2 as
(select id
, concat(
(case when (email = '' OR email is NULL) THEN '' ELSE concat(email,',') END),
(case when (email2 = '' OR email2 is NULL) THEN '' ELSE concat(email2,',') END),
(case when (email3 = '' OR email3 is NULL) THEN '' ELSE concat(email3,',') END)) as PrimaryMail
from hiring_manager)

, tmp3 as
(select id
, concat(
	(case when (mgr_id = '' OR mgr_id is NULL) THEN '' ELSE concat('Skills: ',ms.parent_skill,' - ',ms.child_skill,char(10)) END),
	(case when (cast(create_date as varchar(max)) = '' OR create_date is NULL) THEN '' ELSE concat('Created on: ',convert(varchar(10),create_date,120),char(10)) END),
	(case when (division = '' OR division is NULL) THEN '' ELSE concat('Division: ',division,char(10)) END),
	(case when (cost_center = '' OR cost_center is NULL) THEN '' ELSE concat('Cost Center: ',cost_center,char(10)) END),
	(case when (department = '' OR department is NULL) THEN '' ELSE concat('Department: ',department,char(10)) END),
	(case when (headline = '' OR headline is NULL) THEN '' ELSE concat('Headline: ',headline) END)
) as ContactNote
from hiring_manager hm
left join mgr_skills ms on hm.id = ms.mgr_id
)

/* , tmp4 as (select id
, CC.Code
from hiring_manager hm
left join CountryCode CC on hm.country = CC.Name) */

, tmp5 as (select
a.mgr_id
, b.user_email
, concat(b.first_name,b.last_name) as ownerName
from (select mgr_id, max(user_id) as user_id from mgr_rep group by mgr_id) a
left join users b on a.user_id = b.ID)

, tag as (SELECT mgr_id,STUFF((SELECT char(10)  + 'Date: ' + convert(varchar(10), actdate, 120) + ' ' + convert(varchar(10), acttime, 120) 
	+ char(10) + 'Author: ' + case when ( c.first_name is null) then '(no author)' else concat(c.first_name,' ',c.last_name) end + char(10) 
	+ '- ' + b.act_name + ': ' + cast(actnotes as varchar(max)) + char(10)
	from cont_activity 
	left join app_activity_type b on acttype = b.id  
	left join users c on act_auth = c.ID
	WHERE mgr_id = a.mgr_id order by actdate desc FOR XML PATH ('')), 1, 1, '')  AS tag 
FROM  cont_activity AS a GROUP BY a.mgr_id)

select --top 50
hm.comp_id as 'contact-companyId'
, hm.branch_id
, hm.id as 'contact-externalId'
, hm.first_name as 'contact-firstName'
, hm.last_name as 'contact-lastName'
, tmp5.user_email as 'contact-owners'
, tmp5.ownerName as ownerName
, hm.title as 'contact-jobTitle'
, substring(tmp1.PrimaryPhone,1,iif((len(tmp1.PrimaryPhone)-1) < 0,0,len(tmp1.PrimaryPhone)-1)) as 'contact-phone'
, substring(tmp2.PrimaryMail,1,iif((len(tmp2.PrimaryMail)-1) < 0,0,len(tmp2.PrimaryMail)-1)) as 'contact-email'
, hm.linkedin as 'contact-linkedinURL'
, tmp3.ContactNote as 'contact-Note'
--, replace(ca.comments,'&#x0D;','') as 'contact-comments'
--, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
--	concat(case when (tag.tag is null) then '' else concat('Activities: ',char(10),tag.tag,char(10)) end,case when (ca.comments is null) then '' else concat('Comments: ',char(10),ca.comments,char(10)) end)
--	,'&amp;',''),'nbsp;',''),'#39;',''),'&#x0D;',''),'&lt;',''),'&gt;',''),'%20',''),'%28','('),'%29',')'),'%2B','+') as 'candidate-comments'

, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
	case when (tag.tag is null) then '' else concat('Activities: ',char(10),tag.tag,char(10)) end
	,'&amp;',''),'nbsp;',''),'#39;',''),'&#x0D;',''),'&lt;',''),'&gt;',''),'%20',''),'%28','('),'%29',')'),'%2B','+') as 'contact-comment'

from hiring_manager hm
left join tmp1 on hm.id = tmp1.id
left join tmp2 on hm.id = tmp2.id
left join tmp3 on hm.id = tmp3.id
left join tmp5 on hm.id = tmp5.mgr_id
left join tag on hm.id = tag.mgr_id
--where tmp3.ContactNote like '%skill%'

--where hm.id = 2359788
/* 

select * from CountryCode where Code = 'GB'

select * from hiring_manager

select * from mgr_rep

select * from users
 */