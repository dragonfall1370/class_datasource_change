with 
--loc as (
--		select cand_ref, iso_2, ltrim(Stuff(
--			  Coalesce(' ' + NULLIF(cand_address, ''), '')
--			+ Coalesce(', ' + NULLIF(cand_address2, ''), '')
--			+ Coalesce(', ' + NULLIF(cand_town, ''), '')
--			+ Coalesce(', ' + NULLIF(cand_county, ''), '')
--			+ Coalesce(', ' + NULLIF(cand_pcode, ''), '')
--			+ Coalesce(', ' + NULLIF(country_name, ''), '')
--			, 1, 1, '')) as 'locationName'
--	from candidate can left join country c on can.cand_country = c.country_ref)

-- loc2 as (
--select c.cand_ref, l.description--, cl.link_ref, cl.loc_ref
--from candidate c left join candlocation cl on c.cand_ref = cl.cand_ref
--left join location l on cl.loc_ref = l.loc_ref
--where cl.loc_ref <> 0)

--, additionalLoc as (SELECT cand_ref,
--     STUFF(
--         (SELECT ', ' + description
--          from  loc2
--          WHERE cand_ref = l.cand_ref
--    order by cand_ref asc
--          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
--          , 1, 1, '')  AS loc2
--FROM loc2 as l
--GROUP BY l.cand_ref)

--select * from additionalLoc

------------------------------------------------Get documents
--, temp as (select cand_ref,  coalesce(cand_ref + '_' + doc_ref + '.' + doc_ext,'') as doc
--from canddocs 
--where left(cand_ref,1) in ('1','2','3','4','5','6','7','8','9','0')
--	and left(doc_ref,1) in ('1','2','3','4','5','6','7','8','9','0'))

--, candocuments as (SELECT cand_ref,
--     STUFF(
--         (SELECT ',' + doc
--          from  temp
--          WHERE cand_ref = cd.cand_ref
--    order by cand_ref asc
--          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
--          , 1, 1, '')  AS canddocs
--FROM temp as cd
--GROUP BY cd.cand_ref)
--select * from candocuments

-----------Job Title 1
--, temp_role as (
--select cr.link_ref, cr.cand_ref, r.description
--from candrole cr left join role r on cr.role_ref = r.role_ref)

--, cand_jobtitle as (SELECT cand_ref, 
--     STUFF(
--         (SELECT ', ' + description
--          from  temp_role
--          WHERE cand_ref =tr.cand_ref
--    order by cand_ref asc
--          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
--          ,1,2,'')  AS canjobtitle
--FROM temp_role as tr
--GROUP BY tr.cand_ref)

-------------------------Skill
-- skill1 as (
--select c.cand_ref, s.description
--from candidate c left join candskill cs on c.cand_ref = cs.cand_ref
--left join skill s on cs.skill_ref = s.skill_ref
--where cs.skill_ref <> 0)

--, candidateskill as (SELECT cand_ref,
--     STUFF(
--         (SELECT ', ' + description
--          from  skill1
--          WHERE cand_ref = s.cand_ref
--    order by cand_ref asc
--          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
--          , 1, 2, '')  AS skill
--FROM skill1 as s
--GROUP BY s.cand_ref)

--CANDIDATE DUPLICATE MAIL REGCONITION
--check email format
EmailDupRegconition as (
SELECT cand_ref,cand_email Email,ROW_NUMBER() OVER(PARTITION BY cand_email ORDER BY cand_ref ASC) AS rn 
from candidate
where cand_email like '%_@_%.__%')

--edit duplicating emails
, CandidateEmail as (select cand_ref, 
case 
when rn=1 then Email
else concat('DUP',rn,'-',Email)
end as CandidateEmail
, rn
from EmailDupRegconition)
--select * from CandidateEmail where rn >1

-----------------Owners
, owners as (select consult_ref, consult_name
	, case consult_name 
		when 'Support' then 'zed@rebelrecruiters.co.uk'
		when 'Azar Hussain' then 'azar@rebelrecruiters.co.uk'
		when 'Mica Bell' then 'mica@rebelrecruiters.co.uk'
		when 'Faisal Faik' then 'fess@rebelrecruiters.co.uk'
		when 'Yas Mahtab' then 'yas@rebelrecruiters.co.uk'
		when 'Loukia Poutziouris' then 'loukia@rebelrecruiters.co.uk'
		when 'Hamzah Ikram' then 'hamzah@rebelrecruiters.co.uk'
		when 'Julija Lipnickaja' then 'julija@rebelrecruiters.co.uk'
		when 'Ben Williamson' then 'ben@rebelrecruiters.co.uk'
		when 'Hayley McGowan' then 'hayley@rebelrecruiters.co.uk'
		else '' end as consult_email
from consultant where consult_inits <> '')

-------------------------------------------------------------MAIN SCRIPT
select concat('REBEL', c.cand_ref) as 'candidate-externalId'
, iif(cand_fname = '' or cand_fname = 'null',concat('NoFirstName-ID',c.cand_ref), rtrim(ltrim(c.cand_fname))) as 'candidate-firstName'
, rtrim(ltrim(c.cand_mname)) as 'candidate-middleName'
, iif(cand_sname = '' or cand_sname = 'null',concat('NoLastName-ID',c.cand_ref), rtrim(ltrim(c.cand_sname))) as 'candidate-Lastname'
, rtrim(ltrim(loc.locationName)) as 'candidate-address'
, cand_town as 'candidate-city'
, cand_county as 'candidate-state'
, cand_pcode as 'candidate-zipCode'
, loc.iso_2 as 'candidate-country'
, como.consult_email as 'candidate-owners'
--, c.Titre as 'candidate-jobTitle1'
--, c.CurrentEmployer as 'candidate-Employer1'
, replace(coalesce(nullif(c.cand_mobnum,''), nullif(c.cand_homenum,''), nullif(c.cand_worknum,'')),':','') as 'candidate-phone'
, c.cand_homenum as 'candidate-homePhone'
, c.cand_worknum as 'candidate-workPhone'
, c.cand_mobnum as 'candidate-mobile'
--, iif(c.SocialMedia like '%linkedin%',c.SocialMedia,'') as 'candidate-linkedin'
--, replace(c.CurrentResumeFileName,'?','') as 'candidate-resume' 
, case
	when c.cand_email is not NULL and ce.cand_ref is not null then ce.CandidateEmail
	else concat('CandidateID-',c.cand_ref,'@noemail.com') end as 'candidate-email'
, cd.canddocs as 'candidate-resume'
, iif(canjobtitle = '' or canjobtitle is null,'',left(cj.canjobtitle,300)) as 'candidate-jobTitle1'
, iif(cs.skill = '' or cs.skill is null,'',cs.skill) as 'candidate-skills'
, iif(c.cand_min_salary = '0','',convert(float,c.cand_min_salary)) as 'candidate-desiredSalary'
, iif(c.cand_min_daily = '0','',convert(float, c.cand_min_daily)) as 'candidate-contractRate'
--, c.SecondaryEmail as 'candidate-workEmail'
, left(concat('Candidate External ID: REBEL',c.cand_ref, char(10)
	, iif(CHARINDEX(':',cand_regdate)<>0,concat(char(10), 'Registration Date: ', substring(cand_regdate,6,2),'/',substring(cand_regdate,9,2),'/',left(cand_regdate,4),char(10)),concat(char(10), 'Registration Date: ',cand_regdate,char(10)))
	, iif(cj.canjobtitle = '' or cj.canjobtitle is null,'',concat(char(10),'Roles: ',cj.canjobtitle,char(10)))
	--, iif(aloc.loc2 = '' or aloc.loc2 is null,'',concat(char(10),'Location: ',aloc.loc2,char(10)))
	, iif(c.cand_min_salary = '0','',concat(char(10),'Min Salary: ',convert(float,c.cand_min_salary),char(10)))
	, iif(c.cand_min_daily = '0','',concat(char(10),'Min Daily: ',convert(float, c.cand_min_daily),char(10)))
	, concat(char(10),'Will Perm? ',replace(replace(c.cand_will_perm,'N','No'),'Y','Yes'),char(10))
	, concat(char(10),'Will Contract? ',replace(replace(c.cand_will_contract,'N','No'),'Y','Yes'),char(10))
	, concat(char(10),'Will Temp? ',replace(replace(c.cand_will_temp,'N','No'),'Y','Yes'),char(10))
	, iif(c.cand_notes = '' or c.cand_notes = 'null','',Concat(char(10),'Notes: ',char(10),c.cand_notes))),32000) as 'candidate-note'
from candidate c left join CandidateEmail ce on c.cand_ref = ce.cand_ref
				 left join tempcandlocation loc on c.cand_ref = loc.cand_ref
				 left join owners como on c.cand_consult = como.consult_ref
				 --left join additionalLoc aloc on c.cand_ref = aloc.cand_ref
				 left join candocuments cd on c.cand_ref = cd.cand_ref
				 left join cand_jobtitle cj on c.cand_ref = cj.cand_ref
				 left join candidateskill cs on c.cand_ref = cs.cand_ref 
--where c.cand_ref = 29858
				 
