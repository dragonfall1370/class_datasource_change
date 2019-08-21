--Contact location: will be added to note
with loc as (
		select cont_ref, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(cont_address, ''), '')
			+ Coalesce(', ' + NULLIF(cont_address2, ''), '')
			+ Coalesce(', ' + NULLIF(cont_town, ''), '')
			+ Coalesce(', ' + NULLIF(cont_county, ''), '')
			+ Coalesce(', ' + NULLIF(cont_pcode, ''), '')
			+ Coalesce(', ' + NULLIF('United Kingdom (Great Britain)', ''), '')
			, 1, 1, '')) as 'locationName'
	from contact)

, loc2 as (
select c.cont_ref, l.description
from contact c left join contactlocation cl on c.cont_ref = cl.cont_ref
left join location l on cl.loc_ref = l.loc_ref
where cl.loc_ref <> 0)

----------Contact Email
--check email format
, EmailDupRegconition as (SELECT cont_ref,cont_email Email,
 ROW_NUMBER() OVER(PARTITION BY cont_email ORDER BY cont_ref ASC) AS rn 
from contact
where cont_email like '%_@_%.__%')

--edit duplicating emails
, ContactEmail as (select cont_ref, 
case 
when rn=1 then Email
else concat(rn,'_',(Email))
end as Email
from EmailDupRegconition)
--select * from ContactEmail where email like '%,%'

, skill1 as (
select c.cont_ref, s.description
from contact c left join contactskill cs on c.cont_ref = cs.cont_ref
left join skill s on cs.skill_ref = s.skill_ref
where cs.skill_ref <> 0)

, contact_skill as (SELECT cont_ref,
     STUFF(
         (SELECT ', ' + description
          from  skill1
          WHERE cont_ref = s.cont_ref
    order by cont_ref asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS skill
FROM skill1 as s
GROUP BY s.cont_ref)

-------------------------Get Document names
, tempdocs as (select cont_ref,  coalesce(cont_ref + '_' + doc_ref + '.' + doc_ext,'') as doc
from contdocs)

, contact_docs as (SELECT cont_ref,
     STUFF(
         (SELECT ', ' + doc
          from  tempdocs
          WHERE cont_ref = td.cont_ref
    order by cont_ref asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS contdocs
FROM tempdocs as td
GROUP BY td.cont_ref)
--select * from contact_docs

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

---------MAIN SCRIPT
--, main as (
select 
iif(cc.client_ref = '' or cc.client_ref is NULL,'REBEL9999999',concat('REBEL',cc.client_ref)) as 'contact-companyId'
, cc.client_ref as '(OriginalCompanyID)'
, c.client_name as '(OriginalCompanyName)'
, concat('REBEL',cc.cont_ref) as 'contact-externalId'
, iif(cc.cont_fname = '' or cc.cont_fname is NULL,concat('NoFirstname-', cc.cont_ref),cc.cont_fname) as 'contact-firstName'
, iif(cc.cont_sname = '' or cc.cont_sname is NULL,concat('NoLastName-', cc.cont_ref),cc.cont_sname) as 'contact-lastName'
, ce.Email as 'contact-email'
, cont_worknum as 'contact-phone'
, cont_role_title as 'contact-jobTitle'
, como.consult_email as 'contact-owners'
, iif(cc.cont_linkedin like '%linkedin%',cont_linkedin,'') as 'contact-linkedin'
, contdocs as 'contact-document'
, left(	
	concat('Contact External ID: REBEL',cc.cont_ref,char(10)
	, iif(c.client_name = '','',concat(char(10),'Company: ',c.client_name,char(10)))
	--, iif(cc.CompanyID = '','',concat(char(10),'Company ID: ',cc.CompanyID,char(10)))
	, iif(cont_mobnum = '' or cont_mobnum is NULL,'',concat(char(10),'Mobile Phone: ',replace(cont_mobnum,' ',''),char(10)))
	, iif(loc.locationName is NULL,'',concat(char(10),'Address 1: ',replace(replace(loc.locationName,',,',','),', ,',','),char(10)))
	, iif(loc2.description = '' or loc2.description is NULL,'',concat(char(10),'Address 2: ',loc2.description,char(10)))
	, iif(s.skill = '' or s.skill is null,'',concat(char(10),'Skills: ',s.skill,char(10)))
	, iif(cc.cont_notes = '' or cc.cont_notes is NULL,'',concat(char(10),'Notes: ',char(10),cc.cont_notes))),32000) 
	as 'contact-note'
from contact cc
	left join client c on cc.client_ref = c.client_ref
	left join ContactEmail ce on cc.cont_ref = ce.cont_ref
	left join loc on cc.cont_ref = loc.cont_ref
	left join loc2 on cc.cont_ref = loc2.cont_ref
	left join owners como on cc.consult_ref = como.consult_ref
	left join contact_skill s on cc.cont_ref = s.cont_ref
	left join contact_docs cd on cc.cont_ref = cd.cont_ref
--where cc.cont_ref = 777--email2 <> ''--cc.id = 30427055
UNION ALL
select 'REBEL9999999','','','REBEL9999999','Default','Contact','','','','','','','This is default contact from Data Import'
--)
--select * from main where [contact-phone] is not null