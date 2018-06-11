--Contact location: will be added to note
with Skills as (
	select ID, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(Technology1, ''), '')
			+ Coalesce(', ' + NULLIF(Technology2, ''), '')
			+ Coalesce(', ' + NULLIF(Technology3, ''), '')
			+ Coalesce(', ' + NULLIF(Technology4, ''), '')
			, 1, 1, '')) as 'skills'
	from company
	where contact <> ''
	)
, tempEmail1 as (
select ID, replace(role,' ','') as Email
from company where role like '%_@_%.__%' and contact <> '')

, tempEmail2 as (
select ID, replace(replace(Email,' ',''),'/',',') as email2
from company where Email like '%_@_%.__%' and contact <> '')

----------Contact Email
--, TempPrimaryEmail 
, ContactEmail as (select * from tempEmail1 union all select * from tempEmail2)
	--select ID, iif(Courriel = '' and Notes ='','',iif(Notes= '',Courriel,iif(Courriel= '',Notes,concat(Courriel,',',Notes)))) as Email
--from company)
--select * from TempPrimaryEmail
--check email format
--, EmailDupRegconition as (SELECT ID,Email,
-- ROW_NUMBER() OVER(PARTITION BY Email ORDER BY ID ASC) AS rn 
--from TempPrimaryEmail)-- no dup email so dont need this 
--select * from EmailDupRegconition
----edit duplicating emails
--, ContactEmail as (select ID, 
--case 
--when rn=1 then Email
--else concat(rn,'_',(Email))
--end as Email
--from EmailDupRegconition)
--select * from ContactEmail where email like '%,%'

---------MAIN SCRIPT
--, main as (
select 
--iif(cc.CompanyID = '' or cc.CompanyID is NULL,'INV9999999',concat('INV',cc.CompanyID)) as 'contact-companyId'
cc.ID as 'contact-companyId'
, cc.Companyname as '(OriginalCompanyName)'
, cc.ID as 'contact-externalId'
--, iif(cc.Prenom = '' or cc.Prenom is NULL,concat('NoFirstname-', cc.ID),cc.Prenom) as 'contact-firstName'
, cc.contact as 'contact-lastName'
, ce.Email as 'contact-email'
, replace(replace(Phone,'.',''),'/',',') as 'contact-phone'
, iif(cc.role = '' or cc.role like '%@%','',cc.role) as 'contact-jobTitle'
, left(
	concat('Contact External ID: ',cc.ID,char(10)
	, iif(Companyname = '','',Concat(char(10), 'Company: ', Companyname, char(10)))
	, iif(IND = '' or IND is NULL,'',Concat(char(10), 'IND: ', IND, char(10)))
			, iif(Industry = '','',Concat(char(10), 'Industry: ', Industry, char(10)))
			, iif(skills = '' or skills is null,'',Concat(char(10), 'Skills: ', skills, char(10)))
			, iif(cc.Gebeld = '','',Concat(char(10), 'Gebeld: ', cc.Gebeld, char(10)))
			, iif(cc.Laatstecontact = '','',concat(char(10),'Laatste contact: ',cc.Laatstecontact,char(10)))
			, iif(cc.Opgenomen = '','',Concat(char(10), 'Opgenomen: ', cc.Opgenomen, char(10)))
			, iif(Gemaild = '','',Concat(char(10), 'Gemaild: ', Gemaild, char(10)))
			, iif(cc.Opmerking = '','',concat(char(10),'Opmerking: ',char(10),cc.Opmerking,char(10)))),32000)
	as 'contact-note'
from company cc
	left join ContactEmail ce on cc.ID = ce.ID
	left join Skills s on cc.ID = s.ID
where contact <> ''
--UNION ALL
--select 'INV9999999','','','INV9999999','Default','Contact','','','','','','This is default contact from Data Import'
--)
--select * from main where [contact-phone] is not null