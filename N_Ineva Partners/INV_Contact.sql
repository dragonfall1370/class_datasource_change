--Contact location: will be added to note
with loc as (
	select ID, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(Adresse, ''), '')
			+ Coalesce(', ' + NULLIF(Ville, ''), '')
			+ Coalesce(', ' + NULLIF(ZipCodeDistance, ''), '')
			+ Coalesce(', ' + NULLIF(Etat, ''), '')
			, 1, 1, '')) as 'locationName'
	from contact)

----------Contact Email
, TempPrimaryEmail as (select ID
		, iif(Courriel = '' and Email2 ='','',iif(Email2= '',Courriel,iif(Courriel= '',Email2,concat(Courriel,',',Email2)))) as Email
from contact)

--check email format
, EmailDupRegconition as (SELECT ID,Email,
 ROW_NUMBER() OVER(PARTITION BY Email ORDER BY ID ASC) AS rn 
from TempPrimaryEmail-- where Courriel <> ''
where Email like '%_@_%.__%')

--edit duplicating emails
, ContactEmail as (select ID, 
case 
when rn=1 then Email
else concat(rn,'_',(Email))
end as Email
from EmailDupRegconition)
--select * from ContactEmail where email like '%,%'

, contactOwner as (
select cc.ID
, case 
	when cc.Titulaire like 'Magdalena M.' then 'magdalena@ineva-partners.com'
	when cc.Titulaire like 'Hamza B.' then 'hamza@ineva-partners.com'
	when cc.Titulaire like 'Charlotte B.' then 'charlotte@ienva-partners.com'
	when cc.Titulaire like 'Aymard d.' then 'aymard@ineva-partners.com'
	when cc.Titulaire like 'Samar S.' then 'samar@ineva-partners.com'
	else '' end as contowner
from contact cc)

, companyOwner as (
select cc.ID
, case 
	when cc.CompanyOwner like 'Magdalena M.' then 'magdalena@ineva-partners.com'
	when cc.CompanyOwner like 'Hamza B.' then 'hamza@ineva-partners.com'
	when cc.CompanyOwner like 'Charlotte B.' then 'charlotte@ienva-partners.com'
	when cc.CompanyOwner like 'Aymard d.' then 'aymard@ineva-partners.com'
	when cc.CompanyOwner like 'Samar S.' then 'samar@ineva-partners.com'
	else '' end as comowner
from contact cc)


---------MAIN SCRIPT
--, main as (
select 
iif(cc.CompanyID = '' or cc.CompanyID is NULL,'INV9999999',concat('INV',cc.CompanyID)) as 'contact-companyId'
, cc.CompanyID as '(OriginalCompanyID)'
, cc.Entreprise as '(OriginalCompanyName)'
, concat('INV',cc.ID) as 'contact-externalId'
, iif(cc.Prenom = '' or cc.Prenom is NULL,concat('NoFirstname-', cc.ID),cc.Prenom) as 'contact-firstName'
, iif(cc.Nom = '' or cc.Nom is NULL,concat('NoLastName-', cc.ID),cc.Nom) as 'contact-lastName'
, ce.Email as 'contact-email'
, CONCAT(iif(cc.Telephone = '','',cc.Telephone)
		,iif(cc.WorkPhone = '','',iif(RIGHT(cc.Telephone,LEN(cc.Telephone)-2) = cc.WorkPhone or RIGHT(cc.Telephone,LEN(cc.Telephone)-3) = cc.WorkPhone,'',CONCAT(',',cc.WorkPhone)))
		,iif(cc.AutreTelephone = '','',CONCAT(',',cc.AutreTelephone))) as 'contact-phone'
--, case 
--	when cc.Titulaire like 'Magdalena M.' then 'magdalena@ineva-partners.com'
--	when cc.Titulaire like 'Hamza B.' then 'hamza@ineva-partners.com'
--	when cc.Titulaire like 'Charlotte B.' then 'charlotte@ienva-partners.com'
--	when cc.Titulaire like 'Aymard d.' then 'aymard@ineva-partners.com'
--	when cc.Titulaire like 'Samar S.' then 'samar@ineva-partners.com'
--	else '' end as 'contact-owners'
, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(contowner, ''), '')
			+ Coalesce(',' + NULLIF(iif(comowner='',contowner,comowner), contowner), '')
			, 1, 1, '')) as 'contact-owners'
--, coalesce(replace(cp.contactphone,'/ ',','),replace(offp.officephone,'/ ',',')) as 'contact-phone'
--, cp.contactphone as 'contact-phone'
--, cs.Num as 'contact-skype'
, cc.SocialMedia as 'contact-linkedin'
, cc.Titre as 'contact-jobTitle'
, left(
	concat('Contact External ID: INV',cc.ID,char(10)
	, iif(cc.Entreprise = '','',concat(char(10),'Entreprise (Company): ',cc.Entreprise,char(10)))
	, iif(cc.CompanyID = '','',concat(char(10),'Company ID: ',cc.CompanyID,char(10)))
	, iif(cc.CompanyOwner = '','',concat(char(10),'Company Owner: ',cc.CompanyOwner,char(10)))
	, iif(cc.NombreDemployes = '' or cc.NombreDemployes is NULL,'',concat(char(10),'Nombre d''employés: ',cc.NombreDemployes,char(10)))
	, iif(cc.WorkPhone = '' or cc.WorkPhone is NULL,'',concat(char(10),'Work Phone(s): ',cc.WorkPhone,char(10)))
	, iif(Cellulaire = '' or Cellulaire is NULL,'',concat(char(10),'Cellulaire (Mobile Phone): ',Cellulaire,char(10)))
	, iif(SecteurDactivite = '' or SecteurDactivite is NULL,'',concat(char(10),'Secteur d''activité (Functional Expertise): ',SecteurDactivite,char(10)))
	, iif(loc.locationName is NULL,'',concat(char(10),'Contact Location: ',loc.locationName,char(10)))
	, iif(cc.Emplacement = '','',concat(char(10),'Emplacement: ',cc.Emplacement,char(10)))
	, iif(cc.CompanyZipCodeDistance = '','',concat(char(10),'Company Zip Code (Distance): ',cc.CompanyZipCodeDistance,char(10)))
	, iif(cc.Cree = '','',concat(char(10),'Créé (Created Date): ',cc.Cree,char(10)))
	, iif(cc.Updated = '','',concat(char(10),'Updated Date: ',cc.Updated,char(10)))
	, iif(cc.LastActivity = '','',concat(char(10),'Last Activity: ',cc.LastActivity,char(10)))
	, iif(ltrim(cc.Statut)
	 = '','',concat(char(10),'Statut: ',cc.Statut,char(10)))
	--, coalesce(char(10) + 'Contact Other Notes: ' + ps.Notes, '')),32000) as 'contact-note'
	, iif(cc.Notes = '' or cc.Notes is NULL,'',concat(char(10),'Notes: ',char(10),cc.Notes))),32000) 
	as 'contact-note'
from contact cc
	left join company c on cc.CompanyID = c.ID
	left join ContactEmail ce on cc.ID = ce.ID
	left join loc on cc.ID = loc.ID
	left join contactOwner co on cc.ID = co.ID
	left join companyOwner como on cc.ID = como.ID
--where email2 <> ''--cc.id = 30427055
UNION ALL
select 'INV9999999','','','INV9999999','Default','Contact','','','','','','This is default contact from Data Import'
--)
--select * from main where [contact-phone] is not null