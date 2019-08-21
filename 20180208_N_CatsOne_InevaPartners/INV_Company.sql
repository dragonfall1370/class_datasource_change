---DUPLICATION REGCONITION
with loc as (
	select ID, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(Adresse, ''), '')
			+ Coalesce(', ' + NULLIF(Ville, ''), '')
			+ Coalesce(', ' + NULLIF(ZipCodeDistance, ''), '')
			+ Coalesce(', ' + NULLIF(Etat, ''), '')
			+ Coalesce(', ' + NULLIF(Pays, ''), '')
			, 1, 1, '')) as 'locationName'
	from company)

, dup as (SELECT ID, Nom, ROW_NUMBER() OVER(PARTITION BY Nom ORDER BY ID ASC) AS rn 
FROM company)

, compOwner as (
select cc.ID
, case 
	when cc.Titulaire like 'Magdalena M.' then 'magdalena@ineva-partners.com'
	when cc.Titulaire like 'Hamza B.' then 'hamza@ineva-partners.com'
	when cc.Titulaire like 'Charlotte B.' then 'charlotte@ienva-partners.com'
	when cc.Titulaire like 'Aymard d.' then 'aymard@ineva-partners.com'
	when cc.Titulaire like 'Samar S.' then 'samar@ineva-partners.com'
	else '' end as compowner
FROM company cc)

, tempcompContOwner as (
select cc.CompanyID as ID
, case 
	when cc.CompanyOwner like 'Magdalena M.' then 'magdalena@ineva-partners.com'
	when cc.CompanyOwner like 'Hamza B.' then 'hamza@ineva-partners.com'
	when cc.CompanyOwner like 'Charlotte B.' then 'charlotte@ienva-partners.com'
	when cc.CompanyOwner like 'Aymard d.' then 'aymard@ineva-partners.com'
	when cc.CompanyOwner like 'Samar S.' then 'samar@ineva-partners.com'
	else '' end as comcontowner
from contact cc where CompanyID <> '' and CompanyOwner <> '')

, compContOwner as (select distinct ID,comcontowner from tempcompContOwner)


----select * from dup
---Main Script---
select
  concat('INV',c.ID) as 'company-externalId'
, C.Nom as '(OriginalName)'
, iif(C.ID in (select ID from dup where dup.rn > 1)
	, iif(dup.Nom = '' or dup.Nom is NULL,concat('No Company Name - ',dup.ID),concat(dup.Nom,' - ',dup.rn))
	, iif(C.Nom = '' or C.Nom is null,concat('No Company Name - ',C.ID),C.Nom)) as 'company-name'
, iif(c.Website = '' or c.Website not like '%_.__%','',left(c.Website,99)) as 'company-website'
, case 
	when c.Titulaire like 'Magdalena M.' then 'magdalena@ineva-partners.com'
	when c.Titulaire like 'Hamza B.' then 'hamza@ineva-partners.com'
	when c.Titulaire like 'Charlotte B.' then 'charlotte@ienva-partners.com'
	when c.Titulaire like 'Aymard d.' then 'aymard@ineva-partners.com'
	when c.Titulaire like 'Samar S.' then 'samar@ineva-partners.com'
	else '' end as 'company-ownerstest'
, ltrim(Stuff(
			  Coalesce(' ' + NULLIF(compowner, ''), '')
			+ Coalesce(',' + NULLIF(comcontowner, compowner), '')
			, 1, 1, '')) as 'company-owners'
, iif(loc.locationName = '' or loc.locationName is NULL,'',ltrim(loc.locationName)) as 'company-locationName'
, iif(loc.locationName = '' or loc.locationName is NULL,'',ltrim(loc.locationName)) as 'company-locationAddress'
, iif(c.Ville <> '' and c.Ville <> '75008',c.Ville,iif(loc.locationName like '%Paris%','Paris','')) as 'company-locationCity'
, iif(loc.locationName like '%United State%','US',iif(loc.locationName like '%Belgique%','BE','FR')) as 'company-locationCountry'
, iif(Telephone = '' or Telephone is NULL,'',Telephone) as 'company-phone'
, c.Fax as 'company-fax'
--, iif(op.switchboard = '' or op.switchboard is NULL,'',concat(op.switchboard,' (Office)')) as 'company-switchBoard'
, left(Concat(
			'Company External ID: INV', C.ID,char(10)
			, iif(c.Emplacement = '' or c.Emplacement is NULL,'',Concat(char(10), 'Emplacement: ', c.Emplacement, char(10)))
			, iif(c.C = '' or c.C is NULL,'',Concat(char(10), 'Nombre de Contacts (C): ', c.C, char(10)))
			, iif(c.J = '' or c.J is NULL,'',Concat(char(10), 'Nombre de mission (J): ', c.J, char(10)))
			, iif(ContactDeFacturation = '','',Concat(char(10), 'Contact de facturation (Billing Contact): ', ContactDeFacturation, char(10)))
			, iif(c.Cree = '','',concat(char(10),'Créé (Created Date): ',c.Cree,char(10)))
			, iif(c.Updated = '','',concat(char(10),'Updated Date: ',c.Updated,char(10)))
			, iif(c.Statut = '','',concat(char(10),'Statut: ',c.Statut,char(10)))
			, iif(c.QuickNotes = '','',concat(char(10),'Quick Notes: ',c.QuickNotes,char(10)))
			, iif(c.SocialMedia = '','',concat(char(10),'Social Media: ',c.SocialMedia,char(10)))
			, iif(C.Notes = '' or C.Notes is NULL,'',Concat(char(10),'Notes: ',char(10),C.Notes))),32000)
			as 'company-note'
FROM company as c
			left join dup on c.ID = dup.ID
			left join loc on c.ID = loc.ID
			left join compOwner co on c.ID = co.ID
			left join compContOwner como on c.ID = como.ID
--			where dup.rn>1
UNION ALL
select 'INV9999999','','Default Company','','','','','','','','','','This is Default Company from Data Import'

