with tempContacts as (select c.ID as contactID, c.Nom, c.Prenom, concat(c.Prenom,' ',c.Nom) as contactName, com.ID as companyID, com.Nom as companyName
from contact c left join company com on c.companyId = com.id
where c.CompanyID <> ''
)
--select * from tempContacts

, jobContact as(select e.ID ,tc.contactID as contactID, tc.contactName, tc.companyID, e.Entreprise, tc.companyName
from job e left join tempContacts tc on e.Contact = tc.contactName and e.Entreprise = tc.companyName
where Contact <> '' and tc.contactID is not null)

--select * from jobContact order by id

, ContactMaxID as (select 
case when CompanyID = '' then '9999999'
else CompanyID end as CompanyID
, max(ID) as ContactMaxID 
from contact --where ID not in (select ContactID from jobContact)
group by CompanyID)

, tempJobCompany as (select e.ID, Entreprise, et.ID as companyID, et.Nom, cm.ContactMaxID
from job e left join company et on e.Entreprise = et.Nom
				left join ContactMaxID cm on et.ID = cm.CompanyID
where e.ID not in (select ID from jobContact))

, jobCompany as (select * from tempJobCompany where ContactMaxID is not null)
--select * from jobCompany
, temp_defaultCompanyContact as (select distinct companyID, Nom from tempJobCompany where ContactMaxID is null and companyID is not null)
--select * from temp_defaultCompanyContact
, defaultCompanyContact as (
select concat('INV',CompanyID) as'contact-companyId'
	, companyID
	, Nom as 'companyName'
	, concat('INVDefaultCon_',CompanyID) as 'contact-externalId'
	, concat('INVDefaultCon_',CompanyID) as 'contactExternalId'
	, concat(Nom,' - Default Contact') as 'contact-LastName'
from temp_defaultCompanyContact)
--select * from defaultCompanyContact

, jobDefaultContact as (select jc.ID, dcc.contactExternalId
from defaultCompanyContact dcc left join tempJobCompany jc on dcc.companyID = jc.companyID)

, loc as (
	select ID, ltrim(Stuff(
			Coalesce(' ' + NULLIF(Ville, ''), '')
			+ Coalesce(', ' + NULLIF(ZipCodeDistance, ''), '')
			+ Coalesce(', ' + NULLIF(Etat, ''), '')
			--+ Coalesce(', ' + NULLIF(Pays, ''), '')--same data with etat so wont get it to location
			, 1, 1, '')) as 'locationName'
	from job)

--DUPLICATION REGCONITION
, dup as (SELECT ID, Titre, ROW_NUMBER() OVER(PARTITION BY Titre ORDER BY ID ASC) AS rn 
from job)

--MAIN SCRIPT
select 
--case 
--	when (j.ClientcontactId = '' or j.ClientContactId is NULL) and j.ClientId in (select CompanyID from ContactMaxID) then concat('MP',CM.ContactMaxID)
--	when (j.ClientcontactId = '' or j.ClientcontactId is NULL) and j.ClientId not in (select CompanyID from ContactMaxID) then 'MP9999999'
--	when j.ClientcontactId is NULL and j.ClientId is NULL then 'MP9999999'
--	else concat('MP',j.ClientContactId) end as 'position-contactId'
--, j.ClientId as 'CompanyID'
--, j.ClientContactId as 'ContactID'
iif(jcon.contactID is not null,concat('INV',jcon.contactID),iif(jcom.ContactMaxID is not null, concat('INV',jcom.ContactMaxID),iif(jdcon.contactExternalId is not null,jdcon.contactExternalId,'INV9999999'))) as 'position-contactId'
, concat('INV',j.ID) as 'position-externalId'
, j.Titre as 'position-title(old)'
, iif(j.ID in (select ID from dup where dup.rn > 1)
	, iif(dup.Titre = '' or dup.Titre is NULL,concat('No job title-',dup.ID),concat(dup.Titre,'-',dup.ID))
	, iif(j.Titre = '' or j.Titre is null,concat('No job title -',j.ID),j.Titre)) as 'position-title'
, j.Description as 'position-publicDescription'
, case 
	when j.Titulaire like 'Magdalena M.' then 'magdalena@ineva-partners.com'
	when j.Titulaire like 'Hamza B.' then 'hamza@ineva-partners.com'
	when j.Titulaire like 'Charlotte B.' then 'charlotte@ienva-partners.com'
	when j.Titulaire like 'Aymard d.' then 'aymard@ineva-partners.com'
	when j.Titulaire like 'Samar S.' then 'samar@ineva-partners.com'
	else '' end as 'position-owners'
--, j.OpeningsTotal as 'position-headcount'
, iif(len(DateDeDebut)=8,concat('20',right(DateDeDebut,2),'-',left(DateDeDebut,2),'-',substring(DateDeDebut,4,2)),left(DateDeDebut,10)) as 'position-startDate'
--, convert(varchar(10),j.StartDate,120) as 'position-startDate'
, left(
	concat('Job External ID: INV',j.ID,char(10),char(10)
	, iif(j.Entreprise = '','', concat('Entreprise: ',j.Entreprise,char(10),char(10)))
	, iif(j.Contact = '','', concat('Contact: ',j.Contact,char(10),char(10)))
	, iif(j.ContactPhone = '','', concat('Contact Phone: ',j.ContactPhone,char(10),char(10)))
	, iif(j.ContactEmail = '','', concat('Contact Email: ',j.ContactEmail,char(10),char(10)))
	, iif(j.Age = '','', concat('Age: ',j.Age,char(10),char(10)))
	, iif(j.S = '','', concat('S: ',j.S,char(10),char(10)))
	, iif(j.P = '','', concat('P: ',j.P,char(10),char(10)))
	, iif(loc.LocationName = '' or loc.LocationName is null,'', concat('Location: ',loc.LocationName,char(10),char(10)))
	, iif(j.Emplacement = '','', concat('Emplacement: ',j.Emplacement,char(10),char(10)))
	, iif(j.MaxRate = '','', concat('Max Rate: ',j.MaxRate,char(10),char(10)))
	, iif(j.Salaire = '','', concat('Salaire: ',j.Salaire,char(10),char(10)))
	, iif(j.Duree = '','', concat('Duree: ',j.Duree,char(10),char(10)))
	, iif(j.Recruteur = '','', concat('Recruteur: ',j.Recruteur,char(10),char(10)))
	, iif(j.Cree = '','',concat('Créé (Created Date): ',j.Cree,char(10)))
	, iif(j.Updated = '','',concat(char(10),'Updated Date: ',j.Updated,char(10)))
	, iif(j.Statut = '','',concat(char(10),'Statut: ',j.Statut,char(10)))
	, iif(j.QuickNotes = '','',concat(char(10),'Quick Notes: ',j.QuickNotes,char(10)))
	, iif(j.IN1 = '','',concat(char(10),'IN: ',j.IN1,char(10)))
	, iif(j.StatusDate = '','',concat(char(10),'Status Date: ',j.StatusDate,char(10)))
	, iif(j.Description = '','',concat(char(10),'Description: ',j.Description,char(10)))
	, iif(j.Notes = '' or j.Notes is NULL,'',Concat(char(10),'Notes: ',char(10),j.Notes))),32000)
	--, coalesce (char(10) + 'Other Notes: ' + j.Notes, '')),32000)
	 as 'position-note'
from job j left join dup on j.ID = dup.ID
				left join loc on j.ID = loc.ID
				left join jobContact jcon on j.ID = jcon.ID
				left join jobCompany jcom on j.ID = jcom.ID
				left join jobDefaultContact jdcon on j.ID = jdcon.ID		
			--left join Locations loc on j.LocationId = loc.LocationId