with tempContacts as (select c.ID as contactID, c.Nom, c.Prenom, concat(c.Prenom,' ',c.Nom) as contactName, com.ID as companyID, com.Nom as companyName
from contacts c left join entreprises com on c.companyId = com.id
where c.CompanyID <> ''
)
--select * from tempContacts

, jobContact as(select e.ID ,tc.contactID as contactID, tc.contactName, tc.companyID, e.Entreprise, tc.companyName
from export e left join tempContacts tc on e.Contact = tc.contactName and e.Entreprise = tc.companyName
where Contact <> '' and tc.contactID is not null)

--select * from jobContact order by id

, ContactMaxID as (select 
case when CompanyID = '' then '9999999'
else CompanyID end as CompanyID
, max(ID) as ContactMaxID 
from contacts --where ID not in (select ContactID from jobContact)
group by CompanyID)

, tempJobCompany as (select e.ID, Entreprise, et.ID as companyID, et.Nom, cm.ContactMaxID
from export e left join entreprises et on e.Entreprise = et.Nom
				left join ContactMaxID cm on et.ID = cm.CompanyID
where e.ID not in (select ID from jobContact))

, jobCompany as (select * from tempJobCompany where ContactMaxID is not null)
, temp_defaultCompanyContact as (select * from tempJobCompany where ContactMaxID is null and companyID is not null)
select distinct companyID from temp_defaultCompanyContact--tao 10 default contact cho các coongty này

--select distinct id from export

--select * from contacts where CompanyID = ''

--select * from contacts where id in (32822284,36629860)