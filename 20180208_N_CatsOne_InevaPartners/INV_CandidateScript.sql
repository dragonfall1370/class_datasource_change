--CANDIDATE DUPLICATE MAIL REGCONITION
with EmailDupRegconition as (SELECT ID, Courriel, ROW_NUMBER() OVER(PARTITION BY Courriel ORDER BY ID ASC) AS rn 
from candidate where Courriel like '%_@_%.__%')

, CandidateEmail as (select ID
, case	when rn = 1 then Courriel
		else concat('DUP',rn,'-',Courriel) end as CandidateEmail
, rn
from EmailDupRegconition)
--select * from CandidateEmail where rn >1

--, tempCandidateLastName as (
--select ID
--, len(Prenom) as lenFirstName,
--case	
--	when right(rtrim(Nom),1)=')' then left(Nom, len(Nom)-charindex('( ',Reverse(Nom)))
--	else Nom end as Fullname
--from candidate
--)

--, candidateLastName as (
--select *, right(FullName, len(FullName) - lenFirstName) as lastName
--from tempCandidateLastName)

-------------------------------------------------------------MAIN SCRIPT
select concat('INV', a.ID) as 'candidate-externalId'
, rtrim(ltrim(a.Prenom)) as 'candidate-firstName'
, rtrim(ltrim(a.MiddleName)) as 'candidate-middleName'
, rtrim(ltrim(a.Nom)) as 'candidate-Lastname'
, FullNom
, a.Titre as 'candidate-jobTitle1'
, a.CurrentEmployer as 'candidate-Employer1'
, a.Telephone as 'candidate-phone'
, a.Telephonemaison as 'candidate-homePhone'
, a.WorkPhone as 'candidate-workPhone'
, a.Cellulaire as 'candidate-mobile'
, iif(a.SocialMedia like '%linkedin%',a.SocialMedia,'') as 'candidate-linkedin'
, replace(a.CurrentResumeFileName,'?','') as 'candidate-resume' 
, case
	when a.Courriel is not NULL and ce.ID is not null then ce.CandidateEmail
	else concat('CandidateID-',a.ID,'@noemail.com') end as 'candidate-email'
, a.SecondaryEmail as 'candidate-workEmail'
, case 
	when a.Titulaire like 'Magdalena M.' then 'magdalena@ineva-partners.com'
	when a.Titulaire like 'Hamza B.' then 'hamza@ineva-partners.com'
	when a.Titulaire like 'Charlotte B.' then 'charlotte@ienva-partners.com'
	when a.Titulaire like 'Aymard d.' then 'aymard@ineva-partners.com'
	when a.Titulaire like 'Samar S.' then 'samar@ineva-partners.com'
	else '' end as'candidate-owners'
, left(concat('Candidate External ID: INV',a.ID, char(10)
	, iif(a.Cree = '','',concat(char(10),'Créé (Created Date): ',a.Cree,char(10)))
	, iif(a.Updated = '','',concat(char(10),'Updated Date: ',a.Updated,char(10)))
	, iif(a.SecondaryEmail = '','',concat(char(10),'Secondary Email: ',a.SecondaryEmail,char(10)))
	, iif(a.DateOfNewestResume = '','',concat(char(10),'Date Of Newest Resume: ',a.DateOfNewestResume,char(10)))
	, iif(a.Entreprise = '','',concat(char(10),'Entreprise: ',a.Entreprise,char(10)))
	, iif(a.Fichedebesoin = '','',concat(char(10),'Fiche de besoin: ',a.Fichedebesoin,char(10)))
	, iif(a.Rating = '','',concat(char(10),'Rating: ',a.Rating,char(10)))
	, iif(a.RecentStatusExtended = '','',concat(char(10),'Recent Status - Extended: ',a.RecentStatusExtended,char(10)))
	, iif(a.SocialMedia = '','',concat(char(10),'Social Media: ',a.SocialMedia,char(10)))
	, iif(a.Notes = '' or a.Notes is NULL,'',Concat(char(10),'Notes: ',char(10),a.Notes))),32000) as 'candidate-note'
from candidate a left join CandidateEmail ce on a.ID = ce.ID
			--left join candidateLastName cln on a.ID = cln.ID
							
--order by a.ID
--where a.id = 230093738
--where cn.file_ like '%hortense-gueneau%'
