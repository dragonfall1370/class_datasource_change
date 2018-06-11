with tempEmail1 as (
select Referencenumber, Emailaddress1,
iif(charindex(',',Emailaddress1)=0, Emailaddress1,left(Emailaddress1,charindex(',',Emailaddress1)-1)) as Email1
from candidate where Emailaddress1 like '%_@_%.__%')

, tempEmail2 as (
select Referencenumber, Emailaddress2,
iif(charindex(',',Emailaddress2)=0, Emailaddress2,left(Emailaddress2,charindex(',',Emailaddress2)-1)) as Email2
from candidate where Emailaddress2 like '%_@_%.__%')
----------Contact Email
, TempPrimaryEmail as (
select c.Referencenumber, te1.Email1,te2.Email2,coalesce(te1.Email1,te2.Email2) as Email
from candidate c left join tempEmail1 te1 on c.Referencenumber = te1.Referencenumber
				 left join tempEmail2 te2 on c.Referencenumber = te2.Referencenumber
				 )

--check email format
, EmailDupRegconition as (SELECT Referencenumber
, Email, Email1, Email2
,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(Email2,'?',''),' ',''),'''',''),'""johnmcg@gmail.com"',''),'"http://gmail.com"',''),'//github.com/','github.com_'),'//twitter.com/',''),'//www.freshports.org/','www.freshports.org_'),'//pkgsrc.se/','pkgsrc.se_'),'/',''),'&','_'),'+','_') as canEmail2
,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(Email,'?',''),' ',''),'''',''),'""johnmcg@gmail.com"',''),'"http://gmail.com"',''),'//github.com/','github.com_'),'//twitter.com/',''),'//www.freshports.org/','www.freshports.org_'),'//pkgsrc.se/','pkgsrc.se_'),'/',''),'&','_'),'+','_') as canEmail
, ROW_NUMBER() OVER(PARTITION BY Email ORDER BY Referencenumber ASC) AS rn 
from TempPrimaryEmail where Email is not null)-- no dup email so dont need this 

----edit duplicating emails
, candidateEmail as (select Referencenumber, 
case 
when rn=1 then canEmail
else concat(rn,'_',(canEmail))
end as canEmail
from EmailDupRegconition)
--select * from candidateEmail

-------------------------------------------------------------MAIN SCRIPT
select a.Referencenumber as 'candidate-externalId'
, iif(rtrim(ltrim(FirstName)) = '', concat('NoFirstname-', a.referencenumber), rtrim(ltrim(FirstName))) as 'candidate-firstName'
, iif(rtrim(ltrim(LastName)) = '', concat('NoLastname-', a.referencenumber), rtrim(ltrim(LastName))) as 'candidate-Lastname'
, iif(ce.canEmail = '' or ce.canEmail is null, concat('CandidateID-',a.referencenumber,'@noemail.com'),ce.CanEmail) as 'candidate-email'
, ed.canEmail2 as 'candidate-workEmail'
, a.Phonenumber1 as 'candidate-phone'
, a.Phonenumber2 as 'candidate-mobile'
, iif(a.Linkedin like '%linkedin%', a.linkedin, '') as 'candidate-linkedin1'
, case 
	when Socialmedia6 like '%linkedin.com%' then Socialmedia6
	when Socialmedia5 like '%linkedin.com%' then Socialmedia5
	when Socialmedia4 like '%linkedin.com%' then Socialmedia4
	when Socialmedia3 like '%linkedin.com%' then Socialmedia3
	when Socialmedia2 like '%linkedin.com%' then Socialmedia2
	when Socialmedia1 like '%linkedin/com%' then Socialmedia1
	when Linkedin like '%linkedin.com%' then a.linkedin
	else '' end as 'candidate-linkedin'
, a.Title as 'candidate-jobTitle1'
, a.Company as 'candidate-Employer1'
, a.Skills as 'candidate-skills'
--, 'candidate-currentSalary'
, a.Location as 'candidate-address'
, case
	when a.Location like '%Afghan%' then 'AF'
	when a.Location like '%Algeri%' then 'DZ'
	when a.Location like '%Africa%' then 'ZA'
	when a.Location like '%Albani%' then 'AL'
	when a.Location like '%America%' then 'US'
	when a.Location like '%Andorr%' then 'AD'
	when a.Location like '%Argentin%' then 'AR'
	when a.Location like '%Austra%' then 'AU'
	when a.Location like '%Austri%' then 'AT'
	when a.Location like '%Belgia%' then 'BE'
	when a.Location like '%Brazil%' then 'BR'
	when a.Location like 'Britis%' then 'GB'
	when a.Location like 'Bucha%' then 'RO'
	when a.Location like '%Bulgari%' then 'BG'
	when a.Location like 'Burmes%' then 'MM'
	when a.Location like 'Cambod%' then 'KH'
	when a.Location like 'Canadi%' then 'CA'
	when a.Location like 'Chines%' then 'CN'
	when a.Location like 'Colombi%' then 'CO'
	when a.Location like 'Costa%' then 'CR'
	when a.Location like '%Cypr%' then 'CY'
	when a.Location like '%Czech%' then 'CZ'
	when a.Location like '%Danish%' then 'DK'
	when a.Location like 'Denmark%' then 'DK'
	when a.Location like '%Dutch%' then 'NL'
	when a.Location like 'East%' then 'ZA'
	when a.Location like '%Egypt%' then 'EG'
	when a.Location like 'Emiria%' then 'AE'
	when a.Location like 'Eritre%' then 'ER'
	when a.Location like 'Estoni%' then 'EE'
	when a.Location like 'Ethiop%' then 'ET'
	when a.Location like 'Europe%' then 'TR'
	when a.Location like 'Fijian%' then 'FJ'
	when a.Location like 'Filipi%' then 'PH'
	when a.Location like 'fili%' then 'PH'
	when a.Location like 'Finnish%' then 'FI'
	when a.Location like 'Flemish%' then 'BE'
	when a.Location like 'French%' then 'FR'
	when a.Location like 'Gabone%' then 'GA'
	when a.Location like 'German%' then 'DE'
	when a.Location like '%Georgi%' then 'GE'
	when a.Location like 'Ghanai%' then 'GH'
	when a.Location like 'Gree%' then 'GR'
	when a.Location like 'Hunga%' then 'HU'
	when a.Location like 'Indian%' then 'IN'
	when a.Location like 'Indone%' then 'ID'
	when a.Location like 'Irania%' then 'IR'
	when a.Location like 'Iraq%' then 'IQ'
	when a.Location like 'Irish%' then 'IE'
	when a.Location like 'Isra%' then 'IL'
	when a.Location like 'Ital%' then 'IT'
	when a.Location like 'Jamaic%' then 'JM'
	when a.Location like 'Japane%' then 'JP'
	when a.Location like 'Keny%' then 'KE'
	when a.Location like 'Leban%' then 'LB'
	when a.Location like 'Lithua%' then 'LT'
	when a.Location like 'Malaga%' then 'MG'
	when a.Location like 'Malays%' then 'MY'
	when a.Location like 'Malt%' then 'MT'
	when a.Location like 'Mauritian%' then 'MU'
	when a.Location like 'Mexi%' then 'MX'
	when a.Location like 'Moroc%' then 'MA'
	when a.Location like 'Namibi%' then 'NA'
	when a.Location like 'New Zea%' then 'NZ'
	when a.Location like 'Nigeri%' then 'NG'
	when a.Location like 'Northern Irish' then 'IE'
	when a.Location like 'Norwe%' then 'NO'
	when a.Location like 'Pakist%' then 'PK'
	when a.Location like 'Philip%' then 'PH'
	when a.Location like 'Phili%' then 'PH'
	when a.Location like 'Polish%' then 'PL'
	when a.Location like 'Portu%' then 'PT'
	when a.Location like 'Romani%' then 'RO'
	when a.Location like 'Russia%' then 'RU'
	when a.Location like 'Senegal%' then 'SN'
	when a.Location like 'Serbia%' then 'RS'
	when a.Location like 'Singap%' then 'SG'
	when a.Location like 'Slovaki%' then 'SK'
	when a.Location like '%South Korea%' then 'KR'
	when a.Location like 'Sri%' then 'LK'
	when a.Location like 'South Africa%' then 'ZA'
	when a.Location like 'Spanish%' then 'ES'
	when a.Location like 'Sri Lanka%' then 'LK'
	when a.Location like 'Sri lanka%' then 'LK'
	when a.Location like 'Swedish%' then 'SE'
	when a.Location like 'Swiss%' then 'CH'
	when a.Location like 'Taiwan%' then 'TW'
	when a.Location like '%Ukrain%' then 'UA'
	when a.Location like 'Thai%' then 'TH'
	when a.Location like 'Trinida%' then 'TT'
	when a.Location like 'Turk%' then 'TR'
	when a.Location like 'Vietna%' then 'VN'
	--when a.Location like 'Yugoslavia%' then 'YU'
	when a.Location like '%UNITED%ARAB%' then 'AE'
	when a.Location like '%UAE%' then 'AE'
	when a.Location like '%U.A.E%' then 'AE'
	when a.Location like '%UNITED%KINGDOM%' then 'GB'
	when a.Location like '%UNITED%STATES%' then 'US'
	when a.Location like '%US%' then 'US'
	when a.Location like '%Zimbab%' then 'ZW'
	when a.Location like '%etherland%' then 'NL'
    when a.Location like '%ederla%' then 'NL'
    when a.Location like '' then 'NL'
    when a.Location like '%USA%' then 'US'
    when a.Location like '%Belg%' then 'BE'
    when a.Location like '%Austra%' then 'AU'
    when a.Location like '%Denemarken%' then 'DK'
    when a.Location like '%uitsland%' then 'DE	'
    when a.Location like '%Finland%' then 'FI'
    when a.Location like '%Frankrijk%' then 'FR'
    when a.Location like '%olland%' then 'NL'
    when a.Location like '%Ierland%' then 'IE'
    when a.Location like '%Ireland%' then 'IE'
    when a.Location like '%Indo%' then 'ID'
    when a.Location like '%Germany%' then 'DE'
    when a.Location like '%ned%' then 'NL'
    when a.Location like '%Nedreland%' then 'NL'
    when a.Location like '%Nedrland%' then 'NL'
    when a.Location like '%Nerderland%' then 'NL'
    when a.Location like '%Ridderkerk%' then 'NL'
    when a.Location like '%NLD%' then 'NL'
    when a.Location like '%Schotland%' then 'GB'
    when a.Location like '%Spanje%' then 'ES'
	when a.Location like '%Deutschland%' then 'DE'
	when a.Location like '%Netherlands%' then 'NL'
	when a.Location like '%Amsterdam%' then 'NL'
	when a.Location like '%Niederlande%' then 'NL'
else '' end as 'candidate-Country'
, left(concat('Candidate External ID: ',a.Referencenumber, char(10)
	, iif(a.Emailaddress1 = '','',concat(char(10),'Email address 1: ',a.Emailaddress1,char(10)))
	, iif(a.Emailaddress2 = '','',concat(char(10),'Email address 2: ',a.Emailaddress2,char(10)))
	, iif(a.Phonenumber1 = '','',concat(char(10),'Phone number 1: ',a.Phonenumber1,char(10)))
	, iif(a.Phonenumber2 = '','',concat(char(10),'Phone number 2: ',a.Phonenumber2,char(10)))
	, iif(a.Linkedin = '','',concat(char(10),'Linkedin: ',a.Linkedin,char(10)))
	, iif(a.Currentannualbasesalary = '','',concat(char(10),'Current annual base salary: ',a.Currentannualbasesalary,char(10)))
	, iif(a.Socialmedia1 = '','',concat(char(10),'Social media 1: ',a.Socialmedia1,char(10)))
	, iif(a.Socialmedia2 = '','',concat(char(10),'Social media 2: ',a.Socialmedia2,char(10)))
	, iif(a.Socialmedia3 = '','',concat(char(10),'Social media 3: ',a.Socialmedia3,char(10)))
	, iif(a.Socialmedia4 = '','',concat(char(10),'Social media 4: ',a.Socialmedia4,char(10)))
	, iif(a.Socialmedia5 = '','',concat(char(10),'Social media 5: ',a.Socialmedia5,char(10)))
	, iif(a.Socialmedia6 = '','',concat(char(10),'Social media 6: ',a.Socialmedia6,char(10)))),32000) as 'candidate-note'
from candidate a 
				left join candidateEmail ce on a.Referencenumber = ce.Referencenumber
				left join EmailDupRegconition ed on a.Referencenumber = ed.Referencenumber
--order by a.ID
where a.Referencenumber = 'REC68631'

--where cn.file_ like '%hortense-gueneau%'
