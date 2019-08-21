--CANDIDATE DUPLICATE MAIL REGCONITION
with 
temp as (select *,replace(replace(candidateexternalId,'NJFS','NJF1S'),'NJFGTP','NJF2GTP') as externalId
from ImportCandidate)

, Email_EditFormat as (
SELECT candidateexternalId
	 , ltrim(rtrim(replace(replace(replace(replace(replace(replace(candidateemail,char(9),''),'*',''),'..',''),'·',''),char(10),''),'---','-'))) as email
from temp)

, EmailDupRegconition as (
SELECT candidateexternalId, email, ROW_NUMBER() OVER(PARTITION BY email ORDER BY candidateexternalId ASC) AS rn 
from Email_EditFormat)

--select * from EmailDupRegconition where Email = 'a.novikovs@gmail.com'--rn>1--intCandidateId = 38603

, CandidateMainEmail as (select candidateexternalId
--, case	when rn = 1 then email
--		else concat('NJFS_',rn,'_',email) end as CandidateEmail
	, iif(rn=1,email
	, case 
		when left(candidateexternalId,4) like 'NJFS' then concat('NJFS_',email)
		when left(candidateexternalId,4) like 'NJFC' then concat('NJFC_',email)
		else concat('NJFGTP_',email)
		end) as CandidateEmail
from EmailDupRegconition)
--, tempp as(select *, ROW_NUMBER() OVER(PARTITION BY CandidateEmail ORDER BY candidateexternalId ASC) AS rn from CandidateMainEmail)
--select * from tempp where rn>1

select * from CandidateMainEmail
order by CandidateEmail

--NOTE: must remove the first dot in the email of this candidate in the excel file: select CandidateEmail, right(CandidateEmail,len(CandidateEmail)-1) from CandidateEmail where intCandidateId = 39740--rn>1


-------------------------------------------------------------MAIN SCRIPT
--insert into ImportCandidate
select 
c.candidateexternalId as 'candidate-externalId'
, PersonId--just for reference afterward
, candidatefirstName as 'candidate-firstName'
, candidateLastname as 'candidate-Lastname'
, candidateMiddlename as 'candidate-Middlename'
, cme.candidateemail as 'candidate-email'
, candidateworkEmail as 'candidate-workEmail'--a lot of email has incorrect format, so if these candidates are skipped importing, remove work email
, candidatedob as 'candidate-dob'
, candidatetitle as 'candidate-title'
, candidategender as 'candidate-gender'
, candidatephone as 'candidate-phone'
, candidatemobile as 'candidate-mobile'
, candidateworkPhone as 'candidate-workPhone'
, candidatehomephone as 'candidate-homephone'
, candidatelinkedin as 'candidate-linkedin'
, candidateskype as 'candidate-skype'
, candidateaddress as 'candidate-address'
, candidatecity as 'candidate-city'
, candidatestate as 'candidate-state'
, candidatezipCode as 'candidate-zipCode'
, candidateCountry as 'candidate-Country'
, candidateowners as 'candidate-owners'
, candidatecitizenship as 'candidate-citizenship'
, candidatejobTitle1 as 'candidate-jobTitle1'
, candidateemployer1 as 'candidate-employer1'
, candidateworkHistory as 'candidate-workHistory'
, candidateresume as 'candidate-resume'
, candidateskills as 'candidate-skills'
, candidatenote as 'candidate-note'
from ImportCandidate c left join CandidateMainEmail cme on c.candidateexternalId = cme.candidateexternalId