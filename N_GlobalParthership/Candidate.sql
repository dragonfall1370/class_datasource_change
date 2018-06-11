with
tempemail as (
select candidateexternalId, coalesce(nullif(candidateemail, ''),nullif(email3, ''),nullif(email1, '')) as email
from candidate
)

, tempemail1 as (select *, ROW_NUMBER() OVER(PARTITION BY email ORDER BY candidateexternalId ASC) AS rn
from tempemail where email is not null and email like '%_@__%')

, candEmail as (
select candidateexternalId,
	iif(rn=1, email, concat('DUP',rn,email)) as email
from tempemail1
)
--select * from candEmail

select c.candidateexternalId 'candidate-externalId'
	, candidatefirstName 'candidate-firstName'
	, candidateLastname 'candidate-Lastname'
	, candidateemployer1 'candidate-employer1'
	, iif(email is null or email = '',concat('candidateID_',c.candidateexternalId,'@noemail.com'),email) as 'candidate-email'
	, candidatejobTitle1 'candidate-jobTitle1'
	, replace(ltrim(Stuff(
			  Coalesce(' ' + NULLIF(candidateAddress, ''), '')
			+ Coalesce(', ' + NULLIF(candidateCity, ''), '')
			+ Coalesce(', ' + NULLIF(candidateState, ''), '')
			+ Coalesce(', ' + NULLIF(candidateZipCode, ''), '')
			+ Coalesce(', ' + NULLIF(candidateCountry, ''), '')
			, 1, 1, '')),' ,',',') as 'candidate-Address'
	, candidateCity 'candidate-City'
	, candidateState 'candidate-State'
	, candidateZipCode 'candidate-ZipCode'
	, candidateCountry 'candidate-Country'
	,candidatemobile 'candidate-mobile'
	, candidateworkphone 'candidate-workphone'
	, coalesce(nullif(candidatemobile,''),nullif(candidateworkphone,''),nullif(candidatephone2,''),nullif(candidatephone3,'')) 'candidate-phone'
	, concat(concat('Candidate External ID: ',c.candidateexternalId,char(10))
			, iif(candidatephone2 = '' and candidatephone3 = '',''
				, concat('Other phone(s): ',Stuff(Coalesce(' ' + NULLIF(candidatephone2, ''), '')
												 + Coalesce(', ' + NULLIF(candidatephone3, ''), '')
												 , 1, 1, ''),char(10)))
			, iif(candidateemailName = '' and email1 = '' and email2 = '' and email3 = '',''
				, concat('Other email address(es): ',Stuff(
														 Coalesce(' ' + NULLIF(candidateemailName, ''), '')
														+ Coalesce(', ' + NULLIF(email1, ''), '')
														+ Coalesce(', ' + NULLIF(email2, ''), '')
														+ Coalesce(', ' + NULLIF(email3, ''), '')
														, 1, 1, ''),char(10)))
				,iif(candidatenote ='','',concat('Candidate Notes:',char(10),candidatenote))
	) as 'candidate-note'
from candidate c left join candEmail ce on c.candidateexternalId = ce.candidateexternalId

--select *, coalesce(nullif(candidateemail, ''),nullif(email3, ''),nullif(email1, '')) as email
--from candidate where candidateemail = ''

--select * from candidate where candidateexternalId in ('A0314','A0361')