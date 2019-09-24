--DUPLICATION REGCONITION
with SplitEmail as (select distinct id
	, translate(value, '!'':"<>[]();,', '            ') as SplitEmail --to translate special characters
	from candidates
	cross apply string_split(trim(emailsprimary),char(10))
	where emailsprimary like '%_@_%.__%')
			
, dup as (select id
	, trim(SplitEmail) as EmailAddress
	, row_number() over(partition by trim(SplitEmail) order by id asc) as rn --distinct email if emails exist more than once
	, row_number() over(partition by id order by trim(SplitEmail)) as Contactrn --distinct if contacts may have more than 1 email
	from SplitEmail
	where SplitEmail like '%_@_%.__%')

, PrimaryEmail as (select id
	, case when rn > 1 then concat(rn,'_',trim(EmailAddress))
	else trim(EmailAddress) end as PrimaryEmail
	from dup
	where EmailAddress is not NULL and EmailAddress <> ''
	and Contactrn = 1)

--CANDIDATE DOCUMENTS (created in separate script)

--CANDIDATE FINAL DOCUMENTS (after creating companyDocument table)
, Documents as (select data_item_id, string_agg(cast(concat(id
	, right(filename, charindex('.',reverse(filename)))) as nvarchar(max)),',') as candidateDocuments
	from attachments
	where data_item_type = 'candidate'
	group by data_item_id)

--CANDIDATE NATIONALITY: candidate may have multiple nationality
, CandidateNationality as (select distinct cv.id
	, cv.cf_value
	, c1.label as candidatenationality
	, cc.countrycode as nationalitycode
	from (select distinct id, cf_value from candidates_custom_fields_171756_value where cf_value is not NULL) cv
	left join [candidates_custom_fields_171756] c1 on c1.id = cv.cf_value
	left join country_code_nationality cc on cc.nationality = c1.label)

/*
select id
from candidates_custom_fields_171756_value
where cf_value is not NULL
group by id
having count(id) > 1

select *
from candidates_custom_fields_171756_value
where id = 138665484

select *
from CandidateNationality
where id in (
select id
from CandidateNationality
group by id
having count(id) >1)
*/

--CANIDATE DUAL NATIONALITY
, CandDualNationality as (select id
	, cf_value
	from [candidates_custom_fields_159676_value]) --CUSTOM SCRIPT #Candidate dual nationality

--CANIDATE ID NUMER
, CandIDNumber as (select id
	, cf_value
	from [candidates_custom_fields_174154_value]) --CUSTOM SCRIPT #Candidate ID Number

--CANDIDATE ACTVITIES (created in separate script)

--CANDIDATE ADDRESS
, CandidateAddress as (select id, concat_ws(', '
	, nullif(nullif(nullif(addressstreet,''),'NA'),',
	0,
	0,
	0   0')
	, nullif(nullif(nullif(addresscity,''),'NA'),'0')
	, nullif(nullif(nullif(addressstate,''),'NA'),'0')
	, nullif(nullif(nullif(addresspostalcode,''),'NA'),'0')
	, nullif(nullif(nullif(countrycode,''),'NA'),'0')) as CandidateAddress
	from candidates)

--CANDIDATE CURRENCY
, CandCurrencySplit as (select id
	, cf_value
	, value as currency
	, row_number() over(partition by id order by value) as rn
	from [candidates_custom_fields_160165_value]
	cross apply string_split(replace(replace(cf_value,'[',''),']',''),',')
	where cf_value <> '[]')

, CandidateCurrency as (select id
	, currency
	, case when currency = 324787 then 'USD'
			when currency = 324790 then 'EUR'
			when currency = 324793 then 'ZAR'
			when currency = 324796 then NULL
			else NULL end as CandidateCurrency
	from CandCurrencySplit
	where rn = 1)

--CANDIDATE GENDER
, Candidategender as (select distinct cv.id
	, cv.cf_value
	, c1.label as candidategender
	from [candidates_custom_fields_162002_value] cv
	left join [candidates_custom_fields_162002] c1 on c1.id = cv.cf_value
	where cv.cf_id = 162002) --candidate gender

--CANDIDATE RACE
, CandidateRace as (select distinct cv.id
	, cv.cf_value
	, c1.label as candidaterace
	from [candidates_custom_fields_161999_value] cv
	left join [candidates_custom_fields_161999] c1 on c1.id = cv.cf_value
	where cv.cf_id = 161999) --candidate race

--CANDIDATE PLACEMENT STATUS
, CandidatePlacement as (select distinct cv.id
	, cv.cf_value
	, c1.label as CandidatePlacement
	from [candidates_custom_fields_153623_value] cv
	left join [candidates_custom_fields_153623] c1 on c1.id = cv.cf_value
	where cv.cf_id = 153623) --candidate placement status

--CANDIDATE EDUCATION SUMMARY
, CandidateEdu as (select id
	, cf_value as CandidateEdu
	from [candidates_custom_fields_153635_value]) --CUSTOM SCRIPT #Candidate Education Summary

--CANDIDATE INDUSTRY (In separate script)

--CANDIDATE NOTICE PERIOD
, CandidateNotice as (select id
	, cf_value as CandidateNotice
	from [candidates_custom_fields_153638_value]) --CUSTOM SCRIPT #Candidate notice period

--CANDIDATE LANGUAGE (select) #CUSTOM FIELD
, CandLangSplit as (select id
	, cf_value
	, value as languageid
	from [candidates_custom_fields_171760_value]
	cross apply string_split(replace(replace(cf_value,'[',''),']',''),',')
	where cf_value <> '[]')

, CandidateLanguage as (select c.id
	, c.languageid
	, c1.label as languages
	from CandLangSplit c
	left join [candidates_custom_fields_171760] c1 on c1.id = c.languageid)

--MAIN SCRIPT
select --count(c.id)
concat('CG',c.id) as [candidate-externalId]
	, coalesce(nullif(c.firstname,''), concat('Firstname - ', c.id)) as [candidate-firstName]
	, nullif(c.middlename,'') as [candidate-middleName]
	, coalesce(nullif(c.lastname,''), 'Lastname') as [candidate-lastName]
	, coalesce(trim(pe.PrimaryEmail), concat(c.id, '_candidate@noemail.com')) as [candidate-email]
	, nullif(nullif(c.emailssecondary,''),'NA') as [candidate-workEmail]
	, trim(nullif(c.phonescell,'-')) as [candidate-phone]
	, trim(nullif(c.phonescell,'-')) as [candidate-mobile]
	, trim(nullif(c.phoneshome,'-')) as [candidate-homePhone]
	, trim(nullif(c.phoneswork,'-')) as [candidate-workPhone]
	, cn.nationalitycode as [candidate-citizenship]
	, convert(varchar(10),c1.cf_value,120) as [candidate-dob]
	, c2.cf_value as Skype --CUSTOM FIELD #Skype
--Candidate Work History
	, nullif(nullif(c.currentemployer,''),'NA') as [candidate-employer1]
	, nullif(nullif(c.title,''),'NA') as [candidate-jobTitle1]
	, cc.CandidateCurrency as [candidate-currency]
	, case when cg.cf_value = '332623' then 'MALE' 
		when cg.cf_value = '332626' then 'FEMALE'
		else NULL end as [candidate-gender]
--Candidate address
	, ca.CandidateAddress as [candidate-address]
	, nullif(nullif(nullif(c.addresscity,''),'NA'),'0') as [candidate-city]
	, nullif(nullif(nullif(c.addressstate,''),'NA'),'0') as [candidate-state]
	, nullif(nullif(nullif(c.addresspostalcode,''),'NA'),'0') as [candidate-zipCode]
	, case when c.countrycode = 'NA' then NULL
		when c.countrycode in ('CD','AN','SS') then NULL
		when c.countrycode = '' or c.countrycode is NULL then NULL
		else c.countrycode end as [candidate-country]
	, nullif(ce.CandidateEdu,'') as [candidate-education]
	, d.candidateDocuments as [candidate-resume]
	, c.keyskills as [candidate-skills]
	, concat_ws(char(10)
			, concat('Candidate External ID: ', c.id)
			, coalesce('Date created: ' + nullif(nullif(convert(varchar(10),c.datecreated,120),''),'NA'),NULL)
			, coalesce('Date modified: ' + nullif(nullif(convert(varchar(10),c.datemodified,120),''),'NA'),NULL)
			, coalesce('Candidate owners: ' + trim(u.username),NULL)
			) as [candidate-note]
from candidates c
left join PrimaryEmail pe on pe.id = c.id
left join CandidateAddress ca on ca.id = c.id
left join users u on u.id = c.ownerid
left join CandidateNationality cn on cn.id = c.id
left join [candidates_custom_fields_159673_value] c1 on c1.id = c.id -- Date of birth
left join Candidategender cg on cg.id = c.id
left join [candidates_custom_fields_177043_value] c2 on c2.id = c.id --Skype ID
left join CandidateCurrency cc on cc.id = c.id
left join Documents d on d.data_item_id = c.id
left join CandidateEdu ce on ce.id = c.id
order by c.id --443176 rows