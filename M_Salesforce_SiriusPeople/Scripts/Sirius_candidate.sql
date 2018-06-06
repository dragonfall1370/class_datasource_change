-------------
--PART 1: MAIN SCRIPT FOR CANDIDATE
-------------
with
--MAIL DUPLICATION
dup as (SELECT ID, EMAIL, ROW_NUMBER() OVER(PARTITION BY EMAIL ORDER BY ID ASC) AS rn 
FROM Candidate where EMAIL is not NULL)

, CandUnionEmail as (select ID, PEOPLECLOUD1__HOME_EMAIL__C as EMAIL from Contact where PEOPLECLOUD1__HOME_EMAIL__C is not NULL
UNION ALL
select ID, PEOPLECLOUD1__WORK_EMAIL__C from Contact where PEOPLECLOUD1__WORK_EMAIL__C is not NULL)

, CandUnionEmaildup as (SELECT ID, EMAIL, ROW_NUMBER() OVER(PARTITION BY EMAIL ORDER BY ID ASC) AS rn 
FROM CandUnionEmail where EMAIL is not NULL)

, CandWorkEmail as (select ID
	, STRING_AGG(case when rn > 1 then concat(rn,'_duplicate_',EMAIL)
	else EMAIL end, ', ') as CandWorkEmail
from CandUnionEmaildup
group by ID)

--CANDIDATE SKILLS
, SkillName as (select PEOPLECLOUD1__CANDIDATE__C, SKILL_GROUP_NAME__C, STRING_AGG(SKILL_NAME__C,', ') as SkillName 
from CandidateSkills
group by PEOPLECLOUD1__CANDIDATE__C, SKILL_GROUP_NAME__C)

, SkillGroup as (select PEOPLECLOUD1__CANDIDATE__C, concat(SKILL_GROUP_NAME__C,': ',SkillName) as SkillGroup
from SkillName)

, CandSkills as (select PEOPLECLOUD1__CANDIDATE__C,  STRING_AGG(SkillGroup,', ') as CandSkills 
from SkillGroup
group by PEOPLECLOUD1__CANDIDATE__C)

--CANDIDATE FILES
, CandidateFiles as (select PEOPLECLOUD1__DOCUMENT_RELATED_TO__C
	, STRING_AGG(NAME, ',') as CandidateFiles
	from ResumeCompliance
	where NAME like '.%doc' or NAME like '%.pdf' or NAME like '%.rtf' or NAME like '%.xls%' or NAME like '%.html'
	group by PEOPLECLOUD1__DOCUMENT_RELATED_TO__C)

--CANDIDATE ATTACHMENTS
, CandAttachments as (select PARENTID
	, STRING_AGG(NAME, ',') as CandAttachments
	from Attachments
	where NAME like '.%doc' or NAME like '%.pdf' or NAME like '%.rtf' or NAME like '%.xls%' or NAME like '%.html'
	group by PARENTID)

--MAIN SCRIPT
select c.ID as 'candidate-externalId'
, case when c.LASTNAME = '' or c.LASTNAME is NULL then 'Lastname'
	else c.LASTNAME end as 'contact-lastName'
, case when c.FIRSTNAME = '' or c.FIRSTNAME is NULL then 'Firstname'
	else c.FIRSTNAME end as 'contact-firstName'
, case when c.SALUTATION = 'Mr.' then 'MR'
	when c.SALUTATION = 'Dr.' then 'DR'
	when c.SALUTATION = 'Mrs.' then 'MRS'
	when c.SALUTATION = 'Ms.' then 'MS'
	else NULL end as 'candidate-title'
, iif(c.ID in (select ID from dup where dup.rn > 1)
	, iif(dup.EMAIL = '' or dup.EMAIL is NULL,concat(dup.ID,'_candidate@noemail.com'),concat(dup.ID,'_duplicate_',dup.EMAIL))
	, iif(c.EMAIL = '' or c.EMAIL is null,concat(c.ID,'_candidate@noemail.com'),c.EMAIL)) as 'candidate-email'

--CANDIDATE ADDRESS
, ltrim(stuff((coalesce(' ' + nullif(rtrim(ltrim(c.MAILINGSTREET)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.MAILINGCITY)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.MAILINGSTATE)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.MAILINGPOSTALCODE)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.MAILINGCOUNTRY)),''),'')),1,1,'')) as 'candidate-address' --remove Mailing Address prefix from candidate address
, c.MAILINGCITY as 'candidate-city'
, c.MAILINGSTATE as 'candidate-State'
, c.MAILINGPOSTALCODE as 'candidate-zipCode'
, case when c.MAILINGCOUNTRY = 'Andorra' then 'AD'
	when c.MAILINGCOUNTRY = 'United Arab Emirates' then 'AE'
	when c.MAILINGCOUNTRY = 'Afghanistan' then 'AF'
	when c.MAILINGCOUNTRY = 'Albania' then 'AL'
	when c.MAILINGCOUNTRY = 'Armenia' then 'AM'
	when c.MAILINGCOUNTRY = 'Argentina' then 'AR'
	when c.MAILINGCOUNTRY = 'American Samoa' then 'AS'
	when c.MAILINGCOUNTRY = 'Austria' then 'AT'
	when c.MAILINGCOUNTRY = 'Aruba' then 'AW'
	when c.MAILINGCOUNTRY = 'Åland Islands' then 'AX'
	when c.MAILINGCOUNTRY = 'Azerbaijan' then 'AZ'
	when c.MAILINGCOUNTRY = 'Bosnia and Herzegovina' then 'BA'
	when c.MAILINGCOUNTRY = 'Bangladesh' then 'BD'
	when c.MAILINGCOUNTRY = 'Belgium' then 'BE'
	when c.MAILINGCOUNTRY = 'Bulgaria' then 'BG'
	when c.MAILINGCOUNTRY = 'Bahrain' then 'BH'
	when c.MAILINGCOUNTRY = 'Benin' then 'BJ'
	when c.MAILINGCOUNTRY = 'Brazil' then 'BR'
	when c.MAILINGCOUNTRY = 'Bhutan' then 'BT'
	when c.MAILINGCOUNTRY = 'Botswana' then 'BW'
	when c.MAILINGCOUNTRY = 'Belarus' then 'BY'
	when c.MAILINGCOUNTRY = 'Canada' then 'CA'
	when c.MAILINGCOUNTRY = 'Congo' then 'CG'
	when c.MAILINGCOUNTRY = 'Switzerland' then 'CH'
	when c.MAILINGCOUNTRY = 'Côte D''ivoire' then 'CI'
	when c.MAILINGCOUNTRY = 'Chile' then 'CL'
	when c.MAILINGCOUNTRY = 'Cameroon' then 'CM'
	when c.MAILINGCOUNTRY = 'China' then 'CN'
	when c.MAILINGCOUNTRY = 'Colombia' then 'CO'
	when c.MAILINGCOUNTRY = 'Costa Rica' then 'CR'
	when c.MAILINGCOUNTRY = 'Cuba' then 'CU'
	when c.MAILINGCOUNTRY = 'Cyprus' then 'CY'
	when c.MAILINGCOUNTRY = 'Czech Republic' then 'CZ'
	when c.MAILINGCOUNTRY = 'Germany' then 'DE'
	when c.MAILINGCOUNTRY = 'Denmark' then 'DK'
	when c.MAILINGCOUNTRY = 'Dominica' then 'DM'
	when c.MAILINGCOUNTRY = 'Algeria' then 'DZ'
	when c.MAILINGCOUNTRY = 'Ecuador' then 'EC'
	when c.MAILINGCOUNTRY = 'Estonia' then 'EE'
	when c.MAILINGCOUNTRY = 'Egypt' then 'EG'
	when c.MAILINGCOUNTRY = 'Spain' then 'ES'
	when c.MAILINGCOUNTRY = 'Ethiopia' then 'ET'
	when c.MAILINGCOUNTRY = 'Finland' then 'FI'
	when c.MAILINGCOUNTRY = 'Fiji' then 'FJ'
	when c.MAILINGCOUNTRY = 'France' then 'FR'
	when c.MAILINGCOUNTRY = 'UK' then 'GB'
	when c.MAILINGCOUNTRY = 'United Kingdom' then 'GB'
	when c.MAILINGCOUNTRY = 'Georgia' then 'GE'
	when c.MAILINGCOUNTRY = 'Ghana' then 'GH'
	when c.MAILINGCOUNTRY = 'Gibraltar' then 'GI'
	when c.MAILINGCOUNTRY = 'Greece' then 'GR'
	when c.MAILINGCOUNTRY = 'Guam' then 'GU'
	when c.MAILINGCOUNTRY = 'Hong Kong' then 'HK'
	when c.MAILINGCOUNTRY = 'Honduras' then 'HN'
	when c.MAILINGCOUNTRY = 'Croatia' then 'HR'
	when c.MAILINGCOUNTRY = 'Haiti' then 'HT'
	when c.MAILINGCOUNTRY = 'Hungary' then 'HU'
	when c.MAILINGCOUNTRY = 'Indonesia' then 'ID'
	when c.MAILINGCOUNTRY = 'Ireland' then 'IE'
	when c.MAILINGCOUNTRY = 'Israel' then 'IL'
	when c.MAILINGCOUNTRY = 'India' then 'IN'
	when c.MAILINGCOUNTRY = 'Iraq' then 'IQ'
	when c.MAILINGCOUNTRY = 'Iran, Islamic Republic Of' then 'IR'
	when c.MAILINGCOUNTRY = 'Italy' then 'IT'
	when c.MAILINGCOUNTRY = 'Jamaica' then 'JM'
	when c.MAILINGCOUNTRY = 'Jordan' then 'JO'
	when c.MAILINGCOUNTRY = 'Japan' then 'JP'
	when c.MAILINGCOUNTRY = 'Kenya' then 'KE'
	when c.MAILINGCOUNTRY = 'Korea, Republic Of' then 'KR'
	when c.MAILINGCOUNTRY = 'Kuwait' then 'KW'
	when c.MAILINGCOUNTRY = 'Cayman Islands' then 'KY'
	when c.MAILINGCOUNTRY = 'Kazakhstan' then 'KZ'
	when c.MAILINGCOUNTRY = 'Lebanon' then 'LB'
	when c.MAILINGCOUNTRY = 'Sri Lanka' then 'LK'
	when c.MAILINGCOUNTRY = 'Lesotho' then 'LS'
	when c.MAILINGCOUNTRY = 'Lithuania' then 'LT'
	when c.MAILINGCOUNTRY = 'Luxembourg' then 'LU'
	when c.MAILINGCOUNTRY = 'Latvia' then 'LV'
	when c.MAILINGCOUNTRY = 'Libyan Arab Jamahiriya' then 'LY'
	when c.MAILINGCOUNTRY = 'Morocco' then 'MA'
	when c.MAILINGCOUNTRY = 'Montenegro' then 'ME'
	when c.MAILINGCOUNTRY = 'Macedonia, The Former Yugoslav Republic Of' then 'MK'
	when c.MAILINGCOUNTRY = 'Myanmar' then 'MM'
	when c.MAILINGCOUNTRY = 'Mongolia' then 'MN'
	when c.MAILINGCOUNTRY = 'Northern Mariana Islands' then 'MP'
	when c.MAILINGCOUNTRY = 'Malta' then 'MT'
	when c.MAILINGCOUNTRY = 'Mauritius' then 'MU'
	when c.MAILINGCOUNTRY = 'MALDIVES' then 'MV'
	when c.MAILINGCOUNTRY = 'Malawi' then 'MW'
	when c.MAILINGCOUNTRY = 'Mexico' then 'MX'
	when c.MAILINGCOUNTRY = 'Malaysia' then 'MY'
	when c.MAILINGCOUNTRY = 'Nigeria' then 'NG'
	when c.MAILINGCOUNTRY = 'Netherlands' then 'NL'
	when c.MAILINGCOUNTRY = 'Norway' then 'NO'
	when c.MAILINGCOUNTRY = 'Nepal' then 'NP'
	when c.MAILINGCOUNTRY = 'New Zealand' then 'NZ'
	when c.MAILINGCOUNTRY = 'New Zealand (Aotearoa)' then 'NZ'
	when c.MAILINGCOUNTRY = 'Oman' then 'OM'
	when c.MAILINGCOUNTRY = 'Peru' then 'PE'
	when c.MAILINGCOUNTRY = 'Papua New Guinea' then 'PG'
	when c.MAILINGCOUNTRY = 'Philippines' then 'PH'
	when c.MAILINGCOUNTRY = 'Pakistan' then 'PK'
	when c.MAILINGCOUNTRY = 'Poland' then 'PL'
	when c.MAILINGCOUNTRY = 'Puerto Rico' then 'PR'
	when c.MAILINGCOUNTRY = 'Palestinian Territory, Occupied' then 'PS'
	when c.MAILINGCOUNTRY = 'Portugal' then 'PT'
	when c.MAILINGCOUNTRY = 'Qatar' then 'QA'
	when c.MAILINGCOUNTRY = 'Romania' then 'RO'
	when c.MAILINGCOUNTRY = 'Serbia' then 'RS'
	when c.MAILINGCOUNTRY = 'Russian Federation' then 'RU'
	when c.MAILINGCOUNTRY = 'Saudi Arabia' then 'SA'
	when c.MAILINGCOUNTRY = 'Seychelles' then 'SC'
	when c.MAILINGCOUNTRY = 'Sweden' then 'SE'
	when c.MAILINGCOUNTRY = 'Singapore' then 'SG'
	when c.MAILINGCOUNTRY = 'Singapre' then 'SG'
	when c.MAILINGCOUNTRY = 'Slovenia' then 'SI'
	when c.MAILINGCOUNTRY = 'Slovak Republic' then 'SK'
	when c.MAILINGCOUNTRY = 'Slovakia' then 'SK'
	when c.MAILINGCOUNTRY = 'El Salvador' then 'SV'
	when c.MAILINGCOUNTRY = 'Syria' then 'SY'
	when c.MAILINGCOUNTRY = 'Syrian Arab Republic' then 'SY'
	when c.MAILINGCOUNTRY = 'Swaziland' then 'SZ'
	when c.MAILINGCOUNTRY = 'Chad' then 'TD'
	when c.MAILINGCOUNTRY = 'Thailand' then 'TH'
	when c.MAILINGCOUNTRY = 'Tunisia' then 'TN'
	when c.MAILINGCOUNTRY = 'Tonga' then 'TO'
	when c.MAILINGCOUNTRY = 'Turkey' then 'TR'
	when c.MAILINGCOUNTRY = 'Trinidad and Tobago' then 'TT'
	when c.MAILINGCOUNTRY = 'Taiwan, Province of China' then 'TW'
	when c.MAILINGCOUNTRY = 'Tanzania, United Republic Of' then 'TZ'
	when c.MAILINGCOUNTRY = 'Ukraine' then 'UA'
	when c.MAILINGCOUNTRY = 'Uganda' then 'UG'
	when c.MAILINGCOUNTRY = 'America' then 'US'
	when c.MAILINGCOUNTRY = 'United States' then 'US'
	when c.MAILINGCOUNTRY = 'USA' then 'US'
	when c.MAILINGCOUNTRY = 'Uruguay' then 'UY'
	when c.MAILINGCOUNTRY = 'Uzbekistan' then 'UZ'
	when c.MAILINGCOUNTRY = 'Saint Vincent and The Grenadines' then 'VC'
	when c.MAILINGCOUNTRY = 'Venezuela, Bolivarian Republic Of' then 'VE'
	when c.MAILINGCOUNTRY = 'Viet Nam' then 'VN'
	when c.MAILINGCOUNTRY = 'Vietnam' then 'VN'
	when c.MAILINGCOUNTRY = 'Vanuatu' then 'VU'
	when c.MAILINGCOUNTRY = 'Yemen' then 'YE'
	when c.MAILINGCOUNTRY = 'South Africa' then 'ZA'
	when c.MAILINGCOUNTRY = 'Zambia' then 'ZM'
	when c.MAILINGCOUNTRY = 'Zimbabwe' then 'ZW'
	when c.MAILINGCOUNTRY in ('AAustralia','Astralia','Asutralia','au','Auatralia','Aus','Ausrralia','Aust','Austalia','Austral;ia','Australia','Australia q','Australlia','Australuia','Austrlaia','Austrlia','Aystralia','Melbourne','sydney') then 'AU'
	else '' end as 'candidate-Country'

, c.MOBILEPHONE as 'candidate-phone'
, c.PHONE as 'candidate-workPhone'
, c.HOMEPHONE as 'candidate-homePhone'
, cw.CandWorkEmail as 'candidate-workEmail'
, c.LINKEDIN_PROFILE__C as 'candidate-linkedln'
, stuff((coalesce(',' + nullif(ltrim(su.EMAIL),''),'') + coalesce(',' + nullif(ltrim(su2.EMAIL),''),'')
	+ coalesce(',' + nullif(ltrim(su3.EMAIL),''),'')
	), 1, 2,'') as 'contact-owners'
, c.DESK__C as 'candidate-keyword'
--, c.CURRENT_DAILY_RATE__C as 'candidate-contractRate'
, case when c.PEOPLECLOUD1__GENDER__C = 'Male' then 'MALE'
	when c.PEOPLECLOUD1__GENDER__C = 'Female' then 'FEMALE'
	else '' end as 'candidate-gender'
, convert(varchar(20),c.BIRTHDATE,120) as 'candidate-dob'

, cs.CandSkills as 'candidate-skills'
--, cf.CandidateFiles as 'candidate-resume'
--, ca.CandAttachments as 'candidate-document'
, concat(coalesce(cf.CandidateFiles,''), coalesce(', ' + ca.CandAttachments,'')) as 'candidate-resume'

--CANDIDATE WORK HISTORY
, c.CURRENT_EMPLOYMENT_TYPE__C as 'candidate-workHistory'
, c.TITLE as 'candidate-jobTitle1'
, c.CURRENT_EMPLOYER__C as 'candidate-employer1'

--CANDIDATE NOTES
, concat('Candidate External ID: ',c.ID,char(10)
	, coalesce('Candidate Account ID: ' + c.ACCOUNTID + char(10),'')
	, coalesce('Record Type ID: ' + c.RECORDTYPEID + char(10),'')
	, coalesce('Created: ' + convert(varchar(20),c.CREATEDDATE,120) + char(10),'')
	, coalesce('Candidate Preferred Name: ' + c.PREFERRED_NAME__C + char(10),'')
	, coalesce('Description: ' + c.CANDIDATE_DESCRIPTION__C + char(10),'')
	, coalesce('Do not contact reason: ' + c.DO_NOT_CONTACT_REASON__C + char(10),'')
	, coalesce('Annual Salary: ' + c.CURRENT_ANNUAL_SALARY__C + char(10),'')
	, coalesce('Desired Annual Salary: ' + c.DESIRED_ANNUAL_SALARY__C + char(10),'')
	, coalesce('Desired Rate Salary: ' + c.DESIRED_DAILY_SALARY__C + char(10),'')
	, coalesce('Contract Rate: ' + c.CURRENT_DAILY_RATE__C + char(10),'')
	, coalesce('If yes to the above please explain: ' + c.IF_YES_TO_THE_ABOVE_PLEASE_EXPLAIN__C + char(10),'')
	, coalesce('Availability start: ' + convert(varchar(20),c.AVAILABILITY_MIRROR__C,120) + char(10),'')
	, coalesce('Current Contract End Date: ' + convert(varchar(20),c.CURRENT_CONTRACT_END_DATE__C,120),'')
	) as 'candidate-note'
from Candidate c
left join dup on dup.ID = c.ID
left join CandWorkEmail cw on cw.ID = c.ID
left join SiriusUsers su on su.ID = c.OWNERID
left join SiriusUsers su2 on su2.NAME = c.HOTLIST__C
left join SiriusUsers su3 on su3.NAME = c.REGISTERED_BY_PICKLIST__C
left join CandSkills cs on cs.PEOPLECLOUD1__CANDIDATE__C = c.ID
left join CandidateFiles cf on cf.PEOPLECLOUD1__DOCUMENT_RELATED_TO__C = c.ID
left join CandAttachments ca on ca.PARENTID = c.ID
where c.PEOPLECLOUD1__STATUS__C not in ('Inactive') or c.PEOPLECLOUD1__STATUS__C is NULL


-------------
--PART 2: CANDIDATE ACTIVITIES
-------------
--CANDIDATE ACTIVITIES (SMS HISTORY)
select sh.SMAGICINTERACT__CONTACT__C as CandidateExtID
	, -10 as Sirius_user_account_id
	, Concat(concat('SMS name: ', sh.NAME),char(10)
		 + concat('Created date: ', convert(varchar(20),sh.CREATEDDATE,120),char(10))
		 + concat('Owner by: ', su.EMAIL,' - ',su.NAME,char(10))
		 + concat('Interact Name: ', sh.SMAGICINTERACT__NAME__C,char(10))
		 + concat('SMS Text: ', sh.SMAGICINTERACT__SMSTEXT__C)
		 ) as Sirius_SMS_content
	, CONVERT(Datetime, sh.CREATEDDATE, 103) as Sirius_insert_timestamp
	, 'comment' as Sirius_category
	, 'candidate' as Sirius_type
	from SMSHistory sh
	left join SiriusUsers su on su.ID = sh.OWNERID
	where sh.SMAGICINTERACT__CONTACT__C is not NULL
	and sh.SMAGICINTERACT__CONTACT__C in (select ID from Candidate)
	
	
-------------
--PART 3: CUSTOM FIELDS
-------------
/* 1. DO NOT CALL (SENT CHEVRON ON CANDIDATE) */
select id, external_id, availability from Candidate --from Vincere
--availability = 1 - DO NOT CALL | 3 - Available

select ID, DONOTCALL
from Candidate --from Sirius DB
where DONOTCALL = 'TRUE'

UNION ALL

select ID
, PEOPLECLOUD1__STATUS__C
from Candidate
where PEOPLECLOUD1__STATUS__C = 'DO NOT CONTACT'


/* 2. REG DATE */
select id, external_id, insert_timestamp from Candidate --from Vincere

select ID, CREATEDDATE from Candidate --from Sirius DB

/* 3. MET / NOT MET */
select id, external_id, status from candidate --from Vincere
--status = 1 - met | 2 - not met

select ID as Cand_ExtID
, case PEOPLECLOUD1__STATUS__C
	when 'ACTIVE' then 2 --not met
	when 'REGISTERED' then 1 --met
from Candidate

/* 4. Compliance fields */
select ID
, EMERGENCY_CONTACT_NAME__C 		--Emergency Contact
, EMERGENCY_CONTACT_PHONE__C		--Emergency Contact
, EMERGENCY_CONTACT_RELATIONSHIP__C	--Emergency Contact
, VISA_TYPE_DEL__C					--Visa Type
, VISA_NOTES__C						--Visa Notes
, VISA_EXPIRY_DEL__C				--Renewal Date
from Candidate

select external_id
, emergency_name
, emergency_phone
, emergency_relationship
, visa_type
, visa_note
, visa_renewal_date
from candidate

/* 5. Custom fields */
select ID
, WHS_COMPLETION_DATE__C 			--WHS COMPLETION DATE
, DIVISION__C						--DIVISION
, WHS_MODULES_COMPLETED__C			--WHS MODULES COMPLETED
, WORKPRO_CIN_DEL__C				--WORPRO CIN
, WORKPRO_PIN_DEL__C				--WORPRO PIN
, NOTICE_PERIOD__C					--NOTICE PERIOD
, DATE_REGISTERED__C				--REGISTERED DATE
, PREFERRED_EMPLOYMENT_TYPE__C		--EMPLOYMENT TYPE
, PREFERRED_SHIFT__C				--PREFERRED SHIFT
, DO_YOU_HAVE_A_VALID_DRIVERS_LICENSE__C
, DO_YOU_HAVE_A_VALID_WHITECARD__C
, ANY_DISABILITIES__C
, DO_YOU_HAVE_A_CAR__C
, CANDIDATE_RATING_CONSULTANT__C
, ANY_MEDICATIONS__C
, CRIMINAL_RECORD__C
, AUTHORISATION_TO_CONTACT_YOUR_REFERENCES__C
, WHS_EXPIRY_DATE__C
, DO_YOU_HAVE_SAFETY_BOOTS__C
, TYPING__C
, NUMERIC__C
, CURRENT_HOURLY_SALARY__C
, DESIRED_HOURLY_SALARY__C
, TYPING_ACCURACY__C
, NUMERIC_ACCURACY__C
, CURRENT_TEAM_SIZE__C
, FORKLIFT_LICENSE__C
from Candidate 

/* 6. Candidate Reference | DATE */
select ID
, AVAILABILITY_MIRROR__C
, CURRENT_CONTRACT_END_DATE__C
from Candidate

--Mapping



/* 7. Preferred Name */
select ID as Sirius_candidateExtID
, PREFERRED_NAME__C as Sirius_PrefferedName
from Candidate
where PREFERRED_NAME__C is not NULL

--Mapping
select nickname from candidate
