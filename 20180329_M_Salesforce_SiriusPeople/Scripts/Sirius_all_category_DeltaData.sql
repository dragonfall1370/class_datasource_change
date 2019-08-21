----- DELTA DATA on 22 Feb 2018
----------------------------
--SEC 1: Company Delta
----------------------------
with
--DUPLICATION REGCONITION
dup as (SELECT ID, replace(NAME,'%','') as NAME, ROW_NUMBER() OVER(PARTITION BY replace(NAME,'%','') ORDER BY ID ASC) AS rn 
FROM CompanyDelta)

--TERM OF BUSINESS --added as company note
, CompTermBusiness as (select CLIENT__C
	, STRING_AGG(concat(NAME,' - ',CREATEDDATE), ', ') as CompTermBusiness
	from TermBusiness
	group by CLIENT__C)

, TermBusinessFinal as (select tb.CLIENT__C
	, STRING_AGG(a.NAME, ',') as TermBusinessFinal
	from (select distinct CLIENT__C from TermBusiness) tb
	left join Attachments a on a.PARENTID = tb.CLIENT__C
	group by tb.CLIENT__C)

--COMPANY ATTACHMENTS /* Support from SQL Server 2017 */
, CompAttachments as (select PARENTID
	, STRING_AGG(NAME, ', ') as CompAttachments
	from Attachments
	where NAME like '.%doc' or NAME like '%.pdf' or NAME like '%.rtf' or NAME like '%.xls%' or NAME like '%.html'
	group by PARENTID)

--MAIN SCRIPT
select c.ID as 'company-externalId'
, iif(c.ID in (select ID from dup where dup.rn > 1)
	, iif(dup.NAME = '' or dup.NAME is NULL,concat('Company name -',dup.ID),concat(dup.NAME,'-DUPLICATE-',dup.ID))
	, iif(c.NAME = '' or c.NAME is null,concat('Company name -',dup.ID),replace(c.NAME,'%',''))) as 'company-name'
--, c.NAME as CompanyOriginal
, ltrim(rtrim(c.PHONE)) as 'company-phone'
, left(ltrim(rtrim(c.WEBSITE)),100) as 'company-website'
, nullif(ltrim(stuff((coalesce(' ' + nullif(rtrim(ltrim(c.BILLINGSTREET)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGSTATE)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGCITY)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGPOSTALCODE)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGCOUNTRY)),''),'')),1,1,'')),'') as 'company-locationName'
, ltrim(stuff((coalesce(' ' + nullif(rtrim(ltrim(c.BILLINGSTREET)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGSTATE)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGCITY)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGPOSTALCODE)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.BILLINGCOUNTRY)),''),'')),1,1,'')) as 'company-locationAddress'  --Remove Billing Address prefix in PROD script
, c.BILLINGCITY as 'company-locationCity'
, c.BILLINGSTATE as 'company-locationState'
, c.BILLINGPOSTALCODE as 'company-locationZipCode'
, case
	when c.BILLINGCOUNTRY in ('USA','United States') then 'US'
	when c.BILLINGCOUNTRY in ('New Zealand','NZ') then 'NZ'
	when c.BILLINGCOUNTRY = 'Hong Kong' then 'HK'
	when c.BILLINGCOUNTRY = 'Malaysia' then 'MY'
	when c.BILLINGCOUNTRY in ('England','United Kingdom') then 'GB'
	when c.BILLINGCOUNTRY in ('Sinapore','Singapore') then 'SG'
	when c.BILLINGCOUNTRY = 'Aruba' then 'AW'
	when c.BILLINGCOUNTRY = 'Bahamas' then 'BS'
	when c.BILLINGCOUNTRY in ('AUSTRALIA','Sydney','Australua') then 'AU'
	when c.BILLINGCOUNTRY = 'India' then 'IN'
	when c.BILLINGCOUNTRY = 'Austria' then 'AT'
	when c.BILLINGCOUNTRY = 'Canada' then 'CA'
	when c.BILLINGCOUNTRY = 'Ireland' then 'IE'
	when c.BILLINGCOUNTRY = 'japan' then 'JP'
	when c.BILLINGCOUNTRY = 'Papua New Guinea' then 'PG'
	when c.BILLINGCOUNTRY = 'Cambodia' then 'KH'
	when c.BILLINGCOUNTRY = 'South Africa' then 'ZA'
	else NULL end as 'company-locationCountry'
, concat(coalesce(ltrim(su.EMAIL),''), coalesce(',' + ltrim(su2.EMAIL),'')) as 'company-owners'
, stuff((coalesce(',' + nullif(ltrim(ca.CompAttachments),''),'') + coalesce(',' + nullif(ltrim(tbf.TermBusinessFinal),''),'')
	), 1, 1,'') as 'company-document'
, concat('Company External ID: ', c.ID, char(10)
	, coalesce('Parent ID: ' + c.PARENTID + char(10),'')
	, coalesce('Description: ' + c.DESCRIPTION + char(10),'')
	, coalesce('Last activity date: ' + convert(varchar(20),c.LASTACTIVITYDATE,120) + char(10),'')
	, coalesce('Division: ' + c.DIVISION__C + char(10),'')
	, coalesce('Business Number - ABN: ' + c.ABN_ACN__C + char(10),'')
	, coalesce('Company Industry: ' + c.INDUSTRY_SECTORS__C + char(10),'')
	, coalesce('Awards: ' + c.MODERN_AWARD__C + char(10),'')
	, coalesce('Special Term Conditions: ' + c.SPECIAL_TERMS_CONDITIONS__C + char(10),'')
	, coalesce('Workplace OHS Assessment Completed: ' + c.WORKPLACE_OHS_ASSESSMENT_COMPLETED__C + char(10),'')
	, coalesce('Workplace OHS Date Completed: ' + convert(varchar(20),c.WORKPLACE_OHS_DATE_COMPLETED__C,120) + char(10),'')
	, coalesce('Company ID Hidden: ' + c.COMPANY_ID_HIDDEN__C + char(10),'')
	, coalesce('Terms of Business: ' + nullif(ctb.CompTermBusiness,''),'')
) as 'company-note'
from CompanyDelta c
left join dup on dup.ID = c.ID
left join SiriusUsers su on su.ID = c.OWNERID
left join SiriusUsers su2 on su2.ID = c.ACCOUNT_MANAGER__C
left join CompTermBusiness ctb on ctb.CLIENT__C = c.ID
left join CompAttachments ca on ca.PARENTID = c.ID
left join TermBusinessFinal tbf on tbf.CLIENT__C = c.ID

----------------------------
--SEC 2: Contact Delta
----------------------------
with 
--MAIL DUPLICATION
UnionEmail as (select ID, replace(replace(replace(EMAIL,'%',''),'..@','@'),'.@','@') as EMAIL from ContactDelta where EMAIL is not NULL
UNION ALL
select ID, replace(replace(replace(PEOPLECLOUD1__HOME_EMAIL__C,'%',''),'..@','@'),'.@','@') from ContactDelta where PEOPLECLOUD1__HOME_EMAIL__C is not NULL
UNION ALL
select ID, replace(replace(replace(PEOPLECLOUD1__WORK_EMAIL__C,'%',''),'..@','@'),'.@','@') from ContactDelta where PEOPLECLOUD1__WORK_EMAIL__C is not NULL
)

, UnionEmailDistinct as (select ID, EMAIL from UnionEmail group by ID, Email)

, dup as (SELECT ID, EMAIL, ROW_NUMBER() OVER(PARTITION BY EMAIL ORDER BY ID ASC) AS rn 
FROM UnionEmailDistinct where EMAIL is not NULL)

/* Support from SQL Server 2017 */
, ContactEmail as (select ID
	, STRING_AGG(case when rn > 1 then concat(rn,'_duplicate_',replace(replace(replace(EMAIL,'%',''),'..@','@'),'.@','@'))
	else replace(replace(replace(EMAIL,'%',''),'..@','@'),'.@','@') end, ', ') as ContactEmail
from dup
group by ID)

--CONTACT FILES
, ContactFiles as (select PEOPLECLOUD1__DOCUMENT_RELATED_TO__C
	, STRING_AGG(NAME, ', ') as ContactFiles
	from ResumeComplianceDelta
	where NAME like '.%doc' or NAME like '%.pdf' or NAME like '%.rtf' or NAME like '%.xls%' or NAME like '%.html'
	group by PEOPLECLOUD1__DOCUMENT_RELATED_TO__C)

--COMPANY ATTACHMENTS /* Support from SQL Server 2017 */
, ConAttachments as (select PARENTID
	, STRING_AGG(NAME, ', ') as ConAttachments
	from Attachments
	where NAME like '.%doc' or NAME like '%.pdf' or NAME like '%.rtf' or NAME like '%.xls%' or NAME like '%.html'
	group by PARENTID)
	
/* Support from SQL Server 2017 */

--CONTACT OWNERS
, MergeContactOwners as (select c.ID
	, CONCAT_WS(';',u.NAME, CONTACT_OWNER_SBS__C, CONTACT_OWNERSSM__C, CONTACT_OWNER_STSSTM__C, CONTACT_OWNER_IND__C
		, CONTACT_OWNER_SAAF__C, TECH_CONTACT_OWNER_CONTRACT__C, TECH_CONTACT_OWNER_PERM__C) as ContactOwners
	from ContactDelta c
	left join SiriusUsers u on c.OWNERID = u.ID)

, SplitContactOwners as (select ID as ContactID, ContactOwners, value as SplitContactOwners
	from MergeContactOwners
	cross apply string_split(ContactOwners,';'))

, ContactOwners as (select ContactID, ltrim(rtrim(SplitContactOwners)) as SplitContactOwners
	from SplitContactOwners
	group by ContactID, SplitContactOwners)

, ContactOwnersFinal as (select co.ContactID, STRING_AGG(su.EMAIL, ',') as ContactOwnersFinal
	from ContactOwners co
	left join SiriusUsers su on su.NAME = co.SplitContactOwners
	group by co.ContactID)

--MAIN SCRIPT
select c.ID as 'contact-externalId'
, case when c.ACCOUNTID is NULL or c.ACCOUNTID = '' then 'SP999999999' 
	else c.ACCOUNTID end as 'contact-companyId'
, case when c.LASTNAME = '' or c.LASTNAME is NULL then 'Lastname'
	else c.LASTNAME end as 'contact-lastName'
, case when c.FIRSTNAME = '' or c.FIRSTNAME is NULL then 'Firstname'
	else c.FIRSTNAME end as 'contact-firstName'
, c.TITLE as 'contact-jobTitle'
, ce.ContactEmail as 'contact-email'
, left(c.LINKEDIN_PROFILE__C,200) as 'contact-linkedIn'
, cof.ContactOwnersFinal as 'contact-owners'
, stuff(concat(coalesce(',' + ltrim(c.PHONE),''), coalesce(',' + ltrim(c.MOBILEPHONE),'')),1,1,'') as 'contact-phone'
, concat('Contact External ID: ',c.ID,char(10)
	, coalesce('Salutation: ' + c.SALUTATION + char(10),'')
	, 'Do not call: ',c.DONOTCALL,char(10)
	, 'Contact status: ',c.CONTACT_STATUS__C,char(10)
	, coalesce('No of perm staff: ' + c.NO_PERM_STAFF_IN_TEAM__C + char(10),'')
	, coalesce('No of contractors: ' + c.NO_CONTRACTORS_IN_TEAM__C + char(10),'')
	, coalesce('Contact Industry: ' + c.INDUSTRY_SECTORS__C + char(10),'')
	, coalesce('No of temps: ' + c.NO_TEMPS_IN_TEAM__C + char(10),'')
	, coalesce('Do not Contact Reason: ' + c.DO_NOT_CONTACT_REASON__C + char(10),'')
	, coalesce('Desk: ' + c.DESK__C + char(10),'')
	, coalesce('No of how many people in your team: ' + c.HOW_MANY_PEOPLE_IN_YOUR_TEAM__C + char(10),'')
	, coalesce('Current team size: '+ c.CURRENT_TEAM_SIZE__C + char(10),'')
	, coalesce('Accouting finance: ' + c.ACCOUNTING_FINANCE__C + char(10),'')
	, coalesce('Development qualification: ' + c.DEVELOPMENT_QUALIFICATION__C + char(10),'')
	, coalesce('Infrastructure qualification: '  + c.INFRASTRUCTURE_QUALIFICATION__C + char(10),'')
	, coalesce('BI data CRM qualification: ' + c.BI_DATA_CRM_QUALIFICATION__C + char(10),'')
	, coalesce('Project services qualification: ' + c.PROJECT_SERVICES_QUALIFICATION__C + char(10),'')
	, coalesce('Support: '+ c.SUPPORT__C + char(10),'')
	, coalesce('Industrious: ' + c.INDUSTRIOUS__C + char(10),'')
	, coalesce('SSM: '+ c.SSM__C + char(10),'')
	, coalesce('Companies packages: ' + c.COMPANIES_PACKAGES__C + char(10),'')
	, coalesce('Digital qualification: ' + c.DIGITAL_QUALIFICATION__C + char(10),'')
	, coalesce('Reports To ID: ' + c.REPORTSTOID,'')
) as 'contact-note'
, stuff((coalesce(',' + nullif(ltrim(cf.ContactFiles),''),'') + coalesce(',' + nullif(ltrim(ca.ConAttachments),''),'')
	), 1, 1,'') as 'contact-document'
from ContactDelta c
left join ContactOwnersFinal cof on cof.ContactID = c.ID
left join ContactEmail ce on ce.ID = c.ID
left join ContactFiles cf on cf.PEOPLECLOUD1__DOCUMENT_RELATED_TO__C = c.ID
left join ConAttachments ca on ca.PARENTID = c.ID
where c.ID not in (select ID from Contact)

----------------------------
--SEC 3: Candidate Delta
----------------------------
with
--MAIL DUPLICATION
dup as (SELECT ID, EMAIL, ROW_NUMBER() OVER(PARTITION BY EMAIL ORDER BY ID ASC) AS rn 
FROM CandidateDelta where EMAIL is not NULL)

, CandUnionEmail as (select ID, PEOPLECLOUD1__HOME_EMAIL__C as EMAIL from CandidateDelta where PEOPLECLOUD1__HOME_EMAIL__C is not NULL
UNION ALL
select ID, PEOPLECLOUD1__WORK_EMAIL__C from CandidateDelta where PEOPLECLOUD1__WORK_EMAIL__C is not NULL)

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
	from (select PEOPLECLOUD1__DOCUMENT_RELATED_TO__C, NAME from ResumeCompliance UNION ALL select PEOPLECLOUD1__DOCUMENT_RELATED_TO__C, NAME from ResumeComplianceDelta) as A
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
, ltrim(stuff((coalesce(', ' + nullif(rtrim(ltrim(c.MAILINGSTREET)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.MAILINGCITY)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.MAILINGSTATE)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.MAILINGPOSTALCODE)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.MAILINGCOUNTRY)),''),'')),1,2,'')) as 'candidate-address' --remove Mailing Address prefix from candidate address
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
	), 1, 1,'') as 'contact-owners'
, c.DESK__C as 'candidate-keyword'
, 'AUD' as 'candidate-currency'
--, c.CURRENT_DAILY_RATE__C as 'candidate-contractRate'

, case when c.PEOPLECLOUD1__GENDER__C = 'Male' then 'MALE'
	when c.PEOPLECLOUD1__GENDER__C = 'Female' then 'FEMALE'
	else '' end as 'candidate-gender'
, convert(varchar(20),c.BIRTHDATE,120) as 'candidate-dob'

, cs.CandSkills as 'candidate-skills'
, stuff((coalesce(',' + nullif(ltrim(cf.CandidateFiles),''),'') + coalesce(',' + nullif(ltrim(ca.CandAttachments),''),'')
	), 1, 1,'') as 'candidate-resume'

--CANDIDATE WORK HISTORY
, concat(coalesce('Current Employment type: ' + c.CURRENT_EMPLOYMENT_TYPE__C + char(10),'')
	, coalesce('Current Employer: ' + c.CURRENT_EMPLOYER__C + char(10),'')
	, coalesce('Job title: ' + c.TITLE + char(10),'')
    ) as 'candidate-workHistory'
, c.TITLE as 'candidate-jobTitle1'
, c.CURRENT_EMPLOYER__C as 'candidate-employer1'

--CANDIDATE NOTES
, concat('Candidate External ID: ',c.ID,char(10)
	, coalesce('Candidate Account ID: ' + c.ACCOUNTID + char(10),'')
	, coalesce('Record Type ID: ' + c.RECORDTYPEID + char(10),'')
	, coalesce('Created: ' + convert(varchar(20),c.CREATEDDATE,120) + char(10),'')
	, coalesce('Candidate Preferred Name: ' + c.PREFERRED_NAME__C + char(10),'')
	, coalesce('Description: ' + c.CANDIDATE_DESCRIPTION__C + char(10),'')
    , coalesce('Home Email: ' + c.PEOPLECLOUD1__HOME_EMAIL__C + char(10),'')
	, coalesce('Do not contact reason: ' + c.DO_NOT_CONTACT_REASON__C + char(10),'')
    , coalesce('Ideal Position: ' + c.IDEAL_POSITION__C + char(10),'')
	, coalesce('Annual Salary: ' + c.CURRENT_ANNUAL_SALARY__C + char(10),'')
	, coalesce('Desired Annual Salary: ' + c.DESIRED_ANNUAL_SALARY__C + char(10),'')
	, coalesce('Desired Rate Salary: ' + c.DESIRED_DAILY_SALARY__C + char(10),'')
	, coalesce('Contract Rate: ' + c.CURRENT_DAILY_RATE__C + char(10),'')
	, coalesce('If yes to the above please explain: ' + c.IF_YES_TO_THE_ABOVE_PLEASE_EXPLAIN__C + char(10),'')
	, coalesce('Availability start: ' + convert(varchar(20),c.AVAILABILITY_MIRROR__C,120) + char(10),'')
	, coalesce('Current Contract End Date: ' + convert(varchar(20),c.CURRENT_CONTRACT_END_DATE__C,120),'')
	) as 'candidate-note'
from CandidateDelta c
left join dup on dup.ID = c.ID
left join CandWorkEmail cw on cw.ID = c.ID
left join SiriusUsers su on su.ID = c.OWNERID
left join SiriusUsers su2 on su2.NAME = c.HOTLIST__C
left join SiriusUsers su3 on su3.NAME = c.REGISTERED_BY_PICKLIST__C
left join CandSkills cs on cs.PEOPLECLOUD1__CANDIDATE__C = c.ID
left join CandidateFiles cf on cf.PEOPLECLOUD1__DOCUMENT_RELATED_TO__C = c.ID
left join CandAttachments ca on ca.PARENTID = c.ID
where c.ID not in (select ID from Candidate)

----------------------------
--SEC 4: JOB Delta
----------------------------
--DUPLICATION REGCONITION
with dup as (SELECT ID, NAME, ROW_NUMBER() OVER(PARTITION BY NAME ORDER BY ID ASC) AS rn 
	FROM JobsDelta)

--MAX JOB ADS - 1 job may have multiple job ads, get the latest job ads name
, MaxJobAds as (select PEOPLECLOUD1__VACANCY__C, max(NAME) as LatestAds
    from AdsDelta 
	where PEOPLECLOUD1__VACANCY__C is not NULL
    group by PEOPLECLOUD1__VACANCY__C)

, JobAds as (select ma.PEOPLECLOUD1__VACANCY__C, ma.LatestAds, a.PEOPLECLOUD1__JOB_CONTENT__C
    from MaxJobAds ma
    left join AdsDelta a on a.NAME = ma.LatestAds)

--CONTACT EXISTING AND DELTA
, UnionContacts as (select ID, ACCOUNTID, FIRSTNAME, LASTNAME from Contact
	UNION ALL
	select ID, ACCOUNTID, FIRSTNAME, LASTNAME from ContactDelta
	where ID not in (select ID from Contact))

--COMPANY EXISTING AND DELTA
, UnionCompany as (select ID, NAME from Company
	UNION ALL
	select ID, NAME from CompanyDelta)

--CANDIDATE EXISTING AND DELTA
, UnionCandidate as (select ID, FIRSTNAME, LASTNAME from Candidate
	UNION ALL
	select ID, FIRSTNAME, LASTNAME from CandidateDelta
	where ID not in (select ID from Candidate))

--CONTACT WITHOUT BEING LISTED IN CONTACT TABLE
, ContactNotFound as (select CLIENT_CONTACT__C 
    from JobsDelta
    where CLIENT_CONTACT__C not in (select ID from UnionContacts))

--COMPARISON BW JOB COMPANY AND CONTACT COMPANY -> if different, get DEFAULT CONTACT within JOB COMPANY
, JobCompContactComp as (select j.ID as JobID
    , j.PEOPLECLOUD1__COMPANY__C as JobCompany
    , j.CLIENT_CONTACT__C as JobContact
    , c.ACCOUNTID as ContactCompany
    from JobsDelta j
	left join UnionContacts c on c.ID = j.CLIENT_CONTACT__C)

, JobCompanyDiff as (select JobID
    , JobCompany
    , JobContact
    , ContactCompany
    from JobCompContactComp
    where JobCompany <> ContactCompany)

--MAIN SCRIPT
select
case 
	when j.ID in (select JobID from JobCompanyDiff) and j.PEOPLECLOUD1__COMPANY__C is not NULL then concat(j.PEOPLECLOUD1__COMPANY__C,'-DC')
	when j.CLIENT_CONTACT__C in (select CLIENT_CONTACT__C from ContactNotFound) and j.PEOPLECLOUD1__COMPANY__C is not NULL then concat(j.PEOPLECLOUD1__COMPANY__C,'-DC')
	when j.CLIENT_CONTACT__C = '' and j.PEOPLECLOUD1__COMPANY__C is not NULL then concat(j.PEOPLECLOUD1__COMPANY__C,'-DC')
	when j.CLIENT_CONTACT__C is NULL and j.PEOPLECLOUD1__COMPANY__C is not NULL then concat(j.PEOPLECLOUD1__COMPANY__C,'-DC')
    when j.PEOPLECLOUD1__COMPANY__C is NULL and j.CLIENT_CONTACT__C is NULL then 'SP999999999'
	else j.CLIENT_CONTACT__C end as 'position-contactId'

, j.ID as 'position-externalId'
, case when exists (select ID from dup where dup.rn > 1 and j.ID = ID) AND exists (select JobID from JobCompanyDiff where j.ID = JobID) AND (dup.NAME is not NULL) then concat(dup.NAME,' - ',con.LASTNAME,' ',con.FIRSTNAME,' - ',dup.ID)
    when exists (select ID from dup where dup.rn > 1 and j.ID = ID) AND exists (select JobID from JobCompanyDiff where j.ID = JobID) AND (dup.NAME = '' or dup.NAME is NULL) then concat('No job title - ',con.LASTNAME,' ',con.FIRSTNAME,' - ',dup.ID)
    when exists (select JobID from JobCompanyDiff where j.ID = JobID) AND j.NAME is not NULL then concat(j.NAME,' - ',con.LASTNAME,' ',con.FIRSTNAME)
    when j.NAME = '' or j.NAME is null then concat('No job title -',j.ID)
    else j.NAME end as 'position-title'
, j.NUMBER_OF_POSITIONS__C as 'position-headcount'
, convert(varchar(10),replace(replace(j.CREATEDDATE,'T',' '),'.000Z',''),120) as 'position-startDate'

--END DATE JOB SHOULD BE USED IN 3 CASES
, case when j.CLOSED_DATE__C is NULL and j.PEOPLECLOUD1__END_DATE__C is not NULL then j.CLOSED_DATE__C
	when j.EXPECTED_CLOSE_DATE__C is NULL and j.PEOPLECLOUD1__END_DATE__C is not NULL then j.PEOPLECLOUD1__END_DATE__C
	when j.PEOPLECLOUD1__END_DATE__C is NULL and  j.EXPECTED_CLOSE_DATE__C is not NULL then j.EXPECTED_CLOSE_DATE__C
    when j.EXPECTED_CLOSE_DATE__C is NULL and j.PEOPLECLOUD1__END_DATE__C is NULL and j.VACANCY_STATUS__C = 'Closed' then convert(varchar(20),getdate() - 1,120)
    else convert(varchar(10),j.PEOPLECLOUD1__END_DATE__C,120) end as 'position-endDate'

, j.PEOPLECLOUD1__BASE_SALARY__C as 'position-actualSalary'
, 'AUD' as 'position-currency'
, concat(coalesce(ltrim(su.EMAIL),''), coalesce(',' + ltrim(su2.EMAIL),'')) as 'position-owners'
, ja.PEOPLECLOUD1__JOB_CONTENT__C as 'position-publicDescription'
, case when j.JOB_TYPE__C = 'Full-Time' then 'FULL_TIME'
	when j.JOB_TYPE__C = 'Part-Time' then 'PART_TIME'
	else '' end as 'position-employmentType'
, case when j.RECORD_TYPE_NAME__C in ('Advertisement (Permanent)','Internal Vacancy','Permanent Vacancy','Replacement Vacancy') then 'PERMANENT'
	when j.RECORD_TYPE_NAME__C in ('Unqualified Vacancy','Temporary Vacancy','Advertisement (Temporary)','Temporary Vacancy Over 3 Months') then 'TEMPORARY'
	when j.RECORD_TYPE_NAME__C in ('Advertisement (Contract)','Contract Vacancy','Fixed Term Contract Vacancy') then 'CONTRACT'
	when j.RECORD_TYPE_NAME__C in ('Temp to Perm Vacancy') then 'TEMPORARY_TO_PERMANENT'
	else 'PERMANENT' end as 'position-type'
, concat('Job External ID: ',j.ID,char(10)
	, coalesce('Job status: ' + j.VACANCY_STATUS__C + char(10),'')
	, coalesce('Company: ' + com.NAME + ' - ' + j.PEOPLECLOUD1__COMPANY__C + char(10),'')
	, coalesce('Placed Candidate: ' + c.LASTNAME + ' ' + c.FIRSTNAME + ' - ' + j.PEOPLECLOUD1__PLACED_CANDIDATE__C + char(10),'')
	, coalesce('Contact: ' + con.LASTNAME + ' ' + con.FIRSTNAME + ' - ' + j.CLIENT_CONTACT__C + char(10),'')
    , coalesce('Candidate charge rate: ' + j.PEOPLECLOUD1__CANDIDATE_CHARGE_RATE__C + char(10),'')
    , coalesce('Client charge rate: ' + j.PEOPLECLOUD1__CLIENT_CHARGE_RATE__C + char(10),'')
	, coalesce('Flat Fee: ' + j.PEOPLECLOUD1__FLAT_FEE__C + char(10),'')
	, coalesce('Super: ' + j.PEOPLECLOUD1__SUPER__C + char(10),'')
	, coalesce('Total Package: ' + j.PEOPLECLOUD1__TOTAL_PACKAGE__C + char(10),'')
	, coalesce('Division: ' + j.DIVISION__C + char(10),'')
	, coalesce('Resourcer: ' + j.RESOURCER__C + char(10),'')
	, coalesce('Hours of Work: ' + j.HOURS_OF_WORK__C + char(10),'')
	, coalesce('Days: ' + j.DAYS__C + char(10),'')
	, coalesce('Fee Based On: ' + j.FEE_BASED_ON__C + char(10),'')
	, coalesce('Estimated Vacancy Value: ' + j.ESTIMATED_VACANCY_VALUE__C + char(10),'')
	, coalesce('Pro Rate Months: ' + j.PRO_RATA_MONTHS__C + char(10),'')
	, coalesce('On Cost: ' + j.ONCOST__C + char(10),'')
	, coalesce('On Cost Value: ' + j.ONCOST_VALUE__C + char(10),'')
	, coalesce('Margin: ' + j.MARGIN__C + char(10),'')
	, coalesce('Margin Percentage: ' + j.MARGIN_PERCENTAGE__C + char(10),'')
	, coalesce('Expected Close Date: ' + convert(varchar(20),j.EXPECTED_CLOSE_DATE__C,120) + char(10),'')
	, coalesce('Job Number: ' + j.JOB_NUMBER__C + char(10),'')
	, coalesce('Rate Type: ' + j.RATE_TYPE__C + char(10),'')
	, coalesce('Hours per Day: ' + j.HOURS_PER_DAY__C + char(10),'')
	, coalesce('Days per Week: ' + convert(varchar(max),j.DAYS_PER_WEEK__C) + char(10),'')
	, coalesce('Weekly Margin: ' + j.WEEKLY_MARGIN__C + char(10),'')
	, coalesce('Estimated Contract Value: ' + j.ESTIMATED_CONTRACT_VALUE__C + char(10),'')
	, coalesce('Estimated Temp Value: ' + j.ESTIMATED_TEMP_VALUE__C + char(10),'')
	, coalesce('Job Picked Up Passed: ' + j.JOB_PICKED_UP_PASSED__C + char(10),'')
	, coalesce('Total Package: ' + j.TOTAL_PACKAGE_1__C + char(10),'')
	, coalesce('Calculated Total Package: ' + convert(varchar(max),j.CALCULATED_TOTAL_PACKAGE__C) + char(10),'')
	, coalesce('Consultant Forecast Percentage: ' + j.CONSULTANT_FORECAST_PERCENTAGE__C + char(10),'')
	, coalesce('Forecast Value Consultant: ' + j.FORECAST_VALUE_CONSULTANT__C + char(10),'')
	, coalesce('Replacement Vacancy: ' + j.REPLACEMENT_VACANCY__C + char(10),'')
	, coalesce('Fore cast notes: ' + j.FORECAST_NOTES__C + char(10),'')
) as 'position-note'
from JobsDelta j
left join JobAds ja on ja.PEOPLECLOUD1__VACANCY__C = j.ID
left join dup on dup.ID = j.ID
left join SiriusUsers su on su.ID = j.OWNERID
left join SiriusUsers su2 on su2.ID = j.CONSULTANT__C
left join UnionCompany com on com.ID = j.PEOPLECLOUD1__COMPANY__C
left join UnionCandidate c on c.ID = j.PEOPLECLOUD1__PLACED_CANDIDATE__C
left join UnionContacts con on con.ID = j.CLIENT_CONTACT__C
where j.ID not in (select ID from Jobs)

-->>>> UPDATE JOB PLACEMENTS AS ACTIVITY
select * from activity
where position_id > 0
and content like '%Job Application ID%'

select * from activity_job
where activity_id in (select id from activity
where position_id > 0
and content like '%Job Application ID%')


----------------------------
--SEC 4: JOB Application Delta
----------------------------
with OriginalJobApp as (
select PEOPLECLOUD1__PLACEMENT__C as A
, PEOPLECLOUD1__CANDIDATE__C as B
, case when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Skills Testing' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Possible / Follow Up' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Shortlist' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Unsuitable - Culture Fit' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Unsuitable - Consider for Other Roles' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'New - Awaiting Approval' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Pre-Employment Screening' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Pre-Employment Screening - Did Not Pass' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Skills Testing - Unsuitable - Skills' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen - Candidate Not Interested - Length of Role' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen - Unsuitable - Already Applied Directly' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen - Unsuitable - Already Applied Through Agency' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen - Unsuitable - Communication Skills' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen - Unsuitable - Salary Too High' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen - Unsuitable - Salary Too Low' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Telephone Screen - Unsuitable - Skills' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'Right to Represent' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent - Unsuitable - Consider for Other Roles' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent - Awaiting Approval' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent - Candidate Not Interested' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent - Client Not Interested - Experience' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent - Client Not Interested - Industry' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent - Unsuitable' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'CV Sent - Unsuitable - Experience' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'Right to Represent - Offer Withdrawn by Client' then '2'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Invoiced' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Awaiting Approval' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Cancelled' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Placement Shortened' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approved Awaiting Invoicing' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Replaced after Placement' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approved' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Awaiting Manager Approval' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approved Manager' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Awaiting Approval' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Awaiting Approval Invoicing' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Placement Shortened' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approved Awaiting Invoicing' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Invoiced' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Cancelled' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Replaced after Placement' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Awaiting Manager Approval' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approved' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approval Rejected Manager' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approval Rejected Finance' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approved Manager' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Temp' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Permanent' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Approval Rejected Contracts' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Placed - Temp to Perm' then '6'
	when STATUS_CANDIDATE_PROGRESS__C = 'Offer' then '5'
	when STATUS_CANDIDATE_PROGRESS__C = 'Withdrawn - Offer Withdrawn by Client' then '5'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Candidate Not Interested' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Candidate Not Interested - Found Other Employment' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Candidate Not Interested - Salary' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Candidate Withdrew from Process' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Unsuitable - Communication Skills' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Unsuitable - Experience' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Internal / Skype Interview - Unsuitable - Skills' then '1'
	when STATUS_CANDIDATE_PROGRESS__C = 'Client Interview (2nd)' then '4'
	when STATUS_CANDIDATE_PROGRESS__C = 'Client Interview (3rd+)' then '4'
	when STATUS_CANDIDATE_PROGRESS__C = 'Client Interview (2nd) - Candidate Not Interested - Found Other Employment' then '4'
	when STATUS_CANDIDATE_PROGRESS__C = 'Client Interview (1st)' then '3'
	when STATUS_CANDIDATE_PROGRESS__C = 'Reference Check' then '3'
	when STATUS_CANDIDATE_PROGRESS__C = 'Interviewed - Client Withdrew' then '3'
	when STATUS_CANDIDATE_PROGRESS__C = 'Interviewed - Candidate Withdrew' then '3'
	when STATUS_CANDIDATE_PROGRESS__C = 'Client Interview (1st) - Candidate Withdrew from Process' then '3'
	when STATUS_CANDIDATE_PROGRESS__C = 'Left Message - Unsuitable - Culture Fit' then '1'
	else 0 end as C
from CandidateManagementDelta
where PEOPLECLOUD1__PLACEMENT__C is not NULL and PEOPLECLOUD1__CANDIDATE__C is not NULL)

, maxJobApp as (select A, B, max(C) as maxJobApp
	from OriginalJobApp
	where C > 0
	group by A, B)

select B as 'application-candidateExternalId'
, A as 'application-positionExternalId'
, case maxJobApp
	when 6 then 'PLACED'
	when 5 then 'OFFERED'
	when 4 then 'SECOND_INTERVIEW'
	when 3 then 'FIRST_INTERVIEW'
	when 2 then 'SENT'
	when 1 then 'SHORTLISTED'
	else '' end as 'application-stage'
from maxJobApp