--CANDIDATE PRIMARY EMAIL
with 
SplitEmail as (select distinct cn_id
	, translate (value, '!'':"<>[]();,', '            ') as SplitEmail --to translate special characters
	from candidate.Candidates
	cross apply string_split(cn_cont_email,' ')
	where cn_cont_email like '%_@_%.__%'
	and can_type = 1 --CANDIDATE type
	)

, dup as (select cn_id
	, trim(' ' from SplitEmail) as EmailAddress
	, row_number() over(partition by trim(' ' from SplitEmail) order by cn_id asc) as rn --distinct email if emails exist more than once
	, row_number() over(partition by cn_id order by trim(' ' from SplitEmail)) as Contactrn --distinct if contacts may have more than 1 email
	from SplitEmail
	where SplitEmail like '%_@_%.__%'
	)

, PrimaryEmail as (select cn_id
	, case when rn > 1 then concat(rn,'_',EmailAddress)
	else EmailAddress end as PrimaryEmail
	from dup
	where EmailAddress is not NULL and EmailAddress <> ''
	and Contactrn = 1)

--CANDIDATE WORK EMAIL
, SplitEmail2 as (select distinct cn_id
	, translate(value, '!'':"<>[]();,', '            ') as SplitEmail2 --to translate special characters
	from candidate.Candidates
	cross apply string_split(cn_cont_email2,' ')
	where cn_cont_email2 like '%_@_%.__%'
	and can_type = 1 --CONTACT type
	)

, dup2 as (select cn_id
	, trim(' ' from SplitEmail2) as EmailAddress2
	, row_number() over(partition by trim(' ' from SplitEmail2) order by cn_id asc) as rn
	, row_number() over(partition by cn_id order by trim(' ' from SplitEmail2)) as Contactrn
	from SplitEmail2
	where SplitEmail2 like '%_@_%.__%'
	)

, WorkEmail as (select cn_id
	, case when rn > 1 then concat(rn,'_',EmailAddress2)
	else EmailAddress2 end as WorkEmail
	from dup2
	where EmailAddress2 is not NULL and EmailAddress2 <> ''
	and Contactrn = 1)

--CANDIDATE CURRENT ADDRESS
, MaxAddressID as (select cn_id, max(address_id) as maxAddressID
	from candidate.Addresses
	where IsDeleted = 0 --valid addresses
	and address_type = 0 --2 types of Candidate addresses
	and exists (select cn_id from candidate.Candidates where can_type = 1 and cn_id = candidate.Addresses.cn_id)
	group by cn_id
	)

, CurrentAddress as (select m.cn_id, m.maxAddressID
	, ca.cn_address_street, ca.cn_address_subdist
	, ca.cn_address_dist
	, ca.cn_address_province, cl.listvalue as province
	, ca.cn_address_postalcode
	, ca.cn_address_pro_other
	, ca.cn_country
	, case when ca.cn_country = 'Afghanistan' then 'AF'
		when ca.cn_country = 'Albania' then 'AL'
		when ca.cn_country = 'Algeria' then 'DZ'
		when ca.cn_country = 'Andorra' then 'AD'
		when ca.cn_country = 'Angola' then 'AO'
		when ca.cn_country = 'Aruba (Netherlands)' then 'NL'
		when ca.cn_country = 'Australia' then 'AU'
		when ca.cn_country = 'Austria' then 'AT'
		when ca.cn_country = 'Bahamas' then 'BS'
		when ca.cn_country = 'Bahrain' then 'BH'
		when ca.cn_country = 'Bangkok' then 'TH'
		when ca.cn_country = 'Belgium' then 'BE'
		when ca.cn_country = 'Brazil' then 'BR'
		when ca.cn_country = 'Cambodia' then 'KH'
		when ca.cn_country = 'Canada' then 'CA'
		when ca.cn_country = 'Chile' then 'CL'
		when ca.cn_country = 'China' then 'CN'
		when ca.cn_country = 'Denmark' then 'DK'
		when ca.cn_country = 'France' then 'FR'
		when ca.cn_country = 'Germany' then 'DE'
		when ca.cn_country = 'Haiti' then 'HT'
		when ca.cn_country = 'Hong Kong' then 'HK'
		when ca.cn_country = 'India' then 'IN'
		when ca.cn_country = 'Indonesia' then 'ID'
		when ca.cn_country = 'Ireland' then 'IE'
		when ca.cn_country = 'Italy' then 'IT'
		when ca.cn_country = 'Japan' then 'JP'
		when ca.cn_country = 'Korea, South' then 'KR'
		when ca.cn_country = 'Kuwait' then 'KW'
		when ca.cn_country = 'Laos' then 'LA'
		when ca.cn_country = 'Lebanon' then 'LB'
		when ca.cn_country = 'Malaysia' then 'MY'
		when ca.cn_country = 'Myanmar, Burma' then 'MM'
		when ca.cn_country = 'Netherlands' then 'NL'
		when ca.cn_country = 'Pakistan ' then 'PK'
		when ca.cn_country = 'Philippines' then 'PH'
		when ca.cn_country = 'Qatar' then 'QA'
		when ca.cn_country = 'Russia' then 'RU'
		when ca.cn_country = 'Saudi Arabia' then 'SA'
		when ca.cn_country = 'Singapore' then 'SG'
		when ca.cn_country = 'South Korea' then 'KR'
		when ca.cn_country = 'Spain' then 'ES'
		when ca.cn_country = 'Sweden' then 'SE'
		when ca.cn_country = 'Switzerland' then 'CH'
		when ca.cn_country = 'Taiwan' then 'TW'
		when ca.cn_country = 'Th' then 'TH'
		when ca.cn_country = 'Th0' then 'TH'
		when ca.cn_country = 'Tha' then 'TH'
		when ca.cn_country = 'Thai' then 'TH'
		when ca.cn_country = 'Thaialnd' then 'TH'
		when ca.cn_country = 'Thaila' then 'TH'
		when ca.cn_country = 'Thailand' then 'TH'
		when ca.cn_country = 'Thailand.' then 'TH'
		when ca.cn_country = 'Thailand3' then 'TH'
		when ca.cn_country = 'Thailang' then 'TH'
		when ca.cn_country = 'Thailnad' then 'TH'
		when ca.cn_country = 'Thiland' then 'TH'
		when ca.cn_country = 'Uganda' then 'UG'
		when ca.cn_country = 'Ukraine' then 'UA'
		when ca.cn_country = 'United Arab Emirates' then 'AE'
		when ca.cn_country = 'United Kingdom' then 'GB'
		when ca.cn_country = 'United States of America' then 'US'
		when ca.cn_country = 'USA' then 'US'
		when ca.cn_country = 'Vietnam' then 'VN'
		when ca.cn_country = 'Yemen' then 'YE'
		else NULL end as Country
	from MaxAddressID m
	left join candidate.Addresses ca on m.maxAddressID = ca.address_id
	left join common.Lists cl on cl.k_id = ca.cn_address_province and cl.listkey = 'tblProvinces'
	)

, CandidateCurrentAddress as (select cn_id
	, concat_ws(' ', nullif(cn_address_street,'')
		, nullif(ltrim(rtrim(cn_address_subdist)),'')
		, nullif(ltrim(rtrim(cn_address_dist)),'')
		, nullif(province,'')
		, nullif(ltrim(rtrim(cn_address_pro_other)),'')
		, nullif(trim(' ' from cn_address_postalcode),'')
		, nullif(ltrim(rtrim(cn_country)),'')) as locationAddress
	, cn_address_dist
	, province
	, cn_address_postalcode
	, cn_address_pro_other --updated as mapping
	, Country
	from CurrentAddress
	)

--CANDIDATE REGISTERED ADDRESS #CUSTOM SCRIPT
, MaxRegAddressID as (select cn_id, max(address_id) as maxAddressID
	from candidate.Addresses
	where IsDeleted = 0 --valid addresses
	and address_type = 1 --2 types of Candidate addresses
	and exists (select cn_id from candidate.Candidates where can_type = 1 and cn_id = candidate.Addresses.cn_id)
	group by cn_id
	)

, RegAddress as (select m.cn_id, m.maxAddressID
	, ca.cn_address_street, ca.cn_address_subdist
	, ca.cn_address_dist
	, ca.cn_address_province, cl.listvalue as province
	, ca.cn_address_postalcode
	, ca.cn_address_pro_other
	, ca.cn_country
	, case when ca.cn_country = 'Afghanistan' then 'AF'
		when ca.cn_country = 'Albania' then 'AL'
		when ca.cn_country = 'Algeria' then 'DZ'
		when ca.cn_country = 'Andorra' then 'AD'
		when ca.cn_country = 'Angola' then 'AO'
		when ca.cn_country = 'Aruba (Netherlands)' then 'NL'
		when ca.cn_country = 'Australia' then 'AU'
		when ca.cn_country = 'Austria' then 'AT'
		when ca.cn_country = 'Bahamas' then 'BS'
		when ca.cn_country = 'Bahrain' then 'BH'
		when ca.cn_country = 'Bangkok' then 'TH'
		when ca.cn_country = 'Belgium' then 'BE'
		when ca.cn_country = 'Brazil' then 'BR'
		when ca.cn_country = 'Cambodia' then 'KH'
		when ca.cn_country = 'Canada' then 'CA'
		when ca.cn_country = 'Chile' then 'CL'
		when ca.cn_country = 'China' then 'CN'
		when ca.cn_country = 'Denmark' then 'DK'
		when ca.cn_country = 'France' then 'FR'
		when ca.cn_country = 'Germany' then 'DE'
		when ca.cn_country = 'Haiti' then 'HT'
		when ca.cn_country = 'Hong Kong' then 'HK'
		when ca.cn_country = 'India' then 'IN'
		when ca.cn_country = 'Indonesia' then 'ID'
		when ca.cn_country = 'Ireland' then 'IE'
		when ca.cn_country = 'Italy' then 'IT'
		when ca.cn_country = 'Japan' then 'JP'
		when ca.cn_country = 'Korea, South' then 'KR'
		when ca.cn_country = 'Kuwait' then 'KW'
		when ca.cn_country = 'Laos' then 'LA'
		when ca.cn_country = 'Lebanon' then 'LB'
		when ca.cn_country = 'Malaysia' then 'MY'
		when ca.cn_country = 'Myanmar, Burma' then 'MM'
		when ca.cn_country = 'Netherlands' then 'NL'
		when ca.cn_country = 'Pakistan ' then 'PK'
		when ca.cn_country = 'Philippines' then 'PH'
		when ca.cn_country = 'Qatar' then 'QA'
		when ca.cn_country = 'Russia' then 'RU'
		when ca.cn_country = 'Saudi Arabia' then 'SA'
		when ca.cn_country = 'Singapore' then 'SG'
		when ca.cn_country = 'South Korea' then 'KR'
		when ca.cn_country = 'Spain' then 'ES'
		when ca.cn_country = 'Sweden' then 'SE'
		when ca.cn_country = 'Switzerland' then 'CH'
		when ca.cn_country = 'Taiwan' then 'TW'
		when ca.cn_country = 'Th' then 'TH'
		when ca.cn_country = 'Th0' then 'TH'
		when ca.cn_country = 'Tha' then 'TH'
		when ca.cn_country = 'Thai' then 'TH'
		when ca.cn_country = 'Thaialnd' then 'TH'
		when ca.cn_country = 'Thaila' then 'TH'
		when ca.cn_country = 'Thailand' then 'TH'
		when ca.cn_country = 'Thailand.' then 'TH'
		when ca.cn_country = 'Thailand3' then 'TH'
		when ca.cn_country = 'Thailang' then 'TH'
		when ca.cn_country = 'Thailnad' then 'TH'
		when ca.cn_country = 'Thiland' then 'TH'
		when ca.cn_country = 'Uganda' then 'UG'
		when ca.cn_country = 'Ukraine' then 'UA'
		when ca.cn_country = 'United Arab Emirates' then 'AE'
		when ca.cn_country = 'United Kingdom' then 'GB'
		when ca.cn_country = 'United States of America' then 'US'
		when ca.cn_country = 'United States' then 'US'
		when ca.cn_country = 'USA' then 'US'
		when ca.cn_country = 'Vietnam' then 'VN'
		when ca.cn_country = 'Yemen' then 'YE'
		else NULL end as Country
	from MaxRegAddressID m
	left join candidate.Addresses ca on m.maxAddressID = ca.address_id
	left join common.Lists cl on cl.k_id = ca.cn_address_province and cl.listkey = 'tblProvinces'
	)

, CandidateRegAddress as (select cn_id
	, concat_ws(' ', nullif(cn_address_street,'')
		, nullif(ltrim(rtrim(cn_address_subdist)),'')
		, nullif(ltrim(rtrim(cn_address_dist)),'')
		, nullif(province,'')
		, nullif(ltrim(rtrim(cn_address_pro_other)),'')
		, nullif(trim(' ' from cn_address_postalcode),'')
		, nullif(ltrim(rtrim(cn_country)),'')) as locationAddress
	, cn_address_dist
	, province
	, cn_address_postalcode
	, cn_address_pro_other
	, Country
	from RegAddress
	)

--NATIONALITIES
, Nationality as (select k_id, listvalue, meta_2
	from common.Lists
	where listkey = 'tblNationalities')

--CANDIDATE DOCUMENTS (may include candidate documents)
, Documents as (select class_parent_id, doc_id, doc_class, doc_name, doc_blob_id, doc_ext 
	, case 
		when charindex('.',doc_blob_id) > 0 and charindex('/',doc_blob_id) = 0 then doc_blob_id
		when charindex('.',doc_blob_id) > 0 and charindex('/',doc_blob_id) > 0 then right(doc_blob_id,CHARINDEX('/',reverse(doc_blob_id))-1)
		when charindex('.',doc_blob_id) = 0 and charindex('/',doc_blob_id) > 0 then concat(right(doc_blob_id,CHARINDEX('/',reverse(doc_blob_id))-1),doc_ext)
		else concat(doc_blob_id,doc_ext) end as Documents
	from common.Documents
	where doc_class = 'Candidate'
	and doc_ext <> '' and class_parent_id > 0
	)

, CandidateDocuments as (select class_parent_id
	, string_agg(convert(nvarchar(max),Documents),',') as CandidateDoc
	from Documents
	where class_parent_id > 0
	group by class_parent_id
	)	
	
--MAIN SCRIPT
select concat('PRTR',c.cn_id) as 'candidate-externalId'
, coalesce(nullif(c.cn_fname,''),'Firstname') as 'candidate-firstName'
, coalesce(nullif(c.cn_lname,''),'Lastname') as 'candidate-Lastname'
, coalesce(nullif(c.cn_fname_thai,''),'') as 'candidate-firstNameKana'
, coalesce(nullif(c.cn_lname_thai,''),'') as 'candidate-lastNameKana'
, case when c.cn_salut_text = 'Miss' then 'MISS'
	when c.cn_salut_text = 'Mr.' then 'MR'
	when c.cn_salut_text = 'Mrs.' then 'MRS'
	when c.cn_salut_text = 'Ms.' then 'MS'
	else NULL end as 'candidate-title'
, case when pe.PrimaryEmail is NULL or pe.PrimaryEmail = '' then concat(c.cn_id,'_candidate@noemail.com')
	else pe.PrimaryEmail end as 'candidate-email'
, we.WorkEmail as 'candidate-workEmail'
, ltrim(rtrim(c.cn_cont_mobile)) as 'candidate-phone'
, ltrim(rtrim(c.cn_cont_mobile)) as 'candidate-mobile'
, ltrim(rtrim(c.cn_cont_home)) as 'candidate-homePhone'
--, c.cn_source as CandidateSource --CUSTOM SCRIPT #1: 3306 values | removed on 24102018 
--, ltrim(rtrim(c.cn_passportno)) as PassportNo --CUSTOM SCRIPT #2
--, concat_ws(char(10)
	-- , coalesce('Passport Number: ' + nullif(convert(varchar(max),c.cn_passportno),''),NULL)
	-- , coalesce('National ID: ' + nullif(convert(varchar(max),c.cn_idno),''),NULL)
	-- ) as Residence_VisaNote --CUSTOM SCRIPT #3
--, c.cn_birthplace --CUSTOM SCRIPT #4
--, c.cn_height --CUSTOM SCRIPT #5
-- , case when c.cn_married = 1 then 'Single'
	-- when c.cn_married = 2 then 'Married'
	-- when c.cn_married = 3 then 'Divorced'
	-- else NULL end MaritalStatus--CUSTOM SCRIPT #6
-- , c.cn_drivecar --CUSTOM SCRIPT #7 YES/NO
, cca.locationAddress as 'candidate-address'
-- , cca.cn_address_dist as 'candidate-city' --updated by mapping on 02/11/2018 #v1Script
-- , cca.cn_address_pro_other as 'candidate-city' --updated by mapping on 02/11/2018 #v2Script
-- , cca.cn_address_dist as 'candidate-city' --should be mapped to VC District/Suburb
-- , cca.province as 'candidate-state' --should be mapped to VC Town/City
-- , cca.cn_address_dist as District --CUSTOM SCRIPT #13 --to be added as District/Suburb
, cca.province as 'candidate-city'
, cca.cn_address_postalcode as 'candidate-zipCode'
, cca.Country as 'candidate-country'
, case when ltrim(rtrim(n.meta_2)) = 'UK' then 'GB' 
	else ltrim(rtrim(n.meta_2)) end as 'candidate-citizenship' 
, convert(varchar(10),c.cn_dob,120) as 'candidate-dob'
-- , c.cn_weight --CUSTOM SCRIPT #8
, case 
	when c.cn_sex = 1 then 'MALE'
	when c.cn_sex = 2 then 'FEMALE'
	else NULL end 'candidate-gender'
-- , c.cn_drivermoto --CUSTOM SCRIPT #9: all NULL
-- , c.cn_english_ability --CUSTOM SCRIPT #10
, c.cn_present_salary_currency
, case when c.cn_present_salary_currency in ('USD','JPY','HKD','SGD','KRW','CNY','EUR','AUD','GBP','INR','VND','PHP'
	,'PLN','RUB','MYR','IDR','THB','TWD','NZD','CAD','MXN','ARS','BRL','CHF','SAR','AED','PKR','MNT','ZAR','MMK','KWD'
	,'CLP','COP','NGN','OMR','BHD','QAR','MAD','LAK','EGP','NOK','DKK','TRY','HRK','RSD','HUF','BGN') then c.cn_present_salary_currency
	else 'THB' end as 'candidate-currency'
-- , c.cn_present_position --CUSTOM SCRIPT #13: Work History - Job Title
-- , c.cn_present_salary --CUSTOM SCRIPT #11: Monthly Salary
--, c.cn_present_salary as AnnualSalary
--, c.cn_salary_sought as DesiredSalary
-- , case when len(c.cn_present_salary) <= 8 then c.cn_present_salary * 12
	-- else c.cn_present_salary end as 'candidate-currentSalary' --updated mannually by script (Candidate Monthly Salary)

/* Request changed to be used as Custom Field for Desired Salary (monthly)
, case when len(c.cn_salary_sought) <= 8 then c.cn_salary_sought * 12
	else c.cn_salary_sought end as 'candidate-desiredSalary'
*/

-- , c.cn_salary_sought --CUSTOM FIELD ##: Desired Salary (Monthly)
-- , c.cn_present_benefits --CUSTOM SCRIPT #12: CF Benefits
, ltrim(rtrim(c.cn_other_itskills)) as 'candidate-skills' --to be checked: cn_computer_skills (not skills) >> cn_other_itskills
, cd.CandidateDoc as 'candidate-resume'
-- , concat_ws (char(10)
	-- , concat('Candidate External ID: ',c.cn_id) --ADD this field in custom field instead on 02/11/2018
	-- , coalesce('Candidate email 3: ' + nullif(ltrim(rtrim(c.cn_cont_email3)),''),NULL)
	-- , coalesce('Candidate source: ' + nullif(ltrim(rtrim(convert(nvarchar(max),c.cn_source))),''),NULL) --Removed on 24102018
	-- , coalesce('Comments: ' + nullif(ltrim(rtrim(convert(nvarchar(max),c.cn_comments))),''),NULL)
	-- ) as 'candidate-note' --Moved this note to Candidate New Note tab
from candidate.Candidates c
left join CandidateCurrentAddress cca on cca.cn_id = c.cn_id
left join PrimaryEmail pe on pe.cn_id = c.cn_id
left join Nationality n on n.k_id = c.cn_nationality_id
left join CandidateDocuments cd on cd.class_parent_id = c.cn_id
left join WorkEmail we on we.cn_id = c.cn_id
where c.can_type = 1
--and c.cn_id between 10608 and 61529
--and c.cn_id between 61530 and 176947
--and c.cn_id between 176948 and 5079032
--and c.cn_id between 5079033 and 5130463
--and c.cn_id between 5130464 and 5168666
order by c.cn_id