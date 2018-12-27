--MARITAL STATUS
/* In Vincere
<option value="1">Single</option>
<option value="2">Married</option>
<option value="3">Divorced</option>
<option value="4">Widowed</option>
<option value="5">Separated</option>
<option value="6">Private</option>
<option value="7">Unmarried</option>
*/

select concat('PRTR',cn_id) as CandidateExtID
, case when cn_married = 1 then 1
	when cn_married = 2 then 2
	when cn_married = 3 then 3
	else 0 end MaritalStatus 
from candidate.Candidates
where can_type = 1
order by cn_id


--PLACE OF BIRTH
--Vincere: candidate > pob

select concat('PRTR',cn_id) as CandidateExtID
, cn_birthplace 
from candidate.Candidates
where can_type = 1
and cn_birthplace is not NULL
order by cn_id

--EDUCATION SUMMARY
with EducationSummary as 
(select cn_id
, string_agg( cast(concat_ws(char(10),char(13)
		, coalesce('Level of Education: ' + nullif(ltrim(rtrim(edu_degree_parsed)),''),NULL)
		, coalesce('Institution: ' + nullif(ltrim(rtrim(edu_inst_name)),''),NULL)
		, coalesce('Country: ' + nullif(ltrim(rtrim(edu_duration)),''),NULL)
		, coalesce('Program	: ' + nullif(ltrim(rtrim(edu_major)),''),NULL)
		, coalesce('Faculty / Program / Field: ' + nullif(ltrim(rtrim(edu_degree_other)),''),NULL)
		, coalesce('Start Year: ' + nullif(ltrim(rtrim(edu_start)),''),NULL)
		, coalesce('Finish Year: ' + nullif(ltrim(rtrim(edu_finish)),''),NULL)
		, coalesce('GPA: ' + nullif(ltrim(rtrim(edu_gpa)),''),NULL)
		) as nvarchar(max)), char(10)) within group (order by edu_id asc) as Education --edu_id as checked from the oldest to the latest
from candidate.Education
where isDeleted = 0
and (edu_degree_parsed is not NULL or edu_inst_name is not NULL or edu_duration is not NULL or edu_major is not NULL
	or edu_degree_other is not NULL or edu_start is not NULL or edu_finish is not NULL or edu_gpa is not NULL)
group by cn_id)

select concat('PRTR',c.cn_id) as CandidateExtID
, concat_ws(char(10)
	, '[Education Summary]'
	, nullif(e.Education,'')
	, coalesce(char(10) + 'Training & Certification Details: ' 
	+ nullif(replace(replace(replace(replace(
		ltrim(rtrim(cast([dbo].[udf_StripHTML]([dbo].[udf_StripCSS](c.cn_training)) as nvarchar(max))))
		,char(9),''),char(10),''),char(13),''),'.','. '),''),NULL)) as Education
from candidate.Candidates c
left join EducationSummary e on e.cn_id = c.cn_id
where c.can_type = 1
and (e.Education <> '' or cast(c.cn_training as nvarchar(max)) <> '')
order by c.cn_id

-->> TRAINING
select cn_training
, ltrim([dbo].[udf_StripHTML]([dbo].[udf_StripCSS](cn_training)))
, replace(replace(replace(ltrim([dbo].[udf_StripHTML]([dbo].[udf_StripCSS](cn_training))),char(9),''),char(10),''),'.','. ')
, cn_other_itskills
, cn_training
from candidate.Candidates

-->> CURRENT ADDRESS: to update District/Suburd + City
with MaxAddressID as (select cn_id, max(address_id) as maxAddressID
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
	, cn_address_pro_other
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
		, nullif(trim(' ' from cn_address_postalcode),'')
		, nullif(ltrim(rtrim(cn_country)),'')) as locationAddress
	, cn_address_dist
	, province
	, cn_address_postalcode
	, Country
	, cn_address_pro_other
	from CurrentAddress)

select concat('PRTR',cn_id) as CandidateExtID
, province as City
, cn_address_dist as District
from CandidateCurrentAddress
where (province is not NULL and province <> '')
or (cn_address_dist is not NULL and cn_address_dist <> '')

-->> CANDIDATE NEW BRIEF NOTE [CANDIDATE NOTE]
select concat('PRTR',cn_id) as CandidateExtID
, 'Candidate Note' as title
, concat_ws (char(10)
	, concat('Candidate External ID: ', convert(varchar(max),cn_id)) 
	, coalesce('Candidate email 3: ' + nullif(ltrim(rtrim(cn_cont_email3)),''),NULL)
	, coalesce('Comments: ' + nullif(ltrim(rtrim(convert(nvarchar(max),cn_comments))),''),NULL)
	) as note
, getdate() as insert_timestamp
from candidate.Candidates
order by cn_id

-->> CANDIDATE LINEID
select concat('PRTR',cn_id) as CandidateExtID
, 'add_com_info' as Additional_type
, cn_custom_field
, json_value(cn_custom_field,'$.CandidateContact.dxTextBox[0].Value') as Custom_value --LINE ID
, 1111 as Form_id
, 2222 as Field_id
, getdate() as Insert_timestamp
from candidate.Candidates
where can_type = 1
and json_value(cn_custom_field,'$.CandidateContact.dxTextBox[0].Value') is not NULL
and json_value(cn_custom_field,'$.CandidateContact.dxTextBox[0].Value') <> ''
order by cn_id

-->> CANDIDATE REFERENCES
select concat('PRTR',cn_id) as CandidateExtID
, string_agg(concat_ws(char(10)
	, coalesce('Contact name: ' + nullif(trim(cn_contact_name),''),NULL)
	, coalesce('Contact phone: ' + nullif(trim(cn_contact_phone),''),NULL)
	, coalesce('Contact address: ' + nullif(trim(cn_contact_address),''),NULL)
	, coalesce('Contact email: ' + nullif(trim(cn_contact_email),''),NULL)
	, coalesce('Contact occupation: ' + nullif(trim(cn_contact_occupation),''),NULL)
	, coalesce('Contact detail: ' + nullif(trim(cn_contact_detail),''),NULL)
	), concat(char(10),char(13))) within group (order by cn_contact_id desc) as Reference
from candidate.OtherContact
group by cn_id

-->> 