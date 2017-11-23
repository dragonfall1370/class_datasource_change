--CANDIDATE COMMENTS
/* , CandidateComments as (SELECT
     cn_id,
     STUFF(
         (SELECT char(10) + 'Comment date: ' + convert(varchar(20),comment_date,120) + char(10)
		 + coalesce('Consultant: ' + nullif(ltrim(rtrim(comment_consultant)),'') + char(10),'')
		 + coalesce('Comment action: ' + nullif(ltrim(rtrim(comment_action)),'') + char(10),'')
		 + coalesce(coalesce('Comment: ' + nullif(ltrim(rtrim(comment)),'')
					,'Comment: ' + nullif(ltrim(rtrim(comment_taleo)),'')),'') --If comment is null then comment_taleo
          from tblCandidateComments
          WHERE cn_id = a.cn_id
		  order by comment_date desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS CandidateComments
FROM tblCandidateComments as a
GROUP BY a.cn_id) */ --> can be combined as 1 comment for candidate

--CANDIADTE EMAIL FILES
with CandidateEmailFiles as (select ec.CandidateID, ec.EmailID, concat(ec.EmailID,rtrim(EmailFileExt)) as EmailFile 
	from tblEmailCandidates ec
	left join tblEmails e on e.EmailID = ec.EmailID)

, CandidateEmailFiles2 as (SELECT
     CandidateID,
     STUFF(
         (SELECT ', ' + EmailFile
          from  CandidateEmailFiles
          WHERE CandidateID = a.CandidateID
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS CandidateEmailFiles
FROM CandidateEmailFiles as a
GROUP BY a.CandidateID)

--CANDIDATE DOCUMENTS
, Documents as (select candidate_id, doc_id, concat(doc_id,'_',replace(replace(doc_name,',',''),'.',''),rtrim(ltrim(doc_ext))) as docfilename 
	from tblCandidateDocs
	where doc_ext is not NULL and doc_ext <> ''
	and doc_ext not like '%msg%') -- 12972 rows without extension

, CandidateDocument as (SELECT
     candidate_id,
     STUFF(
         (SELECT ', ' + docfilename
          from  Documents
          WHERE candidate_id = a.candidate_id
		  order by doc_id desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  AS CandidateDocument
FROM Documents as a
GROUP BY a.candidate_id)

--CANDIDATE EDUCATION,
, CandidateEducationRow as (select edu_candidate, edu_id, concat(coalesce('Education finish: ' + nullif(ltrim(rtrim(e.edu_finish)),'') + char(10),NULL)
	, coalesce('Institute name: ' + nullif(ltrim(rtrim(e.edu_inst_name)),'') + char(10),NULL)
	, coalesce('Major: ' + nullif(ltrim(rtrim(e.edu_major)),'') + char(10),NULL)
	, coalesce('Degree: ' + nullif(ltrim(rtrim(e.edu_degree)),'') + char(10),NULL)
	, coalesce('Degree other: ' + nullif(ltrim(rtrim(e.edu_degree_other)),'') + char(10),NULL)
	, coalesce('Degree parsed: ' + nullif(ltrim(rtrim(e.edu_degree_parsed)) + char(10),''),NULL)) as candidate_edu
	from tblEducation e
	left join tblEduDegree ed on ed.deg_id = e.edu_degree
	where e.edu_candidate <> 0)

, CandidateEducations as (SELECT
     edu_candidate,
     STUFF(
         (SELECT char(10) + candidate_edu
          from  CandidateEducationRow
          WHERE edu_candidate = a.edu_candidate
		  order by edu_id asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS CandidateEducations
FROM CandidateEducationRow as a
GROUP BY a.edu_candidate)

--CANDIDATE WORK HISTORY
, CandidateWorkHistory as (select job_candidate, job_id
	, concat(coalesce('From: ' + nullif(ltrim(rtrim(convert(varchar(10),job_from,120))),'') + char(10),NULL)
	, coalesce('To: ' + nullif(ltrim(rtrim(convert(varchar(10),job_to,120))),'') + char(10),NULL)
	, coalesce('Period: ' + nullif(ltrim(rtrim(job_period)),'') + char(10),NULL)
	, coalesce('Company name: ' + nullif(ltrim(rtrim(job_company_name)),'') + char(10),NULL)
	, coalesce('Company address: ' + nullif(ltrim(rtrim(job_company_address)),'') + char(10),NULL) --additional added
	, coalesce('Position: ' + nullif(ltrim(rtrim(job_position)),'') + char(10),NULL)
	, coalesce('Employment History - Details: ' + nullif(ltrim(rtrim(cast(job_txtDetails as nvarchar(max)))),''),NULL)) as candidate_employment
	from tblJobs where job_candidate <> 0
	and nullif(job_company_name,'') is not NULL and nullif(cast(job_txtDetails as nvarchar(max)),'') is not NULL)

, CandidateWorkHistories as (SELECT
     job_candidate,
     STUFF(
         (SELECT char(10) + char(10) + candidate_employment
          from  CandidateWorkHistory
          WHERE job_candidate = a.job_candidate 
		  and candidate_employment is not NULL and candidate_employment <> ''
		  order by job_id asc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  as CandidateEmployments
FROM CandidateWorkHistory as a
GROUP BY a.job_candidate)

--CANDIDATE TAGS
, CandidateTags as (SELECT
     cantag_candidate,
     STUFF(
         (SELECT ', ' + cantag_name
          from  tblCandidateTags
          WHERE cantag_candidate = a.cantag_candidate
		  and cantag_candidate <> 0
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 2, '')  as CandidateTags
FROM tblCandidateTags as a
where a.cantag_candidate <> 0
GROUP BY a.cantag_candidate)

--EMAIL DUPLICATION
, CandidateEmail as (SELECT cn_id, replace(replace(ltrim(rtrim(CONVERT(NVARCHAR(MAX), cn_cont_email))),CHAR(13),''),CHAR(10),'') as CandidateEmail
	, ROW_NUMBER() OVER (PARTITION BY replace(replace(ltrim(rtrim(CONVERT(NVARCHAR(MAX), cn_cont_email))),CHAR(13),''),CHAR(10),'') ORDER BY cn_id desc) as rn
	FROM tblCandidate
	where ltrim(rtrim(cn_cont_email)) <> '' and ltrim(rtrim(cn_cont_email)) like '%_@_%.__%')

--select cn_id, CandidateEmail, rn FROM CandidateEmail where cn_id in (13904,10815)

--MAIN SCRIPT
select top 100
concat('RC',c.cn_id) as 'candidate-externalId'
, case when c.cn_salut = 1 then 'MR'
	when c.cn_salut = 2 then 'MS'
	when c.cn_salut = 3 then 'MRS'
	when c.cn_salut = 4 then 'MISS'
	else NULL end as 'candidate-title'
, coalesce(nullif(ltrim(rtrim(c.cn_fname)),''),'Firstname') as 'candidate-firstName'
, coalesce(nullif(ltrim(rtrim(c.cn_lname)),''),concat('Lastname-',c.cn_id)) as 'candidate-lastName'
, case 
	when n.nat_name like '%Motswana (singular), Batswana (plural)' then 'BW'
	when n.nat_name like '%Singaporean' then 'SG'
	when n.nat_name like '%Lao,Laotian' then 'LA'
	when n.nat_name like '%Vietnamese' then 'VN'
	when n.nat_name like '%Cambodian' then 'KH'
	when n.nat_name like '%Yemeni' then 'YE'
	when n.nat_name like '%Kazakhstani' then 'KZ'
	when n.nat_name like '%Polish/Pole' then 'PL'
	when n.nat_name like '%Nigerian' then 'NG'
	when n.nat_name like '%Mauritian' then 'MU'
	when n.nat_name like '%Malaysian' then 'MY'
	when n.nat_name like '%Taiwanese' then 'TW'
	when n.nat_name like '%Lebanese' then 'LB'
	when n.nat_name like '%Bahraini' then 'BH'
	when n.nat_name like '%Indian' then 'IN'
	when n.nat_name like '%Greek' then 'GR'
	when n.nat_name like '%Russian' then 'RU'
	when n.nat_name like '%Indonesian' then 'ID'
	when n.nat_name like '%Pakistani' then 'PK'
	when n.nat_name like '%Syrian' then 'SY'
	when n.nat_name like '%Chinese' then 'CN'
	when n.nat_name like '%Icelander' then 'IS'
	when n.nat_name like '%Iranian' then 'IR'
	when n.nat_name like '%Thai' then 'TH'
	when n.nat_name like '%Romanian' then 'RO'
	when n.nat_name like '%Algerian' then 'DZ'
	when n.nat_name like '%French' then 'FR'
	when n.nat_name like '%South African' then 'ZA'
	when n.nat_name like '%Turkish/Turk' then 'TR'
	when n.nat_name like '%Canadian' then 'CA'
	when n.nat_name like '%Croatian' then 'HR'
	when n.nat_name like '%American' then 'US'
	when n.nat_name like '%Dutch' then 'NL'
	when n.nat_name like '%Spanish' then 'ES'
	when n.nat_name like '%British' then 'GB'
	when n.nat_name like '%Azerbaijani' then 'AZ'
	when n.nat_name like '%Uzbekistani' then 'UZ'
	when n.nat_name like '%Portuguese' then 'PT'
	when n.nat_name like '%Ukrainian' then 'UA'
	when n.nat_name like '%Colombian' then 'CO'
	when n.nat_name like '%Latvian' then 'LV'
	when n.nat_name like '%South Korean' then 'KR'
	when n.nat_name like '%British/Briton' then 'GB'
	when n.nat_name like '%Bangladeshi' then 'BD'
	when n.nat_name like '%Japanese' then 'JP'
	when n.nat_name like '%Hungarian' then 'HU'
	when n.nat_name like '%German' then 'DE'
	when n.nat_name like '%Zimbabwean' then 'ZW'
	when n.nat_name like '%Nepalese' then 'NP'
	when n.nat_name like '%Swiss' then 'CH'
	when n.nat_name like '%Iraqi' then 'IQ'
	when n.nat_name like '%Chilean' then 'CL'
	when n.nat_name like '%Korean' then 'KR'
	when n.nat_name like '%Austrian' then 'AT'
	when n.nat_name like '%Ghanaian' then 'GH'
	when n.nat_name like '%Burmese' then 'MM'
	when n.nat_name like '%New Zealander' then 'NZ'
	when n.nat_name like '%Belarusian' then 'BY'
	when n.nat_name like '%Filipino' then 'PH'
	when n.nat_name like '%Estonian' then 'EE'
	when n.nat_name like '%Sri lankan' then 'LK'
	when n.nat_name like '%Jordanian' then 'JO'
	when n.nat_name like '%Australian' then 'AU'
	when n.nat_name like '%Egyptian' then 'EG'
	when n.nat_name like '%Papua New Guinean' then 'PG'
	when n.nat_name like '%Serbian' then 'RS'
	when n.nat_name like '%Cameroonian' then 'CM'
	when n.nat_name like '%Italian' then 'IT'
	when n.nat_name like '%Bulgarian' then 'BG'
	when n.nat_name like '%Belgian' then 'BE'
	when n.nat_name like '%Israeli' then 'IL'
	when n.nat_name like '%Albanian' then 'AL'
	when n.nat_name like '%Brazilian' then 'BR'
	when n.nat_name like '%Ethiopian' then 'ET'
	when n.nat_name like '%Danish/Dane' then 'DK'
	when n.nat_name like '%Ugandan' then 'UG'
	when n.nat_name like '%Cuban' then 'CU'
	else NULL end as 'candidate-citizenship'
, n.nat_name as OriginalNationality
, ltrim(rtrim(c.cn_import_email)) as 'candidate-owners'
, case when c.cn_sex = 0 then 'MALE'
	when c.cn_sex = 1 then 'FEMALE'
	else NULL end as 'candidate-gender'
, convert(varchar(10),c.cn_dob,120) as 'candidate-dob'
, ltrim(rtrim(c.cn_cont_mobile)) as 'candidate-phone'
, ltrim(rtrim(c.cn_cont_home)) as 'candidate-homePhone'
, case 
	when c.cn_id in (select cn_id from CandidateEmail where rn = 1) then cem.CandidateEmail
	when c.cn_id in (select cn_id from CandidateEmail where rn > 1) then concat(cem.rn,'_',cem.CandidateEmail)
	else coalesce(nullif(nullif(nullif(replace(replace(ltrim(rtrim(c.cn_cont_email)),'no email',''),'No Email',''),''),'-'),'%email%')
		,concat('candidate-',c.cn_id,'@noemail.com')) end as 'candidate-email'
, nullif(ltrim(rtrim(c.cn_cont_email2)),'') as 'candidate-workEmail'
, ltrim(stuff((coalesce(' ' + nullif(rtrim(ltrim(c.cn_address_street)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(c.cn_address_subdist)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.cn_address_dist)),''),'') + coalesce(', ' + nullif(rtrim(ltrim(p.pro_name)),''),'') 
	+ coalesce(', ' + nullif(rtrim(ltrim(c.cn_address_postalcode)),''),'')),1,1,'')) as 'candidate-address'
, nullif(rtrim(ltrim(p.pro_name)),'') as 'candidate-State'
, nullif(rtrim(ltrim(c.cn_address_postalcode)),'') as 'candidate-zipCode'
--EDUCATION
, ce.CandidateEducations as 'candidate-education'
--SKILLS
, iif(cn_english_ability = 0,concat('English Language: ','No Data')
	, iif(cn_english_ability = 5,concat('English Language: ','Good'),NULL)) as 'candidate-skills' -- Other skills have no data in DB
, ct.CandidateTags as 'candidate-keyword'
--WORK HISTORY
, cwh.CandidateEmployments as 'candidate-workHistory'
, rtrim(ltrim(c.cn_present_company)) as 'candidate-employer1'
, coalesce(rtrim(ltrim(c.cn_present_position)),rtrim(ltrim(c.cn_latest_position))) as 'candidate-jobTitle1'
, nullif(c.cn_present_salary,0) as 'candidate-currentSalary'
, nullif(c.cn_salary_sought,0) as 'candidate-desiredSalary'
--DOCUMENT
--, stuff(', ' + coalesce(cd.CandidateDocument + ',','') + coalesce(cef.CandidateEmailFiles,''),1,2,'') as 'candidate-resume'
--> cannot insert .msg via data import
, coalesce(cd.CandidateDocument,'') as 'candidate-resume'
--NOTES
, concat('RecruitCraft External ID: ',c.cn_id,char(10)
	, coalesce('Nationality: ' + nullif(ltrim(rtrim(n.nat_name)),'') + char(10),'')) as 'candidate-note'
from tblCandidate c
--left join ContactSplitEmails2Dup cse on cse.contact_id = c.cn_id --> the comments will be run separatel, or else
--left join tblUser u on u.usr_id = c.cn_consultant_id --> all consultant id is null
left join tblNationality n on n.nat_id = c.cn_nationality_id
left join tblProvinces p on p.pro_id = c.cn_address_province
left join CandidateEducations ce on ce.edu_candidate = c.cn_id
left join CandidateWorkHistories cwh on cwh.job_candidate = c.cn_id
left join CandidateTags ct on ct.cantag_candidate = c.cn_id
left join CandidateDocument cd on cd.candidate_id = c.cn_id
left join CandidateEmailFiles2 cef on cef.CandidateID = c.cn_id
left join CandidateEmail cem on cem.cn_id = c.cn_id
where c.cn_show = 'Y'
order by c.cn_id
--and c.cn_cont_email like '%noraritk@hotmail.com%'
--and c.cn_id in (13904,10815)
--order by c.cn_id desc

---------
---PART 2: INSERT CANDIDATE COMMENTS
select concat('RC',cn_id) as 'RC_candidateID'
	, -10 as 'RC_user_account_id'
	, comment_date as 'RC_feedback_timestamp'
	, concat('Comment date: ',convert(varchar(20),comment_date,120),char(10)
		 , coalesce('Consultant: ' + nullif(ltrim(rtrim(comment_consultant)),'') + char(10),'')
		 , coalesce('Comment action: ' + nullif(ltrim(rtrim(comment_action)),'') + char(10),'')
		 , coalesce(coalesce('Comment: ' + nullif(ltrim(rtrim(comment)),'')
					,'Comment: ' + nullif(ltrim(rtrim(comment_taleo)),'')),'')) as RC_candidate_comment
	, 0 as 'RC_feedback_score'
	, comment_date as 'RC_insert_timestamp'
	, 4 as 'RC_contact_method'
	, 1 as 'RC_related_status'
from tblCandidateComments
order by cn_id, comment_date desc