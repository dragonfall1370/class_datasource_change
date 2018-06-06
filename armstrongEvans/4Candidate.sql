------------
-- DOCUMENT
------------
with doc (Candidate,files) as (
        SELECT cast(Candidate as varchar(max)), Files = STUFF(( SELECT DISTINCT ', ' + cast(Filename as varchar(max)) FROM Documents b WHERE cast(b.Candidate as varchar(max)) <> '' and cast(Candidate as varchar(max)) = cast(a.Candidate as varchar(max)) FOR XML PATH (''), TYPE).value('.', 'varchar(MAX)'), 1, 1, '') FROM Documents a GROUP BY cast(a.Candidate as varchar(max))
        )
--select * from doc
--select * from Documents where convert(varchar,Candidate) <> ''
/*
	select --top 20
                C.userID as '#userID'
		, case C.gender when 'M' then 'MR' when 'F' then 'MISS'	else '' end as 'candidate-title'
		, case C.gender when 'M' then 'MALE' when 'F' then 'FEMALE' else '' end as 'candidate-gender'
		, C.candidateID as 'candidate-externalId'
		, Coalesce(NULLIF(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
                , Coalesce(NULLIF(replace(C.LastName,'?',''), ''), concat('Lastname-',C.candidateID)) as 'contact-lastName'
		, C.middleName as 'candidate-middleName'
		, CONVERT(VARCHAR(10),C.dateOfBirth,120) as 'candidate-dob'
		, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation = 'NULL' OR tc.abbreviation = 'ZR') THEN '' ELSE tc.abbreviation END as 'candidate-citizenship'
		, iif(e1.ID in (select ID from ed where rn > 1),concat(ed.email,'_',ed.rn), iif(e1.email = '' or e1.email is null, concat('candidate_',cast(C.userID as varchar(max)),'@noemail.com'),e1.email) ) as 'candidate-email'
		, e2.email as 'candidate-workEmail'
		, C.mobile as 'candidate-phone'
		, C.mobile as 'candidate-mobile'
		, C.phone2 as 'candidate-homePhone'	
		, C.workPhone as 'candidate-workPhone'
		, 'PERMANENT' as 'candidate-jobTypes'
		, C.address1 as 'candidate-address'
		, C.city as 'candidate-city'
		, CASE WHEN (tc.abbreviation = 'NONE' OR tc.abbreviation in ('NULL','ZR') ) THEN '' ELSE tc.abbreviation END as 'candidate-Country'
		, C.zip as 'candidate-zipCode'
		, C.state as 'candiadte-state'
		, cast(C.salaryLow as int) as 'candidate-currentSalary'
		, cast(C.salary as int) as 'candidate-desiredSalary'
		, Education.school as 'candidate-schoolName'
		, Education.graduationDate as 'candidate-graduationDate'
		, Education.degree as 'candidate-degreeName'
		--, Education.major as '#candidate-major'
		, SN.SkillName as 'candidate-skills'
		, C.companyName as 'candidate-company1'
		, C.occupation as 'candidate-jobTitle1'
		, C.companyName as 'candidate-employer1'
		--, C.recruiterUserID as '#recruiterUserID'
		, owner.email as 'candidate-owners'
		--, t4.finame as '#Candidate File'
		, files.ResumeId as 'candidate-resume'
		, note.note as 'candidate-note'
		--, left(comment.comment,32760) as 'candidate-comments'
*/

, his as (
        select 
          ch.Candidate as 'candidate_externalId'
        , CONVERT(date, CONVERT(VARCHAR(10),replace(convert(varchar(50),ch.FromDate),'Date Added',''),120) , 103) as 'Date Range'
        , iif( CONVERT(date, CONVERT(VARCHAR(10),replace(convert(varchar(50),ch.ToDate),'Date Added',''),120) , 103) = '1900-01-01', '' --GETDATE() --CURRENT_TIMESTAMP
              ,CONVERT(date, CONVERT(VARCHAR(10),replace(convert(varchar(50),ch.ToDate),'Date Added',''),120) , 103)
              ) as 'To'
        , ch.JobTitle as 'JobTitle'
        , ch.Company as 'EmployerName'
        , ch.current_ as 'CurrentFlag'
        , ltrim(Stuff( Coalesce('Address: ' + NULLIF(cast(ch.Address as nvarchar(max)) , '') + char(10), '')
                        + Coalesce('Phone: ' + NULLIF(cast(ch.Phone as nvarchar(max)) , '') + char(10), '')
                        + Coalesce('Email: ' + NULLIF(cast(ch.Email as nvarchar(max)), '') + char(10), '')
                        + Coalesce('Checked: ' + NULLIF(cast(ch.Checked as nvarchar(max)), '') + char(10), '')
                        --+ Coalesce('Current Flag: ' + NULLIF(cast(ch.current_ as nvarchar(max)), ''), '')
                , 1, 0, '') ) as 'Candidate-WorkHistory'
        , ROW_NUMBER() OVER(PARTITION BY convert(varchar(max),ch.Candidate) ORDER BY CONVERT(date, CONVERT(VARCHAR(10),replace(convert(varchar(50),ch.FromDate),'Date Added',''),120) , 103) desc ) AS rn 
        --, ROW_NUMBER() OVER(PARTITION BY CONVERT(VARCHAR(10),ch.FromDate,120) ORDER BY convert(varchar(max),ch.Candidate)) AS rn 
        from CandidatesHistory ch
        )
--select top 200 * from his where convert(varchar(max),[candidate_externalId]) = '10030'
, whis as (
        SELECT cast([candidate_externalId] as varchar(max)) as 'candidate_externalId'
                , whis = STUFF(( 
                                SELECT char(10) + 'Date Range: ' + cast([Date Range] as varchar(max)) + char(10) --Coalesce('Date Range: ' + NULLIF(cast([Date Range] as nvarchar(max)) , ''), '') 
                                                + 'To: ' + cast([To] as varchar(max)) + char(10) --Coalesce('To: ' + NULLIF(cast([To] as nvarchar(max)) , ''), '') 
                                                + 'Job Title: ' + cast(JobTitle as varchar(max)) + char(10)
                                                + 'Employer Name: ' + cast(EmployerName as varchar(max)) + char(10)
                                                + 'Current Flag: ' + cast(CurrentFlag as varchar(max)) + char(10)
                                               -- + 'Work History: ' + cast([Candidate-WorkHistory] as varchar(max)) + char(10)
                                FROM his b 
                                WHERE cast([candidate_externalId] as varchar(max)) = cast(a.[candidate_externalId] as varchar(max)) and b.rn > 3 --and cast([candidate_externalId] as varchar(max)) <> '' 
                                FOR XML PATH (''), TYPE).value('.', 'varchar(MAX)'), 1, 1, '')
        FROM his a WHERE a.rn > 3 GROUP BY cast(a.[candidate_externalId] as varchar(max))
        )
--select * from whis where convert(varchar(max),[candidate_externalID]) = '10030'
/*	select whis.*
	from CandidateImportAutomappingTemplateversion c
	left join whis on convert(varchar(max),whis.candidate_externalId) = convert(varchar(max),c.candidate_externalId)
        where convert(varchar(max),c.candidate_externalId) in ('10030')
*/        
/*, wh as (
        select 
          ch.[candidate_externalId] as 'candidate_externalId'
        --, CONVERT(date, CONVERT(VARCHAR(10),replace(convert(varchar(50),ch.FromDate),'Date Added',''),120) , 103) as 'Date Range'
        --, CONVERT(date, CONVERT(VARCHAR(10),replace(convert(varchar(50),ch.ToDate),'Date Added',''),120) , 103) as 'To'
        --, ch.JobTitle as 'JobTitle'
        --, ch.Company as 'EmployerName'
        --, ch.current_ as 'CurrentFlag'
        , Stuff( 
                          Coalesce('Date Range: ' + NULLIF(cast(ch.[Date Range] as nvarchar(max)), '') + char(10), '')
                        + Coalesce('To: ' + NULLIF(cast(ch.[To] as nvarchar(max)) , '') + char(10), '')
                        + Coalesce('Job Title: ' + NULLIF(cast(ch.JobTitle as nvarchar(max)), '') + char(10), '')
                        + Coalesce('Employer Name: ' + NULLIF(cast(ch.EmployerName as nvarchar(max)), '') + char(10), '')
                        + Coalesce('Current Flag: ' + NULLIF(cast(ch.CurrentFlag as nvarchar(max)), '') + char(10), '')
                        + Coalesce('Work History: ' + NULLIF(cast(ch.[Candidate-WorkHistory] as nvarchar(max)), '') + char(10), '')
                        --+ Coalesce('Phone: ' + NULLIF(cast(ch.Phone as nvarchar(max)) , ''), '')
                        --+ Coalesce('Email: ' + NULLIF(cast(ch.Email as nvarchar(max)), ''), '')
                        --+ Coalesce('Checked: ' + NULLIF(cast(ch.Checked as nvarchar(max)), ''), '')
                        --+ Coalesce('Current Flag: ' + NULLIF(cast(ch.current_ as nvarchar(max)), ''), '')
                , 1, 0, '') as 'Candidate-WorkHistory'
        from his ch where rn > 3 ) --and ch.[candidate_externalId] = '10030')
--select * from wh where convert(varchar(max),[candidate_externalID]) = '10030'
*/

select --top 200
  c.candidate_externalId as 'candidate-externalId'
, c.candidate_title as 'candidate-title'
, c.candidate_firstName as 'candidate-firstName'
, c.candidate_Lastname as 'candidate-Lastname'
, c.candidate_email as 'candidate-email'
, c.candidate_workEmail as 'candidate-workEmail'
, replace(replace(convert(varchar(max),c.candidate_employmentType),'Full-time','FULL_TIME'),'Part-time','PART_TIME') as 'candidate-employmentType'
, c.candidate_jobTypes as 'candidate-jobTypes'
, c.Gender as 'candidate-gender'
--, c.candidate_dob as 'candidate-dob'
, CONVERT(date, CONVERT(VARCHAR(10),replace(convert(varchar(50),c.candidate_dob),'',''),120) , 103) as 'candidate-dob'
        , ltrim(Stuff( Coalesce(' ' + NULLIF(cast(c.candidate_address as nvarchar(max)) , ''), '')
                        + Coalesce(', ' + NULLIF(cast(c.candidate_city as nvarchar(max)) , ''), '')
                        + Coalesce(', ' + NULLIF(cast(c.candidate_State as nvarchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(c.candidate_zipCode as nvarchar(max)), ''), '')
                        + Coalesce(', ' + NULLIF(cast(c.candidate_Country as nvarchar(max)), ''), '')
                , 1, 1, '') ) as 'company-Address'
        --, c.candidate_address as 'candidate-address'
        --, c.candidate_citizenship as 'candidate-citizenship'
, case
		when c.candidate_citizenship like 'Abkhazi%' then ''
		when c.candidate_citizenship like 'African%' then 'ZA'
		when c.candidate_citizenship like 'Africa%' then 'ZA'
		when c.candidate_citizenship like 'America%' then 'US'
		when c.candidate_citizenship like 'Austral%' then 'AU'
		when c.candidate_citizenship like 'Banglad%' then 'BD'
		when c.candidate_citizenship like 'Belgium%' then 'BE'
		when c.candidate_citizenship like 'British%' then 'GB'
		when c.candidate_citizenship like 'Bulgari%' then 'BG'
		when c.candidate_citizenship like 'Canada%' then 'CA'
		when c.candidate_citizenship like 'Chinese%' then 'MO'
		when c.candidate_citizenship like 'Denmark%' then 'DK'
		when c.candidate_citizenship like 'Dutch%' then 'NL'
		when c.candidate_citizenship like 'Finland%' then 'FI'
		when c.candidate_citizenship like 'France%' then 'FR'
		when c.candidate_citizenship like 'German%' then 'DE'
		when c.candidate_citizenship like 'Germany%' then 'DE'
		when c.candidate_citizenship like 'Greece%' then 'GR'
		when c.candidate_citizenship like 'Greek%' then 'GR'
		when c.candidate_citizenship like 'Indian%' then 'IN'
		when c.candidate_citizenship like 'India%' then 'IN'
		when c.candidate_citizenship like 'Indones%' then 'ID'
		when c.candidate_citizenship like 'Iranian%' then 'IR'
		when c.candidate_citizenship like 'Iraqi%' then 'IQ'
		when c.candidate_citizenship like 'Ireland%' then 'IE'
		when c.candidate_citizenship like 'Irish%' then 'IE'
		when c.candidate_citizenship like 'Italian%' then 'IT'
		when c.candidate_citizenship like 'Italy%' then 'IT'
		when c.candidate_citizenship like 'Jamaica%' then 'JM'
		when c.candidate_citizenship like 'Netherl%' then 'NL'
		when c.candidate_citizenship like 'Nigeria%' then 'NG'
		when c.candidate_citizenship like 'Pakista%' then 'PK'
		when c.candidate_citizenship like 'Philipp%' then 'PH'
		when c.candidate_citizenship like 'Poland%' then 'PL'
		when c.candidate_citizenship like 'Polish%' then 'PL'
		when c.candidate_citizenship like 'Portuge%' then 'PT'
		when c.candidate_citizenship like 'Romania%' then 'RO'
		when c.candidate_citizenship like 'Singapo%' then 'SG'
		when c.candidate_citizenship like 'Spain%' then 'ES'
		when c.candidate_citizenship like 'Spanish%' then 'ES'
		when c.candidate_citizenship like 'Ugandan%' then 'UG'
		when c.candidate_citizenship like 'Zimbabw%' then 'ZW'
		when c.candidate_citizenship like '%UNITED%ARAB%' then 'AE'
		when c.candidate_citizenship like '%UAE%' then 'AE'
		when c.candidate_citizenship like '%U.A.E%' then 'AE'
		when c.candidate_citizenship like '%UNITED%KINGDOM%' then 'GB'
		when c.candidate_citizenship like '%UNITED%STATES%' then 'US'
		when c.candidate_citizenship like '%US%' then 'US'
                end as 'candidate-citizenship'
, case
		when c.candidate_Country like 'Abkhazi%' then ''
		when c.candidate_Country like 'African%' then 'ZA'
		when c.candidate_Country like 'Africa%' then 'ZA'
		when c.candidate_Country like 'America%' then 'US'
		when c.candidate_Country like 'Austral%' then 'AU'
		when c.candidate_Country like 'Banglad%' then 'BD'
		when c.candidate_Country like 'Belgium%' then 'BE'
		when c.candidate_Country like 'British%' then 'GB'
		when c.candidate_Country like 'Bulgari%' then 'BG'
		when c.candidate_Country like 'Canada%' then 'CA'
		when c.candidate_Country like 'Chinese%' then 'MO'
		when c.candidate_Country like 'Denmark%' then 'DK'
		when c.candidate_Country like 'Dutch%' then 'NL'
		when c.candidate_Country like 'Finland%' then 'FI'
		when c.candidate_Country like 'France%' then 'FR'
		when c.candidate_Country like 'German%' then 'DE'
		when c.candidate_Country like 'Germany%' then 'DE'
		when c.candidate_Country like 'Greece%' then 'GR'
		when c.candidate_Country like 'Greek%' then 'GR'
		when c.candidate_Country like 'Indian%' then 'IN'
		when c.candidate_Country like 'India%' then 'IN'
		when c.candidate_Country like 'Indones%' then 'ID'
		when c.candidate_Country like 'Iranian%' then 'IR'
		when c.candidate_Country like 'Iraqi%' then 'IQ'
		when c.candidate_Country like 'Ireland%' then 'IE'
		when c.candidate_Country like 'Irish%' then 'IE'
		when c.candidate_Country like 'Italian%' then 'IT'
		when c.candidate_Country like 'Italy%' then 'IT'
		when c.candidate_Country like 'Jamaica%' then 'JM'
		when c.candidate_Country like 'Netherl%' then 'NL'
		when c.candidate_Country like 'Nigeria%' then 'NG'
		when c.candidate_Country like 'Pakista%' then 'PK'
		when c.candidate_Country like 'Philipp%' then 'PH'
		when c.candidate_Country like 'Poland%' then 'PL'
		when c.candidate_Country like 'Polish%' then 'PL'
		when c.candidate_Country like 'Portuge%' then 'PT'
		when c.candidate_Country like 'Romania%' then 'RO'
		when c.candidate_Country like 'Singapo%' then 'SG'
		when c.candidate_Country like 'Spain%' then 'ES'
		when c.candidate_Country like 'Spanish%' then 'ES'
		when c.candidate_Country like 'Ugandan%' then 'UG'
		when c.candidate_Country like 'Zimbabw%' then 'ZW'
		when c.candidate_Country like '%UNITED%ARAB%' then 'AE'
		when c.candidate_Country like '%UAE%' then 'AE'
		when c.candidate_Country like '%U.A.E%' then 'AE'
		when c.candidate_Country like '%UNITED%KINGDOM%' then 'GB'
		when c.candidate_Country like '%UNITED%STATES%' then 'US'
		when c.candidate_Country like '%US%' then 'US'
                end as 'candidate-country'
--, c.candidate_Country as 'candidate-country'
, c.candidate_city as 'candidate-city'
, c.candidate_linkedln as 'candidate-linkedln'
, c.candidate_currentSalary as 'candidate-currentSalary'
, c.candidate_desiredSalary as 'candidate-desiredSalary'
, c.candidate_homePhone as 'candidate-homePhone'
, c.candidate_workPhone as 'candidate-workPhone'
, c.candidate_mobile as 'candidate-mobile'
, c.candidate_mobile as 'candidate-primaryphone'
, c.candidate_keyword as 'candidate-keyword'
, c.candidate_State as 'candidate-State'
, c.candidate_zipCode as 'candidate-zipCode'
, o.email as 'candidate-owners' --c.candidate_owners
, c.Source as 'Source'
, c.Status as 'Status'
        , ltrim(Stuff(    Coalesce('Job Title 1: ' + NULLIF(cast(c.candidate_jobTitle1 as varchar(max)) , '') + char(10), '')
                        + Coalesce('Job Title 2: ' + NULLIF(cast(c.candidate_jobTitle2 as varchar(max)) , '') + char(10), '')
                        + Coalesce('Note: ' + NULLIF(cast(c.candidate_note as varchar(max)), ''), '')
                , 1, 0, '') ) as 'company-note'
, doc.files as 'candidate-resume'
, his1.JobTitle as 'candidate-jobTitle1' , his1.EmployerName as 'candidate-employer1', his1.EmployerName as 'candidate-company1', his1.[Date Range] as 'candidate-startDate1', his1.[To] as 'candidate-enddate1'
, his2.JobTitle as 'candidate-jobTitle2' , his2.EmployerName as 'candidate-employer2', his2.EmployerName as 'candidate-company2', his2.[Date Range] as 'candidate-startDate2', his1.[To] as 'candidate-enddate2'
, his3.JobTitle as 'candidate-jobTitle3' , his3.EmployerName as 'candidate-employer3', his3.EmployerName as 'candidate-company3', his3.[Date Range] as 'candidate-startDate3', his1.[To] as 'candidate-enddate3'
, whis.whis as 'candidate-workhistory'
	-- select count (*) --12791
	-- select distinct convert(varchar,c.candidate_Country)
	from CandidateImportAutomappingTemplateversion c
	left join doc on cast(c.candidate_externalId as varchar(max)) = cast(doc.Candidate as varchar(max))
	left join owner o on cast(o.fullname as varchar(max)) =  cast(c.candidate_owners as varchar(max))
        left join (select * from his where rn = 1) his1 on convert(varchar(max),his1.candidate_externalId) = convert(varchar(max),c.candidate_externalId)
        left join (select * from his where rn = 2) his2 on convert(varchar(max),his2.candidate_externalId) = convert(varchar(max),c.candidate_externalId)
        left join (select * from his where rn = 3) his3 on convert(varchar(max),his3.candidate_externalId) = convert(varchar(max),c.candidate_externalId)
        left join whis on convert(varchar(max),whis.candidate_externalId) = convert(varchar(max),c.candidate_externalId)
        where convert(varchar(max),c.candidate_externalId) in ('10030')
        --where doc.files is not null
