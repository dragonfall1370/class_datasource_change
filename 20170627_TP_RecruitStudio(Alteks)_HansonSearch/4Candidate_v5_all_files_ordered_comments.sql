
with 
  skill as (select c.contactid,ss.skill from Contacts c left join SkillInstances si on c.contactid = si.objectid  left join skills ss on si.skillid = ss.skillid)
--SELECT contactid, skill = STUFF((SELECT skill + char(10) FROM skill b WHERE b.contactid = a.contactid FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 0, '') FROM skill a GROUP BY contactid

, mail1 (candidateID,email) as (select ContactId, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(email)),',',ltrim(rtrim(email2))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' ') as email from Contacts WHERE type in ('Active','Candidat','Candidate','Freelance','Internal candidate','Placed','Placed Candidate','Prospective candidate','Works for Client') )
, mail2 (candidateID,email) as (SELECT candidateID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT candidateID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (candidateID,email) as (SELECT candidateID, email from mail2 WHERE email like '%_@_%.__%')
, mail4a (candidateID,email,rn) as ( SELECT candidateID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY candidateID ORDER BY candidateID desc) FROM mail3 )
, mail4 (candidateID,email,rn) as ( select candidateID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email, rn from mail4a ) --where RIGHT(email, 1) = '.'
--, mailpe as (select candidateID,email as email1 from mail4 where rn = 1)
--, mailwe as (select candidateID,email as email1 from mail4 where rn = 2)
--, mailoe as (SELECT candidateID,email = STUFF((SELECT DISTINCT ',' + email from mail4 WHERE rn > 2 and candidateID = a.candidateID FOR XML PATH ('')), 1, 1, '') FROM mail4 AS a where rn >2 GROUP BY a.candidateID)
, mail5 (candidateid, email1, email2, email3) as (
		select pe.candidateID, email as email1, we.email2, oe.email3 from mail4 pe
		left join (select candidateID, email as email2 from mail4 where rn = 2) we on we.candidateID = pe.candidateID
		left join (SELECT candidateID, STUFF((SELECT DISTINCT ',' + email from mail4 WHERE rn > 2 and candidateID = a.candidateID FOR XML PATH ('')), 1, 1, '')  AS email3 FROM mail4 AS a where rn > 2 GROUP BY a.candidateID ) oe on oe.candidateid = pe.candidateid
		where pe.rn = 1 )
--select top 100 * from mail5 where email2 is not null

, jobapp as ( SELECT Contactid
                , jobapp = '-----' + char(10) + STUFF((SELECT case 
                                when NotInterested = 'true' then concat(
                                        'Job Application Date: ', lastupdate,char(10),
                                        'Job Company Name: ', Company,char(10),
                                        'Job Contact Name: ', ClientName,char(10),
                                        'Job Application: ', position,char(10),
                                        'Job Application Sub Status: Not Interested',char(10))
                                when CVRevised = 'true' then concat(
                                        'Job Application Date: ', lastupdate,char(10),
                                        'Job Company Name: ', Company,char(10),
                                        'Job Contact Name: ', ClientName,char(10),
                                        'Job Application: ', position,char(10),
                                        'Job Application Sub Status: CVRevised',char(10))
                                when Withdrawn = 'true' then concat(
                                        'Job Application Date: ', lastupdate,char(10),
                                        'Job Company Name: ', Company,char(10),
                                        'Job Contact Name: ', ClientName,char(10),
                                        'Job Application: ', position,char(10),
                                        'Job Application Sub Status: Withdrawn',char(10))
                                when Rejected = 'true' then concat(
                                        'Job Application Date: ',lastupdate,char(10),
                                        'Job Company Name: ', Company,char(10),
                                        'Job Contact Name: ', ClientName,char(10),
                                        'Job Application: ', position,char(10),
                                        'Job Application Sub Status: Rejected',char(10))
                                when RejectedOffer = 'true' then concat(
                                        'Job Application Date: ',lastupdate,char(10),
                                        'Job Company Name: ', Company,char(10),
                                        'Job Contact Name: ', ClientName,char(10),
                                        'Job Application: ', position,char(10),
                                        'Job Application Sub Status: Rejected Offer',char(10))
                                --else ''
                                end + char(10)
                                from CandidatesList2 WHERE Contactid = a.Contactid order by lastupdate desc FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 0, '' ) 
                FROM CandidatesList2 a
                --left join Contacts cl on cl.contactid = a.contactid
                WHERE Contactid = a.Contactid 
                --and cl.type in ('Active','Candidat','Candidate','Freelance','Internal candidate','Placed','Placed Candidate','Prospective candidate','Works for Client')
                and NotInterested = 'true' or CVRevised = 'true' or Withdrawn = 'true' or Rejected = 'true' or RejectedOffer = 'true'
                GROUP BY Contactid )

 --select top 30
 select
	 CL.ContactId as 'candidate-externalId'
	, owner.email as 'candidate-owners'
	, CL.username as '#candidate-owners'
	, CL.UserId as '#candidate-owners-id'
	--, case when ( CL.FirstName = '' or CL.FirstName is null) then 'No Firstname' else replace(CL.FirstName,'?','') end as 'candidate-firstName'
	--, case when ( CL.LastName = '' or CL.LastName is null) then 'No Lastname' else replace(CL.LastName,'?','') end as 'candidate-Lastname'
	, Coalesce(NULLIF(replace(CL.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
	, Coalesce(NULLIF(replace(CL.LastName,'?',''), ''), 'No Lastname') as 'contact-lastName'
	--, CL.Title as 'candidate-title'
	, case
                when CL.Title like '%Sir%' then 'MR'
                when CL.Title like '%Ms.%' then 'MS'
                when CL.Title like '%Ms%' then 'MS'
                when CL.Title like '%Mrs?%' then 'MRS'
                when CL.Title like '%Mrs%' then 'MRS'
                when CL.Title like '%Mrr%' then 'MR'
                when CL.Title like '%Mr`%' then 'MR'
                when CL.Title like '%Mr.%' then 'MR'
                when CL.Title like '%MR Profess%' then 'MR'
                when CL.Title like '%Mr luca%' then 'MR'
                when CL.Title like '%Mr%' then 'MR'
                when CL.Title like '%Miss`%' then 'MISS'
                when CL.Title like '%Miss%' then 'MISS'
                when CL.Title like '%Md%' then 'MRS'
                when CL.Title like '%M%' then 'MR'
                when CL.Title like '%F%' then 'MS'
        else '' 
	end as 'candidate-title'
		
	, case
		when CL.Title like '%Ms%' then 'FEMALE' 
		when CL.Title like '%Mrs%' then 'FEMALE' 
		when CL.Title like '%Miss%' then 'FEMALE'
		when CL.Title like '%Md%' then 'FEMALE'
		when CL.Title like '%Mr%' then 'MALE' 
		when CL.Title = 'M' then 'MALE' 
		when CL.Title like '%Sir%' then 'MALE' 
		else '' 
		end as 'candidate-gender'
	, CL.JobTitle as 'candidate-jobTitle1'
	, CL.Company as 'candidate-Company1'
	, CL.CompanyId as '#candidate-Company External ID'
	
	/*, replace(replace(replace(replace(replace(
		case when ( CL.Email != '' and CL.Email is not null and CL.Email like '%@%')
	 	then CL.Email else (case when ( CL.EMail2 != '' and CL.EMail2 is not null and CL.Email2 like '%@%') then CL.EMail2 else '' end) end
	 	,'?',''),'&',''),'gmail','@gmail'),'yahoo','@yahoo'),'@@','@') as 'candidate-email' */
	 , mail5.email1 as 'candidate-email'
	 , mail5.email2 as 'candidate-workEmail'
	
	, case when ( cast(CL.DirectTel as varchar(max)) != '' and cast(CL.DirectTel as varchar(max)) is not null) then cast(CL.DirectTel as varchar(max)) else
	 (case when ( cast(CL.MobileTel as varchar(max)) != '' and cast(CL.MobileTel as varchar(max)) is not null) then cast(CL.MobileTel as varchar(max)) else CL.WorkTel end)
	  end as 'candidate-phone' --primary phone
	--, CL.DirectTel as 'candidate-phone'
	, CL.MobileTel as 'candidate-mobile'
	, CL.WorkTel as 'candidate-workPhone'
	, CL.HomeTel as 'candidate-homePhone'
	
	, CL.Address1 as 'candidate-address'
	, Coalesce(NULLIF(CL.City, ''), CL.SubLocation) as 'candidate-city' --CL.SubLocation if city empty
	--, CL.City, CL.SubLocation
	, CL.County as 'candidate-state'
        , CL.PostCode as 'candidate-zipCode'
        
	--, CL.Country as 'candidate-country'
	, case
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Africa%' then 'ZA'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Algeri%' then 'DZ'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Americ%' then 'US'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Argent%' then 'AR'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Austra%' then 'AU'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Austri%' then 'AT'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Bahrai%' then 'BH'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Belgiu%' then 'BE'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Belgui%' then 'BE'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Boulog%' then 'FR'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Brazil%' then 'BR'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Brusse%' then 'BE'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Bulgar%' then 'BG'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Cambod%' then 'KH'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Canada%' then 'CA'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Chezc%' then 'CZ'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Chian%' then 'CN'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Chile%' then 'CL'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'China%' then 'CN'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Clichy%' then 'FR'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Colomb%' then 'LK'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Croati%' then 'HR'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Czech%' then 'CZ'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Denmar%' then 'DK'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Dubai%' then 'AE'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Egypt%' then 'EG'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Englad%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Englan%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Finlan%' then 'FI'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'France%' then 'FR'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'German%' then 'DE'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Great%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Greece%' then 'GR'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Hampsh%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Herns%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Hong%' then 'CN'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Hungar%' then 'HU'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'India%' then 'IN'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Indone%' then 'ID'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Irelan%' then 'IE'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Israel%' then 'IL'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Italy%' then 'IT'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'ITALY%' then 'IT'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Japan%' then 'JP'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Jordan%' then 'JO'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Korea%' then 'KR'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'latvia%' then 'LV'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Lebano%' then 'LB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Levall%' then 'FR'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Lithua%' then 'LT'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Lodno%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Londno%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'London%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Londop%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Lond%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'lon%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'LUXEMB%' then 'LU'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Lyon%' then 'FR'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Malays%' then 'MY'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Mexico%' then 'MX'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Morocc%' then 'MA'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Münch%' then 'DE'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Nether%' then 'NL'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Neuill%' then 'FR'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Norway%' then 'NO'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Pakist%' then 'PK'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Paris%' then 'FR'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Peru%' then 'PE'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Philip%' then 'PH'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Poland%' then 'PL'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Portug%' then 'PT'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'P.R%' then 'CN'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'PR%' then 'CN'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Qatar%' then 'QA'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Quebec%' then 'CA'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'ROMANI%' then 'RO'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Russia%' then 'RU'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Saint%' then 'LC'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Saudi%' then 'SA'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Scotla%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Sevres%' then 'FR'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Singap%' then 'SG'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Sloven%' then 'SI'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Spain%' then 'ES'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Sweden%' then 'SE'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Switze%' then 'CH'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Swizer%' then 'CH'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Thaila%' then 'TH'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'The%' then 'NL'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Tunisi%' then 'TN'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Turkey%' then 'TR'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'UAE%' then 'AE'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'U.A.E%' then 'AE'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'UKrain%' then 'UA'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'UKr%' then 'UA'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'UK%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'United%' then 'US'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'USA%' then 'US'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'US%' then 'US'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Wales%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Zealan%' then 'NZ'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like 'Zurich%' then 'CH'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like '%UNITED%ARAB%' then 'AE'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like '%UAE%' then 'AE'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like '%U.A.E%' then 'AE'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like '%UNITED%KINGDOM%' then 'GB'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like '%UNITED%STATES%' then 'US'
		when Coalesce(NULLIF(CL.Country, ''), CL.Location) like '%US%' then 'US'
	end as 'candidate-country' --CL.Location if country empty
	--,CL.Country,CL.Location
	--, CA.Nationality as 'candidate-citizenship'
	, case
		when CA.nationality like 'Africa%' then 'CF'
		when CA.nationality like 'Afrika%' then ''
		when CA.nationality like 'Algeri%' then 'DZ'
		when CA.nationality like 'Americ%' then 'AS'
		when CA.nationality like 'Arabic%' then ''
		when CA.nationality like 'Armani%' then ''
		when CA.nationality like 'Asian%' then ''
		when CA.nationality like 'Austra%' then 'AU'
		when CA.nationality like 'Bahrai%' then 'BH'
		when CA.nationality like 'Belgia%' then ''
		when CA.nationality like 'Britis%' then 'GB'
		when CA.nationality like 'Canadi%' then ''
		when CA.nationality like 'Croati%' then 'HR'
		when CA.nationality like 'Czech%' then 'CZ'
		when CA.nationality like 'Danish%' then ''
		when CA.nationality like 'Dutch%' then ''
		when CA.nationality like 'Egypti%' then ''
		when CA.nationality like 'egypt%' then 'EG'
		when CA.nationality like 'Emirat%' then 'AE'
		when CA.nationality like 'Filipi%' then ''
		when CA.nationality like 'French%' then 'TF'
		when CA.nationality like 'German%' then 'DE'
		when CA.nationality like 'Greek%' then ''
		when CA.nationality like 'Hungar%' then 'HU'
		when CA.nationality like 'Indian%' then ''
		when CA.nationality like 'India%' then 'IN'
		when CA.nationality like 'irania%' then ''
		when CA.nationality like 'Iraqi%' then ''
		when CA.nationality like 'Iraq%' then 'IQ'
		when CA.nationality like 'Irish%' then ''
		when CA.nationality like 'Italia%' then ''
		when CA.nationality like 'Jordan%' then 'JO'
		when CA.nationality like 'Lebane%' then ''
		when CA.nationality like 'Morocc%' then 'MA'
		when CA.nationality like 'Nether%' then 'NL'
		when CA.nationality like 'Nigeri%' then 'NG'
		when CA.nationality like 'Omani%' then 'RO'
		when CA.nationality like 'Pakist%' then 'PK'
		when CA.nationality like 'palest%' then 'PS'
		when CA.nationality like 'Palest%' then 'PS'
		when CA.nationality like 'Philip%' then 'PH'
		when CA.nationality like 'Polish%' then ''
		when CA.nationality like 'Portug%' then 'PT'
		when CA.nationality like 'Qatari%' then ''
		when CA.nationality like 'romani%' then 'RO'
		when CA.nationality like 'Russia%' then 'RU'
		when CA.nationality like 'Saudi%' then 'SA'
		when CA.nationality like 'Scotis%' then ''
		when CA.nationality like 'Scotti%' then ''
		when CA.nationality like 'Serbia%' then 'RS'
		when CA.nationality like 'Singap%' then 'SG'
		when CA.nationality like 'Spanis%' then ''
		when CA.nationality like 'Sudane%' then ''
		when CA.nationality like 'Swedis%' then ''
		when CA.nationality like 'Swiss%' then 'CH'
		when CA.nationality like 'Syrian%' then 'SY'
		when CA.nationality like 'Tanzan%' then 'TZ'
		when CA.nationality like 'Thai%' then 'TH'
		when CA.nationality like 'Tunisi%' then 'TN'
		when CA.nationality like 'Turkis%' then ''
		when CA.nationality like 'UAE%' then 'AE'
		when CA.nationality like 'Urdu%' then ''
		when CA.nationality like 'Venezu%' then 'VE'
		when CA.nationality like 'Zealan%' then 'NZ'
		else ''
	end as 'candidate-citizenship'
/*	
	, case
		when j.PermanentJob is null then 'PERMANENT'
		when j.PermanentJob = '' then  'PERMANENT'
		when j.PermanentJob like '%Contract%' then 'CONTRACT'
		when j.PermanentJob like '%FREELANCE%' then 'TEMPORARY'
		when j.PermanentJob like '%Part Time%' then 'TEMPORARY'
		when j.PermanentJob like '%perm%' then 'PERMANENT'
		when j.PermanentJob like '%Permamaent %' then 'PERMANENT'
		when j.PermanentJob like '%Permamant %' then 'PERMANENT'
		when j.PermanentJob like '%Permanent%' then 'PERMANENT'
		when j.PermanentJob like '%Temporary%' then 'TEMPORARY'
	end as 'candidate-jobTypes'
*/
	, case
	       when CA.JobType = 'Contract' then 'CONTRACT'
	       when CA.JobType = 'Temporary' then 'TEMPORARY'
	       when CA.JobType = 'Permanent' then 'PERMANENT'
	       when CA.JobType = '' then ''
	       when CA.JobType is null then ''
        end as 'candidate-jobTypes'
	
	, case
	       when CA.FullTimeJob = 'Part Time' then 'PART_TIME'
	       when CA.FullTimeJob = 'Full Time' then 'FULL_TIME'
	       when CA.FullTimeJob = '' then ''
	       when CA.FullTimeJob is null then ''
        end as 'candidate-employmentType'
	
	, CA.SalaryWanted as 'candidate-desiredSalary'	
	, CA.CurrentSalary as 'candidate-currentSalary'
	, Coalesce( NULLIF(skills.skill, ''), '') as 'candidate-skills' --case when (skills.skill = '' OR skills.skill is NULL) THEN '' ELSE concat ('Skills: ',replace(skills.skill,'&amp; ',''),char(10)) END 
	, at.Filename as 'candidate-resume'
	
	, CONVERT(VARCHAR(10),CA.dob,110) as 'candidate-dob'
	--, Education.school as 'candidate-schoolName'
	--, Education.graduationDate as 'candidate-graduationDate'
	--, Education.degree as 'candidate-degreeName'
	--, Education.major as '(candidate-major)'
	--, CA.currency1 as 'candidate-currency'
	 , case
                when CA.currency1 like '%£%' then 'GBP'
                when CA.currency1 like '%AED%' then 'AED'
                when CA.currency1 like '%AUD%' then 'AUD'
                when CA.currency1 like '%AUS%' then 'AUS'
                when CA.currency1 like '%Euro%' then 'EUR'
                when CA.currency1 like '%HK%' then 'HKD'
                when CA.currency1 like '%SwFr%' then 'CHF'
                when CA.currency1 like '%UK£%' then 'GBP'
                when CA.currency1 like '%US$%' then 'USD'
        else '' 
	end as 'candidate-currency'
	, concat(
	        --  case when (CL.RegDate = '' OR CL.RegDate is NULL) THEN '' ELSE concat ('RegDate: ',CL.RegDate,char(10)) END
	        --, coalesce( NULLIF('Candidate ID: ' + CL.ContactId, '') + char(10), '')
                --, case when (CL.CandidateRef = '' OR CL.CandidateRef is NULL) THEN '' ELSE concat ('Candidate Ref: ',CL.CandidateRef,char(10)) END
                  coalesce( 'Candidate Type: ' + NULLIF(CL.Type, '') + char(10), '')
		--, case when (CL.Sector = '' OR CL.Sector is NULL) THEN '' ELSE concat ('Sector: ',CL.Sector,char(10)) END
                --, case when (CL.Segment = '' OR CL.Segment is NULL) THEN '' ELSE concat ('Segment: ',CL.Segment,char(10)) END
                , case when (CA.PositionWanted = '' OR CA.PositionWanted is NULL) THEN '' ELSE concat ('Position Wanted: ',CA.PositionWanted,char(10)) END
		, case when (CA.SectorWanted = '' OR CA.SectorWanted is NULL) THEN '' ELSE concat ('Sector Wanted: ',CA.SectorWanted,char(10)) END
		--, case when (CL.Discipline = '' OR CL.Discipline is NULL) THEN '' ELSE concat ('Discipline: ',CL.Discipline,char(10)) END
		--, case when (CL.Department = '' OR CL.Department is NULL) THEN '' ELSE concat ('Department: ',CL.Department,char(10)) END
		--, coalesce( NULLIF('Availability: ' + CONVERT(VARCHAR(10),ca.Availability,110), '') + char(10), '')
		, case when (CA.NoticePeriod = '' OR CA.NoticePeriod is NULL) THEN '' ELSE concat ('Notice Period: ',CA.NoticePeriod,char(10)) END
		--, case when (CA.ReasonDeclined = '' OR CA.ReasonDeclined is NULL) THEN '' ELSE concat ('Reason Declined: ',CA.ReasonDeclined,char(10)) END
		--, coalesce( NULLIF('Star Rating: ' + convert(nvarchar(max), cl.StarRating), '') + char(10), '')
		--, case when (CL.Fax = '' OR CL.Fax is NULL) THEN '' ELSE concat ('Fax: ',CL.Fax,char(10)) END
		, case when (CL.WebSite = '' OR CL.WebSite is NULL) THEN '' ELSE concat ('WebSite: ',CL.WebSite,char(10)) END
                --, case when (CC.companystatus = '' or CC.companystatus is null) then '' else concat('Company Status: ',CC.companystatus,char(10)) end		
		--, case when (CL.Location = '' OR CL.Location is NULL) THEN '' ELSE concat ('Location: ',CL.Location,char(10)) END -- if country empty
		--, case when (CL.SubLocation = '' OR CL.SubLocation is NULL) THEN '' ELSE concat ('SubLocation: ',CL.SubLocation,char(10)) END -- if city empty
		--, case when (CL.Address2 = '' OR CL.Address2 is NULL) THEN '' ELSE concat ('Address2: ',CL.Address2,char(10)) END
		--, case when (CL.Address3 = '' OR CL.Address3 is NULL) THEN '' ELSE concat ('Address3: ',CL.Address3,char(10)) END
		, case when (CA.Currency1 = '' OR CA.Currency1 is NULL) THEN '' ELSE concat ('Currency: ',CA.Currency1,char(10)) END
		--, coalesce( 'Source: ' + NULLIF(cl.Source, '') + char(10), '')
		, case when (CL.ContactSource = '' OR CL.ContactSource is NULL) THEN '' ELSE concat ('Candidate Source: ',CL.ContactSource,char(10)) END
		, case when (CL.ContactStatus = '' OR CL.ContactStatus is NULL) THEN '' ELSE concat ('Status: ',CL.ContactStatus,char(10)) END
		--, case when (CL.LastUpdate = '' OR CL.LastUpdate is NULL) THEN '' ELSE concat ('LastUpdate: ',CL.LastUpdate,char(10)) END
                --, case when AgreedToEmail = 'false' then 'Can email: No' + char(10) when AgreedToEmail = 'true' then 'Can Email: Yes' + char(10) end
                --, case when Newsletter = 'false' then 'Newsletter: No' + char(10) when Newsletter = 'true' then 'Newsletter: Yes' + char(10) end
		--, select distinct hotlist from contacts
                --, case when CL.Embargoed = 'false' then 'Embargoed: No' + char(10) when CL.Embargoed = 'true' then 'Embargoed: Yes' + char(10) end --, select distinct Embargoed from contacts
                --, jobapp.jobapp
	  ) as 'candidate-note'
/*
	, concat(
		  case when (CL.Comments = '' OR CL.Comments is NULL) THEN '' ELSE concat ('Comments: ',CL.Comments,char(10)) END
		, case when (n1.NotesID = '' OR n1.NotesId is NULL) THEN '' ELSE concat ('NotesId: ',n1.NotesId,char(10)) END
		, case when (n.Text = '' OR n.Text is NULL) THEN '' ELSE replace(concat (n.Text,char(10)),'&amp; ','') END
	) as 'contact-comments'
*/
-- select count(*) --90951
-- select top 2000 Source,ContactSource
from Contacts CL 
left join (SELECT contactid, text = STUFF((SELECT char(10) + 'Note: ' + text + char(10) FROM notes b WHERE b.contactid = a.contactid FOR XML PATH('')), 1, 0, '') FROM notes a GROUP BY contactid) n on CL.contactid = n.contactid
left join (SELECT contactid, notesid = STUFF((SELECT ',' + notesid + char(10) FROM notes b WHERE b.contactid = a.contactid FOR XML PATH('')), 1, 1, '') FROM notes a GROUP BY contactid) n1 on CL.contactid = n1.contactid
left join (SELECT id, filename = STUFF((SELECT DISTINCT ',' + replace(filename,',','') from Attachments WHERE id = a.id FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') FROM Attachments a GROUP BY id) at on cl.contactid = at.Id
left join (SELECT contactid, skill = STUFF((SELECT skill + char(10) FROM skill b WHERE b.contactid = a.contactid FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 0, '') FROM skill a GROUP BY contactid) skills on CL.contactid = skills.contactid
left join candidates CA on CL.ContactId = CA.ContactId --91379
--left join vacancies j on CL.ContactId = j.ContactId
left join Companies CC on CL.CompanyId = CC.CompanyId
--left join ( select CL.username as name, case when (CL.Email like '%_@_%.__%') THEN CL.Email ELSE '' END as 'email' from Contacts CL where CL.displayname = CL.username and CL.Email <> '' and CL.displayname is not null ) owner on CL.username = owner.name
left join (select CL.username as name, Coalesce( NULLIF(cl.Email, ''), '') as email from Contacts CL where CL.Email like '%_@_%.__%' and CL.displayname = CL.username) owner on CL.username = owner.name
left join mail5 on mail5.candidateid = CL.contactid
--left join jobapp on jobapp.contactid = CL.contactid
--where CL.type in ('Active','Candidat','Candidate','Freelance','Internal candidate','Placed','Placed Candidate','Prospective candidate','Works for Client',null,'','##')
where cl.descriptor = 2
--and CL.FirstName = 'Hadil'
and CL.username = 'Nikki Samson' or CL.username = 'Elaine Hardman'
--and  cl.contactid in ('406978-2364-10249')
--and CL.Country is not null
--and CL.Title is not null or CL.Title != ''
--CL.email is null or CL.email = ''
--and CL.email2 is not null or CL.email2 != ''
--order by CL.email2 desc
--and CL.type is null or CL.type = '' or CL.type = '##'

/*
left join tmp_country tc ON c.countryID = tc.code
left join bullhorn1.BH_UserContact UC2 on C.userID = UC2.userID
left join tmp_email_3 on C.recruiterUserID = tmp_email_3.recruiterUserID
left join tmp_email_1 on C.candidateID = tmp_email_1.candidateID
--left join (select userID, max(userEducationID) as userEducationID from bullhorn1.BH_UserEducation group by userID) UE_2 on C.userID = UE_2.userID
--left join (select * from [bullhorn1].[BH_UserEducation] where isDeleted = 0) UE on UE.userEducationID = UE_2.userEducationID
left join Education on C.userID = Education.userID
left join t4 on t4.candidateUserID = C.userID
left join tmp_6 on C.userID = tmp_6.candidateUserID
left join tmp_note on C.userID = tmp_note.Userid
left join tmp_addednote AN on C.userID = AN.Userid
--left join (SELECT candidateID, STUFF((SELECT '  Summary: ' + convert(varchar(max),comments) + char(10) from  bullhorn1.BH_CandidateHistory WHERE candidateID = b.candidateID FOR XML PATH ('')), 1, 2, '')  AS URLList FROM bullhorn1.BH_CandidateHistory as b GROUP BY b.candidateID) sum on C.candidateID = sum.candidateID
left join (SELECT candidateID, STUFF((SELECT case when (convert(varchar(max),comments) = '' or comments is null) then '' else char(10) + ' ' + 'Summary: ' end + convert(varchar(max),comments) from  bullhorn1.BH_CandidateHistory WHERE candidateID = b.candidateID FOR XML PATH ('')), 1, 2, '')  AS URLList FROM bullhorn1.BH_CandidateHistory as b GROUP BY b.candidateID) sum on C.candidateID = sum.candidateID
where C.isPrimaryOwner = 1
--inner join bullhorn1.BH_UserContact UC_2 on C.recruiterUserID = UC_2.userID
--left join bullhorn1.BH_ClientCorporation CC ON CC.clientCorporationID = C.clientCorporationID
)

--select top 20 [candidate-firstName], [candidate-Lastname], [candidate-notes]
select *
from t1 
--where [candidate-Lastname] like '%Sheary%'
--where [candidate-notes] like '%Summary%' --order by [candidate-firstName]

--inner join tmp_email_2 on t1.[candidate-externalId] = tmp_email_2.candidateID
--order by userID
*/

/* Check if candidate is not primary owner
select userID from bullhorn1.Candidate
where isPrimaryOwner = 1
group by userID having count(*) > 1
*/

/*
select at.*
from Contacts CL
left join ( SELECT id, filename, ref, replace(replace(ref,'\\rsserver\',''),'c:\','') as newref from Attachments ) at on cl.contactid = at.Id
where cl.descriptor = 2 and at.filename is not null and at.filename <> '' 

select distinct username, email,email2 from Contacts where username <> '' and username is not null and username = 'Nikki Samson' --or username = 'Elaine Hardman'

select CL.username as name, Coalesce( NULLIF(cl.Email, ''), '') as email from Contacts CL where CL.displayname like '%Nikki%' or   CL.displayname like '%Elaine%'
select CL.username, CL.displayname, cl.Email, CL.FirstName,CL.lastname from Contacts CL where CL.FirstName like '%Nikki%' and  CL.lastname like '%Samson%'
select CL.username, CL.displayname, cl.Email, CL.FirstName,CL.lastname from Contacts CL where CL.FirstName like '%Elaine%' and  CL.lastname like '%Hardman%' 

select CL.username, CL.displayname, cl.Email, CL.FirstName,CL.lastname from Contacts CL where CL.FirstName like '%Ravindra%' --and  CL.lastname like '%Hardman%' 

Basil al essa – basil.alessa@gmail.com
Reshma Sharma - reshmacs@hotmail.com
Ravindra Duddukuru - duddukururavi@gmail.com

select CL.username, owner.email
from Contacts CL 
left join (select CL.username as name, Coalesce( NULLIF(cl.Email, ''), '') as email from Contacts CL where CL.Email like '%_@_%.__%' and CL.displayname = CL.username) owner on CL.username = owner.name

*/