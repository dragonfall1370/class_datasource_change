--CANDIDATE PRIMARY EMAIL
with SplitEmail as (select distinct PersonID
, value as SplitEmail
from CandidateMaster
cross apply string_split(EMail,' ')
where EMail like '%_@_%.__%')

, SplitEmail2 as (select distinct PersonID
	, value as SplitEmail
	from SplitEmail
	cross apply string_split(SplitEmail,';')
	where SplitEmail like '%_@_%.__%')

, EditedEmail as (select PersonID
	, case when charindex('.',SplitEmail) = 1 then right(SplitEmail,case when len(SplitEmail) < 1 then 0 else len(SplitEmail)-1 end) --check if '.' begins
	when charindex('.',reverse(SplitEmail)) = 1 then left(SplitEmail,case when len(SplitEmail) < 1 then 0 else len(SplitEmail)-1 end) --check if '.' ends
	else SplitEmail end as EditedEmail
	from SplitEmail2
	where SplitEmail like '%_@_%.__%')

, dup as (select PersonID
	, trim(' ' from translate(EditedEmail,'!'':"<>[]','        ')) as EmailAddress
	, row_number() over(partition by trim(' ' from translate(EditedEmail,'!'':"<>[]','        ')) order by PersonID asc) as rn
	from EditedEmail
	where EditedEmail like '%_@_%.__%')

--EDIT CANDIDATE EMAIL TO AVOID DUPLICATES
, NewEmail as (select PersonID
	, case when rn > 1 then concat(rn,'_',EmailAddress)
	else EmailAddress end as NewMail
	from dup)

--CANDIDATE MAY HAVE MULTIPLE EMAILS, TO FILTER THE 1ST EMAIL
, CandidateEmail as (select PersonID, NewMail, row_number() over (partition by PersonID order by NewMail asc) as rn
	from NewEmail)

--CANDIDATE DOCUMENTS
, DocumentsRow as (select distinct PersonID
	, right(FileServerLocation,CHARINDEX('\',reverse(FileServerLocation))-1) as NewFile
	, row_number() over(partition by right(FileServerLocation,CHARINDEX('\',reverse(FileServerLocation))-1) order by PersonID desc) as  rn
	from CandidateDocs
	where lower(right(FileServerLocation,CHARINDEX('.',reverse(FileServerLocation)))) in ('.pdf','.doc','.docx','.xls','.xlsx','.rtf','.msg','.txt','.htm','.html'))

--RENAME DOCUMENTS WITH ROW NUMBERS
, Documents as (select PersonID
	, case when rn > 1 then concat(left(NewFile,CHARINDEX('.',NewFile)-1),'_',rn-1,right(NewFile,CHARINDEX('.',reverse(NewFile))))
		else NewFile end as NewFile
	from DocumentsRow)

, CandidateDocuments as (select PersonID, string_agg(NewFile,',') as CandidateDocuments
	from Documents
	group by PersonID)

--CANDIDATE OWNERS
, Owners as (select c.PersonID, c.ConsultantID, u.Email from CandidateMaster c
	left join Users u on u.ConsultantID = c.ConsultantID
	where c.ConsultantID is not NULL and u.Email is not NULL

	UNION ALL
	select c.CandidateID, c.ConsultantID, u.Email from CandidateConsultants c
	left join Users u on u.ConsultantID = c.ConsultantID
	where c.ConsultantID is not NULL and u.Email is not NULL)

, CandidateOwner as (select PersonID, string_agg (Email,',') as CandidateOwner
	from Owners
	group by PersonID)

--CANDIDATE SKILLS
, CandidateSkill as (select CandidateID, string_agg (CategoryName,', ') as CandidateSkill
	from CandidateSearchCriteria
	group by CandidateID)

--MAIN SCRIPT
select concat('TF',cm.PersonID) as 'candidate-externalId'
, case when cm.FirstName = '' or cm.FirstName is NULL then 'Lastname'
	else cm.FirstName end as 'candidate-firstName'
, case when cm.MiddleNames = '' or cm.MiddleNames is NULL then ''
	else cm.MiddleNames end as 'candidate-middleName'
, case when cm.Surname = '' or cm.Surname is NULL then 'Lastname'
	else cm.Surname end as 'candidate-lastName'
, case when ce.NewMail is NULL then concat(cm.PersonID,'_candidate@noemail.com')
	else ce.NewMail end as 'candidate-email'
, concat_ws(' ',nullif(cm.MobileArea,''),nullif(cm.MobilePhone,'')) as 'candidate-phone'
, concat_ws(' ',nullif(cm.MobileArea,''),nullif(cm.MobilePhone,'')) as 'candidate-mobile'
, concat_ws(' ',nullif(cm.WorkArea,''),nullif(cm.WorkPhone,'')) as 'candidate-workPhone'
, concat_ws(' ',nullif(cm.HomeArea,''),nullif(cm.HomePhone,'')) as 'candidate-homePhone'
, cm.ConsultantID
, co.CandidateOwner as 'candidate-owner'

-->>CANDIDATE ADDRESS
, concat_ws(', ', nullif(cm.StreetAddress,'')
		, nullif(nullif(cm.City,''),'NA')
		, nullif(nullif(cm.Suburb,''),'NA')
		, nullif(nullif(cm.PostCode,''),'NA')
		, nullif(nullif(cm.StreetCountry,''),'NA')) as 'candidate-address'
, cm.City as 'candidate-city'
, cm.PostCode as 'candidate-zipCode'
, cm.StreetCountry as OriginalCountry
, case when cm.StreetCountry = 'FINLAND' then 'FI'
	when cm.StreetCountry = 'REPUBLIC OF KOREA' then 'KR'
	when cm.StreetCountry = 'BAHRAIN' then 'BH'
	when cm.StreetCountry = 'SOUTH KOREA' then 'KR'
	when cm.StreetCountry = 'PHILLIPENES' then 'PH'
	when cm.StreetCountry = 'VIETNAM' then 'VN'
	when cm.StreetCountry = 'USA' then 'US'
	when cm.StreetCountry = 'NEW ZEALAND' then 'NZ'
	when cm.StreetCountry = 'ANGOLA' then 'AO'
	when cm.StreetCountry = 'HAWAII' then 'US'
	when cm.StreetCountry = 'EGYPT' then 'EG'
	when cm.StreetCountry = 'INDIA/ USA' then 'IN'
	when cm.StreetCountry = 'ITALY' then 'IT'
	when cm.StreetCountry = 'BRAZIL' then 'BR'
	when cm.StreetCountry = 'COLUMBIA' then 'CO'
	when cm.StreetCountry = 'NETHERLANDS' then 'NL'
	when cm.StreetCountry = 'NZ' then 'NZ'
	when cm.StreetCountry = 'HONG KONG' then 'HK'
	when cm.StreetCountry = 'MALAYSIA' then 'MY'
	when cm.StreetCountry = 'SRI LANKA' then 'LK'
	when cm.StreetCountry = 'INDONESIA' then 'ID'
	when cm.StreetCountry = 'GERMANY' then 'DE'
	when cm.StreetCountry = 'PHILIPPINES' then 'PH'
	when cm.StreetCountry = 'ENGLAND' then 'GB'
	when cm.StreetCountry = 'AMERICAN SAMOAND' then 'AS'
	when cm.StreetCountry = 'KUWAIT' then 'KW'
	when cm.StreetCountry = 'HUNGARY' then 'HU'
	when cm.StreetCountry = 'PHILLIPNES' then 'PH'
	when cm.StreetCountry = 'SWITZERLAND' then 'CH'
	when cm.StreetCountry = 'UNITED STATES' then 'US'
	when cm.StreetCountry = 'KOREA' then 'KR'
	when cm.StreetCountry = 'SAMOA' then 'WS'
	when cm.StreetCountry = 'PHILLIPPINES' then 'PH'
	when cm.StreetCountry = 'MALTA' then 'MT'
	when cm.StreetCountry = 'BOSNIA AND HERZEGOVINA' then 'BA'
	when cm.StreetCountry = 'ETHIOPIA' then 'ET'
	when cm.StreetCountry = 'LEBANON' then 'LB'
	when cm.StreetCountry = 'SAUDI ARABIA' then 'SA'
	when cm.StreetCountry = 'IRAQ' then 'IQ'
	when cm.StreetCountry = 'AMERICAN SAMOA' then 'AS'
	when cm.StreetCountry = 'AUSTRALIA' then 'AU'
	when cm.StreetCountry = 'MAURITIUS' then 'MU'
	when cm.StreetCountry = 'UNITED KINGDOM' then 'GB'
	when cm.StreetCountry = 'MEXICO' then 'MX'
	when cm.StreetCountry = 'MÉXICO' then 'MX'
	when cm.StreetCountry = 'SWEDEN' then 'SE'
	when cm.StreetCountry = 'PHILLIPINES' then 'PH'
	when cm.StreetCountry = 'KENYA' then 'KE'
	when cm.StreetCountry = 'UKRAINE' then 'UA'
	when cm.StreetCountry = 'CHINA' then 'CN'
	when cm.StreetCountry = 'THE BAHAMAS' then 'BS'
	when cm.StreetCountry = 'ARGENTINA' then 'AR'
	when cm.StreetCountry = 'FIJI' then 'FJ'
	when cm.StreetCountry = 'SERBIA' then 'RS'
	when cm.StreetCountry = 'India' then 'IN'
	when cm.StreetCountry = 'NEW CALEDONIA' then 'NC'
	when cm.StreetCountry = 'CROATIA' then 'HR'
	when cm.StreetCountry = 'P.R.CHINA' then 'CN'
	when cm.StreetCountry = 'AUSTRIA' then 'AT'
	when cm.StreetCountry = 'GREECE' then 'GR'
	when cm.StreetCountry = 'BOTSWANA' then 'BW'
	when cm.StreetCountry = 'UNITED ARAB EMIRATES' then 'AE'
	when cm.StreetCountry = 'RUSSIA' then 'RU'
	when cm.StreetCountry = 'STATE OF QATAR' then 'QA'
	when cm.StreetCountry = 'UK' then 'GB'
	when cm.StreetCountry = 'KINGDOM OF SAUDI ARABIA' then 'SA'
	when cm.StreetCountry = 'ISRAEL' then 'IL'
	when cm.StreetCountry = 'ZIMBABWE' then 'ZW'
	when cm.StreetCountry = 'QATAR' then 'QA'
	when cm.StreetCountry = 'SWAZILAND' then 'SZ'
	when cm.StreetCountry = 'TONGA' then 'TO'
	when cm.StreetCountry = 'SYRIA' then 'SY'
	when cm.StreetCountry = 'POLAND' then 'PL'
	when cm.StreetCountry = 'CANADA' then 'CA'
	when cm.StreetCountry = 'BRUNEI' then 'BN'
	when cm.StreetCountry = 'MALAYSIA.' then 'MY'
	when cm.StreetCountry = 'SUDAN' then 'SD'
	when cm.StreetCountry = 'SRILANKA' then 'LK'
	when cm.StreetCountry = 'MOLDOVA' then 'MD'
	when cm.StreetCountry = 'IRELAND' then 'IE'
	when cm.StreetCountry = 'THAILAND' then 'TH'
	when cm.StreetCountry = 'NORWAY' then 'NO'
	when cm.StreetCountry = 'FRANCE' then 'FR'
	when cm.StreetCountry = 'THE NETHERLANDS' then 'NL'
	when cm.StreetCountry = 'BELGIUM' then 'BE'
	when cm.StreetCountry = 'JAPAN' then 'JP'
	when cm.StreetCountry = 'SPAIN' then 'ES'
	when cm.StreetCountry = 'OMAN' then 'OM'
	when cm.StreetCountry = 'TURKEY' then 'TR'
	when cm.StreetCountry = 'PAPUA NEW GUINEA' then 'PG'
	when cm.StreetCountry = 'VENEZUELA' then 'VE'
	when cm.StreetCountry = 'BANGLADESH' then 'BD'
	when cm.StreetCountry = 'DENMARK' then 'DK'
	when cm.StreetCountry = 'NORTHEN IRELAND' then 'IE'
	when cm.StreetCountry = 'ROMANIA' then 'RO'
	when cm.StreetCountry = 'PAKISTAN' then 'PK'
	when cm.StreetCountry = 'SINGAPORE' then 'SG'
	when cm.StreetCountry = 'SOUTH AFRICA,' then 'ZA'
	when cm.StreetCountry = 'COLOMBIA' then 'CO'
	when cm.StreetCountry = 'CAMBODIA' then 'KH'
	when cm.StreetCountry = 'TANZANIA' then 'TZ'
	when cm.StreetCountry = 'IRAN' then 'IR'
	when cm.StreetCountry = 'NEPAL' then 'NP'
	when cm.StreetCountry = 'CHILE' then 'CL'
	when cm.StreetCountry = 'SOUTH AFRICA' then 'ZA'
	when cm.StreetCountry = 'CZECH REPUBLIC' then 'CZ'
	when cm.StreetCountry = 'QATER' then 'QA'
	when cm.StreetCountry = 'ZAMBIA' then 'ZM'
	when cm.StreetCountry = 'PORTUGAL' then 'PT'
	when cm.StreetCountry = 'TAIWAN' then 'TW'
	when cm.StreetCountry = 'KAZAKHSTAN' then 'KZ'
	when cm.StreetCountry = 'NORTHERN IRELAND' then 'IE'
	when cm.StreetCountry = 'COSTA RICA' then 'CR'
	when cm.StreetCountry = 'UAE' then 'AE'
	when cm.StreetCountry = 'VANUATU' then 'VU'
else NULL end as 'candidate-Country'

-->> CANDIDATE INFO
, case when cm.Title = 'Mr' then 'MR'
	when cm.Title = 'DR' then 'DR'
	when cm.Title = 'Mrs' then 'MRS'
	when cm.Title in ('Ms','Miss') then 'MS'
	else NULL end as 'candidate-title'
, case when cm.Sex = 'F' then 'FEMALE'
	when cm.Sex = 'M' then 'MALE'
	else NULL end as 'candidate-gender'
--, case when cm.DateOfBirth > 0 then convert(varchar(10),convert(date,cm.DateOfBirth,112))
--	else '' end as 'candidate-dob'
, case when cm.DateOfBirth is not NULL then concat(left(cm.DateOfBirth,4),'-',substring(cm.DateOfBirth,5,2),'-',right(cm.DateOfBirth,2))
	else NULL end as 'candidate-dob'
, cm.Agency --CUSTOM FIELD: Source

-->> CANDIDATE WORK HISTORY
--, cm.EmploymentCategory --all NULL value | incorrect mapping >> not 'Type' field
, case when cm.PermanentRqd = 'X' then 'PERMANENT'
	when cm.ContractRqd = 'X' then 'CONTRACT'
	when cm.TemporaryRqd = 'X' then 'TEMPORARY'
	else 'PERMANENT' end 'candidate-jobTypes'
, cm.CurrentEmployer as 'candidate-employer1'
, cm.CurrentPosition as 'candidate-jobTitle1'
, case when isnumeric(cm.SalaryCurrent) = 1 then cm.SalaryCurrent
	else NULL end as 'candidate-currentSalary'
, case when isnumeric(cm.SalaryWanted) = 1 then cm.SalaryWanted
	else NULL end as 'candidate-desiredSalary'
, cm.UserCombo3 --CUSTOM FIELD: Residency Status
, s.CandidateSkill as 'candidate-skills'
, d.CandidateDocuments as 'candidate-resume'
, concat_ws(char(10),concat('Candidate External ID: ', cm.PersonID)
	, coalesce('Date Entered: ' + nullif(convert(varchar(10),convert(date,cm.DateEntered,112)),''),'')
	, coalesce('Date Next Available: ' + nullif(convert(varchar(10),convert(date,cm.DateNextAvailable,112)),''),'')
	, coalesce('Last Amended: ' + nullif(convert(varchar(10),convert(date,cm.LastAmended,112)),''),'')
	, coalesce('Known As: ' + nullif(cm.KnownAs,''),'')
	, coalesce('Email Secondary: ' + nullif(cm.EmailSecondary,''),'')
	, coalesce('Comment Date: ' + nullif(convert(varchar(10),convert(date,cm.CommDate,112)),''),'')
	, coalesce('Comments: ' + nullif(cm.Comments,''),'')
	) as 'candidate-note'
from CandidateMaster cm
left join CandidateEmail ce on ce.PersonID = cm.PersonID and ce.rn = 1
left join CandidateOwner co on co.PersonID = cm.PersonID
left join CandidateDocuments d on d.PersonID = cm.PersonID
left join CandidateSkill s on s.CandidateID = cm.PersonID
where cm.Status = 'A' --12743