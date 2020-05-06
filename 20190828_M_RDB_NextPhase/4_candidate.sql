with candidateinIDobject as (
        select o.ObjectID,o.LocationId,o.FileAs,o.FlagText,o.SourceId,o.CreatedOn
        from dbo.Objects o
        where o.ObjectTypeId=1 --candidate
)
, alladdress as (
        select ad.ObjectId,ad.AddressId,adt.Description,ad.Building,ad.Street,ad.District,ad.City,ad.PostCode
		, lvct.ValueName as 'County'
		, lvc.ValueName as 'Country'
		, lvc.SystemCode as CountryCode
		, row_number() OVER(partition by ad.ObjectID order by ad.AddressID desc) AS rn
        from dbo.Address ad
        left join dbo.AddressTypes adt on ad.AddressTypeId = adt.AddressTypeId
        left join dbo.ListValues lvc on lvc.ListValueId = ad.CountryValueId --country
        left join dbo.ListValues lvct on lvct.ListValueId = ad.CountyValueId --county
) --select distinct Country from alladdress
, candidate_address as (--All addresses group by candidate
        select ObjectId
		, string_agg(concat_ws(', '
				, nullif(trim(Building),'')
				, nullif(trim(Street),'')
				, nullif(trim(District),'')
				, nullif(trim(City),'')
				, nullif(trim(PostCode),'')
				, nullif(trim(County),'')
				, nullif(trim(Country),'')), char(10)) as candidate_address
        from alladdress
        group by ObjectId
)
, address_1 as (
        select  aad.ObjectId
                , nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.Building),'') as Building
                , nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.Street),'') as Street
                , nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.District),'') as District
                , nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.City),'') as City
                , nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.PostCode),'') as PostCode
                , nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.County),'') as County
                , nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.Country),'') as Country
				, nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.CountryCode),'') as CountryCode
                , concat_ws(', ',
					nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.Building),''),
					nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.Street),''),
					nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.District),''),
					nullif(TRIM(' ,''".+|*()[]\/{}' FROM aad.City),''),
					nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.PostCode),''),
					nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.County),''),
					nullif(TRIM(' ,''".+|*[]\/{}' FROM aad.Country),'')) as 'candidate_address'
        from alladdress aad
        where rn =1
		)
, mobilephone as (--MOBILES AS PRIMARY PHONE
		select p.ObjectID,p.PhoneId,p.CreatedOn
		, case when patindex('%[a-zA-Z]%',Num) > 0 then left(trim(num), patindex('%[a-zA-Z]%',Num) - 1) 
			else trim(Num) end as phone
        from dbo.Phones p
        where p.CommunicationTypeId = 83 --Mobile
		) --reused for other entities
, mobile as (--CANDIDATE MOBILE AS PRIMARY PHONE
		select ObjectID
		, string_agg(nullif(phone,''),',') as mobile
		from mobilephone
		group by ObjectID
		)
, otherphone as (--ALL OTHER PHONES
				select p.ObjectID, p.PhoneId, p.CreatedOn, p.CommunicationTypeId
				, case when patindex('%[a-zA-Z]%',Num) > 0 then left(trim(num), patindex('%[a-zA-Z]%',Num) - 1) 
				else trim(Num) end as phone
        from Phones p
        where p.CommunicationTypeId in (80, 84, 79, 81, 82, 87, 88) --CommunicationTypes: fax, pager, phone, day, evening, office, home
		) 
, otherphone_group as (select ObjectID, CommunicationTypeId
				, string_agg(nullif(phone, ''),', ') as otherphone
				from otherphone
				group by ObjectID, CommunicationTypeId
		)
, allphone as (select ObjectID
		, string_agg(concat_ws(' '
			, case when CommunicationTypeId = 80 then 'Fax:'
					when CommunicationTypeId = 84 then 'Pager:'
					when CommunicationTypeId = 79 then 'Phone:'
					when CommunicationTypeId = 81 then 'Phone (Day):'
					when CommunicationTypeId = 82 then 'Phone (Evening):'
					when CommunicationTypeId = 87 then 'Phone (Home):'
					when CommunicationTypeId = 88 then 'Phone (Office):'
					end
			, nullif(otherphone,'')
			), char(10)) as allphone
		from otherphone_group
		group by ObjectID
		)
, othernetwork as (--ALL URL, NETWORK
        select p.ObjectID, CommunicationTypeId, STRING_AGG(TRIM('.' FROM p.Num),',') as othernetwork
        from Phones p
        where p.CommunicationTypeId in (89, 90) --CommunicationTypes: URL, Social Networking
		group by p.ObjectID, CommunicationTypeId
		)
, allnetwork as (select ObjectID
		, string_agg(concat_ws(' '
			, case when CommunicationTypeId = 89 then 'URL:'
					when CommunicationTypeId = 90 then 'Social networking:'
					end
			, nullif(othernetwork,'')
			), char(10)) as allnetwork
		from othernetwork
		group by ObjectID
		)
, email as (--EMAIL
        select p.ObjectID, p.PhoneId
		, TRIM('.' FROM p.Num) as Email--,ROW_NUMBER() OVER(PARTITION BY p.ObjectID ORDER BY p.CreatedOn ASC) AS rn
        from Phones p
        where p.CommunicationTypeId=78 --CommunicationTypes: Email --Email cannot be retrieved from PrimaryEmailAddressPhoneId
        and p.Num like '%_@_%.__%'
		)
, mail_transform as (select ObjectID, PhoneId
				, replace(translate (Email, '!'':"<>[]();,+', '             '), char(10), ' ') as Email
				from email
		)
, mail_split as (select ObjectID, PhoneId
				, trim(value) as email
				from mail_transform
				cross apply string_split(Email, ' ')
				where Email like '%_@_%.__%'
			) --select * from mail_split where ObjectId = 52038
, dup as (select ObjectID --EMAIL CHECK DUPLICATION
		, trim(' ' from email) as EmailAddress
		, row_number() over(partition by trim(' ' from email) order by ObjectID asc) as rn --distinct email if emails exist more than once
		, row_number() over(partition by ObjectID order by PhoneId desc) as Contactrn --distinct if contacts may have more than 1 email
		from mail_split
		where Email like '%_@_%.__%'
		)
, primaryEmail as (select ObjectID --PRIMARY EMAIL
		, case when rn > 1 then concat(rn,'_',EmailAddress)
		else EmailAddress end as PrimaryEmail
		from dup
		where EmailAddress is not NULL and EmailAddress <> ''
		and Contactrn = 1
		)
, officeemail as (--OFFICE EMAIL
        select p.ObjectID, STRING_AGG(TRIM('.' FROM p.Num),',') as officeemail
        from dbo.Phones p
        where p.CommunicationTypeId=85 --Email (Office)
        and p.Num like '%_@_%.__%'
        group by p.ObjectID
)
, personemail as (--PERSON EMAIL
        select p.ObjectID, STRING_AGG(TRIM('.' FROM p.Num),',') as personemail
        from dbo.Phones p
        where p.CommunicationTypeId=86 --Email (Personal)
        and p.Num like '%_@_%.__%'
        group by p.ObjectID
)
, consultant as (select ac.ApplicantConsultantId
				, ac.ApplicantId
				, ac.userId
				, u.EmailAddress
				, u.UserFullName
				, ac.UserRelationshipId
				, ur.Description
				, g.GroupName
				from ApplicantConsultants ac
				left join Users u on u.UserId = ac.UserId
				left join UserRelationships ur on ur.UserRelationshipId = ac.UserRelationshipId
				left join Groups g on g.GroupId = ac.UserGroupId
)
, owners as (select ApplicantId
				, string_agg(EmailAddress, ',') within group (order by UserRelationshipId) as owners
				from consultant
				group by ApplicantId
			)
, consultant_info as (
        select ApplicantId
		, string_agg(
			concat_ws(' - '
				, nullif(UserFullName,'')
				, , coalesce('Relationship: ' + nullif(Description, ''), NULL)
				, , coalesce('Group name: ' + nullif(GroupName, ''), NULL)
				--, , coalesce('[Commission(%): ' + convert(varchar(max),CommissionPerc, 1)+ ']',NULL))
			), ', ') as consultant_info
        from consultant
        group by ApplicantId
)
, userinfo as (--USER
        select u.UserId,u.LoginName,u.UserName,u.UserFullName,u.JobTitle,u.Inactive
        from Users u
)
, person_info as (--CANDIDATE INFO
		select p.PersonID, p.PersonName, p.Surname, p.Dob, p.Notes
                , p.GenderValueId
                , case when lv1.ValueName='Female' then 'FEMALE'
					when lv1.ValueName='Male' then 'MALE'  
					else null end as Gender
                , p.MaritalStatusValueId
				, lv2.ValueName as Marital
                , p.TitleValueId
				--, lv3.ValueName as Title
				, case 
					when lv3.ValueName = 'Dame' then 'MISS'
					when lv3.ValueName = 'Dr' then 'DR'
					when lv3.ValueName = 'Miss' then 'MISS'
					when lv3.ValueName = 'Mr' then 'MR'
					when lv3.ValueName = 'Mrs' then 'MRS'
					when lv3.ValueName = 'Ms' then 'MISS'
					when lv3.ValueName = 'Sir' then 'MR'
				end as Title
                , p.Salutation
				, p.NationalityId
				, case when n.Nationality = 'Dutch' then 'NL'
					when n.Nationality = 'German' then 'DE'
					when n.Nationality = 'French' then 'FR'
					when n.Nationality = 'Jamaican' then 'JM'
					when n.Nationality = 'N Zealander' then 'NZ'
					when n.Nationality = 'Lithuanian' then 'LT'
					when n.Nationality = 'Maltese' then 'MT'
					when n.Nationality = 'Chilean' then 'CL'
					when n.Nationality = 'Venezuelan' then 'VE'
					when n.Nationality = 'USA' then 'US'
					when n.Nationality = 'Lebanese' then 'LB'
					when n.Nationality = 'Moroccan' then 'MA'
					when n.Nationality = 'Estonian' then 'EE'
					when n.Nationality = 'Egyptian' then 'EG'
					when n.Nationality = 'Pakistani' then 'PK'
					when n.Nationality = 'Swiss' then 'CH'
					when n.Nationality = 'Cuban' then 'CU'
					when n.Nationality = 'Ukranian' then 'UA'
					when n.Nationality = 'Hong Kong (Chinese)' then 'HK'
					when n.Nationality = 'Swedish' then 'SE'
					when n.Nationality = 'Japanese' then 'JP'
					when n.Nationality = 'Taiwanese' then 'TW'
					when n.Nationality = 'Peruvian' then 'PE'
					when n.Nationality = 'Czech' then 'CZ'
					when n.Nationality = 'Jordanian' then 'JO'
					when n.Nationality = 'Portugese' then 'PT'
					when n.Nationality = 'Vincentian' then 'VC'
					when n.Nationality = 'Philipino' then 'PH'
					when n.Nationality = 'Norwegian' then 'NO'
					when n.Nationality = 'Danish' then 'DK'
					when n.Nationality = 'Romanian' then 'RO'
					when n.Nationality = 'Kenyan' then 'KE'
					when n.Nationality = 'Thai' then 'TH'
					when n.Nationality = 'Hungarian' then 'HU'
					when n.Nationality = 'Latvian' then 'LV'
					when n.Nationality = 'Chinese' then 'CN'
					when n.Nationality = 'Colombian' then 'CO'
					when n.Nationality = 'Myanmar' then 'MM'
					when n.Nationality = 'Netherland' then 'NL'
					when n.Nationality = 'Turkish' then 'TR'
					when n.Nationality = 'Malawian' then 'MW'
					when n.Nationality = 'Fijian' then 'FJ'
					when n.Nationality = 'Nepalese' then 'NP'
					when n.Nationality = 'Slovakian' then 'SK'
					when n.Nationality = 'Iranian' then 'IR'
					when n.Nationality = 'Russian' then 'RU'
					when n.Nationality = 'Montenegri' then 'ME'
					when n.Nationality = 'Sri Lankan' then 'LK'
					when n.Nationality = 'Bulgarian' then 'BG'
					when n.Nationality = 'CROATIAN' then 'HR'
					when n.Nationality = 'Eritrean' then 'ER'
					when n.Nationality = 'Canadian' then 'CA'
					when n.Nationality = 'Namibian' then 'NA'
					when n.Nationality = 'Panamanian' then 'PA'
					when n.Nationality = 'Serbian' then 'RS'
					when n.Nationality = 'Sudanese' then 'SD'
					when n.Nationality = 'Israeli' then 'IL'
					when n.Nationality = 'Mexican' then 'MX'
					when n.Nationality = 'SYRIAN' then 'SY'
					when n.Nationality = 'Congolese' then 'CG'
					when n.Nationality = 'Irish' then 'IE'
					when n.Nationality = 'Yemen' then 'YE'
					when n.Nationality = 'Macedonian' then 'MK'
					when n.Nationality = 'Spanish' then 'ES'
					when n.Nationality = 'Filipino' then 'PH'
					when n.Nationality = 'Finnish' then 'FI'
					when n.Nationality = 'Malaysian' then 'MY'
					when n.Nationality = 'Vietnamese' then 'VN'
					when n.Nationality = 'Polish' then 'PL'
					when n.Nationality = 'Liberian' then 'LR'
					when n.Nationality = 'Icelandic' then 'IS'
					when n.Nationality = 'Guyanese' then 'GY'
					when n.Nationality = 'Indian' then 'IN'
					when n.Nationality = 'Cypriot' then 'CY'
					when n.Nationality = 'Nigerian' then 'NG'
					when n.Nationality = 'Italian' then 'IT'
					when n.Nationality = 'Greek' then 'GR'
					when n.Nationality = 'Korean' then 'KR'
					when n.Nationality = 'British' then 'GB'
					when n.Nationality = 'Australian' then 'AU'
					when n.Nationality = 'South African' then 'ZA'
					when n.Nationality = 'Iraqi' then 'IQ'
					when n.Nationality = 'Ugandan' then 'UG'
					when n.Nationality = 'Argentinean' then 'AR'
					when n.Nationality = 'American' then 'US'
					when n.Nationality = 'Zimbabwean' then 'ZW'
					when n.Nationality = 'Belgian' then 'BE'
					when n.Nationality = 'Austrian' then 'AT'
					when n.Nationality = 'Brazilian' then 'BR'
					else NULL end as Nationality
        from Person p
        left join ListValues lv1 on p.GenderValueId=lv1.ListValueId
        left join ListValues lv2 on p.MaritalStatusValueId=lv2.ListValueId
        left join ListValues lv3 on p.TitleValueId=lv3.ListValueId
        left join Nationality n on n.NationalityId=p.NationalityId)
, profile as (
        select ap.ApplicantId,dbo.RTF2TXT(ap.ProfileDocument) as profile
        from dbo.ApplicantProfile ap
)
, candidate_source as (--CANDIDATE SOURCE #Inject
		select a.ApplicantId, s.SourceId, s.SystemCode, s.Description
        from Applicants a
		left join dbo.Sources s on s.SourceId=a.SourceId
		)
, location as (--LOCATION
		select l.LocationId,l.Code,l.Description
        from dbo.Locations l
)
, candidateprofile as (--PROFILE DOCUMENT
        select ap.ApplicantId,dbo.RTF2TXT(ap.ProfileDocument) as candidateprofile
        from dbo.ApplicantProfile ap
		)
, sector_all as (select so.SectorObjectId
		, so.SectorId
		, s.SectorName
		, so.ObjectId
		, so.Notes
		from SectorObjects so
		left join Sectors s on so.SectorId = s.SectorId
		--where so.SectorId = 50 --Old jobs
		)
, sector_share as (
		select ObjectId
		, string_agg(SectorName, ', ') as sector_share
		from sector_all
		group by ObjectId
		)
, workhistory as (--WORK HISTORY
        select wh.ApplicantID,
			STRING_AGG(
                concat('<p>'
					, concat_ws('<br/>'
						, coalesce('Client name: ' + wh.Company,NULL)
						, coalesce('From Date: ' + convert(nvarchar(max),wh.FromDate,120),NULL)
						, coalesce('To Date: ' + convert(nvarchar(max),wh.ToDate,120),NULL)
						, coalesce('Chronology: ' + case when wh.ToDate is null then 'Current' else 'Past' end, NULL)
						, coalesce('Position: ' + coalesce(nullif(a.Description,''), a.Notes),NULL)
						, coalesce('Description: ' + nullif(wh.Description,''),NULL)
						, coalesce('Department: ' + nullif(wh.Department,''),NULL)
						, coalesce('Salary: ' + convert(nvarchar(max), wh.Rate),NULL)
						, coalesce('Length: ' + nullif(l.ValueName, ''),NULL)
						, coalesce('Work History Type: ' + wht.Description,NULL)
						--, coalesce('Created On: ' + convert(nvarchar(max),wh.CreatedOn,120),NULL)
						, coalesce('Report To: ' + nullif(concat_ws(' ',pi.PersonName,pi.Surname),''),NULL)
						, coalesce('Feedback Score: ' + convert(nvarchar(max),pl.FeedbackScore),NULL)
						, coalesce('Employment Type: ' + et.Description + ' - ' + et.SystemCode,NULL)
						, coalesce('Notes: ' + wh.Description,NULL)
					),'</p>'),'<br/>') within group (order by wh.ToDate desc, wh.FromDate desc, WorkHistoryId desc) as WorkHistory
        from WorkHistory wh
        left join WorkHistoryTypes wht on wht.WorkHistoryTypeId=wh.WorkHistoryTypeId
        left join EmploymentTypes et on et.EmploymentTypeId=wh.EmploymentTypeId
        left join Placements pl on pl.PlacementID=wh.PlacementID
        left join Attributes a on a.AttributeId=wh.PositionAttributeId
        left join personinfo pi on pi.PersonID=wh.ReportsToPersonId
        left join listvalueid l on l.ListValueId=wh.RateUnitId
        left join dbo.Applicants a on a.ApplicantID=wh.ApplicantID
        group by wh.ApplicantID
		)
, candidate_cv as (select cv.ApplicantId
		,STRING_AGG(concat_ws('_','NP_CV',cv.CVId,concat(cv.ApplicantId,cvc.FileExtension)),',') 
			within group (order by cv.CVId desc) as candidate_cv
        from CV
        left join CVContents cvc on cvc.CVId = cv.CVId
        where cvc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.txt','.htm','.html')
        GROUP BY cv.ApplicantId
		)
, candidate_doc as (select aa.ApplicantId
		, string_agg(concat_ws('_','NP_D', d.DocumentID, concat(aa.ApplicantId, dc.FileExtension)),',') 
			within group (order by d.DocumentID desc) as candidate_doc
		from CVSendDocuments cvs
		left join ApplicantActions aa on aa.ApplicantActionId = cvs.ApplicantActionId
		left join Documents d on d.DocumentID = cvs.DocumentId
		left join DocumentContent dc on d.DocumentId = dc.DocumentId
		where cvs.CVId is NULL --10321 --CV already parsed
		and dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.txt','.htm','.html')
		group by aa.ApplicantId
		)
, custom_columns as (--CUSTOM DEFINED COLUMNS
		select adc.ApplicantId
		, concat_ws('<br/>'
				, coalesce('Driving Licence: ' + nullif(adc.DrivingLicence,''),NULL)
				, coalesce('Transport: ' + nullif(lv.ValueName,''),NULL) --CustomDefinedColumns: LookupListId
				, coalesce('Type of Drivin: ' + nullif(adc.Alpha2,''),NULL)
				, coalesce('Relocate?: ' + nullif(adc.Flag1,''),NULL)
				, coalesce('National Insurance No.: ' + nullif(adc.NationalInsurance,''),NULL)
				, coalesce('Contractor Company Name: ' + nullif(adc.ContractorCompanyName,''),NULL)
				, coalesce('Umbrella Company?: ' + nullif(adc.Umbrella,''),NULL)
				, coalesce('VAT no. (for limited company Contractors only): ' + nullif(adc.VATno,''),NULL)
				, coalesce('Company Number: ' + nullif(adc.Companynumber,''),NULL)
				, coalesce('Middle Name: ' + nullif(adc.MiddleName,''),NULL)
				, coalesce('Unique Taxpayer Ref: ' + nullif(adc.WorkerUTR,''),NULL)
			) as custom_columns
		from ApplicantSectorDefinedColumns adc
		left join ListValues lv on lv.ListValueId = adc.Alpha1
		)
--MAIN SCRIPT
select  
        concat('NP',a.ApplicantId) as [candidate-externalId]
        , coalesce(nullif(TRIM(':-§ .0123456789' FROM pi.PersonName),''),'NOFIRSTNAME') as [candidate-firstName]
        , coalesce(nullif(TRIM(':-§ .0123456789' FROM pi.Surname),''),'NOLASTNAME') as [candidate-Lastname]
        , pi.Title  [candidate-title]
        , o.owners as [candidate-owner]
        , case 
                when j.CurrencyId =10 then 'GBP'
                when j.CurrencyId =11 then 'USD'
                --when j.CurrencyId =12 then 'SEK' --not available
				when j.CurrencyId =13 then 'CHF'
				when j.CurrencyId =14 then 'EUR'
                else 'GBP' end as [candidate-currency]
        , convert(nvarchar(max),pi.Dob,120) as 'candidate-dob'
        , pi.Gender as [candidate-gender]
        , pi.Marital as [candidate-marital]
        , pi.Nationality as [candidate-nationality]
		, pi.Salutation as PreferredName --#Inject
		, a.JobTitle as [candidate-jobTitle1]
        , convert(money,a.CurrentBasic) as [candidate-currentSalary]
        , convert(varchar(max),a.Rate) as [candidate-contractRate]
        , convert(varchar(max),a.RateUnit) as RateUnit --#Inject: compensation
		, case 
				when j.EmploymentTypeId in (5, 6, 7, 8, 10, 11, 13) then 'CONTRACT'
				when j.EmploymentTypeId in (4, 9, 12) then 'PERMANENT'
				else 'PERMANENT' end as [candidate-employmentType] --from [EmploymentTypes]
        , a.Candidate_address as [candidate-locationAddress]
		, a.District as [candidate-locationDistrict]
        , a.City as [candidate-locationCity]
        , a.PostCode as [candidate-locationZipCode]
        , a.County as [candidate-locationState]
        , a.CountryCode as [candidate-locationCountry]
        , m.Phone as [candidate-phone]
        , oe.officeemail as [candidate-workEmail]
        , coalesce(pe.Email, concat(a.ApplicantId,'_candidate@noemail.com')) as [candidate-email]
        , wh.WorkHistory as [candidate-workHistory]
        -----NOTE----
        , concat('<p>',
			concat_ws('<br/>',
                , coalesce('External ID: ' + convert(varchar(max),a.ApplicantId),NULL)
                , coalesce('Created By: ' + uic.UserFullName,NULL)
                , coalesce('Created On: ' + convert(varchar(max),a.CreatedOn,120),NULL)
                , coalesce('File As: ' + nullif(ci.FileAs,''),NULL)
                , coalesce('Flag Text: ' + nullif(ci.FlagText,''),NULL)
                , coalesce('Candidate Status: ' + ast.Description,NULL)
				, coalesce('Status Date: ' + convert(varchar(max),a.StatusDate,120),NULL)
                , coalesce('Noticed Period: ' + lv.ValueName,NULL)
                , coalesce('Available Date: ' + convert(varchar(max),a.AvailableDate,120),NULL)
                , coalesce('Call At Work: ' + a.WorkCall,NULL)
				, coalesce('Candidate Location: ' + nullif(l.Description,''),NULL)
                , coalesce('Min Basic: ' + convert(varchar(max),a.MinBasic),NULL)
                , coalesce('Min Package: ' + convert(varchar(max),a.MinPackage),NULL)
                , coalesce('Current Package: ' + convert(varchar(max),a.CurrentPackage),NULL)
				, coalesce('Share this person: ' + nullif(ss.sector_share,''),NULL)
				, coalesce('Personal Email: ' + nullif(pse.personemail,''),NULL)
                , coalesce('<br/>' + '--Communication Types--' + '<br/>' + nullif(ap.allphone,'') + '<br/>',NULL)
				, coalesce('<br/>' + '--Social networking--' + '<br/>' + nullif(aw.allnetwork,''),NULL)
				, coalesce('<br/>' + '--Consultant Info--' + '<br/>' + nullif(ci.consultant_info,''), NULL)
				, coalesce('<br/>' + '--Contractor Details--' + '<br/>' + nullif(cc.custom_columns,''), NULL)
                , coalesce('<br/>' + '--Profile--' + '<br/>' + coalesce(nullif(pr.profile,''),'NONE') + '<br/>',NULL),
                , coalesce('<br/>' + '--Notes--' + '<br/>' + coalesce(nullif(pi.Notes,''),'NONE'),NULL)
			), '</p>'
        ) as [candidate-note]
		, concat_ws(',', cd.candidate_doc, cc.candidate_cv) as [candidate-resume]
from dbo.Applicants a
left join personinfo pi on pi.PersonID = a.ApplicantId
left join owners o on o.ApplicantId = a.ApplicantId
left join userinfo u on u.UserId = a.CreatedUserId
left join userinfo u2 on u2.UserId = a.UpdatedUserId
left join candidateinIDobject ci on ci.ObjectID = a.ApplicantId
left join ApplicantStatus ast on ast.ApplicantStatusId = a.StatusId
left join ListValues lv on lv.ListValueId = a.AvailabilityId
left join ListValues lv2 on lv2.ListValueId = a.PriorityValueId
left join Locations l on l.LocationId = j.LocationId --locations
left join address_1 a on a.ObjectId = c.ClientID --current address
left join sector_share ss on ss.ObjectID = a.ApplicantId --share this person
left join officeemail oe on oe.ObjectId = a.ApplicantId --Work Email
left join primaryEmail pe on pe.ObjectID = a.ApplicantId --Primary Email from CommunicationTypes: Email
left join personemail pse on pse.ObjectID = a.ApplicantId --Personal Email
left join mobile m on m.ObjectID = a.ApplicantId --Mobile
left join candidate_cv cc on cc.ApplicantId = a.ApplicantId --Candidate cv
left join candidate_doc cd on cd.ApplicantId = a.ApplicantId --Candidate documents
left join profile pr on pr.ApplicantId = a.ApplicantId
left join workhistory wh on wh.ApplicantId = a.ApplicantId
left join allphone ap on ap.ObjectId = a.ApplicantId --all other phones
left join allnetwork aw on aw.ObjectId = a.ApplicantId --all other networking
left join consultant_info ci on ci.ApplicantId = a.ApplicantId --all consultant group info
left join custom_columns cc on cc.ApplicantId = a.ApplicantId --all custom columns
where 1=1
and a.ApplicantId not in (select ObjectId from SectorObjects where SectorId = 48) --Deleted Candidates