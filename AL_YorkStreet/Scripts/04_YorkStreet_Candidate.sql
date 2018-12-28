declare @NewLineChar as char(2) = char(13) + char(10);
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar;

drop table if exists VCCandidates

select

cis.CanId as [candidate-externalId]

, upper(trim(isnull(t.Description, ''))) as [candidate-title]

, iif(len(trim(isnull(x.FirstName, ''))) = 0
	, concat('NoName-', x.CandidateID)
	, trim(isnull(x.FirstName, ''))
) as [candidate-firstName]

, trim(isnull(x.MiddleName, '')) as [candidate-middleName]

, iif(len(trim(isnull(x.Surname, ''))) = 0
	, concat('NoName-', x.CandidateID)
	, trim(isnull(x.Surname, ''))
) as [candidate-Lastname]

, cis.Emails as [candidate-email]

, trim(isnull(x.Email2, '')) as [candidate-workEmail]

, upper(trim(isnull(g.Description, ''))) as [candidate-gender]

, isnull(convert(varchar(50), x.DateOfBirth, 111), '') as [candidate-dob]

, [dbo].[ufn_RefinePhoneNumber](
	iif(len(trim(isnull(x.Telephone, ''))) = 0
		, iif(len(trim(isnull(x.Telephone2, ''))) = 0
			, iif(len(trim(isnull(x.Mobile, ''))) = 0
				, iif(len(trim(isnull(x.WorkTelephone, ''))) = 0
					, ''
					, concat(trim(isnull(x.WorkTelephone, '')), trim(isnull(x.WorkTelephoneExtension, '')))
				)
				, trim(isnull(x.Mobile, ''))
			)
			, trim(isnull(x.Telephone2, ''))
		)
		, trim(isnull(x.Telephone, ''))
	)
)
as [candidate-phone]

, [dbo].[ufn_RefinePhoneNumber](trim(isnull(x.Telephone2, ''))) as [candidate-workPhone]

, [dbo].[ufn_RefinePhoneNumber](trim(isnull(x.Mobile, ''))) as [candidate-mobile]

, cis.FullAddress as [candidate-address]

, [dbo].[ufn_TrimSpecifiedCharacters](x.Town, '., ') as [candidate-city]

, [dbo].[ufn_TrimSpecifiedCharacters](x.County, '., ') as [candidate-State]

, cs.Code as [candidate-Country]

, [dbo].[ufn_TrimSpecifiedCharacters](x.Postcode, '., ') as [candidate-zipCode]

, isnull(cs2.Code, '') as [candidate-citizenship]

, isnull(cu.Reference, 'GBP') as [candidate-currency]

, iif(isnumeric(trim(isnull(x.CurrentRemuneration, ''))) = 0
	, ''
	, iif(len(trim(isnull(x.CurrentRemuneration, ''))) = 0, '', 
	convert(varchar(20), cast(trim(isnull(x.CurrentRemuneration, '')) as money)))
) as [candidate-currentSalary]

, x.MinimumSalary as [candidate-desiredSalary]

, x.MinimumRate as [candidate-contractRate]

, isnull(ri.Description, 'Monthly') as [candidate-contractInterval]

--, isnull((select string_agg([dbo].ufn_ConvertJobTypeYS2VC(cvt.VacancyTypeID), ',') as JobTypes
--	from CandidateVacancyTypes cvt
--	where cvt.CandidateID = c.CandidateID
--	group by cvt.CandidateID
--), 'PERMANENT') as [candidate-jobTypes]

, cis.JobTypes as [candidate-jobTypes]

, ceh.EducationHistory as [candidate-education]

, cwh.WorkHistory as [candidate-workHistory]

, csk.Skills as [candidate-skills]

, cis.OwnerEmails as [candidate-owners]

, dc.Docs as [candidate-resume]

, concat(
	concat('External ID: ', cis.CanId)
	, concat(@DoubleNewLine, 'Date Registered: ', x.CreationDate)
	, iif(x.ContactId = 0, ''
		, concat(@DoubleNewLine, 'Contact Name: ', conIdxs.FullName)
	)
	, iif(x.CompanyID = 0, ''
		, concat(@DoubleNewLine, 'Company Name: ', comIdxs.ComName)
	)
	, iif(x.Status = 0, ''
		, concat(@DoubleNewLine, 'Status: ', trim(isnull(sta.Description, '')))
	)
	, iif(x.EthnicOrigin = 0, ''
		, concat(@DoubleNewLine, 'Ethnic Origin: ', trim(isnull(eor.Description, '')))
	)
	, iif(x.WorkPermit is null, ''
		, concat(@DoubleNewLine, 'Do you need a work permit: ', iif(x.WorkPermit = 1, 'Yes', 'No'))
	)
	, iif(x.WorkPermitTypeID = 0, ''
		, concat(@DoubleNewLine, 'Work Permit Type: ', trim(isnull(wpt.Description, '')))
	)
	, iif(x.WorkPermitStatusID = 0, ''
		, concat(@DoubleNewLine, 'Work Permit Status: ', trim(isnull(wps.Description, '')))
	)
	, iif(x.WorkPermitVerificationDate is null, ''
		, concat(@DoubleNewLine, 'Work Permit Verification Date: ', x.WorkPermitVerificationDate)
	)
	, iif(x.WorkPermitVerifierID = 0, ''
		, concat(@DoubleNewLine, 'Work Permit Verified By: ', trim(isnull(urs.UserDisplayName, '')))
	)
	, iif(len(trim(isnull(cast(x.WorkPermitComments as nvarchar(max)), ''))) = 0, ''
		, concat(@DoubleNewLine, 'Work Permit Comments: ', trim(isnull(cast(x.WorkPermitComments as nvarchar(max)), '')))
	)
	, iif(x.DrivingLicence is null, ''
		, concat(@DoubleNewLine, 'Do you hold a full current driving licence: ', iif(x.DrivingLicence = 1, 'Yes', 'No'))
	)
	, iif(x.OwnACar is null, ''
		, concat(@DoubleNewLine, 'Do you own a car: ', iif(x.OwnACar = 1, 'Yes', 'No'))
	)
	, concat(@DoubleNewLine, 'Qualifications:', @NewLineChar, cq.Qualifications)
	, concat(@DoubleNewLine, 'Preferences:', @NewLineChar, cp.Preferences)
	, iif(len(trim(isnull(x.NoticeRequired, ''))) = 0, ''
		, concat(@DoubleNewLine, 'Notice Required: ', trim(isnull(x.NoticeRequired, '')))
	)
	, iif(len(trim(isnull(cast(x.PreferredJobDescription1 as nvarchar(max)), ''))) = 0, ''
		, concat(@DoubleNewLine, 'Please describe the type of position you are looking for: ', trim(isnull(cast(x.PreferredJobDescription1 as nvarchar(max)), '')))
	)
	, iif(len(trim(isnull(x.InfluenceToApplyDescription, ''))) = 0, ''
		, concat(@DoubleNewLine, 'Any specific companies you wish to be considered for: ', trim(isnull(x.InfluenceToApplyDescription, '')))
	)
	, iif(len(trim(isnull(cast(x.IncludeCompanies as nvarchar(max)), ''))) = 0, ''
		, concat(@DoubleNewLine, 'Any companies you do not want to work for: ', trim(isnull(cast(x.IncludeCompanies as nvarchar(max)), '')))
	)
	, iif(len(trim(isnull(x.DistanceToTravel, ''))) = 0, ''
		, concat(@DoubleNewLine, 'Distance willing to travel: ', trim(isnull(x.DistanceToTravel, '')))
	)
	, iif(x.WorkPermitVerificationDate is null, ''
		, concat(@DoubleNewLine, 'Available From: ', x.AvailableFromDate)
	)
	, iif(x.WorkPermitVerificationDate is null, ''
		, concat(@DoubleNewLine, 'Available To: ', x.AvailableToDate)
	)
	, iif(len(trim(isnull(cast(x.Comments as nvarchar(max)), ''))) = 0, ''
		, concat(@DoubleNewLine, 'Comments: ', trim(isnull(cast(x.Comments as nvarchar(max)), '')))
	)
)
as [candidate-note]

into VCCandidates

from
VCCanIdxs cis
left join Candidates x on cis.CanId = x.CandidateID
left join VCCanDocs dc on cis.CanId = dc.CanId
left join VCConIdxs conIdxs on x.ContactID = conIdxs.ConId
left join VCComIdxs comIdxs on x.CompanyID = comIdxs.ComId
left join [Titles] t on x.Title = t.TitleID
left join [Genders] g on x.Gender = g.GenderID
left join Currency cu on x.CurrencyID = cu.CurrencyID
left join RateIntervals ri on ri.RateIntervalID = x.MinimumRateIntervalID
left join VCCanWorkHistory cwh on cis.CanId = cwh.CandidateId
left join VCCanEducationHistory ceh on cis.CanId = ceh.CandidateId
left join VCCanSkills csk on cis.CanId = csk.CandidateId
left join [Status] sta on x.Status = sta.StatusID
left join EthnicOrigins eor on x.EthnicOrigin = eor.EthnicOriginID
left join WorkPermitTypes wpt on x.WorkPermitTypeID = wpt.WorkPermitTypeID
left join WorkPermitStatus wps on x.WorkPermitStatusID = wps.WorkPermitStatusID
left join Users urs on urs.UserID = x.WorkPermitVerifierID
left join VCCanQualifications cq on cis.CanId = cq.CandidateId
left join VCCanPreferences cp on cis.CanId = cp.CandidateId
left join VCCountries cs on
	iif(lower(trim(isnull(cis.Country, ''))) = 'uk', 'gb', lower(trim(isnull(cis.Country, '')))) = lower(trim(cs.Name))
	or iif(lower(trim(isnull(cis.Country, ''))) = 'uk', 'gb', lower(trim(isnull(cis.Country, '')))) = lower(trim(cs.Code))
left join VCCountries cs2 on
	iif(lower(trim(isnull(cis.Nationality, ''))) = 'uk', 'gb', lower(trim(isnull(cis.Nationality, '')))) = lower(trim(cs2.Name))
	or iif(lower(trim(isnull(cis.Nationality, ''))) = 'uk', 'gb', lower(trim(isnull(cis.Nationality, '')))) = lower(trim(cs2.Code))


select * from VCCandidates

--with
--CountryDic as (
--	select * from VincereCountryCodeDic
--)

--select distinct
--  trim(coalesce(cast(c.CandidateID as varchar(20)), '')) as [candidate-externalId]

--, UPPER(trim(coalesce((select top 1 t.Description from Titles t where t.TitleID = c.Title), ''))) as [candidate-title]

--, iif(len(trim(coalesce(c.FirstName, ''))) = 0
--	, 'NoFName-' + cast(c.CandidateID as varchar(10))
--	, trim(coalesce(c.FirstName, ''))
--) as [candidate-firstName]

--, trim(coalesce(c.MiddleName, '')) as [candidate-middleName]

--, iif(len(trim(coalesce(c.Surname, ''))) = 0
--	, 'NoLName-' + cast(c.CandidateID as varchar(10))
--	, trim(coalesce(c.Surname, ''))
--) as [candidate-Lastname]

----, db-field-not-found as [candidate-FirstNameKana]
----, db-field-not-found as [candidate-LastNameKana]
--, iif(len(trim(coalesce(c.Email, ''))) = 0
--	, 'NoEmail-' + cast(c.CandidateID as varchar(10)) + '@noemail.com'
--	, trim(coalesce(c.Email, ''))
--) as [candidate-email]

--, trim(coalesce(c.Email2, '')) as [candidate-workEmail]

----, db-field-not-found as [candidate-employmentType]

--, isnull((select string_agg([dbo].ufn_ConvertJobTypeYS2VC(cvt.VacancyTypeID), ',') as JobTypes
--	from CandidateVacancyTypes cvt
--	where cvt.CandidateID = c.CandidateID
--	group by cvt.CandidateID
--), 'PERMANENT') as [candidate-jobTypes]
--, upper(trim(coalesce((select top 1 g.Description from Genders g where c.Gender = g.GenderID), ''))) as [candidate-gender]
--, isnull(convert(varchar(50), c.DateOfBirth, 111), '') as [candidate-dob]
--, iif(len(trim(coalesce(c.Address1, ''))) = 0,
--	iif(len(trim(coalesce(c.Address2, ''))) = 0,
--		 iif(len(trim(coalesce(c.Address3, ''))) = 0
--			, ''
--			, trim(coalesce(c.Address3, ''))
--		)
--		, trim(coalesce(c.Address2, ''))
--	)
--	, trim(coalesce(c.Address1, ''))
--) as [candidate-address]

--, trim(coalesce(c.Town, '')) as [candidate-city]

--, trim(coalesce(c.County, '')) as [candidate-State]

--, trim(coalesce(
--	iif((select top 1 co.Description from Countries co where c.CountryID = co.CountryID) = 'UK', 'GB', (select top 1 cd.Code from Countries co left join CountryDic cd on co.Description = cd.Code OR co.Description = cd.Name where c.CountryID = co.CountryID)), ''))
--as [candidate-Country]

--, trim(coalesce(c.Postcode, '')) as [candidate-zipCode]

--, trim(coalesce(c.Telephone, '')) as [candidate-phone]

--, trim(coalesce(c.Telephone, '')) as [candidate-homePhone]

--, trim(coalesce(c.Telephone2, '')) as [candidate-workPhone]

--, trim(coalesce(c.Mobile, '')) as [candidate-mobile]

--, trim(',' from concat_ws(','
--	, trim(coalesce(
--		(select top 1 cd.Code from Nationality n left join CountryDic cd on upper(n.Description) = upper(cd.Name) where n.NationalityID = c.Nationality), ''))
--	, trim(coalesce(
--		(select top 1 cd.Code from Nationality n left join CountryDic cd on upper(n.Description) = upper(cd.Name) where n.NationalityID = c.Nationality2), ''))
--)) as [candidate-citizenship]

----, db-field-not-found as [candidate-linkedln]
--, iif(isnumeric(trim(isnull(c.CurrentRemuneration, ''))) = 0
--	, ''
--	, iif(len(trim(isnull(c.CurrentRemuneration, ''))) = 0, '', 
--	convert(varchar(20), cast(trim(isnull(c.CurrentRemuneration, '')) as money)))
--) as [candidate-currentSalary]

--, iif(isnumeric(trim(coalesce(c.RemunerationRequired, ''))) = 0
--	, ''
--	, trim(coalesce(c.RemunerationRequired, ''))
--) as [candidate-desiredSalary]

--, c.MinimumRate as [candidate-contractRate]

--, isnull((select top 1 ri.Description from RateIntervals ri where ri.RateIntervalID = c.MinimumRateIntervalID), 'Monthly') as [candidate-contractInterval]

--, coalesce((select cu.Reference from Currency cu where cu.CurrencyID = c.CurrencyID), 'GBP') as [candidate-currency]
----, db-field-not-found as [candidate-degreeName]
----, db-field-not-found as [candidate-education]
----, db-field-not-found as [candidate-educationLevel]
----, db-field-not-found as [candidate-gpa]
----, db-field-not-found as [candidate-grade]
----, db-field-not-found as [candidate-graduationDate]
----, db-field-not-found as [candidate-schoolName]
--, iif(c.CompanyID <> 0
--	, (select top 1 com.Name from CompanyDetails com where com.CompanyID = c.CompanyID)
--	, null
--) as [candidate-company1]
----, db-field-not-found as [candidate-company2]
----, db-field-not-found as [candidate-company3]
--, trim(coalesce(c.CurrentEmployer, '')) as [candidate-employer1]
----, db-field-not-found as [candidate-employer2]
----, db-field-not-found as [candidate-employer3]
--, iif(c.CurrentPositionID <> 0
--	, (select top 1 p.Description from Positions p where p.PositionID = c.CurrentPositionID)
--	, ''
--) as [candidate-jobTitle1]
----, db-field-not-found as [candidate-jobTitle2]
----, db-field-not-found as [candidate-jobTitle3]
--, trim(coalesce(cast(c.Keywords as nvarchar(max)), '')) as [candidate-keyword]

--, ''
--+ 'Date Registered: ' + isnull(cast(c.CreationDate as varchar(50)), '')
--+ @NewLineChar + 'Exported System: ' + isnull(cast(c.DateExported as varchar(50)), '')
--+ @NewLineChar + 'Contact Name: ' + iif(c.ContactID <> 0, isnull((select top 1 ct.Firstname + ' ' + ct.Surname + '(External ID:' + cast(ct.ContactID as varchar(20)) + ')' from Contacts ct where ct.ContactID = c.ContactID), ''), '')
--+ @NewLineChar + 'N.I. Number: ' + isnull(c.NINumber, '')
--+ @NewLineChar + 'Status: ' + isnull(iif(c.Status <> 0, (select top 1 s.Description from [Status] s where s.StatusID = c.Status), ''), '')
--+ @NewLineChar + 'Ethnic Origin: ' + isnull(iif(len(trim(isnull(c.EthnicOriginOther, ''))) = 0, iif(c.EthnicOrigin <> 0, isnull((select top 1 eo.Description from EthnicOrigins eo where c.EthnicOrigin = eo.EthnicOriginID), ''), ''), ''), '')
--+ @NewLineChar + 'Do you need a work permit: ' + iif(c.WorkPermit is not null, iif(c.WorkPermit = 1, 'YES', 'NO'), 'N/A')
--+ @NewLineChar + 'Work Permit Type: ' + iif(c.WorkPermitTypeID is not null and c.WorkPermitTypeID <> 0, (select top 1 wpt.Description from WorkPermitTypes wpt  where c.WorkPermitTypeID = wpt.WorkPermitTypeID), '')
--+ @NewLineChar + 'Work Permit Status: ' + iif(c.WorkPermitStatusID is not null and c.WorkPermitStatusID <> 0, (select top 1 wps.Description from WorkPermitStatus wps where c.WorkPermitStatusID = wps.WorkPermitStatusID), '')
--+ @NewLineChar + 'Work Permit Verification Date: ' + isnull(cast(c.WorkPermitVerificationDate as varchar(50)), '')
--+ @NewLineChar + 'Work Permit Verified By: ' + iif(c.WorkPermitVerifierID is not null and c.WorkPermitVerifierID <> 0, (select top 1 u.UserDisplayName + iif(len(trim(coalesce(u.Email, ''))) = 0, '', ' - ' + trim(coalesce(u.Email, ''))) from Users u where c.WorkPermitVerifierID = u.UserID), '')
--+ @NewLineChar + 'Work Permit Comments: ' + isnull(cast(c.WorkPermitComments as nvarchar(max)), '')
--+ @NewLineChar + 'Do you hold a full current driving licence: ' + iif(c.DrivingLicence is not null, iif(c.DrivingLicence = 1, 'YES', 'NO'), 'N/A')
--+ @NewLineChar + 'Do you own a car: ' + iif(c.OwnACar is not null, iif(c.OwnACar = 1, 'YES', 'NO'), 'N/A')
--+ @NewLineChar + 'Verification Date: ' + isnull(cast(c.VerificationDate as varchar(50)), '')
--+ @NewLineChar + 'Candidate Qualifications: ' + @NewLineChar + isnull((select string_agg(cq.Title, @NewLineChar) from CandidateQualifications cq where cq.CandidateID = c.CandidateID), '')
--+ @NewLineChar + 'Preferred Position 1: ' + isnull(iif(len(trim(isnull(c.PreferredPosition1Other, ''))) = 0, iif(c.PreferredPosition1 <> 0, isnull((select top 1 p.Description from Positions p where c.PreferredPosition1 = p.PositionID), ''), ''), ''), '')
--+ @NewLineChar + 'Preferred Position 2: ' + isnull(iif(len(trim(isnull(c.PreferredPosition2Other, ''))) = 0, iif(c.PreferredPosition2 <> 0, isnull((select top 1 p.Description from Positions p where c.PreferredPosition2 = p.PositionID), ''), ''), ''), '')
--+ @NewLineChar + 'Preferred Position 3: ' + isnull(iif(len(trim(isnull(c.PreferredPosition3Other, ''))) = 0, iif(c.PreferredPosition3 <> 0, isnull((select top 1 p.Description from Positions p where c.PreferredPosition3 = p.PositionID), ''), ''), ''), '')
--+ @NewLineChar + 'Preferred Job Title: ' + trim(isnull(c.PreferredJobTitle1, ''))
--+ @NewLineChar + 'Preferred Industry 1: ' + isnull(iif(len(trim(isnull(c.PreferredIndustry1Other, ''))) = 0, iif(c.PreferredIndustry1 <> 0, isnull((select top 1 p.Description from Positions p where c.PreferredIndustry1 = p.PositionID), ''), ''), ''), '')
--+ @NewLineChar + 'Preferred Industry 2: ' + isnull(iif(len(trim(isnull(c.PreferredIndustry2Other, ''))) = 0, iif(c.PreferredIndustry2 <> 0, isnull((select top 1 p.Description from Positions p where c.PreferredIndustry2 = p.PositionID), ''), ''), ''), '')
--+ @NewLineChar + 'Preferred Industry 3: ' + isnull(iif(len(trim(isnull(c.PreferredIndustry3Other, ''))) = 0, iif(c.PreferredIndustry3 <> 0, isnull((select top 1 p.Description from Positions p where c.PreferredIndustry3 = p.PositionID), ''), ''), ''), '')
--+ @NewLineChar + 'Preferred Location 1: ' + isnull(iif(c.PreferredLocation1 <> 0, isnull((select top 1 p.Description from Positions p where c.PreferredLocation1 = p.PositionID), ''), ''), '')
--+ @NewLineChar + 'Preferred Location 2: ' + isnull(iif(c.PreferredLocation2 <> 0, isnull((select top 1 p.Description from Positions p where c.PreferredLocation2 = p.PositionID), ''), ''), '')
--+ @NewLineChar + 'Preferred Location 3: ' + isnull(iif(c.PreferredLocation3 <> 0, isnull((select top 1 p.Description from Positions p where c.PreferredLocation3 = p.PositionID), ''), ''), '')
--+ @NewLineChar + 'Notice Required: ' + trim(isnull(c.NoticeRequired, ''))
--+ @NewLineChar + 'Please describe the type of position you are looking for: ' + trim(isnull(cast(c.PreferredJobDescription1 as nvarchar(max)), ''))
--+ @NewLineChar + 'Any specific companies you wish to be considered for: ' + trim(isnull(c.InfluenceToApplyDescription, ''))
--+ @NewLineChar + 'Any companies you do not want to work for: ' + trim(isnull(cast(c.IncludeCompanies as nvarchar(max)), ''))
--+ @NewLineChar + 'Distance willing to travel: ' + trim(isnull(c.DistanceToTravel, ''))
--+ @NewLineChar + 'Available From: ' + trim(isnull(cast(c.AvailableFromDate as varchar(50)), ''))
--+ @NewLineChar + 'Available To: ' + trim(isnull(cast(c.AvailableToDate as varchar(50)), ''))
--+ @NewLineChar + 'Comments: ' + trim(isnull(cast(c.Comments as nvarchar(max)), ''))

--as [candidate-note]

----, db-field-not-found as [candidate-numberOfEmployers]
--, (
--	select string_agg(sfp.FileName, ',')
--	from
--	StoredFilePaths sfp
--	left join RecordTypes rt on sfp.RecordTypeID = rt.RecordTypeID
--	left join StoredFileTypes sft on sfp.StoredFileTypeID = sft.StoredFileTypeID
--	where rt.Description = 'Candidates' and sft.Description = 'Photos' and sfp.RecordID = c.CandidateID
--	group by sfp.RecordID
--) as [candidate-photo]

--, (
--	select string_agg(sfp.FileName, ',')
--	from
--	StoredFilePaths sfp
--	left join RecordTypes rt on sfp.RecordTypeID = rt.RecordTypeID
--	left join StoredFileTypes sft on sfp.StoredFileTypeID = sft.StoredFileTypeID
--	where rt.Description = 'Candidates' and sft.Description in ('Candidate CV', 'Admin CV', 'General') and sfp.RecordID = c.CandidateID
--	group by sfp.RecordID
--) as [candidate-resume]

--, iif(len(cast(c.TechnicalSkills as nvarchar(max))) = 0
--	, coalesce(
--		(
--			select top 1 string_agg(s.Description, ',')
--			from Skills s left join CandidateSkills cs on s.SkillID = cs.SkillID
--			where cs.CandidateID = c.CandidateID
--			group by cs.CandidateID
--		)
--		, ''
--	)
--	, ''
--) as [candidate-skills]

--, isnull(convert(varchar(50), c.AvailableFromDate, 111), '') as [candidate-startDate1]

----, db-field-not-found as [candidate-startDate2]
----, db-field-not-found as [candidate-startDate3]

--, isnull(convert(varchar(50), c.AvailableToDate, 111), '') as [candidate-endDate1]

----, db-field-not-found as [candidate-endDate2]
----, db-field-not-found as [candidate-endDate3]
--, (
--	select
--		string_agg(
--			'Position: '
--				+ trim(coalesce(ce.Position, '')) + @NewLineChar
--			+ 'Employer: '
--				+ trim(coalesce(ce.Employer, '')) + @NewLineChar
--			+ 'Start Date: '
--				+ trim(coalesce(ce.StartDate, '')) + @NewLineChar
--			+ 'End Date: '
--				+ trim(coalesce(ce.EndDate, '')) + @NewLineChar
--			+ 'Responsibilities: ' + trim(coalesce(cast(ce.Responsibilities as nvarchar(max)), ''))
	
--			, @DoubleNewLine
--		) within group (order by OrderID desc)
--	from CandidateEmployment ce
--	where ce.CandidateID = c.CandidateID
--) as [candidate-workHistory]

----, db-field-not-found as [candidate-owners]
--from
--Candidates c