-- TODO: update logic to populate unique email by the code at the bottom of file
declare @NewLineChar as char(1) = char(10);
declare @DoubleNewLine as char(2) = @NewLineChar + @NewLineChar;
declare @chars4trim varchar(20) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + N'%?*,. '
declare @dummyNote varchar(max) = ''

--select trim(@chars4trim, ' sagheerahmad@hotmail.co.uk ')

drop table if exists VC_Can

;with
CanEmailsTmp1 as (
	select
	CAND_ID as CanId
	, lower(
		isnull(
			nullif(
				iif(
					dbo.ufn_CheckEmailAddress(trim(@chars4trim from isnull(x.EMAIL, ''))) = 1
					, trim(@chars4trim from isnull(x.EMAIL, ''))
					, 'no_email@no_email.io'
				)
				, 'no_email@no_email.io'
			)
			, iif(
					dbo.ufn_CheckEmailAddress(trim(@chars4trim from isnull(x.EMAIL_WORK, ''))) = 1
					, trim(@chars4trim from isnull(x.EMAIL_WORK, ''))
					, 'no_email@no_email.io'
			)
		)
	) as Email
	from CANDINFO_DATA_TABLE x
)

--select * from CanEmailsTmp1

, CanEmails as (
	select
	x.CanId
	, iif(x.rn = 1
		, iif(x.Email <> 'no_email@no_email.io'
			, x.Email
			, replace(x.Email, '@', concat('_', replicate('0', 4 - len(cast(x.rn as varchar(10)))), x.rn, '@'))
		)
		, iif(x.Email <> 'no_email@no_email.io'
			, replace(x.Email, '@', concat('_', replicate('0', 4 - len(cast(x.rn - 1 as varchar(10)))), x.rn - 1, '@'))
			, replace(x.Email, '@', concat('_', replicate('0', 4 - len(cast(x.rn as varchar(10)))), x.rn, '@'))
		)
	) as Email
	from (
		select *
		, row_number() over(partition by Email order by CanId) as rn
		from CanEmailsTmp1
	) x
)

--select * from CanEmails
--where Email like 'no_email%'

, CanNamesTmp1 as (
	select
	x.CAND_ID as CanId
	, isnull(nullif(trim(@chars4trim from isnull(x.FORENAME, '')), ''), '[No Name') as FirstName
	--, db-field-not-found as [candidate-middleName]
	, isnull(nullif(trim(@chars4trim from isnull(x.SURNAME, '')), ''), 'No Last Name]') as LastName
	from CANDINFO_DATA_TABLE x
)

, CanNames as (
	select
	CanId
	, isnull(nullif(iif(FirstName like '%_.[^ ]%', left(FirstName, patindex('%[^ ].[^ ][^ ]%', FirstName)), FirstName), ''), '[No Name') as FirstName
	, isnull(nullif(iif(LastName like '%_.[^ ]%', left(LastName, patindex('%[^ ].[^ ][^ ]%', LastName)), LastName), ''), 'No Last Name]') as LastName
	from CanNamesTmp1
)

, CanCountry as (
	select
	CanId
	, isnull(nullif(trim(isnull(Country, '')), ''), 'GB') as Country
	from (
		select
		x.CAND_ID as CanId
		, y.ABBREVIATION as Country
		from
		CANDINFO_DATA_TABLE x
		left join VC_Countries y on lower(trim(@chars4trim from isnull(x.COUNTRY, ''))) = lower(y.COUNTRY)
			or lower(trim(@chars4trim from isnull(x.COUNTRY, ''))) = lower(y.ABBREVIATION)
	) x
)

--select * from CanCountry

, CanDocs as (
	select
	x.CAND_ID as CanId
	, string_agg(y.Name, ',') as Docs
	from CANDINFO_DATA_TABLE x
	left join VC_DocsIdx y on x.CAND_ID = y.CandID
	where len(trim(isnull(y.Name, ''))) > 0
	group by x.CAND_ID
)

, CanSkills as (
	  select
	  CAND_ID as CanId
	  , STRING_AGG(trim(@chars4trim from isnull(Skill, '')), ',') as Skills
	  from
	  [SKILLINFO_DATA_TABLE]
	  group by CAND_ID
)

, CanNotes as (
	--declare @NewLineChar as char(1) = char(10);
	--declare @DoubleNewLine as char(2) = @NewLineChar + @NewLineChar;
	--declare @chars4trim varchar(20) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + N'%?*,. '
	select
		x.CAND_ID as CanId
		, concat(
			--concat('External ID: ', x.CAND_ID)
			nullif(concat(@NewLineChar, 'Entered: ', FORMAT(dateadd(day, -2, cast(x.Entered as datetime)), 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Entered: '))
			, nullif(concat(@NewLineChar, 'Personal Email: ', trim(@chars4trim from isnull(x.CUSTOM1, ''))), concat(@NewLineChar, 'Personal Email: '))
			, nullif(concat(@NewLineChar, 'Preferred Salary (High): ', FORMAT(x.PREF_SALARYHIGH, '#,#', 'en-gb'), ' GBP'), concat(@NewLineChar, 'Preferred Salary (High): ', ' GBP'))
			, nullif(concat(@NewLineChar, 'Prefered Location: ', trim(@chars4trim from isnull(x.PREF_LOCATION, ''))), concat(@NewLineChar, 'Prefered Location: '))
			, nullif(concat(@NewLineChar, 'Notice: ', trim(@chars4trim from isnull(x.NOTICE, ''))), concat(@NewLineChar, 'Notice: '))
		) as Notes

	from CANDINFO_DATA_TABLE x
)

, CanWorkHistory as (
	--declare @NewLineChar as char(1) = char(10);
	--declare @DoubleNewLine as char(2) = @NewLineChar + @NewLineChar;
	--declare @chars4trim varchar(20) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + N'%?*,. '
	select
		x.CAND_ID as CanId
		, string_agg(
			concat(
				'------------------------------------------------------------------------------------------'
				, nullif(concat(@NewLineChar, 'Company: ', trim(@chars4trim from isnull(x.company, ''))), concat(@NewLineChar, 'Company: '))
				, nullif(concat(@NewLineChar, 'Job Title: ', trim(@chars4trim from isnull(x.jobtitle, ''))), concat(@NewLineChar, 'Job Title: '))
				, nullif(concat(@NewLineChar, 'Type: ', trim(@chars4trim from isnull(x.type, ''))), concat(@NewLineChar, 'Type: '))
				, nullif(concat(@NewLineChar, 'Salary: ', trim(@chars4trim from isnull(x.salary, ''))), concat(@NewLineChar, 'Salary: '))
				, nullif(concat(@NewLineChar, 'Started: ', FORMAT(dateadd(day, -2, cast(x.started as datetime)), 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Started: '))
				, nullif(concat(@NewLineChar, 'Ended: ', FORMAT(dateadd(day, -2, cast(x.ended as datetime)), 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Ended: '))
				, nullif(concat(@NewLineChar, 'Placed by: ', trim(@chars4trim from isnull(x.placed_by, ''))), concat(@NewLineChar, 'Placed by: '))
				, nullif(concat(@DoubleNewLine, 'Description: ', @DoubleNewLine, trim(@chars4trim from isnull(x.description, '')))
					, concat(@DoubleNewLine, 'Description: ', @DoubleNewLine))
			)	
			, @DoubleNewLine
		) within group (order by dateadd(day, -2, cast(x.STARTED as datetime)) desc) as WorkHistory

	from EMPLOY_DATA_TABLE x
	group by x.CAND_ID
)

, CanEducation as (
	--declare @NewLineChar as char(1) = char(10);
	--declare @DoubleNewLine as char(2) = @NewLineChar + @NewLineChar;
	--declare @chars4trim varchar(20) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + N'%?*,. '
	select
		x.CAND_ID as CanId
		, string_agg(
			concat(
				'------------------------------------------------------------------------------------------'
				, nullif(concat(@NewLineChar, 'Educational Institution: ', trim(@chars4trim from isnull(x.company, ''))), concat(@NewLineChar, 'Educational Institution: '))
				, nullif(concat(@NewLineChar, 'Job Title: ', trim(@chars4trim from isnull(x.jobtitle, ''))), concat(@NewLineChar, 'Job Title: '))
				, nullif(concat(@NewLineChar, 'Type: ', trim(@chars4trim from isnull(x.type, ''))), concat(@NewLineChar, 'Type: '))
				, nullif(concat(@NewLineChar, 'Started: ', FORMAT(dateadd(day, -2, cast(x.started as datetime)), 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Started: '))
				, nullif(concat(@NewLineChar, 'Ended: ', FORMAT(dateadd(day, -2, cast(x.ended as datetime)), 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Ended: '))
				, nullif(concat(@DoubleNewLine, 'Description: ', @DoubleNewLine, trim(@chars4trim from isnull(x.description, '')))
					, concat(@DoubleNewLine, 'Description: ', @DoubleNewLine))
			)	
			, @DoubleNewLine
		) within group (order by dateadd(day, -2, cast(x.STARTED as datetime)) desc) as Education

	from EDUCATION_DATA_TABLE x
	group by x.CAND_ID
)

--select
----STUFF('abc cn.g dkf.doc',
--left('abc cn.g dkf.doc',
--PATINDEX('%[^ ].[^ ][^ ]%', 'abc cn.g dkf.doc'))

--select * from CanNames
--where FirstName = '' or LastName = ''

select

trim(@chars4trim from isnull(cast(x.CAND_ID as varchar), '')) as [candidate-externalId]

, case(lower(trim(@chars4trim from isnull(x.SALUTATION, ''))))
	when lower('Cand') then ''
	when lower('Dr') then 'DR'
	when lower('Dr.') then 'DR'
	when lower('Miss') then 'MISS'
	when lower('Miss.') then 'MISS'
	when lower('Mr') then 'MR'
	when lower('Mr.') then 'MR'
	when lower('Mrs') then 'MRS'
	when lower('Mrs.') then 'MRS'
	when lower('Ms') then 'MS'
	when lower('Ms.') then 'MS'
	when lower('Sree') then ''
	else ''
end as [candidate-title]

, cn.FirstName as [candidate-firstName]
--, db-field-not-found as [candidate-middleName]

, cn.LastName as [candidate-Lastname]
--, db-field-not-found as [candidate-FirstNameKana]
--, db-field-not-found as [candidate-LastNameKana]

, ce.Email as [candidate-email]

, iif(
	dbo.ufn_CheckEmailAddress(trim(@chars4trim from isnull(x.EMAIL_WORK, ''))) = 1
	, trim(@chars4trim from isnull(x.EMAIL_WORK, ''))
	, ''
) as [candidate-workEmail]

, case(lower(trim(@chars4trim from isnull(x.[TYPE], 'Permanent'))))
	when lower('Permanent') then 'PERMANENT'
	when lower('Contract') then 'CONTRACT'
	when lower('Permanent Part-time') then 'PERMANENT'
	when lower('Temp and Perm') then 'PERMANENT'
	when lower('Temporary') then 'TEMPORARY'
	when lower('Temporary Part-time') then 'TEMPORARY'
end as [candidate-jobTypes]

, case(lower(trim(@chars4trim from isnull(x.hours, 'Full-Time'))))
	when lower('9 - 5, Mon - Fri') then 'FULL_TIME'
	when lower('Full Time') then 'FULL_TIME'
	when lower('Full-Time') then 'FULL_TIME'
	when lower('Full-Time or Part-Time') then 'FULL_TIME'
	when lower('Mon - Fri') then 'FULL_TIME'
	when lower('Part Time') then 'PART_TIME'
end as [candidate-employmentType]

--, db-field-not-found as [candidate-gender]

, trim(@chars4trim from isnull(convert(varchar(20), dateadd(day, -2, cast(x.DOB as datetime)), 111), '')) as [candidate-dob]

, replace(
	replace(
		trim( ', ' from
			dbo.ufn_PopulateLocationAddress(
				concat(trim(@chars4trim from isnull(x.ADDRESS1, ''))
					, ', '
					, trim(@chars4trim from isnull(x.ADDRESS2, ''))
					, ', '
					, trim(@chars4trim from isnull(x.ADDRESS3, ''))
				)
				, trim(@chars4trim from isnull(x.CITY, ''))
				, trim(@chars4trim from isnull(x.COUNTY, ''))
				, trim(@chars4trim from isnull(x.ZIPCODE, ''))
				, isnull(nullif(cc.Country, 'GB'), 'UK')
				, ''
			)
		)
		, ',,'
		, ','
	)
	, ', ,'
	, ','
) as [candidate-address]

, trim(@chars4trim from isnull(x.CITY, '')) as [candidate-city]

, trim(@chars4trim from isnull(x.COUNTY, '')) as [candidate-State]

, cc.Country as [candidate-Country]

, trim(@chars4trim from isnull(x.ZIPCODE, '')) as [candidate-zipCode]

, [dbo].[ufn_RefinePhoneNumber_V2](isnull(
	nullif(trim(@chars4trim from isnull(x.MOBILE, '')), '')
	, isnull(
		nullif(trim(@chars4trim from isnull(x.HOME_PHONE, '')), '')
		, trim(@chars4trim from isnull(x.WORK_PHONE, ''))
	)
)) as [candidate-phone]

, [dbo].[ufn_RefinePhoneNumber_V2](trim(@chars4trim from isnull(x.HOME_PHONE, ''))) as [candidate-homePhone]

--, db-field-not-found as [candidate-workPhone]

, [dbo].[ufn_RefinePhoneNumber_V2](trim(@chars4trim from isnull(x.MOBILE, ''))) as [candidate-mobile]

--, db-field-not-found as [candidate-citizenship]

, iif(
	charindex('?', trim(@chars4trim from isnull(x.CUSTOM3, ''))) > 0
	, left(trim(@chars4trim from isnull(x.CUSTOM3, '')), charindex('?', trim(@chars4trim from isnull(x.CUSTOM3, ''))) - 1)
	, trim(@chars4trim from isnull(x.CUSTOM3, ''))
) as [candidate-linkedln]

, trim(@chars4trim from isnull(cast(x.SALARY as varchar(20)), '0')) as [candidate-currentSalary]

, trim(@chars4trim from isnull(cast(x.PREF_SALARY as varchar(20)), '0')) as [candidate-desiredSalary]
--, db-field-not-found as [candidate-contractInterval]
--, db-field-not-found as [candidate-contractRate]
, 'GBP' as [candidate-currency]
--, db-field-not-found as [candidate-degreeName]

--, db-field-not-found as [candidate-educationLevel]
--, db-field-not-found as [candidate-gpa]
--, db-field-not-found as [candidate-grade]
--, db-field-not-found as [candidate-graduationDate]
--, db-field-not-found as [candidate-schoolName]
--, db-field-not-found as [candidate-company1]
--, db-field-not-found as [candidate-company2]
--, db-field-not-found as [candidate-company3]

, trim(@chars4trim from isnull(cast(x.EMPLOYER as varchar(20)), '')) as [candidate-employer1]
--, db-field-not-found as [candidate-employer2]
--, db-field-not-found as [candidate-employer3]

, trim(@chars4trim from isnull(x.TITLE, '')) as [candidate-jobTitle1]
--, db-field-not-found as [candidate-jobTitle2]
--, db-field-not-found as [candidate-jobTitle3]
--, db-field-not-found as [candidate-keyword]

, iif(dbo.ufn_CheckEmailAddress(trim(@chars4trim from isnull(x.USER_ID, ''))) = 0
	, ''
	, lower(trim(@chars4trim from isnull(x.USER_ID, '')))
) as [candidate-owners]

--, db-field-not-found as [candidate-numberOfEmployers]
--, db-field-not-found as [candidate-photo]

, isnull(cd.Docs, '') as [candidate-resume]

, isnull(cs.Skills, '') as [candidate-skills]
--, db-field-not-found as [candidate-startDate1]
--, db-field-not-found as [candidate-startDate2]
--, db-field-not-found as [candidate-startDate3]
--, db-field-not-found as [candidate-endDate1]
--, db-field-not-found as [candidate-endDate2]
--, db-field-not-found as [candidate-endDate3]

, cno.Notes as [candidate-note]

, isnull(ced.Education, '') as [candidate-education]

, isnull(cw.WorkHistory, '') as [candidate-workHistory]

--, db-field-not-found as [candidate-comments]

into VC_Can

from
CANDINFO_DATA_TABLE x
left join CanSkills cs on x.CAND_ID = cs.CanId
left join CanEmails ce on x.CAND_ID = ce.CanId
left join CanNames cn on x.CAND_ID = cn.CanId
left join CanCountry cc on x.CAND_ID = cc.CanId
left join CanWorkHistory cw on x.CAND_ID = cw.CanId
left join CanEducation ced on x.CAND_ID = ced.CanId
left join CanNotes cno on x.CAND_ID = cno.CanId
left join CanDocs cd on x.CAND_ID = cd.CanId
--select
--x.EMAIL
--, x.EMAIL_WORK
--, x.PREF_EMAIL
--from
--CANDINFO_DATA_TABLE x
----where x.EMAIL like '\_%' escape '\'
--where x.EMAIL_WORK is not null

select * from VC_Can
--where charindex('.', [candidate-firstName]) > 0 or charindex('.', [candidate-lastName]) > 0
--where [candidate-firstName] like '%_.[^ ]%'
-- where len([candidate-email]) = 0
--where len([candidate-firstName]) = 0 or len([candidate-Lastname]) = 0
--where charindex(',', [candidate-email]) > 1
order by cast([candidate-externalId] as int) -- this is a MUST there must be ORDER BY statement
-- the paging comes here
--OFFSET     53000 ROWS       -- skip N rows
--FETCH NEXT 10000 ROWS ONLY; -- take M rows

--select * from CANDINFO_DATA_TABLE where CAND_ID in (10011, 10039)

--declare @NewLineChar as char(1) = char(10);
--declare @DoubleNewLine as char(2) = @NewLineChar + @NewLineChar;
--declare @chars4trim varchar(20) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + N'%?*,. '
--declare @dummyNote varchar(max) = ''

--;with abc as(
--select

--CAND_ID as CanId
--	, lower(
--		isnull(
--			nullif(
--				iif(
--					dbo.ufn_CheckEmailAddress(trim(@chars4trim from isnull(x.EMAIL, ''))) = 1
--					, trim(@chars4trim from isnull(x.EMAIL, ''))
--					, 'no_email@no_email.io'
--				)
--				, 'no_email@no_email.io'
--			)
--			, iif(
--					dbo.ufn_CheckEmailAddress(trim(@chars4trim from isnull(x.EMAIL_WORK, ''))) = 1
--					, trim(@chars4trim from isnull(x.EMAIL_WORK, ''))
--					, 'no_email@no_email.io'
--			)
--		)
--	) as Email

--from CANDINFO_DATA_TABLE x where CAND_ID in (10011, 10039)
--)

--select *
--		, row_number() over(partition by Email order by CanId) as rn
--		from abc

--select
--	x.CanId
--	, iif(x.rn = 1
--		, iif(x.Email <> 'no_email@no_email.io'
--			, x.Email
--			, replace(x.Email, '@', concat('_', replicate('0', 4 - len(cast(x.rn as varchar(10)))), x.rn, '@'))
--		)
--		, iif(x.Email <> 'no_email@no_email.io'
--			, replace(x.Email, '@', concat('_', replicate('0', 4 - len(cast(x.rn - 1 as varchar(10)))), x.rn - 1, '@'))
--			, replace(x.Email, '@', concat('_', replicate('0', 4 - len(cast(x.rn as varchar(10)))), x.rn, '@'))
--		)
--	) as Email
--	from (
--		select *
--		, row_number() over(partition by Email order by CanId) as rn
--		from abc
--	) x