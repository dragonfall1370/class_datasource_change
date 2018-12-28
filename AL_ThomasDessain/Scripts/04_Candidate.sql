declare @NewLineChar as char(1) = char(10);
declare @DoubleNewLine as char(2) = @NewLineChar + @NewLineChar;
declare @chars4trim varchar(20) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + N'%?*,. '
declare @dummyNote varchar(max) = ''

--select trim(@chars4trim, ' sagheerahmad@hotmail.co.uk ')

drop table if exists VC_Can

;with
Cans as (
	select row_number() over(partition by '' order by Comments) as CanId
	, *
	from [All Candidates]
)

--select * from Cans


, CanEmailsTmp1 as (
	select
	CanId
	, lower(
		isnull(
			nullif(
				iif(
					dbo.ufn_CheckEmailAddress(trim(@chars4trim from isnull(x.[Email], ''))) = 1
					, trim(@chars4trim from isnull(x.[Email], ''))
					, 'no_email@no_email.io'
				)
				, 'no_email@no_email.io'
			)
			, iif(
					dbo.ufn_CheckEmailAddress(trim(@chars4trim from isnull(x.[Email (Home)], ''))) = 1
					, trim(@chars4trim from isnull(x.[Email (Home)], ''))
					, 'no_email@no_email.io'
			)
		)
	) as Email
	from Cans x
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
	CanId
	, isnull(nullif(trim(@chars4trim from isnull(x.[First Name], '')), ''), '[No Name') as FirstName
	--, db-field-not-found as [candidate-middleName]
	, isnull(nullif(trim(@chars4trim from isnull(x.[Last Name], '')), ''), 'No Last Name]') as LastName
	from Cans x
)

, CanNames as (
	select
	CanId
	, isnull(nullif(iif(FirstName like '%_.[^ ]%', left(FirstName, patindex('%[^ ].[^ ][^ ]%', FirstName)), FirstName), ''), '[No Name') as FirstName
	, isnull(nullif(iif(LastName like '%_.[^ ]%', left(LastName, patindex('%[^ ].[^ ][^ ]%', LastName)), LastName), ''), 'No Last Name]') as LastName
	from CanNamesTmp1
)

--, CanCountry as (
--	select
--	CanId
--	, isnull(nullif(trim(isnull(Country, '')), ''), 'GB') as Country
--	from (
--		select
--		x.CAND_ID as CanId
--		, y.ABBREVIATION as Country
--		from
--		CANDINFO_DATA_TABLE x
--		left join VC_Countries y on lower(trim(@chars4trim from isnull(x.COUNTRY, ''))) = lower(y.COUNTRY)
--			or lower(trim(@chars4trim from isnull(x.COUNTRY, ''))) = lower(y.ABBREVIATION)
--	) x
--)

--select * from CanCountry

, CanDocs as (
	select
	x.CanId
	, string_agg(y.CanDoc, ',') as Docs
	from Cans x
	left join VCDocs y on
		lower(trim(isnull(y.CanDoc, ''))) like lower(concat(replace(trim(isnull(x.[Last Name], '')), ' ', '_'), '_', replace(trim(isnull(x.[First Name], '')), ' ', '_'))) + '%'
		or
		lower(trim(isnull(y.CanDoc, ''))) like lower(concat(replace(trim(isnull(x.[First Name], '')), ' ', '_'), '_', replace(trim(isnull(x.[Last Name], '')), ' ', '_'))) + '%'
	group by x.CanId
)

--select
--CanId
----, value as Doc
--from CanDocs
----cross apply string_split(Docs, ',')
--where Docs is not null
--order by CanId

-- 3091/7697 candidates has doc to be associated (total physical docs: 3933)
-- 3352/3933 docs are mapped to candidates


--, CanSkills as (
--	  select
--	  CAND_ID as CanId
--	  , STRING_AGG(trim(@chars4trim from isnull(Skill, '')), ',') as Skills
--	  from
--	  [SKILLINFO_DATA_TABLE]
--	  group by CAND_ID
--)

--, CanNotes as (
--	--declare @NewLineChar as char(1) = char(10);
--	--declare @DoubleNewLine as char(2) = @NewLineChar + @NewLineChar;
--	--declare @chars4trim varchar(20) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + N'%?*,. '
--	select
--		x.CAND_ID as CanId
--		, concat(
--			--concat('External ID: ', x.CAND_ID)
--			nullif(concat(@NewLineChar, 'Entered: ', FORMAT(dateadd(day, -2, cast(x.Entered as datetime)), 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Entered: '))
--			, nullif(concat(@NewLineChar, 'Personal Email: ', trim(@chars4trim from isnull(x.CUSTOM1, ''))), concat(@NewLineChar, 'Personal Email: '))
--			, nullif(concat(@NewLineChar, 'Preferred Salary (High): ', FORMAT(x.PREF_SALARYHIGH, '#,#', 'en-gb'), ' GBP'), concat(@NewLineChar, 'Preferred Salary (High): ', ' GBP'))
--			, nullif(concat(@NewLineChar, 'Prefered Location: ', trim(@chars4trim from isnull(x.PREF_LOCATION, ''))), concat(@NewLineChar, 'Prefered Location: '))
--			, nullif(concat(@NewLineChar, 'Notice: ', trim(@chars4trim from isnull(x.NOTICE, ''))), concat(@NewLineChar, 'Notice: '))
--		) as Notes

--	from CANDINFO_DATA_TABLE x
--)

--, CanWorkHistory as (
--	--declare @NewLineChar as char(1) = char(10);
--	--declare @DoubleNewLine as char(2) = @NewLineChar + @NewLineChar;
--	--declare @chars4trim varchar(20) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + N'%?*,. '
--	select
--		x.CAND_ID as CanId
--		, string_agg(
--			concat(
--				'------------------------------------------------------------------------------------------'
--				, nullif(concat(@NewLineChar, 'Company: ', trim(@chars4trim from isnull(x.company, ''))), concat(@NewLineChar, 'Company: '))
--				, nullif(concat(@NewLineChar, 'Job Title: ', trim(@chars4trim from isnull(x.jobtitle, ''))), concat(@NewLineChar, 'Job Title: '))
--				, nullif(concat(@NewLineChar, 'Type: ', trim(@chars4trim from isnull(x.type, ''))), concat(@NewLineChar, 'Type: '))
--				, nullif(concat(@NewLineChar, 'Salary: ', trim(@chars4trim from isnull(x.salary, ''))), concat(@NewLineChar, 'Salary: '))
--				, nullif(concat(@NewLineChar, 'Started: ', FORMAT(dateadd(day, -2, cast(x.started as datetime)), 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Started: '))
--				, nullif(concat(@NewLineChar, 'Ended: ', FORMAT(dateadd(day, -2, cast(x.ended as datetime)), 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Ended: '))
--				, nullif(concat(@NewLineChar, 'Placed by: ', trim(@chars4trim from isnull(x.placed_by, ''))), concat(@NewLineChar, 'Placed by: '))
--				, nullif(concat(@DoubleNewLine, 'Description: ', @DoubleNewLine, trim(@chars4trim from isnull(x.description, '')))
--					, concat(@DoubleNewLine, 'Description: ', @DoubleNewLine))
--			)	
--			, @DoubleNewLine
--		) within group (order by dateadd(day, -2, cast(x.STARTED as datetime)) desc) as WorkHistory

--	from EMPLOY_DATA_TABLE x
--	group by x.CAND_ID
--)

--, CanEducation as (
--	--declare @NewLineChar as char(1) = char(10);
--	--declare @DoubleNewLine as char(2) = @NewLineChar + @NewLineChar;
--	--declare @chars4trim varchar(20) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + N'%?*,. '
--	select
--		x.CAND_ID as CanId
--		, string_agg(
--			concat(
--				'------------------------------------------------------------------------------------------'
--				, nullif(concat(@NewLineChar, 'Educational Institution: ', trim(@chars4trim from isnull(x.company, ''))), concat(@NewLineChar, 'Educational Institution: '))
--				, nullif(concat(@NewLineChar, 'Job Title: ', trim(@chars4trim from isnull(x.jobtitle, ''))), concat(@NewLineChar, 'Job Title: '))
--				, nullif(concat(@NewLineChar, 'Type: ', trim(@chars4trim from isnull(x.type, ''))), concat(@NewLineChar, 'Type: '))
--				, nullif(concat(@NewLineChar, 'Started: ', FORMAT(dateadd(day, -2, cast(x.started as datetime)), 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Started: '))
--				, nullif(concat(@NewLineChar, 'Ended: ', FORMAT(dateadd(day, -2, cast(x.ended as datetime)), 'dd-MMM-yyyy', 'en-gb')), concat(@NewLineChar, 'Ended: '))
--				, nullif(concat(@DoubleNewLine, 'Description: ', @DoubleNewLine, trim(@chars4trim from isnull(x.description, '')))
--					, concat(@DoubleNewLine, 'Description: ', @DoubleNewLine))
--			)	
--			, @DoubleNewLine
--		) within group (order by dateadd(day, -2, cast(x.STARTED as datetime)) desc) as Education

--	from EDUCATION_DATA_TABLE x
--	group by x.CAND_ID
--)

--select
----STUFF('abc cn.g dkf.doc',
--left('abc cn.g dkf.doc',
--PATINDEX('%[^ ].[^ ][^ ]%', 'abc cn.g dkf.doc'))

--select * from CanNames
--where FirstName = '' or LastName = ''

select

trim(@chars4trim from isnull(cast(x.CanId as varchar), '')) as [candidate-externalId]

--, case(lower(trim(@chars4trim from isnull(x.SALUTATION, ''))))
--	when lower('Cand') then ''
--	when lower('Dr') then 'DR'
--	when lower('Dr.') then 'DR'
--	when lower('Miss') then 'MISS'
--	when lower('Miss.') then 'MISS'
--	when lower('Mr') then 'MR'
--	when lower('Mr.') then 'MR'
--	when lower('Mrs') then 'MRS'
--	when lower('Mrs.') then 'MRS'
--	when lower('Ms') then 'MS'
--	when lower('Ms.') then 'MS'
--	when lower('Sree') then ''
--	else ''
--end as [candidate-title]

, cn.FirstName as [candidate-firstName]
--, db-field-not-found as [candidate-middleName]

, cn.LastName as [candidate-Lastname]
--, db-field-not-found as [candidate-FirstNameKana]
--, db-field-not-found as [candidate-LastNameKana]

, ce.Email as [candidate-email]

, iif(
	dbo.ufn_CheckEmailAddress(trim(@chars4trim from isnull(x.[Email (Home)], ''))) = 1
	, trim(@chars4trim from isnull(x.[Email (Home)], ''))
	, ''
) as [candidate-workEmail]

--, case(lower(trim(@chars4trim from isnull(x.[TYPE], 'Permanent'))))
--	when lower('Permanent') then 'PERMANENT'
--	when lower('Contract') then 'CONTRACT'
--	when lower('Permanent Part-time') then 'PERMANENT'
--	when lower('Temp and Perm') then 'PERMANENT'
--	when lower('Temporary') then 'TEMPORARY'
--	when lower('Temporary Part-time') then 'TEMPORARY'
--end as [candidate-jobTypes]

--, case(lower(trim(@chars4trim from isnull(x.hours, 'Full-Time'))))
--	when lower('9 - 5, Mon - Fri') then 'FULL_TIME'
--	when lower('Full Time') then 'FULL_TIME'
--	when lower('Full-Time') then 'FULL_TIME'
--	when lower('Full-Time or Part-Time') then 'FULL_TIME'
--	when lower('Mon - Fri') then 'FULL_TIME'
--	when lower('Part Time') then 'PART_TIME'
--end as [candidate-employmentType]

--, db-field-not-found as [candidate-gender]

--, trim(@chars4trim from isnull(convert(varchar(20), dateadd(day, -2, cast(x.DOB as datetime)), 111), '')) as [candidate-dob]

--, replace(
--	replace(
--		trim( ', ' from
--			dbo.ufn_PopulateLocationAddress(
--				concat(trim(@chars4trim from isnull(x.ADDRESS1, ''))
--					, ', '
--					, trim(@chars4trim from isnull(x.ADDRESS2, ''))
--					, ', '
--					, trim(@chars4trim from isnull(x.ADDRESS3, ''))
--				)
--				, trim(@chars4trim from isnull(x.CITY, ''))
--				, trim(@chars4trim from isnull(x.COUNTY, ''))
--				, trim(@chars4trim from isnull(x.ZIPCODE, ''))
--				, isnull(nullif(cc.Country, 'GB'), 'UK')
--				, ''
--			)
--		)
--		, ',,'
--		, ','
--	)
--	, ', ,'
--	, ','
--) as [candidate-address]

--, trim(@chars4trim from isnull(x.CITY, '')) as [candidate-city]

--, trim(@chars4trim from isnull(x.COUNTY, '')) as [candidate-State]

, 'GB' as [candidate-Country]

--, trim(@chars4trim from isnull(x.ZIPCODE, '')) as [candidate-zipCode]

, [dbo].[ufn_RefinePhoneNumber_V2](isnull(
	nullif(trim(@chars4trim from isnull(x.[Number (Mobile)], '')), '')
	, isnull(
		nullif(trim(@chars4trim from isnull(x.[Number (Home)], '')), '')
		, trim(@chars4trim from isnull(x.[Telephone Number], ''))
	)
)) as [candidate-phone]

, [dbo].[ufn_RefinePhoneNumber_V2](trim(@chars4trim from isnull(x.[Number (Home)], ''))) as [candidate-homePhone]

--, db-field-not-found as [candidate-workPhone]

, [dbo].[ufn_RefinePhoneNumber_V2](trim(@chars4trim from isnull(x.[Number (Mobile)], ''))) as [candidate-mobile]

--, db-field-not-found as [candidate-citizenship]

--, iif(
--	charindex('?', trim(@chars4trim from isnull(x.CUSTOM3, ''))) > 0
--	, left(trim(@chars4trim from isnull(x.CUSTOM3, '')), charindex('?', trim(@chars4trim from isnull(x.CUSTOM3, ''))) - 1)
--	, trim(@chars4trim from isnull(x.CUSTOM3, ''))
--) as [candidate-linkedln]

--, trim(@chars4trim from isnull(cast(x.SALARY as varchar(20)), '0')) as [candidate-currentSalary]

--, trim(@chars4trim from isnull(cast(x.PREF_SALARY as varchar(20)), '0')) as [candidate-desiredSalary]
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

--, trim(@chars4trim from isnull(cast(x.EMPLOYER as varchar(20)), '')) as [candidate-employer1]
--, db-field-not-found as [candidate-employer2]
--, db-field-not-found as [candidate-employer3]

--, trim(@chars4trim from isnull(x.TITLE, '')) as [candidate-jobTitle1]
--, db-field-not-found as [candidate-jobTitle2]
--, db-field-not-found as [candidate-jobTitle3]
--, db-field-not-found as [candidate-keyword]

--, iif(dbo.ufn_CheckEmailAddress(trim(@chars4trim from isnull(x.USER_ID, ''))) = 0
--	, ''
--	, lower(trim(@chars4trim from isnull(x.USER_ID, '')))
--) as [candidate-owners]

--, db-field-not-found as [candidate-numberOfEmployers]
--, db-field-not-found as [candidate-photo]

, isnull(cd.Docs, '') as [candidate-resume]

--, isnull(cs.Skills, '') as [candidate-skills]
--, db-field-not-found as [candidate-startDate1]
--, db-field-not-found as [candidate-startDate2]
--, db-field-not-found as [candidate-startDate3]
--, db-field-not-found as [candidate-endDate1]
--, db-field-not-found as [candidate-endDate2]
--, db-field-not-found as [candidate-endDate3]

--, cno.Notes as [candidate-note]

--, isnull(ced.Education, '') as [candidate-education]

, replace(trim(isnull(x.[Job Title – Previous Employment], '')), ', ', char(10)) as [candidate-workHistory]

--, db-field-not-found as [candidate-comments]

, [Industry Sector] as CanIndustry
, [Owning Consultant – Permanent Candidate] as OwningConsultantPermanentCandidate
, [Owning Consultant - Temporary Candidate] as OwningConsultantTemporaryCandidate

into VC_Can

from
Cans x
--left join CanSkills cs on x.CAND_ID = cs.CanId
left join CanEmails ce on x.CanId = ce.CanId
left join CanNames cn on x.CanId = cn.CanId
--left join CanCountry cc on x.CAND_ID = cc.CanId
--left join CanWorkHistory cw on x.CAND_ID = cw.CanId
--left join CanEducation ced on x.CAND_ID = ced.CanId
--left join CanNotes cno on x.CAND_ID = cno.CanId
left join CanDocs cd on x.CanId = cd.CanId
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
;with TmpTab1 as (
select
cast([candidate-externalId] as int) as entityExtId
, case(lower(trim(isnull(OwningConsultantPermanentCandidate, ''))))
	when lower('Administrator') then lower('enquiries@thomasdessain.com')
	when lower('Temp User') then lower('enquiries@thomasdessain.com')
	when lower('Karen Mars') then lower('karen.mars@thomasdessain.com')
	when lower('Rosy Dessain') then lower('rosy.dessain@thomasdessain.com')
	when lower('Ross Hunter') then lower('ross.hunter@thomasdessain.com')
	when lower('Edward Le Gallais') then lower('edward.legallais@thomasdessain.com')
	when lower('Emma O''Connell') then lower('emma.connell@thomasdessain.com')
	when lower('Hattie Carlton') then lower('hattie.carlton@thomasdessain.com')
	when lower('James Troy') then lower('james.troy@thomasdessain.com')
	when lower('Caroline Sullivan') then lower('caroline.sullivan@thomasdessain.com')
	when lower('Matt Cole') then lower('matthew.cole@thomasdessain.com')
	when lower('Sally Temple') then lower('sally.temple@thomasdessain.com')
	else lower('enquiries@thomasdessain.com')
end as OwningConsultantPermanentCandidate

, case(lower(trim(isnull(OwningConsultantTemporaryCandidate, ''))))
	when lower('Administrator') then lower('enquiries@thomasdessain.com')
	when lower('Temp User') then lower('enquiries@thomasdessain.com')
	when lower('Karen Mars') then lower('karen.mars@thomasdessain.com')
	when lower('Rosy Dessain') then lower('rosy.dessain@thomasdessain.com')
	when lower('Ross Hunter') then lower('ross.hunter@thomasdessain.com')
	when lower('Edward Le Gallais') then lower('edward.legallais@thomasdessain.com')
	when lower('Emma O''Connell') then lower('emma.connell@thomasdessain.com')
	when lower('Hattie Carlton') then lower('hattie.carlton@thomasdessain.com')
	when lower('James Troy') then lower('james.troy@thomasdessain.com')
	when lower('Caroline Sullivan') then lower('caroline.sullivan@thomasdessain.com')
	when lower('Matt Cole') then lower('matthew.cole@thomasdessain.com')
	when lower('Sally Temple') then lower('sally.temple@thomasdessain.com')
	else lower('enquiries@thomasdessain.com')
end as OwningConsultantTemporaryCandidate

from VC_Can
where len(trim(isnull(OwningConsultantPermanentCandidate, ''))) > 0
or len(trim(isnull(OwningConsultantTemporaryCandidate, ''))) > 0
)

, TmpTab2 as (
select
entityExtId
, case(lower(OwningConsultantPermanentCandidate))
	when lower('Edward.LeGallais@thomasdessain.com') then '28958'
	when lower('james.troy@thomasdessain.com') then '28960'
	when lower('ross.hunter@thomasdessain.com') then '28957'
	when lower('rosy.dessain@thomasdessain.com') then '28955'
	when lower('sally.temple@thomasdessain.com') then '28954'
	when lower('emma.connell@thomasdessain.com') then '28968'
	when lower('hattie.carlton@thomasdessain.com') then '28962'
	when lower('enquiries@thomasdessain.com') then '28966'
	when lower('caroline.sullivan@thomasdessain.com') then '28961'
	when lower('Matthew.Cole@thomasdessain.com') then '28965'
	when lower('karen.mars@thomasdessain.com') then '28967'
	else '28966'
end as OwningConsultantPermanentCandidate

, case(lower(OwningConsultantTemporaryCandidate))
	when lower('Edward.LeGallais@thomasdessain.com') then '28958'
	when lower('james.troy@thomasdessain.com') then '28960'
	when lower('ross.hunter@thomasdessain.com') then '28957'
	when lower('rosy.dessain@thomasdessain.com') then '28955'
	when lower('sally.temple@thomasdessain.com') then '28954'
	when lower('emma.connell@thomasdessain.com') then '28968'
	when lower('hattie.carlton@thomasdessain.com') then '28962'
	when lower('enquiries@thomasdessain.com') then '28966'
	when lower('caroline.sullivan@thomasdessain.com') then '28961'
	when lower('Matthew.Cole@thomasdessain.com') then '28965'
	when lower('karen.mars@thomasdessain.com') then '28967'
	else '28966'
end as OwningConsultantTemporaryCandidate

from TmpTab1
)

, TmpTab3 as (
	select
	entityExtId
	, concat_ws(',', OwningConsultantPermanentCandidate, OwningConsultantTemporaryCandidate) as Owners
	from TmpTab2
)

, TmpTab4 as (
	select
	entityExtId
	, cast(value as int) as [ownerId]
	--, 'false' as [primary]
	--, 0 as [ownership]
	from TmpTab3
	cross apply string_split(Owners, ',')
)

, TmpTab5 as (
select distinct
entityExtId
, ownerId
--, [primary]
--, [ownership]
from TmpTab4
--order by entityExtId
)

--select * from TmpTab5
--order by entityExtId

select distinct
entityExtId
, (
	select
	ownerId--, [primary], [ownership]
	from TmpTab5
	where entityExtId = x.entityExtId
	order by ownerId
	for json auto
) as Owners
from TmpTab5 x
group by entityExtId
order by entityExtId

;with
TmpTab1 as (
select
[candidate-externalId] as entityExtId
, dbo.ufn_TrimSpecialCharacters_V2(replace(replace([candidate-workHistory], '\x0d\x0a', char(10)), '\x0a', char(10)), '?.') as workHistorySummary
from VC_Can
where dbo.ufn_TrimSpecialCharacters_V2(replace(replace([candidate-workHistory], '\x0d\x0a', char(10)), '\x0a', char(10)), '?.') <> ''
)
--select * from TmpTab1
, TmpTab2 as (
	select
	entityExtId
	--, workHistory
	--, iif(
	--	charindex(char(10), workHistory) = 0
	--	, workHistory
	--	, left(workHistory, charindex(char(10), workHistory) - 1)
	--) as currentWork
	, dbo.ufn_TrimSpecialCharacters_V2(value, '?.,') as work
	, workHistorySummary
	from TmpTab1
	cross apply string_split(workHistorySummary, char(10))
)

, TmpTab3 as (
	select
	entityExtId
	, work
	, row_number() over(partition by entityExtId order by entityExtId) as rn
	, workHistorySummary
	from TmpTab2
)

--select * from TmpTab3

, TmpTab4 as(
select
entityExtId
, trim(left(replace(work, '–', '-'), iif(charindex('-', replace(work, '–', '-')) > 0, charindex('-', replace(work, '–', '-')) - 1, len(replace(work, '–', '-'))))) as jobTitle
from TmpTab3
where rn = 1
)


, TmpTab5 as (
select
entityExtId
, '' as company
, jobTitle as jobTitle
, '' as currentEmployer
--, '' as dateRangeFrom
--, '' as dateRangeTo
from TmpTab4
)

select distinct
entityExtId
, (
	select
	company, jobTitle, currentEmployer
	from TmpTab5
	where entityExtId = x.entityExtId
	--order by ownerId
	for json auto
) as Owners
from TmpTab5 x
group by entityExtId
order by entityExtId



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