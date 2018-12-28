declare @NewLineChar as char(2) = char(13) + char(10);
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar;

drop table if exists VCCanPreferences

select
CandidateId
, concat(
	iif(x.PreferredIndustry1 = 0, ''
		, concat(@NewLineChar, 'Preferred Industry 1: ', trim(isnull(i1.Description, '')))
	)
	, iif(len(trim(isnull(x.PreferredJobTitle1, ''))) = 0, ''
		, concat(@NewLineChar, 'Preferred Job Title 1: ', trim(isnull(x.[PreferredJobTitle1], '')))
	)
	, iif(len(trim(isnull(cast(x.[PreferredJobDescription1] as nvarchar(max)), ''))) = 0, ''
		, concat(@NewLineChar, 'Preferred Job Description 1: ', trim(isnull(cast(x.[PreferredJobDescription1] as nvarchar(max)), '')))
	)
	, iif(x.PreferredPosition1 = 0, ''
		, concat(@NewLineChar, 'Preferred Position 1: ', trim(isnull(p1.Description, '')))
	)
	, iif(x.PreferredLocation1 = 0, ''
		, concat(@NewLineChar, 'Preferred Location 1: ', trim(isnull(l1.Description, '')))
	)
	, concat(@NewLineChar, '------------------------------')
	, iif(x.PreferredIndustry2 = 0, ''
		, concat(@NewLineChar, 'Preferred Industry 2: ', trim(isnull(i2.Description, '')))
	)
	, iif(x.PreferredPosition2 = 0, ''
		, concat(@NewLineChar, 'Preferred Position 2: ', trim(isnull(p2.Description, '')))
	)
	, iif(x.PreferredLocation2 = 0, ''
		, concat(@NewLineChar, 'Preferred Location 2: ', trim(isnull(l2.Description, '')))
	)
	, concat(@NewLineChar, '------------------------------')
	, iif(x.PreferredIndustry3 = 0, ''
		, concat(@NewLineChar, 'Preferred Industry 3: ', trim(isnull(i3.Description, '')))
	)
	, iif(x.PreferredPosition3 = 0, ''
		, concat(@NewLineChar, 'Preferred Position 3: ', trim(isnull(p3.Description, '')))
	)
	, iif(x.PreferredLocation3 = 0, ''
		, concat(@NewLineChar, 'Preferred Location 3: ', trim(isnull(l3.Description, '')))
	)
) as Preferences
	--, string_agg(cast(
	--	concat(
	--		'--------------------------------------------------'
	--		, concat(@NewLineChar, 'Title: ', trim(isnull(x.Title, '')))
	--		, concat(@NewLineChar, 'Date Achieved: ', trim(isnull(x.DateAchieved, '')))
	--	) as nvarchar(max))
	--	, @DoubleNewLine
	--) within group (order by x.CreationDate desc) as Qualifications

into VCCanPreferences

from
Candidates x
left join Industries i1 on x.PreferredIndustry1 = i1.IndustryID
left join Industries i2 on x.PreferredIndustry2 = i2.IndustryID
left join Industries i3 on x.PreferredIndustry3 = i3.IndustryID
left join Positions p1 on x.PreferredPosition1 = p1.PositionID
left join Positions p2 on x.PreferredPosition2 = p2.PositionID
left join Positions p3 on x.PreferredPosition3 = p3.PositionID
left join Locations l1 on x.PreferredLocation1 = l1.LocationID
left join Locations l2 on x.PreferredLocation2 = l2.LocationID
left join Locations l3 on x.PreferredLocation3 = l3.LocationID

--group by CandidateId
order by CandidateID

select * from VCCanPreferences