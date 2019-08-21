declare @NewLineChar as char(2) = char(13) + char(10);
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar;

drop table if exists VCCanSkills

select
	CandidateId
	, string_agg(
		concat(
			'------------------------------------------------------------------------------------------'
			, concat(@NewLineChar, 'Skill Group: ', trim(isnull(sg.Description, '')))
			, concat(@NewLineChar, 'Skill: ', trim(isnull(s.Description, '')))
			, concat(@NewLineChar, 'Skill Level: ', trim(isnull(sl.Description, '')))
			, concat(@NewLineChar, 'From Date: ', x.FromDate)
			, concat(@NewLineChar, 'To Date: ', x.ToDate)
			, concat(@NewLineChar, 'Months Experience: ', x.MonthsExperience)
			, concat(@NewLineChar, 'Test Score: ', x.TestScore)
			, concat(@NewLineChar, 'Test Date: ', x.TestDate)
			, concat(@DoubleNewLine, 'Comments: ', @DoubleNewLine
				, trim(isnull(cast(x.Comments as nvarchar(max)), '')))
		)
		, @DoubleNewLine
	) within group (order by
		isnull(x.ToDate, cast('1753-01-01' as datetime)) desc) as Skills

into VCCanSkills

from CandidateSkills x
left join Skills s on x.SkillID = s.SkillID
left join SkillLevels sl on x.SkillLevel = sl.SkillLevelID
left join SkillGroups sg on s.SkillGroupID = sg.SkillGroupID
group by CandidateId
order by CandidateID

--select * from VCCanSkills