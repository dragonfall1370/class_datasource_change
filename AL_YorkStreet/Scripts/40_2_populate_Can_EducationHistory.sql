declare @NewLineChar as char(2) = char(13) + char(10);
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar;

drop table if exists VCCanEducationHistory

select
	CandidateId
	, string_agg(
		concat(
			'------------------------------------------------------------------------------------------'
			, concat(@NewLineChar, 'School Name: ', trim(isnull(x.SchoolName, '')))
			, concat(@NewLineChar, 'Course Title: ', trim(isnull(x.CourseTitle, '')))
			, concat(@NewLineChar, 'Subject: ', trim(isnull(x.Subject, '')))
			, concat(@NewLineChar, 'Start Date: ', trim(isnull(x.StartDate, '')))
			, concat(@NewLineChar, 'End Date: ', trim(isnull(x.EndDate, '')))
			, concat(@DoubleNewLine, 'Description: ', @DoubleNewLine
				, trim(isnull(cast(x.Description as nvarchar(max)), '')))
		)
		, @DoubleNewLine
	) within group (order by
		iif(
			len(trim(isnull(x.EndDate, ''))) = 0
			, 1753
			, cast(right(trim(isnull(x.EndDate, '')), 4) as int)
		) desc) as EducationHistory

into VCCanEducationHistory

from CandidateSchools x
group by CandidateId
order by CandidateID

--select * from VCCanEducationHistory