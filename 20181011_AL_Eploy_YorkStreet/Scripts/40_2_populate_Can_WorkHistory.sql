declare @NewLineChar as char(2) = char(13) + char(10);
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar;

drop table if exists VCCanWorkHistory

select
	CandidateId
	, string_agg(
		concat(
			'------------------------------------------------------------------------------------------'
			, concat(@NewLineChar, 'Position: ', trim(isnull(ce.Position, '')))
			, concat(@NewLineChar, 'Employer: ', trim(isnull(ce.Employer, '')))
			, concat(@NewLineChar, 'Start Date: ', ce.StartDate)
			, concat(@NewLineChar, 'End Date: ', ce.EndDate)
			, concat(@DoubleNewLine, 'Responsibilities: ', @DoubleNewLine
				, trim(isnull(cast(ce.Responsibilities as nvarchar(max)), '')))
		)	
		, @DoubleNewLine
	) within group (order by OrderID desc) as WorkHistory

into VCCanWorkHistory

from CandidateEmployment ce
group by CandidateId
order by CandidateID

--select * from VCCanWorkHistory
