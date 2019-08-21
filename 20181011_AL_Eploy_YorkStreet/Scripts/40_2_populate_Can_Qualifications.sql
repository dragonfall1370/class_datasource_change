declare @NewLineChar as char(2) = char(13) + char(10);
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar;

drop table if exists VCCanQualifications

select
	CandidateId
	, string_agg(cast(
		concat(
			'--------------------------------------------------'
			, concat(@NewLineChar, 'Title: ', trim(isnull(x.Title, '')))
			, concat(@NewLineChar, 'Date Achieved: ', trim(isnull(x.DateAchieved, '')))
		) as nvarchar(max))
		, @DoubleNewLine
	) within group (order by x.CreationDate desc) as Qualifications

into VCCanQualifications

from CandidateQualifications x
group by CandidateId
order by CandidateID

--select * from VCCanQualifications