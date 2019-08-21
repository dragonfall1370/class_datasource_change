declare @newLineChar1 as char(1) = char(10);
declare @NewLineChar as char(2) = char(13) + char(10);
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar
declare @DoubleNewLine2 as char(6) = @NewLineChar + @DoubleNewLine

drop table if exists #VCCanWorkHistoryJson

select
CandidateId as CanExtId

, concat (
	'['
	, string_agg(
		cast(
			concat(
				'{'
				, concat(
					concat('"company":"', replace(replace(replace(replace(
						concat('Responsibilities: ', @DoubleNewLine
							, trim(isnull(cast(Responsibilities as nvarchar(max)), '')))
						, '"', '\"'), @NewLineChar, '\n'), @newLineChar1, '\n'), '''', ''''''), '"')
					, concat(',"jobTitle":"', replace(replace(replace(replace(trim(isnull(Position, '')), '"', '\"'), @NewLineChar, '\n'), @newLineChar1, '\n'), '''', ''''''), '"')
					, concat(',"currentEmployer":"', replace(replace(replace(replace(trim(isnull(Employer, '')), '"', '\"'), @NewLineChar, '\n'), @newLineChar1, '\n'), '''', ''''''), '"')
					--, concat(',"industry":"', '28732', '"')
					--, concat(',"functionalExpertiseId":"', '3096', '"')
					--, concat(',"subFunctionId":"', '33', '"')
					--, concat(',"cbEmployer":"', '0', '"')
					, iif(len(trim(isnull(cwh.StartDate, ''))) = 0
						, ''
						, concat(
							',"dateRangeFrom":"'
							, cwh.StartDate
							, '"'
						)
					)
					--, concat(',"address":"', 'VC_test', '"')
					, iif(len(trim(isnull(cwh.EndDate, ''))) = 0
						, ''
						, concat(
							',"dateRangeTo":"'
							, cwh.EndDate
							, '"'
						)
					)
					, '}'
				)
			) as nvarchar(max)
		)
		, ','
	)  within group (order by ce.OrderID desc)
	, ']'
) as [experience_details_json]

into #VCCanWorkHistoryJson

from CandidateEmployment ce
left join VCCanWorkHistoryRefine cwh on ce.CandidateEmploymentID = cwh.CandidateEmploymentID
group by CandidateId
order by CandidateID


select * from #VCCanWorkHistoryJson where CanExtId = 100

--select count(*) from Contact where RecordTypeId = '012b0000000J2RD'