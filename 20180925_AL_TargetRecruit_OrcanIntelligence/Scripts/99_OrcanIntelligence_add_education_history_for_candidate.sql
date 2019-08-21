declare @newLineChar1 as char(1) = char(10);
declare @NewLineChar as char(2) = char(13) + char(10);
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar
declare @DoubleNewLine2 as char(6) = @NewLineChar + @DoubleNewLine

select
eh.AVTRRT__Candidate__c as CanExtId
, concat(
	concat('Education Details: ', @DoubleNewLine, c.AVTRRT__Education_Details__c)
	, @DoubleNewLine
	, @DoubleNewLine
	, 'Education History:'
	, @DoubleNewLine
	, string_agg(
		concat(
			concat('#', eh.Name)
			, @NewLineChar
			, concat('School Name: ', trim(isnull(eh.AVTRRT__SchoolName__c, '')))
			, @DoubleNewLine
			, concat('Degree Name: ', trim(isnull(eh.AVTRRT__DegreeName__c, '')))
			, @DoubleNewLine
			, concat('Degree Date: ', trim(isnull(eh.AVTRRT__DegreeDate__c, '')))
			, @DoubleNewLine
			, concat('Major: ', trim(isnull(eh.AVTRRT__Major__c, '')))
		)
		, @DoubleNewLine2
	)
) as [education_summary]

, concat (
	'['
	, string_agg(
		cast(
			concat(
				'{'
				, concat(
					concat('"schoolName":"', replace(replace(replace(replace(trim(isnull(eh.AVTRRT__SchoolName__c, '')), '"', '\"'), @NewLineChar, '\n'), @newLineChar1, '\n'), '''', ''''''), '"')
					, concat(',"degreeName":"', replace(replace(replace(replace(trim(isnull(eh.AVTRRT__DegreeName__c, '')), '"', '\"'), @NewLineChar, '\n'), @newLineChar1, '\n'), '''', ''''''), '"')
					, concat(',"major":"', replace(replace(replace(replace(trim(isnull(eh.AVTRRT__Major__c, '')), '"', '\"'), @NewLineChar, '\n'), @newLineChar1, '\n'), '''', ''''''), '"')
					, iif(len(trim(isnull(eh.AVTRRT__DegreeDate__c, ''))) = 0
						, ''
						, concat(
							',"graduationDate":"'
							, case(len(trim(isnull(eh.AVTRRT__DegreeDate__c, ''))))
								when 4 then trim(isnull(eh.AVTRRT__DegreeDate__c, '')) + '-01-01'
								when 7 then trim(isnull(eh.AVTRRT__DegreeDate__c, '')) + '-01'
								when 10 then trim(isnull(eh.AVTRRT__DegreeDate__c, ''))
							end
							, '"'
						)
					)
				)
				, '}'
			) as nvarchar(max)
		)
		, ','
	)
	, ']'
) as [edu_details_json]

from AVTRRT__Educational_History__c eh
join Contact c on eh.AVTRRT__Candidate__c = c.Id

where
c.IsDeleted = 0 and eh.IsDeleted = 0
and len(trim(isnull(eh.AVTRRT__Candidate__c, ''))) > 0
and trim(isnull(eh.AVTRRT__Candidate__c, '')) <> '000000000000000AAA'

group by eh.AVTRRT__Candidate__c, c.AVTRRT__Education_Details__c

--select count(*) from Contact where RecordTypeId = '012b0000000J2RD'

--"[{"schoolName":"University of Glasgow","degreeName":"Doctor of Philosophy","major":"Statistical Ecology","graduationDate":"1975-01-01"}]"