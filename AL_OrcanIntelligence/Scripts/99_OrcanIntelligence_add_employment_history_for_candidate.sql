declare @newLineChar1 as char(1) = char(10);
declare @NewLineChar as char(2) = char(13) + char(10);
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar
declare @DoubleNewLine2 as char(6) = @NewLineChar + @DoubleNewLine

select
AVTRRT__Candidate__c as CanExtId
, string_agg(
	concat(
		concat('#', [Name])
		, @NewLineChar
		, concat('Organization Name: ', trim(isnull(AVTRRT__EmployerOrgName__c, '')))
		, @DoubleNewLine
		, concat('PositionTitle: ', trim(isnull(AVTRRT__Position_Title__c, '')))
		, @DoubleNewLine
		, concat('Start Date: ', trim(isnull(AVTRRT__Start_Date__c, '')))
		, @DoubleNewLine
		, concat('End Date: ', trim(isnull(AVTRRT__End_Date__c, '')))
		, @DoubleNewLine
		, concat('Description: ', @DoubleNewLine, trim(isnull(AVTRRT__Description__c, '')))
	)
	, @DoubleNewLine2
) as [experience]

, concat (
	'['
	, string_agg(
		cast(
			concat(
				'{'
				, concat(
					concat('"company":"', replace(replace(replace(replace(trim(isnull(AVTRRT__Description__c, '')), '"', '\"'), @NewLineChar, '\n'), @newLineChar1, '\n'), '''', ''''''), '"')
					, concat(',"jobTitle":"', replace(replace(replace(replace(trim(isnull(AVTRRT__Position_Title__c, '')), '"', '\"'), @NewLineChar, '\n'), @newLineChar1, '\n'), '''', ''''''), '"')
					, concat(',"currentEmployer":"', replace(replace(replace(replace(trim(isnull(AVTRRT__EmployerOrgName__c, '')), '"', '\"'), @NewLineChar, '\n'), @newLineChar1, '\n'), '''', ''''''), '"')
					--, concat(',"industry":"', '28732', '"')
					--, concat(',"functionalExpertiseId":"', '3096', '"')
					--, concat(',"subFunctionId":"', '33', '"')
					--, concat(',"cbEmployer":"', '0', '"')
					, iif(len(trim(isnull(AVTRRT__Start_Date__c, ''))) = 0
						, ''
						, concat(
							',"dateRangeFrom":"'
							, case(len(trim(isnull(AVTRRT__Start_Date__c, ''))))
								when 4 then trim(isnull(AVTRRT__Start_Date__c, '')) + '-01-01'
								when 7 then trim(isnull(AVTRRT__Start_Date__c, '')) + '-01'
								when 10 then trim(isnull(AVTRRT__Start_Date__c, ''))
							end
							, '"'
						)
					)
					--, concat(',"address":"', 'VC_test', '"')
					, iif(len(trim(isnull(AVTRRT__End_Date__c, ''))) = 0
						, ''
						, concat(
							',"dateRangeTo":"'
							, case(len(trim(isnull(AVTRRT__End_Date__c, ''))))
								when 4 then trim(isnull(AVTRRT__End_Date__c, '')) + '-01-01'
								when 7 then trim(isnull(AVTRRT__End_Date__c, '')) + '-01'
								when 10 then trim(isnull(AVTRRT__End_Date__c, ''))
							end
							, '"'
						)
					)
					, '}'
				)
			) as nvarchar(max)
		)
		, ','
	)
	, ']'
) as [experience_details_json]

from AVTRRT__Employment_History__c

where
IsDeleted = 0 
and len(trim(isnull(AVTRRT__Candidate__c, ''))) > 0
and trim(isnull(AVTRRT__Candidate__c, '')) <> '000000000000000AAA'

group by AVTRRT__Candidate__c

--select count(*) from Contact where RecordTypeId = '012b0000000J2RD'