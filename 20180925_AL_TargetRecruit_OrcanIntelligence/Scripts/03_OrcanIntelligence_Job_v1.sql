declare @NewLineChar as char(2) = char(13) + char(10);

with
JobDupCheck as (
	--select * from (
	select Id, AVTRRT__Job_Title__c as JobTitle, row_number() over(partition by AVTRRT__Job_Title__c order by CreatedDate) as RowNum
	from AVTRRT__Job__c
	--) abc where abc.RowNum > 1
)

select

iif(len(trim(isnull(j.AVTRRT__Hiring_Manager__c, ''))) > 0
	, trim(isnull(j.AVTRRT__Hiring_Manager__c, ''))
	, iif(len(trim((select top 1 c.Id from Contact c where c.[RecordTypeId] = '012b0000000J2RE' and c.AccountId = j.AVTRRT__Account_Job__c))) > 0
		, trim((select top 1 c.Id from Contact c where c.[RecordTypeId] = '012b0000000J2RE' and c.AccountId = j.AVTRRT__Account_Job__c))
		, 'vc.intergration' -- dummy contact
	)
) as [position-contactId]

, trim(isnull(j.Id, '')) as [position-externalId]
, (select top 1 iif(cdc.RowNum > 1, trim(isnull(j.AVTRRT__Job_Title__c, '')) + ' (' + cast(cdc.RowNum as varchar(10)) + ')', trim(isnull(j.AVTRRT__Job_Title__c, '')))
	from JobDupCheck cdc
	where cdc.Id = j.Id and cdc.JobTitle = j.AVTRRT__Job_Title__c
) as [position-title]

, trim(isnull(cast(j.AVTRRT__Number_of_Positions__c as varchar(10)), '')) as [position-headcount]
--, db-field-not-found as [position-owners]
--, db-field-not-found as [position-type]

, iif(trim(isnull(j.BCST__Idibu_Currency__c, 'EUR')) = 'EU', 'EUR', trim(isnull(j.BCST__Idibu_Currency__c, 'EUR'))) as [position-currency]
--, db-field-not-found as [position-actualSalary]
--, db-field-not-found as [position-payRate]
, iif(len(trim(isnull(AVTRRT__Job_Term__c, ''))) > 0
	, iif(isnumeric([dbo].ufn_GetContractLengthFromJobTerm(trim(isnull(AVTRRT__Job_Term__c, '')))) = 1
		, [dbo].ufn_GetContractLengthFromJobTerm(trim(isnull(AVTRRT__Job_Term__c, '')))
		, ''
	)
	, '') as [position-contractLength]

, concat(
	trim(isnull(j.AVTRRT__Job_Summary__c, ''))
	, @NewLineChar
	, @NewLineChar
	, trim(isnull(j.AVTRRT__Job_Description__c, ''))
) as [position-publicDescription]

--, trim(isnull(j.AVTRRT__Job_Summary__c, '')) as [position-internalDescription]

, trim(isnull(convert(varchar(50), cast(j.AVTRRT__Start_Date__c as datetime), 111), '')) as [position-startDate]

, trim(isnull(convert(varchar(50), cast(j.AVTRRT__Estimated_Close_Date__c as datetime), 111), '')) as [position-endDate]

, trim(@NewLineChar from ''
	+ iif(len(trim(isnull(j.AVTRRT__Internal_Notes__c, ''))) > 0, 'Internal Notes: ' + trim(isnull(AVTRRT__Internal_Notes__c, '')), '')
	+ iif(len(trim(isnull(j.AVTRRT__Stage__c, ''))) > 0, @NewLineChar + 'Stage: ' + trim(isnull(AVTRRT__Stage__c, '')), '')
	+ iif(len(trim(isnull(j.AVTRRT__Account_Manager__c, ''))) > 0, @NewLineChar + 'Account Manager: ' + trim(isnull(j.AVTRRT__Account_Manager__c, '')), '')
	+ iif(len(trim(isnull(j.AVTRRT__Recruiter__c, ''))) > 0, @NewLineChar + 'Recruiter: ' + trim(isnull(j.AVTRRT__Recruiter__c, '')), '')
	+ iif(len(trim(isnull(cast(j.AVTRRT__Rate__c as varchar(20)), ''))) > 0, @NewLineChar + 'Rating: ' + trim(isnull(cast(j.AVTRRT__Rate__c as varchar(20)), '')), '')
	+ iif(len(trim(isnull(cast(j.AVTRRT__Number_of_Applicants__c as varchar(20)), ''))) > 0, @NewLineChar + 'Number of Applicants: ' + trim(isnull(cast(j.AVTRRT__Number_of_Applicants__c as varchar(20)), '')), '')
	+ iif(len(trim(isnull(cast(j.AVTRRT__Number_of_Interviews__c as varchar(20)), ''))) > 0, @NewLineChar + 'Number of Interviews: ' + trim(isnull(cast(j.AVTRRT__Number_of_Interviews__c as varchar(20)), '')), '')
	+ iif(len(trim(isnull(j.AVTRRT__Job_Term__c, ''))) > 0, @NewLineChar + 'Job Term: ' + trim(isnull(j.AVTRRT__Job_Term__c, '')), '')
	+ iif(len(trim(isnull(cast(j.AVTRRT__Start_Date__c as varchar(50)), ''))) > 0, @NewLineChar + 'Start Date: ' + trim(isnull(cast(j.AVTRRT__Start_Date__c as varchar(50)), '')), '')
	+ iif(len(trim(isnull(cast(j.AVTRRT__Estimated_Close_Date__c as varchar(50)), ''))) > 0, @NewLineChar + 'Estimated Close Date: ' + trim(isnull(cast(j.AVTRRT__Estimated_Close_Date__c as varchar(50)), '')), '')
	+ iif(len(trim(isnull(cast(j.AVTRRT__Closed__c as char(1)), ''))) > 0, @NewLineChar + 'Closed: ' + iif(trim(isnull(cast(j.AVTRRT__Closed__c as char(1)), '')) = '1', 'Yes', 'No'), '')
	+ iif(len(trim(isnull(cast(j.Job_Creation_Date__c as varchar(50)), ''))) > 0
		, @NewLineChar + 'Start Date: ' + trim(isnull(cast(j.Job_Creation_Date__c as varchar(50)), ''))
		, iif(len(trim(isnull(cast(j.CreatedDate as varchar(50)), ''))) > 0
			, @NewLineChar + 'Start Date: ' + trim(isnull(cast(j.CreatedDate as varchar(50)), ''))
			, ''
		)
	)
	+ iif(len(trim(isnull(j.AVTRRT__Country_Locale__c, ''))) > 0, @NewLineChar + 'Country: ' + trim(isnull(j.AVTRRT__Country_Locale__c, '')), '')
	--+ iif(len(trim(isnull(j.LastModifiedById, ''))) > 0 and u.Id is not null
	--	, @NewLineChar + 'Last Modified By: ' + trim(isnull(u.FirstName, '') + ' ' + isnull(u.LastName, '') + ' - ' + isnull(u.Email, ''))
	--	, '')
) as [position-note]
--, db-field-not-found as [position-document]
--, db-field-not-found as [position-otherDocument]

from
AVTRRT__Job__c j
order by j.CreatedDate