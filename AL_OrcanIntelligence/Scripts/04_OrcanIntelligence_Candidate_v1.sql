declare @NewLineChar as char(2) = char(13) + char(10);

with
CandidateDupCheck as (
	--select * from (
	select Id, Email, row_number() over(partition by Email order by CreatedDate) as RowNum
	from Contact
	where
	[RecordTypeId] = '012b0000000J2RD' and len(trim(isnull(Email, ''))) > 0
	--) abc where abc.RowNum > 1
)

--select * from (
select

trim(isnull(c.Id, '')) as [candidate-externalId]

, upper(replace(trim(isnull(c.Salutation, '')), '.', '')) as [candidate-title]

, iif(len(trim(isnull(c.FirstName, ''))) = 0, 'N/A', trim(isnull(c.FirstName, ''))) as [candidate-firstName]
--, db-field-not-found as [candidate-middleName]

, iif(len(trim(isnull(c.LastName, ''))) = 0, 'N/A', trim(isnull(c.LastName, ''))) as [candidate-lastname]
--, db-field-not-found as [candidate-FirstNameKana]
--, db-field-not-found as [candidate-LastNameKana]

, iif(len(trim(isnull(c.Email, ''))) = 0
	, iif(len(trim(isnull(c.AVTRRT__Other_Emails__c, ''))) = 0
		, 'NoEmail-' + c.Id + '@noemail.com'
		, trim(isnull(c.AVTRRT__Other_Emails__c, ''))
	)
	, (select top 1 iif(cdc.RowNum > 1, '(' + cast(cdc.RowNum as varchar(10)) + ')' + trim(isnull(c.Email, '')), trim(isnull(c.Email, '')))
		from CandidateDupCheck cdc
		where cdc.Id = c.Id and cdc.Email = c.Email
	)
)
as [candidate-email]
--, db-field-not-found as [candidate-workEmail]
--, db-field-not-found as [candidate-employmentType]
--, db-field-not-found as [candidate-jobTypes]

, trim(isnull(c.AVTRRT__Gender__c, '')) as [candidate-gender]

, trim(isnull(convert(varchar(50), cast(c.Birthdate as datetime), 111), '')) as [candidate-dob]

-- populate full address
, trim('., ' from concat(
		trim(isnull(c.MailingStreet, ''))
		, iif(len(trim('., ' from isnull(c.MailingCity, ''))) > 0, ', ' + trim('., ' from isnull(c.MailingCity, '')), '')
		, iif(len(trim('., ' from isnull(c.MailingState, ''))) > 0, ', ' + trim('., ' from isnull(c.MailingState, '')), '')
		, iif(len(trim('., ' from isnull(c.MailingPostalCode, ''))) > 0, ', ' + trim('., ' from isnull(c.MailingPostalCode, '')), '')
		, iif(len(trim('., ' from isnull(c.MailingCountry, ''))) > 0, ', ' + trim('., ' from isnull(c.MailingCountry, '')), '')
		)
)  as [candidate-address]

--, trim(isnull(c.MailingStreet, '')) as [candidate-address]

, trim(isnull(c.MailingCity, '')) as [candidate-city]

, trim(isnull(c.MailingState, '')) as [candidate-State]

, iif(len(trim(isnull(c.MailingCountry, ''))) > 0
	, iif(upper(trim(isnull(c.MailingCountry, ''))) = 'UK'
		, 'GB'
		, isnull((select top 1 [Code] from [VC_Countries] ccd 
				where lower(ccd.Name) like lower(trim(isnull(c.MailingCountry, '')))
				or lower(ccd.Code) like lower(trim(isnull(c.MailingCountry, '')))), 'GB')
	)
	, ''
) as [candidate-Country]

, trim(isnull(c.MailingPostalCode, '')) as [candidate-zipCode]

, replace(replace(replace(trim('.,!/ '  from
	iif(len(trim(isnull(c.phone, ''))) = 0
		, iif(len(trim(isnull(c.MobilePhone, ''))) = 0
			, iif(len(trim(isnull(c.AssistantPhone, ''))) = 0
				, iif(len(trim(isnull(c.HomePhone, ''))) = 0
					, iif(len(trim(isnull(c.OtherPhone, ''))) = 0
						, ''
						, trim(isnull(c.OtherPhone, ''))
					)
					, trim(isnull(c.HomePhone, ''))
				)
				, trim(isnull(c.AssistantPhone, ''))
			)
			, trim(isnull(c.MobilePhone, ''))
		)
		, trim(isnull(c.phone, ''))
	)
), ' ', ''), '/', ','), 'or', ',')
as [candidate-phone]

, replace(replace(replace(trim('.,!/ '  from
	trim(isnull(c.HomePhone, ''))
), ' ', ''), '/', ','), 'or', ',') as [candidate-homePhone]

, replace(replace(replace(trim('.,!/ '  from
	trim(isnull(c.OtherPhone, ''))
), ' ', ''), '/', ','), 'or', ',') as [candidate-workPhone]

, replace(replace(replace(trim('.,!/ '  from
	trim(isnull(c.MobilePhone, ''))
), ' ', ''), '/', ','), 'or', ',') as [candidate-mobile]

--, trim(isnull(c.OtherPhone, '')) as [candidate-workPhone]

--, trim(isnull(c.MobilePhone, '')) as [candidate-mobile]
--, db-field-not-found as [candidate-citizenship]

, trim(isnull(c.FCMS__LinkedInId__c, '')) as [candidate-linkedln]
--, db-field-not-found as [candidate-currentSalary]
--, db-field-not-found as [candidate-desiredSalary]
--, db-field-not-found as [candidate-contractInterval]
--, db-field-not-found as [candidate-contractRate]
--, db-field-not-found as [candidate-currency]
--, db-field-not-found as [candidate-degreeName]
--, db-field-not-found as [candidate-education]
--, db-field-not-found as [candidate-educationLevel]
--, db-field-not-found as [candidate-gpa]
--, db-field-not-found as [candidate-grade]
--, db-field-not-found as [candidate-graduationDate]
--, db-field-not-found as [candidate-schoolName]
--, db-field-not-found as [candidate-company1]
--, db-field-not-found as [candidate-company2]
--, db-field-not-found as [candidate-company3]
--, db-field-not-found as [candidate-employer1]
--, db-field-not-found as [candidate-employer2]
--, db-field-not-found as [candidate-employer3]
--, db-field-not-found as [candidate-jobTitle1]
--, db-field-not-found as [candidate-jobTitle2]
--, db-field-not-found as [candidate-jobTitle3]
--, db-field-not-found as [candidate-keyword]
, trim(@NewLineChar from 'External ID: ' + c.Id
	+ iif(len(trim(isnull(c.Consent__c, ''))) > 0, @NewLineChar + 'Consent: ' + trim(isnull(Consent__c, '')), '')
	+ iif(len(trim(isnull(cast(c.Date_of_Consent__c as varchar(50)), ''))) > 0, @NewLineChar + 'Date of Consent: ' + trim(isnull(cast(c.Date_of_Consent__c as varchar(50)), '')), '')
	+ iif(len(trim(isnull(c.Privacy_consent__c, ''))) > 0, @NewLineChar + 'Privacy Consent: ' + trim(isnull(c.Privacy_consent__c, '')), '')
	+ iif(len(trim(isnull(c.Email_consent__c, ''))) > 0, @NewLineChar + 'Email Consent: ' + trim(isnull(c.Email_consent__c, '')), '')
	+ iif(len(trim(isnull(c.Fax, ''))) > 0, @NewLineChar + 'Fax: ' + trim(isnull(c.Fax, '')), '')
	+ iif(len(trim(isnull(c.OtherPhone, ''))) > 0, @NewLineChar + 'Other Phone: ' + trim(isnull(c.OtherPhone, '')), '')
	+ iif(len(trim(isnull(c.AssistantName, ''))) > 0, @NewLineChar + 'Assistant: ' + trim(isnull(c.AssistantName, '')), '')
	+ iif(len(trim(isnull(c.AssistantPhone, ''))) > 0, @NewLineChar + 'Assistant Phone: ' + trim(isnull(c.AssistantPhone, '')), '')
	+ iif(len(trim(isnull(c.Description, ''))) > 0, @NewLineChar + 'Description: ' + trim(isnull(c.Description, '')), '')
	+ iif(len(trim(isnull(c.LeadSource, ''))) > 0, @NewLineChar + 'Lead Source: ' + trim(isnull(c.LeadSource, '')), '')
	--+ iif(len(trim(isnull(c.LastModifiedById, ''))) > 0 and u.Id is not null
	--	, @NewLineChar + 'Last Modified By: ' + trim(isnull(u.FirstName, '') + ' ' + isnull(u.LastName, '') + ' - ' + isnull(u.Email, ''))
	--	, '')
)
as [candidate-note]
--, db-field-not-found as [candidate-numberOfEmployers]
--, db-field-not-found as [candidate-photo]

, cd.Docs as [candidate-resume]

, replace(trim(isnull(c.AVTRRT__AutoPopulate_Skillset__c, '')), ';', ',') as [candidate-skills]
--, db-field-not-found as [candidate-startDate1]
--, db-field-not-found as [candidate-startDate2]
--, db-field-not-found as [candidate-startDate3]
--, db-field-not-found as [candidate-endDate1]
--, db-field-not-found as [candidate-endDate2]
--, db-field-not-found as [candidate-endDate3]
--, db-field-not-found as [candidate-workHistory]
--, db-field-not-found as [candidate-owners]
--, db-field-not-found as [candidate-comments]

from
Contact c
left join VC_Can_Docs cd on c.Id = cd.ContactId
where
[RecordTypeId] =
'012b0000000J2RD' --candidate -- 9049 => 8844 has attachment
--'012b0000000J2RE' -- contact -- 8427 => 5 has attachment
--and cd.Docs is not null
--order by c.CreatedDate
--) abc
--where len(trim(isnull(abc.[candidate-Country], ''))) = 0
--where len(isnull(abc.[candidate-resume], '')) > 0
--select 9049 + 8427 -- => 17476

--select count(*) from Contact

--select * from RecordType
--where id = '012b0000000J2RD'
--Id	Name
--012b0000000J2RDAA0	Candidate
--012b0000000J2REAA0	Contact

--declare @PageSize int = 10
--declare @PageNumber int = 1

--SELECT
--	Id
--	, AVTRRT__Previous_Employers__c
--	, AVTRRT__Previous_Titles__c
--	, AVTRRT__Skill_Matched__c
--	, AVTRRT__Current_Employer__c
--	, AVTRRT__Current_Pay__c
--	, CreatedDate
--  FROM dbo.Contact
--  where
--  len(trim(isnull(AVTRRT__Previous_Employers__c, ''))) > 0
--  --len(trim(isnull(AVTRRT__Skill_Matched__c, ''))) > 0
--  ORDER BY CreatedDate 
--  OFFSET @PageSize * (@PageNumber - 1) ROWS
--  FETCH NEXT @PageSize ROWS ONLY;

--  select top 3
--  (select value from string_split(trim(isnull(AVTRRT__Previous_Employers__c, '')), ',')) as abc
--  from Contact
--  where len(trim(isnull(AVTRRT__Previous_Employers__c, ''))) > 0

--select a.Id as AttachmentId, c.Id as ContentVersionId, a.Name, c.PathOnClient
--from [Attachment] a
--left join [ContentVersion] c on a.Name = c.PathOnClient
--where c.Id is not null
---- 24354
--Order by c.PathOnClient