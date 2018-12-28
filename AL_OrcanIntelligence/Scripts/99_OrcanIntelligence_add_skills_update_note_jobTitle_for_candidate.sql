declare @NewLineChar as char(2) = char(13) + char(10);
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

select
Id as CanExtId

--, Title as JobTitle,
--AVTRRT__Job_Title__c,
, concat(
	'External ID: ' + Id
	, concat(@DoubleNewLine, 'Previous Titles: ', @DoubleNewLine, AVTRRT__Previous_Titles__c)
	--, AVTRRT__Candidate_Status__c
	, concat(@DoubleNewLine, 'Candidate Write Up: ', @NewLineChar, AVTRRT__Candidate_Write_Up__c)
	, concat(@DoubleNewLine, 'Open_For_Relocation: ', iif(AVTRRT__Open_For_Relocation__c = '1', 'Yes', 'No'))
	, iif(len(trim(isnull(c.Consent__c, ''))) > 0, @DoubleNewLine + 'Consent: ' + trim(isnull(Consent__c, '')), '')
	, iif(len(trim(isnull(cast(c.Date_of_Consent__c as varchar(50)), ''))) > 0, @DoubleNewLine + 'Date of Consent: ' + trim(isnull(cast(c.Date_of_Consent__c as varchar(50)), '')), '')
	, iif(len(trim(isnull(c.Privacy_consent__c, ''))) > 0, @DoubleNewLine + 'Privacy Consent: ' + trim(isnull(c.Privacy_consent__c, '')), '')
	, iif(len(trim(isnull(c.Email_consent__c, ''))) > 0, @DoubleNewLine + 'Email Consent: ' + trim(isnull(c.Email_consent__c, '')), '')
	, iif(len(trim(isnull(c.Fax, ''))) > 0, @DoubleNewLine + 'Fax: ' + trim(isnull(c.Fax, '')), '')
	, iif(len(trim(isnull(c.OtherPhone, ''))) > 0, @DoubleNewLine + 'Other Phone: ' + trim(isnull(c.OtherPhone, '')), '')
	, iif(len(trim(isnull(c.AssistantName, ''))) > 0, @DoubleNewLine + 'Assistant: ' + trim(isnull(c.AssistantName, '')), '')
	, iif(len(trim(isnull(c.AssistantPhone, ''))) > 0, @DoubleNewLine + 'Assistant Phone: ' + trim(isnull(c.AssistantPhone, '')), '')
	, iif(len(trim(isnull(c.Description, ''))) > 0, @DoubleNewLine + 'Description: ' + trim(isnull(c.Description, '')), '')
	, iif(len(trim(isnull(c.LeadSource, ''))) > 0, @DoubleNewLine + 'Lead Source: ' + trim(isnull(c.LeadSource, '')), '')
) as Brief

,concat(
	concat('Skillset: ', @DoubleNewLine, AVTRRT__AutoPopulate_Skillset__c)
	, concat(@DoubleNewLine, 'IT Competency: ',  @DoubleNewLine, AVTRRT__IT_Competency__c)
	, concat(@DoubleNewLine, 'General Competency: ', @DoubleNewLine, AVTRRT__General_Competency__c)
	, concat(@DoubleNewLine, 'Other Competency: ', @DoubleNewLine, AVTRRT__Other_Competency__c)
	, concat(@DoubleNewLine, 'Candidate Short List: ',  @DoubleNewLine, AVTRRT__Candidate_Short_List__c)
) as Skills

--, MailingStreet
--, MailingCity
--, MailingState
--, MailingPostalCode
--, MailingCountry
--, MailingGeocodeAccuracy
--, MailingLatitude
--, MailingLongitude
--*
from Contact c
where
RecordTypeId = '012b0000000J2RD'
--and AccountId <> '001b00000044tF3AAI'
--and AccountId <> '000000000000000AAA'