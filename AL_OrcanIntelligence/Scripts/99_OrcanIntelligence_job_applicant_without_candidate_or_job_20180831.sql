select
Id
, AVTRRT__Contact_Candidate__c
, AVTRRT__Job__c
, AVTRRT__Job_Title__c
, AVTRRT__Stage__c
from AVTRRT__Job_Applicant__c
where len(trim(isnull(AVTRRT__Contact_Candidate__c, ''))) = 0 or len(trim(isnull(AVTRRT__Job__c, ''))) = 0