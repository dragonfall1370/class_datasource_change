select 
ts2__Candidate_Contact__c as 'application-candidateExternalId',
ts2__Job__c as 'application-positionExternalId',
case when ts2__Stage__c = 'Offer' then 'OFFERED'
when ts2__Stage__c = 'Application' then 'SHORTLISTED'
when ts2__Stage__c = 'Interview' then 'FIRST_INTERVIEW'
when ts2__Stage__c = 'Placement' then 'OFFERED'
else '' end
as 'application-stage',
case when ts2__Stage__c = 'Offer' then 'OFFERED'
when ts2__Stage__c = 'Application' then 'SHORTLISTED'
when ts2__Stage__c = 'Interview' then 'FIRST_INTERVIEW'
when ts2__Stage__c = 'Placement' then 'PLACEMENT'
else '' end
as 'old-application-stage'
from ts2__Application__c
where ts2__Candidate_Contact__c <> ''

