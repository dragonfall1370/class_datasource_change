select * from VC_Company

select * from VC_Contact
where [contact-companyId] not in (
	select [company-externalId]
	from VC_Company
)

select
isnull(convert(varchar(50), cast(AVTRRT__Start_Date__c as datetime), 111), '') as StartDate,
isnull(convert(varchar(50), cast(AVTRRT__Estimated_Close_Date__c as datetime), 111), '') as CloseDate,
isnull(convert(varchar(50), cast(Job_Creation_Date__c as datetime), 111), '') as CreationDate
from AVTRRT__Job__c
order by cast(AVTRRT__Start_Date__c as datetime)

select Id, Birthdate, AVTRRT__Birthday__c from Contact
where Birthdate is not null and AVTRRT__Birthday__c is not null

select * from VC_Job
where [position-contractLength] is null or isnumeric([position-contractLength]) = 1
where [position-contactId] not in (
	select [contact-externalId]
	from VC_Contact
)

select * from VC_Candidate

select * from VC_Contact where [contact-externalId] = '003b0000020VInSAAW'

select * from VC_Application
where
[application-positionExternalId] not in (
	select [position-externalId] from VC_Job
	where [position-contactId] not in ('vc.intergration', '003b0000020VInSAAW')
)
or
[application-candidateExternalId] not in (
	select [candidate-externalId] from VC_Candidate
)

--application-candidateExternalId	application-positionExternalId	application-stage
--003b000000LhAiVAAV	a0F0X00000crdw6UAA	PLACEMENT_CONTRACT

select Id, AVTRRT__Hiring_Manager__c, AVTRRT__Account_Job__c from AVTRRT__Job__c where id = 'a0F0X00000crdw6UAA'

select * from Contact where Id = '003b0000020VInSAAW'

select * from AVTRRT__Job__c where AVTRRT__Hiring_Manager__c = '003b0000020VInSAAW'