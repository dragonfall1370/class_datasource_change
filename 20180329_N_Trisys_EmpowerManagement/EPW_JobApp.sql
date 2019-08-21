select concat('EPW',RequirementId) as 'application-positionExternalId',
		concat('EPW',ContactId) as 'application-candidateExternalId',
		iif(Placed is not null, 'PLACED',
		iif(Rejected is not null or OfferRejected is not null, 'SHORTLISTED',
		iif(OfferAccepted is not null or OfferMade is not null, 'OFFERED',
		iif(FirstInterview is not null or TelephoneInterview is not null, 'FIRST_INTERVIEW',
		iif(ThirdInterview is not null or SecondInterview is not null, 'SECOND_INTERVIEW',
		'SHORTLISTED'))))) as 'application-Stage'
from  RequirementShortlistStage