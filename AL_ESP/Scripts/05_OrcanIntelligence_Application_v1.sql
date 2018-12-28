with abc as (
	select [application-candidateExternalId], [application-positionExternalId], [application-stage], aso.[Order] as StageOrder, apps.CreatedDate from (
		select

		iif(len(trim(isnull(ja.AVTRRT__Job__c, ''))) > 0
			, trim(isnull(ja.AVTRRT__Job__c, ''))
			, isnull(
				(select top 1 j.Id from AVTRRT__Job__c j
					where j.AVTRRT__Account_Job__c = ja.AVTRRT__Account_Job__c
						and upper(trim(isnull(ja.AVTRRT__Job_Title__c, ''))) = upper(trim(isnull(j.AVTRRT__Job_Title__c, ''))))
				, '')
		) as [application-positionExternalId]

		, trim(isnull(ja.AVTRRT__Contact_Candidate__c, '')) as [application-candidateExternalId]

		, isnull((
		case(upper(trim(isnull(ja.AVTRRT__Stage__c, ''))))
			when upper('Closed - Won') then 'PLACEMENT_CONTRACT'
			when upper('Interviewing') then 'FIRST_INTERVIEW'
			when upper('Marketed CV') then 'SHORTLISTED'
			when upper('Negotiating') then 'SHORTLISTED'
			when upper('New Application') then 'SHORTLISTED'
			when upper('Reapplied By Candidate') then 'SHORTLISTED'
			when upper('Placement') then 'PLACEMENT_CONTRACT'
			when upper('Rejected by Account Manager') then 'SHORTLISTED > REJECTED'
			when upper('Rejected by Candidate') then 'SHORTLISTED > REJECTED'
			when upper('Rejected by Hiring Manager') then 'SHORTLISTED > REJECTED'
			when upper('Rejected by Recruiter') then 'SHORTLISTED'
			when upper('Short Listed') then 'SHORTLISTED'
			when upper('Submitted to Account Manager') then 'SENT'
			when upper('Submitted to Hiring Manager') then 'SENT'
			when upper('Waiting for Approval') then 'SENT'
		end
		), '') as [application-stage]
		, trim(isnull(ja.AVTRRT__Stage__c, '')) as OIStage
		, ja.CreatedDate
		--, row_number() over(partition by 

		from
		AVTRRT__Job_Applicant__c ja
		where upper(trim(isnull(ja.AVTRRT__Stage__c, ''))) not in (
			upper('Rejected by Account Manager'),
			upper('Rejected by Candidate'),
			upper('Rejected by Hiring Manager')
		)
			and (select top 1 c.Id from Contact c where Id = ja.AVTRRT__Contact_Candidate__c and c.RecordTypeId = '012b0000000J2RD') is not null
		--order by ja.CreatedDate
	) apps left join VC_JAStages aso on apps.[application-stage] = aso.Stage
	where len(trim(isnull(apps.[application-positionExternalId], ''))) > 0
		and len(trim(isnull(apps.[application-candidateExternalId], ''))) > 0
		and (select count(j.Id) from AVTRRT__Job__c j where j.Id = apps.[application-positionExternalId]) > 0
	--order by apps.CreatedDate
)

select distinct t1.[application-candidateExternalId], t1.[application-positionExternalId], t1.[application-stage]
from abc t1
left outer join abc t2
	on t1.[application-positionExternalId] = t2.[application-positionExternalId]
	and t1.[application-candidateExternalId] = t2.[application-candidateExternalId]
	and t1.StageOrder < t2.StageOrder
where t2.[application-candidateExternalId] is null and t2.[application-positionExternalId] is null
--inner join (
--    select [application-candidateExternalId], [application-positionExternalId], MAX(StageOrder) HighestStage
--    FROM abc
--    GROUP BY [application-candidateExternalId], [application-positionExternalId]
--) t2 ON t1.[application-positionExternalId] = t2.[application-positionExternalId] AND t1.StageOrder = t2.HighestStage
--order by t1.CreatedDate

--SELECT a.*
--FROM YourTable a
--LEFT OUTER JOIN YourTable b
--    ON a.id = b.id AND a.rev < b.rev
--WHERE b.id IS NULL;


--SHORTLISTED
--SENT
--FIRST_INTERVIEW
--SECOND_INTERVIEW
--OFFERED
--PLACEMENT_PERMANENT
--PLACEMENT_CONTRACT
--PLACEMENT_TEMP
--ONBOARDING