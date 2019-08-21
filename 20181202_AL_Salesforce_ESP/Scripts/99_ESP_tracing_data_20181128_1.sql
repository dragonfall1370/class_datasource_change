;with

ConNoComLink as (
	select Id from Contact
	where AccountId is null
	or AccountId = '000000000000000AAA'
	--or AccountId not in (
	--	select Id from VCAccIdxs
	--)
)

--select * from ConNoComLink

, ConNoWorkHistory as (
	select Id from Contact
	where
	(Company_Name__c is null and Date_Joined__c is null)
	and (Compamy_Name__c is null and Date_Joined_2__c is null)
	and (Company_Name_3__c is null and (Date_joined_3__c is null or ISDATE(Date_joined_3__c) = 0))
)

select x.FirstName, x.LastName, x.Email, x.Assist_email__c, x.Email_2__c, x.Job_Tiltle__c, y.Name from Contact x
left join Account y on x.AccountId = y.Id
where x.id not in (select Id from ConNoWorkHistory) -- Candidate = 17899

--select * from Contact where id not in (select Id from ConNoComLink) and id in (select id from ConNoWorkHistory) -- contact only = 12709
--select * from Contact where id in (select Id from ConNoComLink) and id in (select id from ConNoWorkHistory) -- contact only = 128
--select * from Contact where id not in (select Id from ConNoComLink) and id not in (select id from ConNoWorkHistory) -- contact only = 17899
--select * from Contact where id in (select Id from ConNoComLink) and id not in (select id from ConNoWorkHistory) -- contact only = 17899
--select * from Contact where id not in (select Id from ConNoComLink)  -- 30608

select count(*) from Contact -- 30736
select count(distinct Email) from Contact
where Email is not null

select distinct BillingCountry from Account

select Phone, Fax from Account

select 17899 + 12709 -- 30608
select 30608 + 128 -- 30736

select * from Contact
where
(
Id not in (select Id from ConNoComLink)
and Id in (select Id from ConNoWorkHistory)
)
or
(
Id not in (select Id from ConNoComLink)
and Id not in (select Id from ConNoWorkHistory)
)

--select * from ConNoWorkHistory -- 12,837

select y.* from ConNoWorkHistory x left join Contact y on x.Id = y.Id

-- total contact = 30,736

--select 30736 - 12837 -- 17899

select * from Contact c
left join [dbo].[VCAccIdxs] ai on c.AccountId = ai.Id
--where ai.Id is not null

select * from Opportunity where ContactId is null

select * from [OpportunityContactRole]


--select * from VCJobs
--where [position-contactId] not in (select [contact-externalId] from VCContacts)
--where len(trim(isnull([position-title], ''))) = 0
--where [position-externalId] is null


--select * from VCContacts
--where [contact-externalId] is null
--where [contact-companyId] not in (select [company-externalId] from VCCompanies)
--where len(trim(isnull([contact-lastName], ''))) = 0

select * from VCCompanies
where len(trim(isnull([company-name], ''))) = 0

select * from VCCandidates
--select distinct [candidate-email] from VCCandidates
select distinct [candidate-country] from VCCandidates
--where len(trim(isnull([candidate-lastName], ''))) = 0
--where len(trim(isnull([candidate-firstName], ''))) = 0
--where len(trim(isnull([candidate-email], ''))) = 0