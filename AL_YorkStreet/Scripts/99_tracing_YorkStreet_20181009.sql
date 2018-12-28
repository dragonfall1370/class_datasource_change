select
AvailableFromDate
from Candidates
--where CandidateID = 312
--where WorkPermitTypeID <> 0

select * from CandidateQualifications

select * from RatePositions

select * from ContactPositions

select * from Positions


select distinct [contact-externalID] from VCContacts
where [contact-companyId] not in
(
	select distinct [company-externalId] from VCCompanies
)

select * from VCCandidates
where len([candidate-email]) = 0
or len([candidate-firstName]) = 0
or len([candidate-lastName]) = 0
or len([candidate-externalId]) = 0

select distinct
--[candidate-externalID]
[candidate-email]
from VCCandidates

select StartDate, EndDate, ExpiryDate, CreationDate, AdvertisingEndDate, ApplicationClosingDate
, InternalAdvertisingStartDate, InternalAdvertisingEndDate
, DatePosted
from Vacancies


select distinct CompanyID from CompanyDetails
where CompanyID not in (
	select distinct CompanyID from Contacts
)

select ContactId from Contacts
where CompanyId = 12

select
distinct CompanyID
from Vacancies
where ContactID = 0

select * from Vacancies
where ContactID = 0

select ContactId from Vacancies
where ContactID <> 0 and ContactID not in (
	select ContactID from Contacts
)

select * from VacancyTypes

select VacancyId
from Vacancies
where VacancyTypeID = 0

select * from PaymentMethods

select * from PayRates

select
AllowChangePaySalary
, DisplaySalary
, PaySalaryID
, Salary
, SalaryDescription
, SalaryFrom
, SalaryIntervalID
, SalaryTimeInterval
, TextDescription
from Vacancies

select * from RateIntervals

select * from Vacancies

select * from VacancyStatus

select
v.ChargeCurrencyID
, v.RateCurrencyID
from Vacancies v

select * from VacancyApplicationStatus

select * from ShortlistItems

select * from VacancyApplications

select NominalCodeID from Vacancies

select * from Vacancies
where CompanyID <> 0

select * from VacancyApplications
where VacancyHistoryID <> 0

select v.VacancyTypeID, vh.* from VacancyHistory vh
left join Vacancies v on vh.VacancyID = v.VacancyID

select ApplicationDate, VacancyID from VacancyApplications
--where VacancyID = 0

select * from VacancyApplications
where VacancyHistoryID = 0

--select * from VacancyApplicationTypes

select * from VacancyApplicationStatus

select * from ActionTypes

select * from Actions

/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [VacancyApplicationID]
      ,[ImportID]
      ,[UserID]
      ,[VacancyID]
      ,[CandidateID]
      ,[ApplicationWorkflowID]
      ,[ApplicationStageID]
      ,[ApplicationSubStageID]
      ,[ActionID]
      ,[VacancyHistoryID]
      ,[ApplicationStatusID]
      ,[ActionOutcomeID]
      ,[VacancyHistoryStatusID]
      ,[StatusDate]
      ,[ApplicationTypeID]
      ,[ApplicationSuitabilityID]
      ,[ApplicationSuitabilityDate]
      ,[ApplicationSuitabilityUserID]
      ,[Source]
      ,[InfluenceToApply]
      ,[InfluenceToApplyDetail]
      ,[InfluenceToApplyDescription]
      ,[ApplicationDate]
      ,[ApplicationHasTime]
      ,[SubmissionDate]
      ,[SubmissionHasTime]
      ,[SubmissionUserID]
      ,[OfferDate]
      ,[OfferHasTime]
      ,[OfferUserID]
      ,[OfferStatusID]
      ,[TerminationDate]
      ,[Salary]
      ,[PercentageRate]
      ,[PercentageCharge]
      ,[Rate]
      ,[RateTimeInterval]
      ,[RateIntervalID]
      ,[CVID]
      ,[AdminCVID]
      ,[CVTitle]
      ,[CoverLetterID]
      ,[CoverLetterTitle]
      ,[CoverLetterContent]
      ,[Comments]
      ,[CreationDate]
      ,[CreatorDisplayName]
      ,[CreatorID]
      ,[ModificationDate]
      ,[ModifierDisplayName]
      ,[ModifierID]
  FROM [YorkStreetProd].[dbo].[VacancyApplications]
  where VacancyHistoryID = 0
  and ApplicationStatusID <> 0

  select * from Actions
  where ActionID = 28

  select * from ActionOutcomes
  select * from ActionPriorities
  select * from ActionConfirmationStatus

  select * from ActionTypes

  select * from VacancyApplicationStatus

  select * from VacancyStatus

  select * from VacancyHistoryStatus


  select * from VacancyApplications
  where VacancyID = 64
 -- where ApplicationStageID > ApplicationSubStageID
 order by ApplicationDate desc

  select * from ActionTypes

  select * from VacancyApplicationStatus

  select * from Vacancies
  where VacancyID = 64

  select * from VacancyApplications
  where ApplicationStatusID = 0
  and ActionID <> 0