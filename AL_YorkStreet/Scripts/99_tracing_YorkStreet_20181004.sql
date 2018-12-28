select * from CompanyDetails -- 337
--where [Name] = 'Touchstone Residential'
--where CountryID <> 0 -- 286
--where CompanyStatusID <> 0 -- 195
--where len(isnull(Reference, '')) > 0 -- nothing
--where ContactType <> 0 -- nothing
--where InvoiceContactID <> 0 -- nothing
--where InvoiceAddressID <> 0 -- nothing
--where TaxCodeID <> 0 -- nothing
--select InvoiceTerms from CompanyDetails where len(cast(InvoiceTerms as nvarchar(max))) > 0
--where ContactByTypeID <> 0 -- 1
--where Turnover <> 0 -- nothing
where NoOfPremises <> 0

select * from VCCountries where Code in ('MU', 'CN')

select * from CompanyDetails where CompanyId = 168

select c.CompanyId, c.LocationId from CompanyDetails c
left join Locations l on c.LocationId = l.LocationId
order by c.CompanyId, c.LocationId

select * from CompanyStatus -- dictionary -- 7
--CompanyStatusID Description
--2               Prospect
--3               Live Vacancies or Interviews
--4               Placed With
--6               Open (receptive)
--7               Closed (no agencies)
--8               Working with Competitor
--9               PSL in Place
select * from CompanyStatusHistory -- activity comment -- 201

select * from CompanyPurchaseOrderTypes -- like job type -- 3
--PurchaseOrderTypeID Description
--1                   Permanent
--2                   Temp
--3                   Contract

select * from CompanyUserOptions -- empty
select * from CompanyPurchaseOrders -- empty
select * from CompanySkills -- empty

select * from Locations -- 19

select * from BusinessAreas -- dictionary -- 12
--BusinessAreaID Description
--1              Advisory
--2              Asset Management
--3              Construction Contractor
--4              Consultancy
--5              Developers
--6              End User/ Occupier
--7              Estate Agency/Management
--8              Fund/Investor/Bank
--9              Government - Central
--10             Government - Local
--11             Service Provider
--12             Workplace Solutions

-- Contacts
select * from Contacts -- 804
--where CompanyID <> 0
where len(trim(isnull(cast(Comments as nvarchar(max)), ''))) > 0
select * from ContactTypes -- dictionary
--ContactTypeID Description
--1             Prospect
--2             Client
--3             Partner
--4             Supplier
--5             Competitor
select * from ContactPositions -- dictionary
--ContactPositionID Description
--5                 Director / Partner
--6                 Finance Director
--9                 Managing Director
--13                Owner / Chairman
--14                C Suite / Exec Level
--16                Admin Support
--17                Line Manager
--18                HR / Recruitment
--19                Procurement / Invoice

select * from ContactStatus -- dictionary
--ContactStatusID Description
--1               Prospect
--2               Client
--3               Ambassador for York Street
--4               Cold
--5               Networking - Westminster Property Association
--6               Networking - MIPIM 2017 BtR Lunch
--7               Open (receptive)
--8               Left the Business
--9               Privacy Request Under Review - Hold Contact
select * from ContactStatusHistory -- activity comments -- 546

select * from ContactUserOptions -- empty
select * from ContactSkills -- empty

select * from ContactByTypes -- dictionary
--ContactByTypeID Description
--1               Email
--2               Telephone
--3               Fax
--4               Post
--5               Never
--6               Text

-- Jobs
select * from Vacancies -- jobs -- 136

-- Candidates
select * from Candidates -- 687
where len(trim(isnull(FirstName, ''))) > 0
select CurrentRemuneration
--, CurrentBonus, CurrentBenefits, RemunerationRequired\
, MinimumSalary, MinimumRate
--, MinimumRateTimeInterval
, MinimumRateIntervalID from Candidates
select * from CandidateEmployment -- work history - 4129
where CandidateID = 425
select * from CandidateExamTypes -- dictionary
--ExamTypeID Description
--1          GCSE (Or Equivalent)
--2          B.T.E.C
--3          NVQ
--4          A-Level
--5          HND
--6          Bachelors Degree
--7          Masters Degree
select * from CandidateLanguages -- candidate language skills -- 175
select * from CandidateQualifications -- candidate education -- 363
select * from CandidateReferences -- references -- 66
select * from CandidateVacancyTypes -- candidate job types -- 541
select * from VacancyTypes
select * from EmploymentStatus -- dictionary -- 8
--EmploymentStatusID Description
--1                  Actively Looking
--2                  Considering  Opportunity
--3                  On Contract
--4                  Found Own Job
--5                  Not Looking
--6                  Placed By Us
--7                  DNU
--8                  Privacy Request Under Review - Hold Contact

select * from CourseDisciplines -- dictionary -- 10
--CourseDisciplineID Description
--1                  Arts
--2                  Business
--3                  Engineering
--4                  Finance
--5                  Humanities
--6                  IT
--7                  Languages
--8                  Law
--9                  Management
--10                 Science

select * from CourseQualifications
--CourseQualificationID Description
--1                     BA
--2                     BCom
--3                     BEd
--4                     BEng
--5                     BMus
--6                     BSc
--7                     LLB
--8                     MA
--9                     MChem
--10                    MEng
--11                    MRes
--12                    MSc
--13                    PgDip
--14                    PhD
--15                    CIM
--16                    IPD
--17                    MBA
--18                    ACCA
--19                    CIMA
--20                    CIPS
--21                    DBA
--22                    MPhil
--23                    PGCE
select * from CourseOutline -- empty

select * from DegreeGrades
--DegreeGradeID Description
--1             1
--2             2:1
--3             2:2
--4             3
--5             Pass
select * from RateIntervals
select * from CandidateAddresses -- empty
select * from CandidateAvailability -- empty
select * from CandidateBank -- empty
select * from CandidateEmploymentStatus -- empty
select * from CandidateExams -- empty

select * from CoverLetters -- empty

select * from Nationality -- 239
select * from Countries -- 195

select * from SkillGroups
--SkillGroupID Description
--1            General Skills
--2            Company Names


-- Job Applications
select * from VacancyApplications -- Applications -- 276
select * from VacancyApplicationStatus -- dictionary -- 11
--VacancyApplicationStatusID Description TerminatesApplication
--1                          Not Suitable 1
--3                          Need to Contact 0
--6                          Withdrawn 1
--7                          Rejected by Client 1
--9                          CV Sent by Competitor 1
--12                         Ready to put CV forward 0
--13                         Need to interview 0
--14                         Telephone Screening 0
--15                         Invite to Meeting 0
--16                         Online Assessment 0
--17                         Screening Complete - Ready to Put Forward 0
select * from VacancyApplicationSuitability -- dictionary -- 5
--VacancyApplicationSuitabilityID Description
--1                               1 Star
--2                               2 Star
--3                               3 Star
--4                               4 Star
--5                               5 Star
select * from VacancyApplicationTypes -- applicant source -- dictionary -- 5
--VacancyApplicationTypeID Description
--1                        Our Website
--2                        Job Board Application
--3                        Online CV Search
--4                        Vendor Portal
--5                        Hiring Manager Portal

-- Placements
select * from VacancyHistory -- Placements -- 21
select * from VacancyHistoryActivity -- activity comments  -- 2
select * from VacancyHistoryActivityTypes -- dictionary -- 6
--ActivityTypeID Description
--1              Initial Pay & Charge
--2              Change in Payment Method
--3              Pay Rate Change
--4              Contract Renewal
--5              Leaver
--6              Other
select * from VacancyHistoryStatus -- Placement status -- dictionary -- 6
--VacancyHistoryStatusID Description
--1                      Awaiting Payment
--2                      Payment Overdue
--3                      Paid
--4                      Terms/Invoice Procedure Disputed
--5                      Invoice Submitted
--6                      Awaiting Start Date (Serving Notice)
select * from VacancyHistoryStatusHistory -- placement status history -- 20
select * from VacancyHistoryUserOptions -- empty
select * from OfferStatus -- empty

select * from AbilityLevels -- skill level -- dictionary -- 4
--AbilityLevelID Description
--1              Basic
--2              Average
--3              Intermediate
--4              Advanced
select * from ActionConfirmationStatus -- dictionary -- 2
--ActionConfirmationStatusID Description
--1                          Accepted
--2                          Declined
select * from ActionOutcomes -- dictionary -- 2
--ActionOutcomeID Description
--1               Progress
--2               Unsuitable
select * from ActionPriorities -- dictionary  -- 2
--ActionPriorityID Description
--1                Low
--2                High

select * from Actions -- activity comments -- 2823
select * from ActionTypes -- sub stage -- dictionary
--ActionTypeID Description
--1            To-Do
--2            Appointment
--3            To-Phone
--4            First Interview
--5            Second Interview
--6            Offer Made
--8            Thomas Assessment
--9            Shortlisted
--10           Client Meeting
--11           CV Sent
--12           Xmas Card
--13           Interview Slot
select * from Addresses -- 11
select * from AddressTypes -- dictionary -- 3
--AddressTypeID Description
--1             Branch
--2             Invoice
--3             Site

select * from QuestionGroups -- dictionary -- 3
--QuestionGroupID Title
--1               Additional Details
--2               Invoice Details
--3               Resources
select * from Questions -- 19
select * from DataTypes -- dictionary -- 19
select * from RecordTypes -- dictionary -- 114
where [Description] like '%Companies%'

select * from Answers -- 111

select * from Articles -- empty

select * from CallOutcomes -- dictionary -- 6
--CallOutcomeID Description
--1             Out
--2             No Dial Tone
--3             Left Message
--4             No Answer
--5             Progress
--6             Unsuitable
select * from CallTypes -- dictionary -- 6
--CallTypeID Description
--1          Candidate Call
--2          New Business Call
--3          Existing Client Call
--4          Reference Check Call
--5          Referral Request Call
--6          Left Message

select * from EPloySubjects -- dictionary -- 10
--SubjectID Description
--1         Electrical Engineering
--2         Power Systems Engineering
--3         Mechanical Engineering
--4         Business 
--5         Management
--6         Commerce
--7         Economics
--8         Information Technology
--9         Information Systems
--10        Business Information Technology

select * from EthnicOrigins -- dictionary -- 9
--EthnicOriginID Description
--1              Black African
--2              Black Caribbean
--3              Black Other
--4              Chinese
--5              Indian
--6              Pakistani
--7              Bangladeshi
--8              White European
--9              White Other

select * from Genders -- dictionary -- 2
--GenderID Description
--1        Male
--2        Female

select * from Invoices -- activity comments 21
select * from InvoiceTypes -- dictionary -- 2
--InvoiceTypeID Description
--1             Service Invoice
--2             Service Credit Note

select * from Notes -- activity comments
select * from NoteTypes
--NoteTypeID Description
--1          General
--2          Profile
--3          Appointment

select * from TelephoneCalls -- activity comments -- 1240

select * from Positions -- dictionary -- 57

select * from StoredFilePaths
select * from StoredFileTypes

select * from [Status]
--StatusID Description
--1        Single
--2        Married
--3        Co-habiting
--4        Divorced
--5        Engaged
select * from [Titles]
--TitleID Description
--1       Mr
--2       Mrs
--3       Miss
--4       Ms
--5       Doctor

select * from Users
select * from [UserOptions]
select * from UserOptionTypes
select * from UserSignatures
select * from [Ownership] -- owner 4903